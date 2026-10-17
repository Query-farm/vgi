// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_aggregate_window_impl.hpp"
#include "vgi_batch_validation.hpp"

#include <cstring>

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/client_context.hpp"

#include "vgi_aggregate_function_impl.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_exception.hpp"
#include "vgi_logging.hpp"
#include "vgi_rpc_types.hpp"
#include "generated/vgi_request_builders.hpp"

namespace duckdb {
namespace vgi {

namespace {

// Encode (4 × int64) frame-delta tuples as little-endian bytes, matching the
// worker-side _unpack_frame_stats decoder.
std::vector<uint8_t> EncodeFrameStats(const FrameStats &stats) {
	std::vector<uint8_t> out(sizeof(int64_t) * 4);
	int64_t vals[4] = {
	    static_cast<int64_t>(stats[0].begin), static_cast<int64_t>(stats[0].end),
	    static_cast<int64_t>(stats[1].begin), static_cast<int64_t>(stats[1].end),
	};
	std::memcpy(out.data(), vals, sizeof(vals));
	return out;
}

// Encode one byte per column for all_valid flags.
std::vector<uint8_t> EncodeAllValid(const std::vector<bool> &all_valid) {
	std::vector<uint8_t> out(all_valid.size());
	for (size_t i = 0; i < all_valid.size(); i++) {
		out[i] = all_valid[i] ? 1 : 0;
	}
	return out;
}

// Pack a ValidityMask into contiguous bytes. Each bit corresponds to one
// row; bit set = valid / selected.
std::vector<uint8_t> PackValidityMask(const ValidityMask &mask, idx_t row_count) {
	idx_t byte_count = (row_count + 7) / 8;
	std::vector<uint8_t> out(byte_count, 0xFF);
	if (mask.IsMaskSet()) {
		for (idx_t i = 0; i < row_count; i++) {
			if (!mask.RowIsValid(i)) {
				out[i / 8] &= static_cast<uint8_t>(~(1 << (i % 8)));
			}
		}
	}
	return out;
}

// Convert the partition's ColumnDataCollection into a batch.
// Handles the inputs=nullptr edge case (no-column aggregate — rare but legal).
VgiBatchPtr PartitionToArrow(ClientContext &context,
                                                     const WindowPartitionInput &partition) {
	if (!partition.inputs || partition.column_ids.empty()) {
		// No input columns — return an empty 0-column batch with just the row count.
		return VgiBatch::FromColumns(VgiSchema::Empty(), static_cast<int64_t>(partition.count), {});
	}

	// Project only the columns this aggregate cares about.
	const auto &all_types = partition.inputs->Types();
	vector<LogicalType> proj_types;
	vector<string> proj_names;
	proj_types.reserve(partition.column_ids.size());
	proj_names.reserve(partition.column_ids.size());
	for (auto col_id : partition.column_ids) {
		proj_types.push_back(all_types[col_id]);
		proj_names.push_back("col_" + std::to_string(col_id));
	}

	auto arrow_schema = BuildArrowSchemaFromDuckDB(context, proj_types, proj_names);

	// Scan the ColumnDataCollection into a single DataChunk sized to the
	// partition row count. For very large partitions this materialises the
	// whole thing in memory — an explicitly accepted tradeoff (see plan).
	DataChunk chunk;
	chunk.Initialize(context, proj_types, partition.count);
	chunk.SetCardinality(partition.count);

	ColumnDataScanState scan_state;
	partition.inputs->InitializeScan(scan_state, partition.column_ids);
	DataChunk read_chunk;
	read_chunk.Initialize(context, proj_types);
	idx_t written = 0;
	while (partition.inputs->Scan(scan_state, read_chunk)) {
		for (idx_t col = 0; col < proj_types.size(); col++) {
			VectorOperations::Copy(read_chunk.data[col], chunk.data[col], read_chunk.size(), 0, written);
		}
		written += read_chunk.size();
		read_chunk.Reset();
	}

	return DataChunkToArrow(context, chunk, arrow_schema);
}

// ============================================================================
// Request builders
// ============================================================================

// The owning catalog schema rides on every window RPC for the same reason it
// rides on the aggregate ones: a function name is unique only within a schema,
// so a bare-name re-resolve on the worker can land on a different schema's
// implementation than the bind did. Empty string serialises as null (the
// legitimate "no schema named" form) — the field stays nullable.
VgiBatchPtr BuildAggregateWindowInitRequest(
    const std::string &function_name, const std::string &schema_name,
    const std::vector<uint8_t> &execution_id,
    const std::vector<uint8_t> &attach_opaque_data, int64_t partition_id, int64_t row_count,
    const VgiBatchPtr &partition_batch,
    const VgiSchemaPtr &output_schema,
    const std::vector<uint8_t> &filter_mask_bytes,
    const std::vector<uint8_t> &frame_stats_bytes,
    const std::vector<uint8_t> &all_valid_bytes) {
	generated::AggregateWindowInitRequest request;
	request.function_name = function_name;
	request.execution_id = execution_id;
	request.partition_id = partition_id;
	request.row_count = row_count;
	request.partition_batch = SerializeToIpcBytes(partition_batch);
	request.output_schema = SerializeSchemaToIpcBytes(output_schema);
	request.filter_mask = filter_mask_bytes;
	request.frame_stats = frame_stats_bytes;
	request.all_valid = all_valid_bytes;
	request.attach_opaque_data = BytesOrNull(attach_opaque_data);
	request.schema_path = OptionalSingleSchemaPath(schema_name);
	return generated::BuildAggregateWindowInitParams(SerializeRecord(request));
}

VgiBatchPtr BuildAggregateWindowRequest(
    const std::string &function_name, const std::string &schema_name,
    const std::vector<uint8_t> &execution_id,
    const std::vector<uint8_t> &attach_opaque_data, int64_t partition_id, int64_t rid,
    const SubFrames &subframes) {
	generated::AggregateWindowRequest request;
	request.function_name = function_name;
	request.execution_id = execution_id;
	request.partition_id = partition_id;
	request.rid = rid;
	for (const auto &fb : subframes) {
		request.frame_starts.push_back(static_cast<int64_t>(fb.start));
		request.frame_ends.push_back(static_cast<int64_t>(fb.end));
	}
	request.attach_opaque_data = BytesOrNull(attach_opaque_data);
	request.schema_path = OptionalSingleSchemaPath(schema_name);
	return generated::BuildAggregateWindowParams(SerializeRecord(request));
}

// Batched window request — sends all (rid, subframes) tuples for one
// Evaluate() call in a single RPC. The frames_per_row array gives the
// subframe cardinality per row so the worker can unflatten frame_starts /
// frame_ends (flat arrays of length sum(frames_per_row)).
VgiBatchPtr BuildAggregateWindowBatchRequest(
    const std::string &function_name, const std::string &schema_name,
    const std::vector<uint8_t> &execution_id,
    const std::vector<uint8_t> &attach_opaque_data, int64_t partition_id,
    const SubFrames *subframes_per_row, idx_t count, idx_t row_idx) {
	generated::AggregateWindowBatchRequest request;
	request.function_name = function_name;
	request.execution_id = execution_id;
	request.partition_id = partition_id;
	request.row_idx = static_cast<int64_t>(row_idx);
	request.count = static_cast<int64_t>(count);
	for (idx_t r = 0; r < count; r++) {
		const auto &subframes = subframes_per_row[r];
		request.frames_per_row.push_back(static_cast<int64_t>(subframes.size()));
		for (const auto &fb : subframes) {
			request.frame_starts.push_back(static_cast<int64_t>(fb.start));
			request.frame_ends.push_back(static_cast<int64_t>(fb.end));
		}
	}
	request.attach_opaque_data = BytesOrNull(attach_opaque_data);
	request.schema_path = OptionalSingleSchemaPath(schema_name);
	return generated::BuildAggregateWindowBatchParams(SerializeRecord(request));
}

VgiBatchPtr BuildAggregateWindowDestructorRequest(
    const std::string &function_name, const std::string &schema_name,
    const std::vector<uint8_t> &execution_id,
    const std::vector<uint8_t> &attach_opaque_data, int64_t partition_id) {
	generated::AggregateWindowDestructorRequest request;
	request.function_name = function_name;
	request.execution_id = execution_id;
	request.partition_id = partition_id;
	request.attach_opaque_data = BytesOrNull(attach_opaque_data);
	request.schema_path = OptionalSingleSchemaPath(schema_name);
	return generated::BuildAggregateWindowDestructorParams(SerializeRecord(request));
}

} // anonymous namespace

// ============================================================================
// Window destructor helper (called from VgiAggregateDestroy)
// ============================================================================

void SendAggregateWindowDestructorRpc(ClientContext &context, const VgiAggregateBindData &bind_data,
                                       int64_t partition_id) {
	auto request = BuildAggregateWindowDestructorRequest(
	    bind_data.function_name, bind_data.schema_name, bind_data.exec_state->execution_id,
	    bind_data.attach_opaque_data, partition_id);
	// enable_logging=false: this path runs on task-scheduler threads during
	// pipeline teardown. VGI_LOG / DrainToLog are unsafe there.
	InvokeAggregateRpc(context, bind_data, "aggregate_window_destructor", request, /*enable_logging=*/false);
}

// ============================================================================
// VgiAggregateWindowInit — called once per OVER partition
// ============================================================================

void VgiAggregateWindowInit(AggregateInputData &aggr_input_data, const WindowPartitionInput &partition,
                             data_ptr_t g_state) {
	auto &bind_data = aggr_input_data.bind_data->Cast<VgiAggregateBindData>();
	auto context_lock = bind_data.context.lock();
	if (!context_lock) {
		throw IOException("VGI aggregate window: ClientContext is gone");
	}
	auto &context = *context_lock;

	// Initialize the buffer as a window state (overwrites whatever
	// VgiAggregateInitialize left there; VgiAggregateState is trivially
	// destructible so no explicit destructor call needed).
	auto *ls = new (g_state) VgiAggregateWindowLocalState();
	ls->tag = VgiAggregateStateTag::WINDOW;
	ls->bind_data = &bind_data;
	ls->partition_id = bind_data.exec_state->partition_id_counter.fetch_add(1);

	auto partition_batch = PartitionToArrow(context, partition);
	auto frame_stats_bytes = EncodeFrameStats(partition.stats);
	auto all_valid_bytes = EncodeAllValid(partition.all_valid);
	auto filter_mask_bytes = PackValidityMask(partition.filter_mask, partition.count);

	auto request = BuildAggregateWindowInitRequest(
	    bind_data.function_name, bind_data.schema_name, bind_data.exec_state->execution_id,
	    bind_data.attach_opaque_data,
	    ls->partition_id, static_cast<int64_t>(partition.count), partition_batch,
	    bind_data.resolved_output_schema, filter_mask_bytes, frame_stats_bytes, all_valid_bytes);

	InvokeAggregateRpc(context, bind_data, "aggregate_window_init", request);
}

// ============================================================================
// VgiAggregateWindow — one RPC per output row
// ============================================================================

void VgiAggregateWindow(AggregateInputData &aggr_input_data, const WindowPartitionInput &partition,
                         const_data_ptr_t g_state, data_ptr_t /*l_state*/,
                         const SubFrames &subframes, Vector &result, idx_t rid) {
	// partition_id lives on the GLOBAL state (set by VgiAggregateWindowInit).
	// The per-thread local state is unused here — DuckDB still allocates it
	// and calls our initialize on it, but we pull the partition_id from the
	// global state because that's where window_init ran.
	auto *global = reinterpret_cast<const VgiAggregateWindowLocalState *>(g_state);
	if (!global || global->tag != VgiAggregateStateTag::WINDOW) {
		throw IOException("VGI aggregate_window called without a matching window_init");
	}

	auto &bind_data = aggr_input_data.bind_data->Cast<VgiAggregateBindData>();
	auto context_lock = bind_data.context.lock();
	if (!context_lock) {
		throw IOException("VGI aggregate window: ClientContext is gone");
	}
	auto &context = *context_lock;

	auto request = BuildAggregateWindowRequest(
	    bind_data.function_name, bind_data.schema_name, bind_data.exec_state->execution_id,
	    bind_data.attach_opaque_data,
	    global->partition_id, static_cast<int64_t>(rid), subframes);

	auto rpc_result = InvokeAggregateRpc(context, bind_data, "aggregate_window", request);
	auto window_result = DecodeAggregateResult<generated::AggregateWindowResult>(rpc_result, bind_data, "aggregate_window");
	auto result_batch = DeserializeFromIpcBytes(window_result.result_batch, GetWorkerBatchValidation(&context));

	if (!result_batch || result_batch->num_rows() != 1 || result_batch->num_columns() != 1) {
		throw IOException(AggregateViolation(bind_data, "aggregate_window"), "VGI aggregate_window returned invalid one-row result");
	}

	// Convert the one-row Arrow result to a DuckDB Vector and copy into result[rid].
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
	vector<LogicalType> out_types;
	vector<string> out_names;
	ArrowSchemaToDuckDBTypes(context, result_batch->schema(), c_schema, arrow_table, out_types, out_names);

	// Type-confusion guard: worker window result vs the bind-declared output type.
	ValidateProjectedWireBatch(&context, *result_batch, bind_data.resolved_output_schema, {},
	                           bind_data.attach_params->worker_path(), bind_data.function_name);

	auto chunk_wrapper = make_uniq<ArrowArrayWrapper>();
	ExportRecordBatch(result_batch, *chunk_wrapper);
	ArrowScanLocalState scan_state(std::move(chunk_wrapper), context);

	DataChunk one_row;
	one_row.Initialize(context, {result.GetType()});
	one_row.SetCardinality(1);
	ArrowTableFunction::ArrowToDuckDB(scan_state, arrow_table.GetColumns(), one_row, false);

	VectorOperations::Copy(one_row.data[0], result, 1, 0, rid);
}

// ============================================================================
// VgiAggregateWindowBatch — one RPC per Evaluate()
// ============================================================================
// DuckDB invokes this once per WindowCustomAggregator::Evaluate call with all
// `count` subframe arrays pre-collected. We ship them in a single batched RPC
// and fill `result[0..count)` from the returned batch. Reduces total RPCs per
// partition from N to ~N/STANDARD_VECTOR_SIZE.

void VgiAggregateWindowBatch(AggregateInputData &aggr_input_data, const WindowPartitionInput &partition,
                              const_data_ptr_t g_state, data_ptr_t /*l_state*/,
                              const SubFrames *subframes_per_row, idx_t count,
                              Vector &result, idx_t row_idx) {
	auto *global = reinterpret_cast<const VgiAggregateWindowLocalState *>(g_state);
	if (!global || global->tag != VgiAggregateStateTag::WINDOW) {
		throw IOException("VGI aggregate_window_batch called without a matching window_init");
	}

	auto &bind_data = aggr_input_data.bind_data->Cast<VgiAggregateBindData>();
	auto context_lock = bind_data.context.lock();
	if (!context_lock) {
		throw IOException("VGI aggregate window: ClientContext is gone");
	}
	auto &context = *context_lock;

	auto request = BuildAggregateWindowBatchRequest(
	    bind_data.function_name, bind_data.schema_name, bind_data.exec_state->execution_id,
	    bind_data.attach_opaque_data,
	    global->partition_id, subframes_per_row, count, row_idx);

	auto rpc_result = InvokeAggregateRpc(context, bind_data, "aggregate_window_batch", request);
	auto window_result = DecodeAggregateResult<generated::AggregateWindowBatchResult>(rpc_result, bind_data, "aggregate_window_batch");
	auto result_batch = DeserializeFromIpcBytes(window_result.result_batch, GetWorkerBatchValidation(&context));

	if (!result_batch || result_batch->num_columns() != 1) {
		throw IOException(AggregateViolation(bind_data, "aggregate_window_batch"), "VGI aggregate_window_batch returned invalid result");
	}
	if (static_cast<idx_t>(result_batch->num_rows()) != count) {
		throw IOException(AggregateViolation(bind_data, "aggregate_window_batch")
		                      .ExpectedActual(std::to_string(count) + " rows",
		                                      std::to_string(result_batch->num_rows()) + " rows"),
		                  "VGI aggregate_window_batch expected %lld rows, got %lld for '%s'",
		                  static_cast<int64_t>(count), static_cast<int64_t>(result_batch->num_rows()),
		                  bind_data.function_name.c_str());
	}

	// Convert the count-row Arrow batch to a DataChunk and copy into result[0..count).
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
	vector<LogicalType> out_types;
	vector<string> out_names;
	ArrowSchemaToDuckDBTypes(context, result_batch->schema(), c_schema, arrow_table, out_types, out_names);

	// Type-confusion guard: worker window-batch result vs the bind-declared output type.
	ValidateProjectedWireBatch(&context, *result_batch, bind_data.resolved_output_schema, {},
	                           bind_data.attach_params->worker_path(), bind_data.function_name);

	auto chunk_wrapper = make_uniq<ArrowArrayWrapper>();
	ExportRecordBatch(result_batch, *chunk_wrapper);
	ArrowScanLocalState scan_state(std::move(chunk_wrapper), context);

	DataChunk batch_out;
	batch_out.Initialize(context, {result.GetType()}, count);
	batch_out.SetCardinality(count);
	ArrowTableFunction::ArrowToDuckDB(scan_state, arrow_table.GetColumns(), batch_out, false);

	VectorOperations::Copy(batch_out.data[0], result, count, 0, 0);
}

} // namespace vgi
} // namespace duckdb
