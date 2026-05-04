#include "vgi_aggregate_window_impl.hpp"

#include <cstring>

#include <arrow/builder.h>
#include <arrow/c/bridge.h>

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/client_context.hpp"

#include "vgi_aggregate_function_impl.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_exception.hpp"
#include "vgi_logging.hpp"
#include "vgi_rpc_types.hpp"

namespace duckdb {
namespace vgi {

namespace {

void ThrowOnArrowError(const arrow::Status &status) {
	if (!status.ok()) {
		throw IOException("Arrow error in VGI aggregate window: %s", status.ToString());
	}
}

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

std::vector<uint8_t> SerializeSchemaBytes(const std::shared_ptr<arrow::Schema> &schema) {
	auto buf = arrow::ipc::SerializeSchema(*schema);
	ThrowOnArrowError(buf.status());
	auto &b = buf.ValueUnsafe();
	return std::vector<uint8_t>(b->data(), b->data() + b->size());
}

// Convert the partition's ColumnDataCollection into an Arrow RecordBatch.
// Handles the inputs=nullptr edge case (no-column aggregate — rare but legal).
std::shared_ptr<arrow::RecordBatch> PartitionToArrow(ClientContext &context,
                                                     const WindowPartitionInput &partition) {
	if (!partition.inputs || partition.column_ids.empty()) {
		// No input columns — return an empty 0-column batch with just the row count.
		auto schema = arrow::schema({});
		return arrow::RecordBatch::Make(schema, partition.count,
		                                std::vector<std::shared_ptr<arrow::Array>> {});
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

std::shared_ptr<arrow::RecordBatch> BuildAggregateWindowInitRequest(
    const std::string &function_name, const std::vector<uint8_t> &execution_id,
    const std::vector<uint8_t> &attach_id, int64_t partition_id, int64_t row_count,
    const std::shared_ptr<arrow::RecordBatch> &partition_batch,
    const std::shared_ptr<arrow::Schema> &output_schema,
    const std::vector<uint8_t> &filter_mask_bytes,
    const std::vector<uint8_t> &frame_stats_bytes,
    const std::vector<uint8_t> &all_valid_bytes) {
	auto batch_bytes = SerializeToIpcBytes(partition_batch);
	auto output_schema_bytes = SerializeSchemaBytes(output_schema);

	auto schema = arrow::schema({
	    arrow::field("function_name", arrow::utf8(), false),
	    arrow::field("execution_id", arrow::binary(), false),
	    arrow::field("partition_id", arrow::int64(), false),
	    arrow::field("row_count", arrow::int64(), false),
	    arrow::field("partition_batch", arrow::binary(), false),
	    arrow::field("output_schema", arrow::binary(), false),
	    arrow::field("filter_mask", arrow::binary(), false),
	    arrow::field("frame_stats", arrow::binary(), false),
	    arrow::field("all_valid", arrow::binary(), false),
	    arrow::field("attach_id", arrow::binary(), true),
	});

	arrow::StringBuilder fn_b;
	arrow::BinaryBuilder eid_b, batch_b, os_b, fm_b, fs_b, av_b, aid_b;
	arrow::Int64Builder pid_b, rc_b;

	ThrowOnArrowError(fn_b.Append(function_name));
	ThrowOnArrowError(eid_b.Append(execution_id.data(), execution_id.size()));
	ThrowOnArrowError(pid_b.Append(partition_id));
	ThrowOnArrowError(rc_b.Append(row_count));
	ThrowOnArrowError(batch_b.Append(batch_bytes.data(), batch_bytes.size()));
	ThrowOnArrowError(os_b.Append(output_schema_bytes.data(), output_schema_bytes.size()));
	ThrowOnArrowError(fm_b.Append(filter_mask_bytes.data(), filter_mask_bytes.size()));
	ThrowOnArrowError(fs_b.Append(frame_stats_bytes.data(), frame_stats_bytes.size()));
	ThrowOnArrowError(av_b.Append(all_valid_bytes.data(), all_valid_bytes.size()));
	if (attach_id.empty()) {
		ThrowOnArrowError(aid_b.AppendNull());
	} else {
		ThrowOnArrowError(aid_b.Append(attach_id.data(), attach_id.size()));
	}

	std::shared_ptr<arrow::Array> fn_a, eid_a, pid_a, rc_a, batch_a, os_a, fm_a, fs_a, av_a, aid_a;
	ThrowOnArrowError(fn_b.Finish(&fn_a));
	ThrowOnArrowError(eid_b.Finish(&eid_a));
	ThrowOnArrowError(pid_b.Finish(&pid_a));
	ThrowOnArrowError(rc_b.Finish(&rc_a));
	ThrowOnArrowError(batch_b.Finish(&batch_a));
	ThrowOnArrowError(os_b.Finish(&os_a));
	ThrowOnArrowError(fm_b.Finish(&fm_a));
	ThrowOnArrowError(fs_b.Finish(&fs_a));
	ThrowOnArrowError(av_b.Finish(&av_a));
	ThrowOnArrowError(aid_b.Finish(&aid_a));

	return WrapAsRpcParams(arrow::RecordBatch::Make(
	    schema, 1, {fn_a, eid_a, pid_a, rc_a, batch_a, os_a, fm_a, fs_a, av_a, aid_a}));
}

std::shared_ptr<arrow::RecordBatch> BuildAggregateWindowRequest(
    const std::string &function_name, const std::vector<uint8_t> &execution_id,
    const std::vector<uint8_t> &attach_id, int64_t partition_id, int64_t rid,
    const SubFrames &subframes) {
	auto schema = arrow::schema({
	    arrow::field("function_name", arrow::utf8(), false),
	    arrow::field("execution_id", arrow::binary(), false),
	    arrow::field("partition_id", arrow::int64(), false),
	    arrow::field("rid", arrow::int64(), false),
	    arrow::field("frame_starts", arrow::list(arrow::field("item", arrow::int64(), true)), false),
	    arrow::field("frame_ends", arrow::list(arrow::field("item", arrow::int64(), true)), false),
	    arrow::field("attach_id", arrow::binary(), true),
	});

	arrow::StringBuilder fn_b;
	arrow::BinaryBuilder eid_b, aid_b;
	arrow::Int64Builder pid_b, rid_b;
	auto starts_values = std::make_shared<arrow::Int64Builder>();
	auto ends_values = std::make_shared<arrow::Int64Builder>();
	arrow::ListBuilder starts_b(arrow::default_memory_pool(), starts_values);
	arrow::ListBuilder ends_b(arrow::default_memory_pool(), ends_values);

	ThrowOnArrowError(fn_b.Append(function_name));
	ThrowOnArrowError(eid_b.Append(execution_id.data(), execution_id.size()));
	ThrowOnArrowError(pid_b.Append(partition_id));
	ThrowOnArrowError(rid_b.Append(rid));

	ThrowOnArrowError(starts_b.Append());
	ThrowOnArrowError(ends_b.Append());
	for (const auto &fb : subframes) {
		ThrowOnArrowError(starts_values->Append(static_cast<int64_t>(fb.start)));
		ThrowOnArrowError(ends_values->Append(static_cast<int64_t>(fb.end)));
	}

	if (attach_id.empty()) {
		ThrowOnArrowError(aid_b.AppendNull());
	} else {
		ThrowOnArrowError(aid_b.Append(attach_id.data(), attach_id.size()));
	}

	std::shared_ptr<arrow::Array> fn_a, eid_a, pid_a, rid_a, starts_a, ends_a, aid_a;
	ThrowOnArrowError(fn_b.Finish(&fn_a));
	ThrowOnArrowError(eid_b.Finish(&eid_a));
	ThrowOnArrowError(pid_b.Finish(&pid_a));
	ThrowOnArrowError(rid_b.Finish(&rid_a));
	ThrowOnArrowError(starts_b.Finish(&starts_a));
	ThrowOnArrowError(ends_b.Finish(&ends_a));
	ThrowOnArrowError(aid_b.Finish(&aid_a));

	return WrapAsRpcParams(arrow::RecordBatch::Make(
	    schema, 1, {fn_a, eid_a, pid_a, rid_a, starts_a, ends_a, aid_a}));
}

// Batched window request — sends all (rid, subframes) tuples for one
// Evaluate() call in a single RPC. The frames_per_row array gives the
// subframe cardinality per row so the worker can unflatten frame_starts /
// frame_ends (flat arrays of length sum(frames_per_row)).
std::shared_ptr<arrow::RecordBatch> BuildAggregateWindowBatchRequest(
    const std::string &function_name, const std::vector<uint8_t> &execution_id,
    const std::vector<uint8_t> &attach_id, int64_t partition_id,
    const SubFrames *subframes_per_row, idx_t count, idx_t row_idx) {
	auto schema = arrow::schema({
	    arrow::field("function_name", arrow::utf8(), false),
	    arrow::field("execution_id", arrow::binary(), false),
	    arrow::field("partition_id", arrow::int64(), false),
	    arrow::field("row_idx", arrow::int64(), false),
	    arrow::field("count", arrow::int64(), false),
	    arrow::field("frames_per_row", arrow::list(arrow::field("item", arrow::int64(), true)), false),
	    arrow::field("frame_starts", arrow::list(arrow::field("item", arrow::int64(), true)), false),
	    arrow::field("frame_ends", arrow::list(arrow::field("item", arrow::int64(), true)), false),
	    arrow::field("attach_id", arrow::binary(), true),
	});

	arrow::StringBuilder fn_b;
	arrow::BinaryBuilder eid_b, aid_b;
	arrow::Int64Builder pid_b, row_idx_b, count_b;
	auto fpr_values = std::make_shared<arrow::Int64Builder>();
	auto starts_values = std::make_shared<arrow::Int64Builder>();
	auto ends_values = std::make_shared<arrow::Int64Builder>();
	arrow::ListBuilder fpr_b(arrow::default_memory_pool(), fpr_values);
	arrow::ListBuilder starts_b(arrow::default_memory_pool(), starts_values);
	arrow::ListBuilder ends_b(arrow::default_memory_pool(), ends_values);

	ThrowOnArrowError(fn_b.Append(function_name));
	ThrowOnArrowError(eid_b.Append(execution_id.data(), execution_id.size()));
	ThrowOnArrowError(pid_b.Append(partition_id));
	ThrowOnArrowError(row_idx_b.Append(static_cast<int64_t>(row_idx)));
	ThrowOnArrowError(count_b.Append(static_cast<int64_t>(count)));

	ThrowOnArrowError(fpr_b.Append());
	ThrowOnArrowError(starts_b.Append());
	ThrowOnArrowError(ends_b.Append());
	for (idx_t r = 0; r < count; r++) {
		const auto &subframes = subframes_per_row[r];
		ThrowOnArrowError(fpr_values->Append(static_cast<int64_t>(subframes.size())));
		for (const auto &fb : subframes) {
			ThrowOnArrowError(starts_values->Append(static_cast<int64_t>(fb.start)));
			ThrowOnArrowError(ends_values->Append(static_cast<int64_t>(fb.end)));
		}
	}

	if (attach_id.empty()) {
		ThrowOnArrowError(aid_b.AppendNull());
	} else {
		ThrowOnArrowError(aid_b.Append(attach_id.data(), attach_id.size()));
	}

	std::shared_ptr<arrow::Array> fn_a, eid_a, pid_a, row_idx_a, count_a, fpr_a, starts_a, ends_a, aid_a;
	ThrowOnArrowError(fn_b.Finish(&fn_a));
	ThrowOnArrowError(eid_b.Finish(&eid_a));
	ThrowOnArrowError(pid_b.Finish(&pid_a));
	ThrowOnArrowError(row_idx_b.Finish(&row_idx_a));
	ThrowOnArrowError(count_b.Finish(&count_a));
	ThrowOnArrowError(fpr_b.Finish(&fpr_a));
	ThrowOnArrowError(starts_b.Finish(&starts_a));
	ThrowOnArrowError(ends_b.Finish(&ends_a));
	ThrowOnArrowError(aid_b.Finish(&aid_a));

	return WrapAsRpcParams(arrow::RecordBatch::Make(
	    schema, 1, {fn_a, eid_a, pid_a, row_idx_a, count_a, fpr_a, starts_a, ends_a, aid_a}));
}

std::shared_ptr<arrow::RecordBatch> BuildAggregateWindowDestructorRequest(
    const std::string &function_name, const std::vector<uint8_t> &execution_id,
    const std::vector<uint8_t> &attach_id, int64_t partition_id) {
	auto schema = arrow::schema({
	    arrow::field("function_name", arrow::utf8(), false),
	    arrow::field("execution_id", arrow::binary(), false),
	    arrow::field("partition_id", arrow::int64(), false),
	    arrow::field("attach_id", arrow::binary(), true),
	});

	arrow::StringBuilder fn_b;
	arrow::BinaryBuilder eid_b, aid_b;
	arrow::Int64Builder pid_b;

	ThrowOnArrowError(fn_b.Append(function_name));
	ThrowOnArrowError(eid_b.Append(execution_id.data(), execution_id.size()));
	ThrowOnArrowError(pid_b.Append(partition_id));
	if (attach_id.empty()) {
		ThrowOnArrowError(aid_b.AppendNull());
	} else {
		ThrowOnArrowError(aid_b.Append(attach_id.data(), attach_id.size()));
	}

	std::shared_ptr<arrow::Array> fn_a, eid_a, pid_a, aid_a;
	ThrowOnArrowError(fn_b.Finish(&fn_a));
	ThrowOnArrowError(eid_b.Finish(&eid_a));
	ThrowOnArrowError(pid_b.Finish(&pid_a));
	ThrowOnArrowError(aid_b.Finish(&aid_a));

	return WrapAsRpcParams(arrow::RecordBatch::Make(schema, 1, {fn_a, eid_a, pid_a, aid_a}));
}

} // anonymous namespace

// ============================================================================
// Window destructor helper (called from VgiAggregateDestroy)
// ============================================================================

void SendAggregateWindowDestructorRpc(ClientContext &context, const VgiAggregateBindData &bind_data,
                                       int64_t partition_id) {
	auto request = BuildAggregateWindowDestructorRequest(
	    bind_data.function_name, bind_data.exec_state->execution_id,
	    bind_data.attach_id, partition_id);
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
	    bind_data.function_name, bind_data.exec_state->execution_id, bind_data.attach_id,
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
	    bind_data.function_name, bind_data.exec_state->execution_id, bind_data.attach_id,
	    global->partition_id, static_cast<int64_t>(rid), subframes);

	auto rpc_result = InvokeAggregateRpc(context, bind_data, "aggregate_window", request);

	if (!rpc_result.response_batch || rpc_result.response_batch->num_rows() == 0) {
		throw IOException("VGI aggregate_window returned empty response for '%s'",
		                  bind_data.function_name);
	}

	// Standard vgi_rpc response envelope: {result: binary} wrapping
	// AggregateWindowResponse{result_batch: binary}.
	auto response_result_col = rpc_result.response_batch->GetColumnByName("result");
	if (!response_result_col) {
		throw IOException("VGI aggregate_window response missing 'result' column");
	}
	auto response_binary = std::dynamic_pointer_cast<arrow::BinaryArray>(response_result_col);
	if (!response_binary || response_binary->IsNull(0)) {
		throw IOException("VGI aggregate_window response has null result");
	}
	auto response_view = response_binary->GetView(0);
	auto response_batch = DeserializeFromIpcBytes(
	    reinterpret_cast<const uint8_t *>(response_view.data()), response_view.size());

	auto rb_col = response_batch->GetColumnByName("result_batch");
	if (!rb_col) {
		throw IOException("VGI aggregate_window response missing 'result_batch' field");
	}
	auto rb_binary = std::dynamic_pointer_cast<arrow::BinaryArray>(rb_col);
	if (!rb_binary || rb_binary->IsNull(0)) {
		throw IOException("VGI aggregate_window response has null result_batch");
	}
	auto rb_view = rb_binary->GetView(0);
	auto result_batch = DeserializeFromIpcBytes(
	    reinterpret_cast<const uint8_t *>(rb_view.data()), rb_view.size());

	if (!result_batch || result_batch->num_rows() != 1 || result_batch->num_columns() != 1) {
		throw IOException("VGI aggregate_window returned invalid one-row result");
	}

	// Convert the one-row Arrow result to a DuckDB Vector and copy into result[rid].
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
	vector<LogicalType> out_types;
	vector<string> out_names;
	ArrowSchemaToDuckDBTypes(context, result_batch->schema(), c_schema, arrow_table, out_types, out_names);

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
	    bind_data.function_name, bind_data.exec_state->execution_id, bind_data.attach_id,
	    global->partition_id, subframes_per_row, count, row_idx);

	auto rpc_result = InvokeAggregateRpc(context, bind_data, "aggregate_window_batch", request);

	if (!rpc_result.response_batch || rpc_result.response_batch->num_rows() == 0) {
		throw IOException("VGI aggregate_window_batch returned empty response for '%s'",
		                  bind_data.function_name);
	}

	// Unwrap {result: binary} envelope -> AggregateWindowBatchResponse{result_batch: binary}.
	auto response_result_col = rpc_result.response_batch->GetColumnByName("result");
	if (!response_result_col) {
		throw IOException("VGI aggregate_window_batch response missing 'result' column");
	}
	auto response_binary = std::dynamic_pointer_cast<arrow::BinaryArray>(response_result_col);
	if (!response_binary || response_binary->IsNull(0)) {
		throw IOException("VGI aggregate_window_batch response has null result");
	}
	auto response_view = response_binary->GetView(0);
	auto response_batch = DeserializeFromIpcBytes(
	    reinterpret_cast<const uint8_t *>(response_view.data()), response_view.size());

	auto rb_col = response_batch->GetColumnByName("result_batch");
	if (!rb_col) {
		throw IOException("VGI aggregate_window_batch response missing 'result_batch' field");
	}
	auto rb_binary = std::dynamic_pointer_cast<arrow::BinaryArray>(rb_col);
	if (!rb_binary || rb_binary->IsNull(0)) {
		throw IOException("VGI aggregate_window_batch response has null result_batch");
	}
	auto rb_view = rb_binary->GetView(0);
	auto result_batch = DeserializeFromIpcBytes(
	    reinterpret_cast<const uint8_t *>(rb_view.data()), rb_view.size());

	if (!result_batch || result_batch->num_columns() != 1) {
		throw IOException("VGI aggregate_window_batch returned invalid result");
	}
	if (static_cast<idx_t>(result_batch->num_rows()) != count) {
		throw IOException("VGI aggregate_window_batch expected %lld rows, got %lld for '%s'",
		                  static_cast<int64_t>(count), static_cast<int64_t>(result_batch->num_rows()),
		                  bind_data.function_name.c_str());
	}

	// Convert the count-row Arrow batch to a DataChunk and copy into result[0..count).
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
	vector<LogicalType> out_types;
	vector<string> out_names;
	ArrowSchemaToDuckDBTypes(context, result_batch->schema(), c_schema, arrow_table, out_types, out_names);

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
