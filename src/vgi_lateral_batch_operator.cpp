// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_lateral_batch_operator.hpp"

#include <cstring>
#include <limits>

#include <arrow/array/builder_primitive.h> // Int32Builder for the Take indices
#include <arrow/compute/api_vector.h>       // arrow::compute::Take (dedup expansion)

#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include <chrono>

#include "vgi_arrow_ipc.hpp"          // SerializeRecordBatch
#include "vgi_arrow_utils.hpp"        // BuildArrowSchemaFromDuckDB / ArrowSchemaToDuckDBTypes / DataChunkToArrow
#include "vgi_cache_control.hpp"      // VgiCacheControl
#include "vgi_exchange_cache_key.hpp" // exchange-mode result cache (M2)
#include "vgi_ifunction_connection.hpp"
#include "vgi_input_dedup.hpp"        // input dedup (ship distinct tuples; expand back)
#include "vgi_logging.hpp"
#include "vgi_result_cache.hpp"       // VgiResultCache singleton

namespace duckdb {
namespace vgi {

// ============================================================================
// LogicalVgiLateralBatch
// ============================================================================

vector<ColumnBinding> LogicalVgiLateralBatch::GetColumnBindings() {
	// Worker output columns under table_index (snapshot), then the child's
	// projected/outer column bindings — byte-for-byte LogicalGet's shape.
	auto result = worker_bindings;
	auto child_bindings = children[0]->GetColumnBindings();
	for (auto entry : projected_input) {
		result.emplace_back(child_bindings[entry]);
	}
	return result;
}

vector<idx_t> LogicalVgiLateralBatch::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

string LogicalVgiLateralBatch::GetName() const {
	return "VGI_LATERAL_BATCH";
}

string LogicalVgiLateralBatch::GetExtensionName() const {
	return "vgi_lateral_batch";
}

void LogicalVgiLateralBatch::ResolveTypes() {
	types = worker_output_types;
	for (auto entry : projected_input) {
		types.push_back(children[0]->types[entry]);
	}
}

void LogicalVgiLateralBatch::Serialize(Serializer & /*serializer*/) const {
	throw NotImplementedException("LogicalVgiLateralBatch serialization is not supported "
	                              "(plan caching across processes is out of scope for v1)");
}

PhysicalOperator &LogicalVgiLateralBatch::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	D_ASSERT(children.size() == 1);
	estimated_cardinality = EstimateCardinality(context);
	auto &child_plan = planner.CreatePlan(*children[0]);
	// input_length = worker-input width = child columns minus the correlated cols.
	const idx_t input_length = children[0]->types.size() - projected_input.size();
	const idx_t base_idx = worker_output_types.size();
	D_ASSERT(worker_column_ids.size() == base_idx);
	auto out_types = types; // [worker | projected]
	auto &op = planner.Make<PhysicalVgiLateralBatch>(std::move(out_types), std::move(bind_data),
	                                                  std::move(projected_input), input_length, base_idx,
	                                                  std::move(worker_column_ids), projection_pushdown,
	                                                  estimated_cardinality);
	op.children.push_back(child_plan);
	return op;
}

// ============================================================================
// Operator state
// ============================================================================

namespace {

struct VgiLateralBatchGlobalState : public GlobalOperatorState {};

struct VgiLateralBatchOperatorState : public OperatorState {
	VgiLateralBatchOperatorState(ClientContext &context, const std::vector<uint8_t> &substream_id)
	    : client_context(context), substream_id(substream_id),
	      scan(make_uniq<ArrowArrayWrapper>(), context),
	      serve_scan(make_uniq<ArrowArrayWrapper>(), context) {
	}
	~VgiLateralBatchOperatorState() override {
		// Mid-stream worker (no finalize map) is never pooled — just drop it.
		connection.reset();
	}

	ClientContext &client_context; // for lazy schema build + (de)serialize
	std::unique_ptr<IFunctionConnection> connection; // this thread's own worker
	std::vector<uint8_t> substream_id;
	VgiTableInOutLocalState scan; // drain buffer for the worker output batch
	// Per-output-row parent input-row index for the batch currently in `scan`.
	// Held across HAVE_MORE_OUTPUT slices (the input chunk is re-passed unchanged
	// by DuckDB while draining, so stamping stays correct). Identity [0..n) when the
	// worker emitted no provenance (1->1 maps). Cleared when the batch is exhausted.
	std::vector<int32_t> parent_index;
	// input.size() when parent_index was decoded (branch B). parent_index was
	// range-validated against THIS size; every drain slice (branch A) gathers from
	// input at those indices and relies on DuckDB re-passing the same-sized chunk.
	// Asserted on each drain so a broken re-pass contract fails loudly, never OOB.
	idx_t input_size_at_decode = 0;

	// ---- Exchange-mode result cache (M2, per-input-chunk memoization) ----
	// Built once on the first chunk: the static key + whether caching is eligible.
	bool cache_key_built = false;
	bool cache_eligible = false;
	VgiResultCacheKey cache_static_key;
	std::string cache_catalog_name;
	int64_t cache_default_ttl_seconds = 0;
	int64_t cache_revalidate_min_bytes = 262144;
	VgiCacheControl cache_cc;   // latched from the first exchange output
	bool cache_cc_latched = false;
	// MISS capture: the current input chunk's full key + its accumulated POST-STAMP
	// output slices (serialized). Committed as one entry when the chunk fully drains.
	VgiResultCacheKey capture_key;
	std::vector<std::shared_ptr<arrow::RecordBatch>> capture_pending;
	bool capturing = false;
	// HIT serve: the cached entry for the current input chunk + replay cursor.
	std::shared_ptr<const VgiResultCacheEntry> serving;
	size_t serve_cursor = 0;
	// Full-output arrow schema + table (all output columns, synthetic names), built
	// once. Capture serializes the post-stamp chunk with `serve_schema`; replay
	// deserializes into `serve_scan` and produces via `serve_arrow_table`.
	bool serve_schema_built = false;
	std::shared_ptr<arrow::Schema> serve_schema;
	ArrowSchemaWrapper serve_c_schema;
	ArrowTableSchema serve_arrow_table;
	VgiTableInOutLocalState serve_scan; // replay drain buffer (full output schema)
};

// Decode the worker's per-output-row provenance. `raw` is the base64-decoded raw
// little-endian int32[] from the batch's ``vgi_rpc.parent_row`` metadata (already
// b64-decoded by the connection). Empty = no provenance: assume an identity 1->1
// map (valid only when output rows == input rows, else a loud error). Validates
// length and that every parent index is in [0, input_rows).
std::vector<int32_t> DecodeParentRow(const std::string &raw, idx_t output_rows, idx_t input_rows,
                                     const VgiTableInOutBindData &bd) {
	std::vector<int32_t> result;
	if (raw.empty()) {
		if (output_rows != input_rows) {
			throw IOException(
			    "vgi_batch_lateral: worker '%s' function '%s' returned %llu output rows for %llu input rows "
			    "without vgi_rpc.parent_row provenance; a fan-out (1->N) or filtering (1->0) blended LATERAL map "
			    "must emit per-output-row parent indices",
			    bd.worker_path(), bd.function_name, (unsigned long long)output_rows, (unsigned long long)input_rows);
		}
		result.resize(output_rows);
		for (idx_t i = 0; i < output_rows; i++) {
			result[i] = static_cast<int32_t>(i);
		}
		return result;
	}
	// Guard the multiply below against overflow (a hostile 0-column batch could claim
	// an enormous num_rows). output_rows this large can never be matched by a deliverable
	// raw payload, so this fails closed with a clear error rather than wrapping.
	if (output_rows > (std::numeric_limits<size_t>::max() / sizeof(int32_t))) {
		throw IOException("vgi_batch_lateral: implausible output row count %llu from worker '%s' function '%s'",
		                  (unsigned long long)output_rows, bd.worker_path(), bd.function_name);
	}
	if (raw.size() != output_rows * sizeof(int32_t)) {
		throw IOException("vgi_batch_lateral: vgi_rpc.parent_row is %llu bytes for %llu output rows "
		                  "(expected %llu); worker '%s' function '%s'",
		                  (unsigned long long)raw.size(), (unsigned long long)output_rows,
		                  (unsigned long long)(output_rows * sizeof(int32_t)), bd.worker_path(), bd.function_name);
	}
	result.resize(output_rows);
	std::memcpy(result.data(), raw.data(), raw.size()); // LE int32[] — DuckDB targets are little-endian
	for (idx_t i = 0; i < output_rows; i++) {
		if (result[i] < 0 || static_cast<idx_t>(result[i]) >= input_rows) {
			throw IOException("vgi_batch_lateral: parent_row[%llu] = %d is out of range [0, %llu); "
			                  "worker '%s' function '%s'",
			                  (unsigned long long)i, result[i], (unsigned long long)input_rows, bd.worker_path(),
			                  bd.function_name);
		}
	}
	return result;
}

// Stamp the projected/correlated columns onto the produced output slice by gathering
// each input correlated column at the parent-row index of every output row. `start`
// is the offset into `parent_index` for the first row of this slice (== the scan's
// chunk_offset before this ProduceOutputFromBatch call).
void StampProjected(DataChunk &chunk, DataChunk &input, const std::vector<int32_t> &parent_index,
                    const std::vector<column_t> &projected_input, idx_t base_idx, idx_t start, idx_t produced) {
	if (produced == 0) {
		return;
	}
	SelectionVector sel(produced);
	for (idx_t r = 0; r < produced; r++) {
		sel.set_index(r, static_cast<idx_t>(parent_index[start + r]));
	}
	for (idx_t k = 0; k < projected_input.size(); k++) {
		// target[r] = input_col[sel[r]] — a flat gather that severs the input
		// dependency for the emitted chunk (like the streaming-window op's copy).
		VectorOperations::Copy(input.data[projected_input[k]], chunk.data[base_idx + k], sel, produced, 0, 0);
	}
}

// Gather (replicate) the worker output batch rows at `take` (dedup expansion): the
// resulting batch has one row per FINAL output row, so the existing drain+stamp path
// runs over it unchanged. Uses Arrow's built-in Take so all column types are handled.
std::shared_ptr<arrow::RecordBatch> TakeRecordBatch(const std::shared_ptr<arrow::RecordBatch> &batch,
                                                    const std::vector<int32_t> &take,
                                                    const VgiTableInOutBindData &bd) {
	arrow::Int32Builder b;
	auto st = b.Reserve(static_cast<int64_t>(take.size()));
	if (!st.ok()) {
		throw IOException("vgi_batch_lateral: dedup Take index reserve failed: %s", st.ToString());
	}
	for (auto v : take) {
		b.UnsafeAppend(v);
	}
	std::shared_ptr<arrow::Array> indices;
	st = b.Finish(&indices);
	if (!st.ok()) {
		throw IOException("vgi_batch_lateral: dedup Take index build failed: %s", st.ToString());
	}
	auto res = arrow::compute::Take(arrow::Datum(batch), arrow::Datum(indices));
	if (!res.ok()) {
		throw IOException("vgi_batch_lateral: worker '%s' function '%s' output Take (dedup expansion) failed: %s",
		                  bd.worker_path(), bd.function_name, res.status().ToString());
	}
	return res.ValueUnsafe().record_batch();
}

// Build the full-output arrow schema + ArrowTableSchema once (synthetic col names).
// Capture serializes the post-stamp chunk with `serve_schema`; replay deserializes
// into `serve_scan` and produces via `serve_arrow_table`.
void EnsureServeSchema(VgiLateralBatchOperatorState &state, const vector<LogicalType> &out_types) {
	if (state.serve_schema_built) {
		return;
	}
	vector<string> names;
	names.reserve(out_types.size());
	for (idx_t i = 0; i < out_types.size(); i++) {
		names.push_back("col" + std::to_string(i));
	}
	state.serve_schema = BuildArrowSchemaFromDuckDB(state.client_context, out_types, names);
	vector<LogicalType> rt;
	vector<string> rn;
	ArrowSchemaToDuckDBTypes(state.client_context, state.serve_schema, state.serve_c_schema,
	                         state.serve_arrow_table, rt, rn);
	state.serve_schema_built = true;
}

// Emit the next cached POST-STAMP slice of the entry being served into `chunk`.
// Each cached batch is exactly one produced slice (<= STANDARD_VECTOR_SIZE), so it
// converts whole. Returns HAVE_MORE_OUTPUT while slices remain, else NEED_MORE_INPUT
// (and clears the serving state).
OperatorResultType EmitServedSlice(VgiLateralBatchOperatorState &state, const ArrowTableSchema &arrow_table,
                                   DataChunk &chunk) {
	const auto &batches = state.serving->streams[0].batches;
	auto batch = DeserializeCachedRecordBatch(*state.serving, batches[state.serve_cursor]);
	state.serve_cursor++;
	LoadBatchIntoScanState(state.serve_scan, batch);
	ProduceOutputFromBatch(state.serve_scan, arrow_table, chunk); // full-output batch → all cols
	chunk.SetCardinality(static_cast<idx_t>(batch->num_rows()));
	if (state.serve_cursor >= batches.size()) {
		state.serving.reset();
		state.serve_cursor = 0;
		return OperatorResultType::NEED_MORE_INPUT;
	}
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

// Commit the current input chunk's accumulated POST-STAMP output slices as one
// memory-only cache entry (allow_disk=false). Clears the capture state.
void CommitCapture(VgiLateralBatchOperatorState &state, const VgiTableInOutBindData &bd, ClientContext &ctx) {
	auto sr = StoreExchangeMemoEntry(state.capture_key, state.cache_cc, state.cache_catalog_name,
	                                 state.cache_default_ttl_seconds, state.capture_pending,
	                                 /*allow_disk=*/true);
	if (sr.stored) {
		VgiResultCache::Instance().RecordExchangeStore();
		VGI_LOG(ctx, "result_cache.store",
		        {{"function", bd.function_name},
		         {"key_hash", state.capture_key.HexDigest()},
		         {"tier", "memory"},
		         {"rows", std::to_string(sr.rows)},
		         {"bytes", std::to_string(sr.bytes)}});
	} else if (sr.reason) {
		VGI_LOG(ctx, "result_cache.store_skipped", {{"function", bd.function_name}, {"reason", sr.reason}});
	}
	state.capturing = false;
	state.capture_pending.clear();
}

} // anonymous namespace

// ============================================================================
// PhysicalVgiLateralBatch
// ============================================================================

PhysicalVgiLateralBatch::PhysicalVgiLateralBatch(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                                 unique_ptr<FunctionData> bind_data_p, vector<column_t> projected_input_p,
                                                 idx_t input_length_p, idx_t base_idx_p,
                                                 vector<column_t> worker_column_ids_p, bool projection_pushdown_p,
                                                 idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      bind_data(std::move(bind_data_p)), projected_input(std::move(projected_input_p)), input_length(input_length_p),
      base_idx(base_idx_p), worker_column_ids(std::move(worker_column_ids_p)),
      projection_pushdown(projection_pushdown_p) {
}

unique_ptr<GlobalOperatorState> PhysicalVgiLateralBatch::GetGlobalOperatorState(ClientContext & /*context*/) const {
	return make_uniq<VgiLateralBatchGlobalState>();
}

unique_ptr<OperatorState> PhysicalVgiLateralBatch::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<VgiLateralBatchOperatorState>(context.client, MintSubstreamId());
}

OperatorResultType PhysicalVgiLateralBatch::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                    GlobalOperatorState & /*gstate*/, OperatorState &state_p) const {
	auto &state = state_p.Cast<VgiLateralBatchOperatorState>();
	auto &client_context = context.client;
	auto &bd = bind_data->Cast<VgiTableInOutBindData>();

	// Build the STATIC cache key once (per-chunk memoization: a hit replays a chunk's
	// POST-STAMP output and skips the worker exchange; the key folds in an
	// order-independent hash of the FULL input chunk — correlated columns are baked
	// into the cached output so they must be part of the key). See docs/result cache.
	if (!state.cache_key_built) {
		state.cache_key_built = true;
		std::vector<int32_t> proj_key;
		if (projection_pushdown) {
			for (auto col_id : worker_column_ids) {
				proj_key.push_back(static_cast<int32_t>(col_id));
			}
		}
		const char *reason = nullptr;
		int64_t catalog_version = 0;
		if (BuildExchangeCacheKeyStatic(client_context, bd, proj_key, state.cache_static_key,
		                                state.cache_catalog_name, catalog_version, reason)) {
			state.cache_eligible = true;
			EnsureServeSchema(state, types);
			Value ttl_v;
			if (client_context.TryGetCurrentSetting("vgi_result_cache_default_ttl_seconds", ttl_v)) {
				state.cache_default_ttl_seconds = static_cast<int64_t>(ttl_v.GetValue<uint64_t>());
			}
			Value rmv;
			if (client_context.TryGetCurrentSetting("vgi_result_cache_revalidate_min_bytes", rmv) && !rmv.IsNull()) {
				state.cache_revalidate_min_bytes = static_cast<int64_t>(rmv.GetValue<uint64_t>());
			}
		} else if (reason) {
			VGI_LOG(client_context, "result_cache.ineligible",
			        {{"function", bd.function_name}, {"reason", reason}});
		}
	}

	// (S) Serve the next cached POST-STAMP slice of a HIT for the current input chunk.
	if (state.serving) {
		return EmitServedSlice(state, state.serve_arrow_table, chunk);
	}

	// (A) Drain the remaining slices of the worker output batch already loaded in
	// `scan` (a MISS in progress). DuckDB re-passes the SAME `input` chunk unchanged
	// across HAVE_MORE_OUTPUT, so the held parent_index still refers to the right rows.
	if (HasRemainingBatchData(state.scan)) {
		// Defense-in-depth: parent_index was range-validated against input.size() at
		// decode time. If DuckDB ever re-passed a smaller chunk during the drain, a
		// held index could point past the input's cardinality — an OOB gather. Fail
		// loudly instead (the invariant is a same-sized re-pass; this should be dead).
		if (input.size() != state.input_size_at_decode) {
			throw IOException("vgi_batch_lateral: input chunk resized mid-drain (%llu -> %llu); "
			                  "worker '%s' function '%s'",
			                  (unsigned long long)state.input_size_at_decode, (unsigned long long)input.size(),
			                  bd.worker_path(), bd.function_name);
		}
		idx_t start = state.scan.chunk_offset; // rows already produced from this batch
		idx_t produced = ProduceOutputFromBatch(state.scan, bd.arrow_table, chunk, projection_pushdown);
		StampProjected(chunk, input, state.parent_index, projected_input, base_idx, start, produced);
		chunk.SetCardinality(produced);
		if (state.capturing) {
			state.capture_pending.push_back(DataChunkToArrow(client_context, chunk, state.serve_schema));
		}
		if (HasRemainingBatchData(state.scan)) {
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}
		if (state.capturing) {
			CommitCapture(state, bd, client_context); // input chunk fully drained → commit
		}
		return OperatorResultType::NEED_MORE_INPUT;
	}

	if (input.size() == 0) {
		chunk.SetCardinality(0);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	// (B) Fresh input chunk. Cache lookup on the static key + order-independent
	// FULL-chunk input hash. A HIT replays the cached POST-STAMP output (correlated
	// columns baked in — no re-stamp; sound because the operator is NO_ORDER).
	std::shared_ptr<const VgiResultCacheEntry> reval_entry; // armed conditional revalidation
	if (state.cache_eligible) {
		state.capture_key = state.cache_static_key;
		state.capture_key.input_hash = HashInputChunkUnordered(client_context, input);
		auto now = std::chrono::steady_clock::now();
		// Probe conditional revalidation FIRST (Lookup evicts a stale entry). If a
		// large-enough stale revalidatable entry exists, arm the exchange with its
		// validators; the worker confirms (304 → reuse stored) or returns fresh data.
		auto reval = VgiResultCache::Instance().LookupForRevalidation(state.capture_key, now);
		if (reval && reval->revalidatable && !reval->streams.empty() &&
		    reval->total_bytes >= state.cache_revalidate_min_bytes) {
			reval_entry = reval;
		} else {
			auto entry = VgiResultCache::Instance().Lookup(state.capture_key, now);
			if (entry) {
				VgiResultCache::Instance().RecordExchangeHit(entry->total_bytes);
				VGI_LOG(client_context, "result_cache.hit",
				        {{"function", bd.function_name}, {"key_hash", state.capture_key.HexDigest()}, {"tier", "memory"}});
				if (entry->streams.empty() || entry->streams[0].batches.empty()) {
					chunk.SetCardinality(0); // cached empty (all-filtered) result — nothing to emit
					return OperatorResultType::NEED_MORE_INPUT;
				}
				state.serving = entry;
				state.serve_cursor = 0;
				return EmitServedSlice(state, state.serve_arrow_table, chunk);
			}
		}
	}

	// MISS: acquire this substream's worker lazily (only on a miss — an all-hit scan
	// never checks one out). Thread the wire projection when the worker narrows output.
	if (!state.connection) {
		std::vector<int32_t> projection_ids;
		if (projection_pushdown) {
			projection_ids.reserve(worker_column_ids.size());
			for (auto col_id : worker_column_ids) {
				projection_ids.push_back(static_cast<int32_t>(col_id));
			}
			state.scan.column_ids = worker_column_ids;
		}
		state.connection = AcquireBlendedInputConnection(client_context, bd, state.substream_id, projection_ids);
	}

	// Arm conditional-revalidation validators for this chunk's exchange (they ride the
	// input batch's metadata). Cleared right after the exchange so they don't leak.
	if (reval_entry) {
		state.connection->SetConditionalRequest(reval_entry->etag, reval_entry->last_modified);
	}

	// [dedup] Ship only the DISTINCT worker-input tuples to the worker (unless disabled,
	// trivial, or the map is VOLATILE — a volatile map's per-row output varies for the
	// same input, so duplicate tuples must NOT be collapsed). The 1:N worker output is
	// expanded back over each tuple's original-row group after the exchange (below).
	bool dedup_enabled = true;
	{
		Value dv;
		if (client_context.TryGetCurrentSetting("vgi_exchange_input_dedup", dv) && !dv.IsNull()) {
			dedup_enabled = dv.GetValue<bool>();
		}
	}
	const bool is_volatile = bd.stability.has_value() && bd.stability.value() == FunctionStability::VOLATILE;
	InputDedup dd;
	bool use_dedup = false;
	if (dedup_enabled && !is_volatile) {
		std::vector<column_t> wcols;
		wcols.reserve(input_length);
		for (idx_t c = 0; c < input_length; c++) {
			wcols.push_back(c);
		}
		dd = BuildInputDedup(input, wcols);
		use_dedup = !dd.trivial;
	}

	// Build the worker-input chunk: K deduped rows (materialized gather) or an N-row view.
	DataChunk worker_input;
	vector<LogicalType> in_types;
	in_types.reserve(input_length);
	for (idx_t c = 0; c < input_length; c++) {
		in_types.push_back(input.data[c].GetType());
	}
	if (use_dedup) {
		worker_input.Initialize(Allocator::Get(client_context), in_types, dd.k == 0 ? 1 : dd.k);
		for (idx_t c = 0; c < input_length; c++) {
			VectorOperations::Copy(input.data[c], worker_input.data[c], dd.distinct, dd.k, 0, 0);
		}
		worker_input.SetCardinality(dd.k);
	} else {
		worker_input.InitializeEmpty(in_types);
		for (idx_t c = 0; c < input_length; c++) {
			worker_input.data[c].Reference(input.data[c]);
		}
		worker_input.SetCardinality(input.size());
	}

	// ONE batched exchange for the whole input chunk.
	auto input_batch = ConvertInputToArrow(client_context, worker_input, bd);
	VGI_LOG(client_context, "table_in_out.write_input",
	        {{"conn", state.connection->GetConnIdHex()},
	         {"worker_path", bd.worker_path()},
	         {"function_name", bd.function_name},
	         {"input_rows", std::to_string(input_batch->num_rows())}});
	state.connection->WriteInputBatch(input_batch);
	auto output_batch = state.connection->ReadDataBatch();

	if (!output_batch) {
		// EOS mid-stream is a worker protocol violation: this operator never closes the
		// input writer (a map keeps exchanging), so the worker must answer every input
		// chunk with a data batch. A silent FINISHED here would drop the rest of the
		// input; surface it. Drop the connection (never pool a mid-stream worker).
		state.connection.reset();
		throw IOException("vgi_batch_lateral: worker '%s' function '%s' returned end-of-stream "
		                  "mid-exchange (expected one output batch per input chunk)",
		                  bd.worker_path(), bd.function_name);
	}

	// Conditional revalidation: clear the armed validators (so the next chunk's exchange
	// isn't a stray conditional request) and, on a 304 (0-row not_modified reply), slide
	// the stored entry's TTL and serve its cached POST-STAMP output instead of the worker
	// response. Otherwise the fresh response falls through to the normal capture path.
	if (reval_entry) {
		state.connection->SetConditionalRequest("", "");
		if (output_batch->num_rows() == 0) {
			auto cc = state.connection->GetLastCacheControl();
			if (cc.not_modified) {
				SlideRevalidatedExchangeEntry(*reval_entry, cc, state.cache_default_ttl_seconds,
				                              /*allow_disk=*/true);
				VgiResultCache::Instance().RecordExchangeRevalidation(reval_entry->total_bytes);
				VGI_LOG(client_context, "result_cache.revalidate",
				        {{"function", bd.function_name},
				         {"key_hash", state.capture_key.HexDigest()},
				         {"outcome", "not_modified"}});
				state.serving = reval_entry;
				state.serve_cursor = 0;
				return EmitServedSlice(state, state.serve_arrow_table, chunk);
			}
		}
	}

	// Reaching here = a fresh exchange for this chunk (not a cache hit, not a 304).
	if (state.cache_eligible) {
		VgiResultCache::Instance().RecordExchangeMiss();
	}

	// Latch the worker's cache-control advertisement off the first exchange output.
	if (state.cache_eligible && !state.cache_cc_latched) {
		state.cache_cc = state.connection->GetLastCacheControl();
		state.cache_cc_latched = true;
	}

	if (output_batch->num_rows() == 0) {
		// 1->0 for the WHOLE chunk: every input row filtered out. Not cached (v1) — a
		// partial 1->0 (some rows survive) carries provenance and IS captured below.
		chunk.SetCardinality(0);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	// Decode provenance (identity when absent + rows align). With dedup the worker's
	// parent indices point into the K-row DEDUPED input; EXPAND them: each worker output
	// row (parent d) fans out to every ORIGINAL row in that tuple's group, and the worker
	// output row itself is replicated (Arrow Take), so the existing drain+stamp path runs
	// unchanged over an expanded batch whose parent_index now points at ORIGINAL rows
	// (each stamped with its OWN outer columns — two rows sharing a worker-input tuple can
	// differ in other outer columns). Range-validated against THIS input.size() (drain guard).
	const idx_t worker_input_rows = use_dedup ? dd.k : input.size();
	auto worker_parent = DecodeParentRow(state.connection->GetLastParentRowBytes(),
	                                     static_cast<idx_t>(output_batch->num_rows()), worker_input_rows, bd);
	if (use_dedup) {
		std::vector<int32_t> take_idx; // worker output row to copy for each final row
		std::vector<int32_t> expanded; // ORIGINAL input row to stamp for each final row
		take_idx.reserve(worker_parent.size());
		expanded.reserve(worker_parent.size());
		for (idx_t m = 0; m < worker_parent.size(); m++) {
			const auto &grp = dd.groups[static_cast<idx_t>(worker_parent[m])];
			for (idx_t o : grp) {
				take_idx.push_back(static_cast<int32_t>(m));
				expanded.push_back(static_cast<int32_t>(o));
			}
		}
		output_batch = TakeRecordBatch(output_batch, take_idx, bd);
		state.parent_index = std::move(expanded);
	} else {
		state.parent_index = std::move(worker_parent);
	}
	state.input_size_at_decode = input.size();

	LoadBatchIntoScanState(state.scan, output_batch);
	idx_t start = state.scan.chunk_offset; // 0 for a freshly loaded batch
	idx_t produced = ProduceOutputFromBatch(state.scan, bd.arrow_table, chunk, projection_pushdown); // fills [0, base_idx)
	StampProjected(chunk, input, state.parent_index, projected_input, base_idx, start, produced);
	chunk.SetCardinality(produced);

	// Begin capturing this input chunk's POST-STAMP output (if the worker opted in).
	if (state.cache_eligible && state.cache_cc.Cacheable()) {
		state.capturing = true;
		state.capture_pending.clear();
		state.capture_pending.push_back(DataChunkToArrow(client_context, chunk, state.serve_schema));
	} else {
		state.capturing = false;
	}
	if (HasRemainingBatchData(state.scan)) {
		return OperatorResultType::HAVE_MORE_OUTPUT;
	}
	if (state.capturing) {
		CommitCapture(state, bd, client_context);
	}
	return OperatorResultType::NEED_MORE_INPUT;
}

string PhysicalVgiLateralBatch::GetName() const {
	return "VGI_LATERAL_BATCH";
}

InsertionOrderPreservingMap<string> PhysicalVgiLateralBatch::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	auto &bd = bind_data->Cast<VgiTableInOutBindData>();
	result["Function"] = bd.function_name;
	result["Projected"] = std::to_string(projected_input.size());
	return result;
}

// ============================================================================
// OptimizerExtension — VgiLateralBatchRewriter
// ============================================================================

namespace {

bool IsBatchableLateralGet(LogicalGet &get) {
	if (!get.bind_data) {
		return false;
	}
	auto *bd = dynamic_cast<VgiTableInOutBindData *>(get.bind_data.get());
	return bd && bd->input_from_args && !bd->has_finalize && !bd->table_buffering &&
	       !get.projected_input.empty() && get.children.size() == 1;
}

void RewriteOne(unique_ptr<LogicalOperator> &op) {
	for (auto &child : op->children) {
		RewriteOne(child);
	}
	if (op->type != LogicalOperatorType::LOGICAL_GET) {
		return;
	}
	auto &get = op->Cast<LogicalGet>();
	if (!IsBatchableLateralGet(get)) {
		return;
	}
	const idx_t base_idx = get.types.size() - get.projected_input.size();
	auto all_bindings = get.GetColumnBindings();
	vector<ColumnBinding> worker_bindings(all_bindings.begin(), all_bindings.begin() + base_idx);
	vector<LogicalType> worker_output_types(get.types.begin(), get.types.begin() + base_idx);

	// Capture the worker-original column indices of the leading base_idx output
	// columns. When the function supports projection pushdown, DuckDB's
	// UNUSED_COLUMNS pass has narrowed get.column_ids to the referenced worker
	// columns (the InOut path uses column_ids, not projection_ids — see
	// plan_get.cpp), so column_ids.size() == base_idx and these are exactly the
	// columns to request from the worker. Without projection pushdown the worker
	// emits its full schema in order, so identity indices are used (and never sent).
	auto *bd = &get.bind_data->Cast<VgiTableInOutBindData>();
	const bool projection_pushdown = bd->projection_pushdown;
	auto col_ids = get.GetColumnIds();
	vector<column_t> worker_column_ids;
	worker_column_ids.reserve(base_idx);
	if (projection_pushdown && col_ids.size() == base_idx) {
		for (idx_t i = 0; i < base_idx; i++) {
			worker_column_ids.push_back(col_ids[i].GetPrimaryIndex());
		}
	} else {
		for (idx_t i = 0; i < base_idx; i++) {
			worker_column_ids.push_back(i);
		}
	}

	auto new_op = make_uniq<LogicalVgiLateralBatch>(get.table_index, std::move(get.projected_input),
	                                                std::move(worker_output_types), std::move(worker_bindings),
	                                                std::move(get.bind_data), std::move(worker_column_ids),
	                                                projection_pushdown);
	new_op->children.push_back(std::move(get.children[0]));
	new_op->ResolveOperatorTypes();
	op = std::move(new_op);
}

class VgiLateralBatchRewriter : public OptimizerExtension {
public:
	VgiLateralBatchRewriter() {
		optimize_function = Optimize;
	}

	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
		Value enabled;
		if (input.context.TryGetCurrentSetting("vgi_batch_lateral", enabled) && !enabled.GetValue<bool>()) {
			return; // rollback: leave the row-by-row PhysicalTableInOutFunction path
		}
		RewriteOne(plan);
	}
};

} // anonymous namespace

void RegisterVgiLateralBatchRewriter(DBConfig &config) {
	OptimizerExtension::Register(config, VgiLateralBatchRewriter());
}

} // namespace vgi
} // namespace duckdb
