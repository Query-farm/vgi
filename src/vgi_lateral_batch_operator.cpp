// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_lateral_batch_operator.hpp"

#include <atomic>
#include <cstring>
#include <limits>

#include <arrow/array/builder_primitive.h> // Int32Builder for the Take indices
#include <arrow/compute/api_vector.h>       // arrow::compute::Take (dedup expansion)
#include <arrow/record_batch.h>             // arrow::ConcatenateRecordBatches (per-value assembly)

#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_profiler.hpp"     // OperatorProfiler (EXPLAIN ANALYZE cache stats)
#include "duckdb/parallel/thread_context.hpp" // context.thread.profiler
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

struct VgiLateralBatchGlobalState : public GlobalOperatorState {
	// Shared across this operator's parallel Execute threads; reported post-execution
	// in EXPLAIN ANALYZE via VgiLateralBatchOperatorState::Finalize (which reads these
	// off op.op_state). The exchange cache here is PER-INPUT-CHUNK, so a single scan
	// legitimately mixes hits and misses — hence a rate, not a boolean.
	std::atomic<bool> cache_eligible {false};
	std::atomic<idx_t> cache_hits {0};    // chunks served without the worker (M2 or full per-value)
	std::atomic<idx_t> cache_misses {0};  // chunks that invoked the worker
	std::atomic<idx_t> cache_stores {0};  // entries committed (M2 whole-chunk + per-value)
};

struct VgiLateralBatchOperatorState : public OperatorState {
	VgiLateralBatchOperatorState(ClientContext &context, const std::vector<uint8_t> &substream_id,
	                             const VgiTableInOutBindData &bind_data)
	    : client_context(context), bind_data(&bind_data), substream_id(substream_id),
	      scan(make_uniq<ArrowArrayWrapper>(), context),
	      serve_scan(make_uniq<ArrowArrayWrapper>(), context) {
	}
	~VgiLateralBatchOperatorState() override {
		ShutdownConnection();
	}

	// Close the stream cleanly and hand the worker back to the pool. This used to
	// just drop the connection, on the theory that a worker left mid-stream can
	// never be reused — but the operator can END the stream, exactly as the scalar
	// path does (VgiScalarFunctionLocalState's destructor). Dropping it meant EVERY
	// lateral query burned a worker process: acquire, use, kill, and the next query
	// pays a fresh spawn. That showed up as a flat per-query cost independent of row
	// count — ~60ms against a Rust worker, ~530ms against Python — which dwarfed the
	// actual work (the same worker handles 1M rows through the scalar path in 18ms).
	// Safe here because the batched-lateral path only ever handles maps with no
	// finalize (see the operator's applicability check), so there is no finalize
	// handshake owed to the worker: closing input is a complete end-of-stream.
	void ShutdownConnection() noexcept {
		if (!connection) {
			return;
		}
		if (!connection->IsFinished()) {
			try {
				connection->CloseInputWriter();
				// Execute reads exactly one output batch per input chunk it writes, so
				// the wire is already drained and this should see EOS on the first read.
				// Bounded anyway: a worker that keeps emitting must not spin us forever.
				for (int i = 0; i < 64 && !connection->IsFinished(); i++) {
					if (!connection->ReadDataBatch()) {
						break;
					}
				}
			} catch (...) {
				connection.reset(); // never pool a worker that failed to close cleanly
				return;
			}
		}
		try {
			ReleaseSubstreamConnection(connection, *bind_data, client_context);
		} catch (...) {
			connection.reset();
		}
	}

	ClientContext &client_context; // for lazy schema build + (de)serialize
	// Owned by the PhysicalOperator, which outlives every operator state it hands out.
	const VgiTableInOutBindData *bind_data; // for use_pool() / worker_path() at shutdown
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
	// Latched `vgi.cache.per_value` advertisement. Per-value memoization is OFF until the
	// worker asks for it on an output batch; see the pv_enabled gate in Execute().
	bool cache_pv_opt_in = false;
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

	// Post-execution: publish this operator's shared per-chunk cache counters into
	// the EXPLAIN ANALYZE plan. Runs per pipeline thread; each reads the same
	// op.op_state aggregate and writes the same "Cache" string, so the profiler's
	// last-write-wins merge keeps one correct copy.
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override;
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
// memory-only cache entry (allow_disk=false). Clears the capture state. Returns
// true iff an entry was actually stored (for the EXPLAIN ANALYZE store counter).
bool CommitCapture(VgiLateralBatchOperatorState &state, const VgiTableInOutBindData &bd, ClientContext &ctx) {
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
	return sr.stored;
}

// Publish the shared per-chunk cache counters (M2 + per-value) into EXPLAIN ANALYZE.
// Only emits when the scan was cache-eligible (else the "misses" would just be
// every chunk with caching off — misleading). Reached via the pipeline executor's
// intermediate-operator finalize; op.op_state is the shared VgiLateralBatchGlobalState.
void VgiLateralBatchOperatorState::Finalize(const PhysicalOperator &op, ExecutionContext &context) {
	if (!op.op_state) {
		return;
	}
	auto &g = op.op_state->Cast<VgiLateralBatchGlobalState>();
	if (!g.cache_eligible.load(std::memory_order_relaxed)) {
		return; // caching not active for this scan → no Cache line
	}
	// Only when query profiling is on (EXPLAIN ANALYZE / enable_profiling). Writing to
	// this thread's OperatorProfiler is merged into the plan at the following Flush; if
	// profiling is off, Flush early-returns and the entry is discarded — so skip the work.
	if (!QueryProfiler::Get(context.client).IsEnabled()) {
		return;
	}
	auto &profiler = context.thread.profiler;
	const auto hits = g.cache_hits.load(std::memory_order_relaxed);
	const auto misses = g.cache_misses.load(std::memory_order_relaxed);
	const auto stores = g.cache_stores.load(std::memory_order_relaxed);
	const auto decided = hits + misses;
	std::string line = StringUtil::Format("%llu hit / %llu miss / %llu store",
	                                       static_cast<unsigned long long>(hits),
	                                       static_cast<unsigned long long>(misses),
	                                       static_cast<unsigned long long>(stores));
	if (decided > 0) {
		line += StringUtil::Format(" (%.0f%% hit)", 100.0 * static_cast<double>(hits) / static_cast<double>(decided));
	}
	profiler.GetOperatorInfo(op).extra_info["Cache"] = line;
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
	return make_uniq<VgiLateralBatchOperatorState>(context.client, MintSubstreamId(),
	                                               bind_data->Cast<VgiTableInOutBindData>());
}

OperatorResultType PhysicalVgiLateralBatch::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                    GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &state = state_p.Cast<VgiLateralBatchOperatorState>();
	auto &gstate = gstate_p.Cast<VgiLateralBatchGlobalState>(); // shared cache counters (EXPLAIN ANALYZE)
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
			gstate.cache_eligible.store(true, std::memory_order_relaxed); // arm EXPLAIN ANALYZE reporting
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
			if (CommitCapture(state, bd, client_context)) { // input chunk fully drained → commit
				gstate.cache_stores.fetch_add(1, std::memory_order_relaxed);
			}
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
				gstate.cache_hits.fetch_add(1, std::memory_order_relaxed); // whole-chunk (M2) hit
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
	// Build the dedup grouping whenever dedup OR per-value is in play (per-value needs the
	// distinct set even for an all-distinct chunk — a value unique in THIS chunk may repeat
	// in a later one). `reduce_worker`: gather only the distinct tuples for the worker (a
	// real reduction, only when there ARE duplicates); `needs_expand`: fan the 1:N output
	// back over each tuple's group (only when we reduced). Both false ⇒ trivial identity.
	bool pv_setting = true;
	{
		Value pv;
		if (client_context.TryGetCurrentSetting("vgi_result_cache_per_value", pv) && !pv.IsNull()) {
			pv_setting = pv.GetValue<bool>();
		}
	}
	// Dedup is the master switch for BOTH the worker-input reduction AND per-value memo
	// (per-value inherently needs the distinct set). `vgi_exchange_input_dedup=false`
	// disables both; per-value additionally needs its own flag + cache eligibility.
	const bool dedup_active = dedup_enabled && !is_volatile;
	InputDedup dd;
	if (dedup_active) {
		std::vector<column_t> wcols;
		wcols.reserve(input_length);
		for (idx_t c = 0; c < input_length; c++) {
			wcols.push_back(c);
		}
		dd = BuildInputDedup(input, wcols);
	}
	const bool reduce_worker = dedup_active && !dd.trivial;
	const bool needs_expand = reduce_worker;

	// Build the worker-input chunk: K deduped rows (materialized gather) when we reduce, else
	// an N-row view. When dedup_active, dd.k == cardinality of worker_input either way.
	DataChunk worker_input;
	vector<LogicalType> in_types;
	in_types.reserve(input_length);
	for (idx_t c = 0; c < input_length; c++) {
		in_types.push_back(input.data[c].GetType());
	}
	if (reduce_worker) {
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

	// [per-value] After dedup, memoize per DISTINCT worker-input tuple — the finer tier
	// UNDER the M2 per-chunk cache (which already missed here). Batch-look-up each distinct
	// tuple's key; a FULL hit (every tuple cached) serves without the worker, catching
	// cross-chunk / cross-query value reuse the whole-chunk key misses. Skipped while
	// revalidation is armed (the 304 flow owns this chunk). See docs/exchange_dedup_pervalue.md.
	//
	// WORKER OPT-IN (default OFF): the worker must advertise `vgi.cache.per_value` on an
	// output batch. Per-value memoization only pays when one worker call costs more than
	// one cache probe + decode + assembly; for a cheap map it is a large net loss, and the
	// engine cannot tell the two apart from the outside. `vgi_result_cache_per_value` is a
	// CEILING over the advertisement (it can veto, never enable).
	//
	// The advertisement rides an output batch, so it is only visible after an exchange.
	// `cache_pv_opt_in` is seeded from the process-wide registry (an earlier exchange
	// against this same function, in this or any previous query) and then latched from
	// this scan's own first exchange. This gates the PROBE only — the STORE below keys off
	// the live cache-control, so the very first exchange still memoizes what the worker
	// asked it to, and the next scan probes for it.
	if (!state.cache_pv_opt_in && state.cache_eligible) {
		state.cache_pv_opt_in = VgiResultCache::Instance().HasPerValueOptIn(state.cache_static_key);
	}
	const bool pv_enabled = pv_setting && state.cache_pv_opt_in && dedup_active && state.cache_eligible &&
	                        !reval_entry;
	std::vector<VgiResultCacheKey> pv_keys;
	std::vector<std::shared_ptr<const VgiResultCacheEntry>> pv_hits;
	bool full_pv_hit = false;
	if (pv_enabled) {
		auto pv_hashes = HashInputRowsPerValue(client_context, worker_input);
		pv_keys.resize(dd.k);
		for (idx_t d = 0; d < dd.k; d++) {
			pv_keys[d] = state.cache_static_key;
			pv_keys[d].input_hash = pv_hashes[d];
		}
		pv_hits = VgiResultCache::Instance().LookupBatch(pv_keys, std::chrono::steady_clock::now());
		full_pv_hit = dd.k > 0;
		for (auto &h : pv_hits) {
			if (!h) {
				full_pv_hit = false;
				break;
			}
		}
	}

	std::shared_ptr<arrow::RecordBatch> output_batch;
	std::vector<int32_t> worker_parent; // per worker-output-row: the DEDUPED tuple index

	if (full_pv_hit) {
		gstate.cache_hits.fetch_add(1, std::memory_order_relaxed); // full per-value hit (no worker)
		// Serve entirely from the per-value tier: concat each distinct tuple's cached output
		// rows and synthesize the parent array (each row → its tuple index d). No worker.
		std::vector<std::shared_ptr<arrow::RecordBatch>> parts;
		for (idx_t d = 0; d < dd.k; d++) {
			const auto &batches = pv_hits[d]->streams[0].batches;
			for (size_t b = 0; b < batches.size(); b++) {
				auto rb = DeserializeCachedRecordBatch(*pv_hits[d], batches[b]);
				for (int64_t r = 0; r < rb->num_rows(); r++) {
					worker_parent.push_back(static_cast<int32_t>(d));
				}
				parts.push_back(rb);
			}
		}
		int64_t served_bytes = 0;
		for (auto &h : pv_hits) {
			served_bytes += h->total_bytes;
		}
		VgiResultCache::Instance().RecordExchangeHit(served_bytes);
		// Latch cache-control off a served per-value entry when this operator state has not
		// yet seen an exchange. Without this, a scan that is a full per-value hit from its
		// very FIRST chunk (a warm memo + a fresh operator state) could never store the
		// coarse whole-chunk entry — cache_cc is normally latched off a worker reply, and
		// there is no worker reply on this path. The stored entry's own lifetime/validators
		// are exactly what the worker advertised when the value was memoized, so reusing
		// them keeps the M2 entry's freshness identical to the per-value entries it derives
		// from (never longer).
		if (!state.cache_cc_latched && !pv_hits.empty() && pv_hits[0]) {
			const auto &src = *pv_hits[0];
			VgiCacheControl cc;
			cc.present = true;
			cc.scope = src.scope;
			cc.etag = src.etag;
			cc.last_modified = src.last_modified;
			cc.revalidatable = src.revalidatable;
			auto lifetime =
			    std::chrono::duration_cast<std::chrono::seconds>(src.expires_at - src.stored_at).count();
			cc.ttl_seconds = src.never_expires ? VGI_CACHE_MAX_TTL_SECONDS : lifetime;
			// Reaching this branch means per-value memoization is already active for this
			// scan, which can only happen when the worker advertised the opt-in.
			cc.per_value = true;
			state.cache_cc = cc;
			state.cache_cc_latched = true;
		}
		VGI_LOG(client_context, "result_cache.hit",
		        {{"function", bd.function_name}, {"key_hash", state.capture_key.HexDigest()}, {"tier", "per_value"}});
		if (parts.empty()) {
			chunk.SetCardinality(0); // every distinct tuple cached an empty (all-1->0) output
			return OperatorResultType::NEED_MORE_INPUT;
		}
		auto cat = arrow::ConcatenateRecordBatches(parts);
		if (!cat.ok()) {
			throw IOException("vgi_batch_lateral: per-value assembly concat failed: %s", cat.status().ToString());
		}
		output_batch = cat.ValueUnsafe();
		if (output_batch->num_rows() == 0) {
			chunk.SetCardinality(0);
			return OperatorResultType::NEED_MORE_INPUT;
		}
	} else {
		if (state.cache_eligible) {
			gstate.cache_misses.fetch_add(1, std::memory_order_relaxed); // this chunk hit the worker
		}
		// [partial per-value] Ship ONLY the distinct tuples with no cached entry; the cached
		// ones are spliced back in below. So a chunk whose distinct set overlaps a prior
		// chunk/query recomputes just the NEW values (a full miss is the empty-cached-prefix
		// special case: miss_indices == the whole distinct set, cached prefix empty). pv off ⇒
		// ship the whole worker_input unchanged. `full_pv_hit` already handled the all-cached
		// case, so with pv on here miss_indices is guaranteed non-empty.
		std::vector<idx_t> miss_indices;
		if (pv_enabled) {
			for (idx_t d = 0; d < dd.k; d++) {
				if (!pv_hits[d]) {
					miss_indices.push_back(d);
				}
			}
		}
		DataChunk miss_input;
		idx_t shipped_rows;
		if (pv_enabled) {
			const idx_t m = miss_indices.size();
			SelectionVector sel(m == 0 ? 1 : m);
			for (idx_t j = 0; j < m; j++) {
				sel.set_index(j, miss_indices[j]);
			}
			miss_input.Initialize(Allocator::Get(client_context), in_types, m == 0 ? 1 : m);
			for (idx_t c = 0; c < input_length; c++) {
				VectorOperations::Copy(worker_input.data[c], miss_input.data[c], sel, m, 0, 0);
			}
			miss_input.SetCardinality(m);
			shipped_rows = m;
		} else {
			// Decode provenance against the full worker input: the distinct set (dd.k rows)
			// when dedup_active, else the full N-row chunk.
			shipped_rows = dedup_active ? dd.k : input.size();
		}
		DataChunk &ship = pv_enabled ? miss_input : worker_input;

		// ONE batched exchange for the (miss-subset, or whole deduped) input chunk.
		auto input_batch = ConvertInputToArrow(client_context, ship, bd);
		VGI_LOG(client_context, "table_in_out.write_input",
		        {{"conn", state.connection->GetConnIdHex()},
		         {"worker_path", bd.worker_path()},
		         {"function_name", bd.function_name},
		         {"input_rows", std::to_string(input_batch->num_rows())}});
		state.connection->WriteInputBatch(input_batch);
		output_batch = state.connection->ReadDataBatch();

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
			if (state.cache_cc.per_value) {
				// Arm this scan's later chunks AND every later scan of this function.
				state.cache_pv_opt_in = true;
				VgiResultCache::Instance().NotePerValueOptIn(state.cache_static_key);
			}
		}

		// Decode the FRESH worker output's provenance (sub-index into the shipped subset).
		// Guard the 0-row case: DecodeParentRow requires output_rows == input_rows when the
		// worker sends no provenance, which a whole-subset 1->0 (0 output rows, m>0 shipped)
		// can't satisfy — a 0-row fresh output simply contributes no rows.
		std::vector<int32_t> fresh_parent;
		if (output_batch->num_rows() > 0) {
			fresh_parent = DecodeParentRow(state.connection->GetLastParentRowBytes(),
			                               static_cast<idx_t>(output_batch->num_rows()), shipped_rows, bd);
		}

		if (pv_enabled) {
			// Splice: cached-hit rows (parent = the distinct index d) come first, then the fresh
			// rows (their sub-index remapped to the real distinct index via miss_indices). The
			// resulting output_batch + worker_parent look exactly like a full exchange, so the
			// store loop and the EXPAND below run unchanged. Full-miss ⇒ empty cached prefix.
			std::vector<std::shared_ptr<arrow::RecordBatch>> parts;
			worker_parent.clear();
			idx_t reused_tuples = 0;
			for (idx_t d = 0; d < dd.k; d++) {
				if (!pv_hits[d]) {
					continue;
				}
				++reused_tuples;
				for (const auto &b : pv_hits[d]->streams[0].batches) {
					auto rb = DeserializeCachedRecordBatch(*pv_hits[d], b);
					for (int64_t r = 0; r < rb->num_rows(); r++) {
						worker_parent.push_back(static_cast<int32_t>(d));
					}
					parts.push_back(std::move(rb));
				}
			}
			if (output_batch->num_rows() > 0) {
				for (auto p : fresh_parent) {
					worker_parent.push_back(static_cast<int32_t>(miss_indices[static_cast<idx_t>(p)]));
				}
				parts.push_back(output_batch);
			}
			if (parts.empty()) {
				output_batch = nullptr; // whole chunk 1->0 across every distinct tuple
			} else if (parts.size() == 1) {
				output_batch = std::move(parts[0]);
			} else {
				auto cat = arrow::ConcatenateRecordBatches(parts);
				if (!cat.ok()) {
					throw IOException("vgi_batch_lateral: partial per-value splice concat failed: %s",
					                  cat.status().ToString());
				}
				output_batch = cat.ValueUnsafe();
			}
			// A partial hit still ran the worker (for the misses), so it stays a MISS for the
			// hit/miss counters (recorded above); this event surfaces the worker-input reduction.
			if (reused_tuples > 0 && reused_tuples < dd.k) {
				VGI_LOG(client_context, "result_cache.partial_hit",
				        {{"function", bd.function_name},
				         {"key_hash", state.capture_key.HexDigest()},
				         {"reused_tuples", std::to_string(reused_tuples)},
				         {"computed_tuples", std::to_string(miss_indices.size())}});
			}
		} else {
			worker_parent = std::move(fresh_parent);
		}

		// Whole chunk produced nothing (all-filtered fresh + no cached rows, or a pv-off 1->0).
		if (!output_batch || output_batch->num_rows() == 0) {
			chunk.SetCardinality(0);
			return OperatorResultType::NEED_MORE_INPUT;
		}

		// [per-value store] Persist each MISSED distinct tuple's output rows (pre-stamp) as
		// its own per-value entry, so a future chunk with the same value serves without the
		// worker. Only when the worker opted into caching (cache_cc) AND asked for per-value
		// memoization. A tuple with 0 output rows (1->0) stores an empty batch (a valid
		// negative memo).
		//
		// Gated on the LIVE advertisement, not on `pv_enabled`: the probe needs to know the
		// opt-in before the exchange and so can be disarmed on a function's very first
		// exchange, but by the time we get here the worker has told us. Storing anyway is
		// what makes a single-chunk warm-up query (`... FROM range(25), LATERAL f(x)`)
		// populate the memo for the next query instead of silently doing nothing.
		const bool pv_store =
		    state.cache_cc.per_value && pv_setting && dedup_active && state.cache_eligible && !reval_entry;
		if (pv_store && !pv_enabled) {
			// Probe was disarmed for this chunk, so the keys were never built. Build them
			// now (over the same distinct set the probe would have used) and treat every
			// tuple as a miss — nothing was looked up, so nothing can be known-cached.
			auto pv_hashes = HashInputRowsPerValue(client_context, worker_input);
			pv_keys.resize(dd.k);
			for (idx_t d = 0; d < dd.k; d++) {
				pv_keys[d] = state.cache_static_key;
				pv_keys[d].input_hash = pv_hashes[d];
			}
		}
		if (pv_store && state.cache_cc.Cacheable()) {
			// Cap new stores per chunk to bound entry-count amplification on a
			// high-cardinality input (0 = unlimited). A store cap, not a lookup gate, so
			// low-cardinality (K < cap) stores everything and store-then-hit is preserved.
			uint64_t store_cap = 256;
			{
				Value scv;
				if (client_context.TryGetCurrentSetting("vgi_result_cache_per_value_max_stores_per_chunk", scv) &&
				    !scv.IsNull()) {
					store_cap = scv.GetValue<uint64_t>();
				}
			}
			uint64_t stored = 0;
			std::vector<std::vector<int32_t>> rows_by_d(dd.k);
			for (idx_t m = 0; m < worker_parent.size(); m++) {
				rows_by_d[static_cast<idx_t>(worker_parent[m])].push_back(static_cast<int32_t>(m));
			}
			for (idx_t d = 0; d < dd.k; d++) {
				if (!pv_hits.empty() && pv_hits[d]) {
					continue; // already cached (partial-hit chunk) — don't re-store
				}
				if (store_cap != 0 && stored >= store_cap) {
					break; // per-chunk store cap reached — leave the rest to recompute
				}
				std::shared_ptr<arrow::RecordBatch> rd = rows_by_d[d].empty()
				                                             ? output_batch->Slice(0, 0)
				                                             : TakeRecordBatch(output_batch, rows_by_d[d], bd);
				auto sr = StoreExchangeMemoEntry(pv_keys[d], state.cache_cc, state.cache_catalog_name,
				                                 state.cache_default_ttl_seconds,
				                                 std::vector<std::shared_ptr<arrow::RecordBatch>>{rd},
				                                 /*allow_disk=*/true);
				if (sr.stored) {
					VgiResultCache::Instance().RecordExchangeStore();
					gstate.cache_stores.fetch_add(1, std::memory_order_relaxed); // per-value entry stored
				}
				stored++; // count attempts toward the cap (a rejected store still cost work)
			}
		}
	}

	// EXPAND (common to the full-hit and miss paths): each worker output row (parent d)
	// fans out to every ORIGINAL row in that tuple's group, and the worker output row is
	// replicated (Arrow Take), so the existing drain+stamp path runs unchanged over an
	// expanded batch whose parent_index points at ORIGINAL rows (each stamped with its OWN
	// outer columns). Only when we reduced (duplicates present); for a trivial/all-distinct
	// chunk each distinct tuple IS one original row, so parent_index = worker_parent directly.
	if (needs_expand) {
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

	// Begin capturing this input chunk's POST-STAMP output for the M2 per-chunk cache (if
	// the worker opted in). ALWAYS store it when eligible — including on a full per-value
	// hit. The two tiers are not redundant: an M2 serve is ONE decode per input chunk,
	// while a per-value serve of the same chunk is K decodes + a K-way assembly. Measured
	// on the `cached_double` fixture, an M2 whole-chunk replay is ~14x cheaper than the
	// per-value reassembly of the identical rows. Suppressing the coarse entry because
	// per-value "already covers" the chunk therefore made the warm path dramatically
	// SLOWER, not cheaper (it was the cause of the 10x `full`-arm cliff). Per-value's job
	// is cross-CHUNK / cross-QUERY value reuse that the whole-chunk key cannot see; it is
	// never a substitute for the whole-chunk entry on an identical-chunk replay.
	bool m2_capture = state.cache_eligible && state.cache_cc.Cacheable();
	if (m2_capture) {
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
		if (CommitCapture(state, bd, client_context)) {
			gstate.cache_stores.fetch_add(1, std::memory_order_relaxed);
		}
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
