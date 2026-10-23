// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_settings_defaults.hpp"
#include "vgi_lateral_batch_operator.hpp"
#include "vgi_batch_validation.hpp"

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
#include "vgi_exception.hpp"
#include "vgi_exchange_cache_key.hpp" // exchange-mode result cache (M2)
#include "vgi_ifunction_connection.hpp"
#include "vgi_input_dedup.hpp"        // input dedup (ship distinct tuples; expand back)
#include "vgi_logging.hpp"
#include "vgi_memo_arena.hpp"         // per-value memo arena (columnar store)
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

// extra_info for a worker that broke the batched-LATERAL contract.
ErrorInfo LateralViolation(const VgiTableInOutBindData &bd) {
	return std::move(ErrorInfo(error_subtype::kProtocolViolation)
	                     .Function(bd.function_name, "TABLE_IN_OUT")
	                     .Worker(bd.worker_path()));
}

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

// One input chunk's worker exchanges (a MISS). The chunk's worker input — the rows it
// ships, after dedup and per-value hits — goes out in calls of at most
// input_rows_per_call rows, and each call's output may continue across several replies
// (vgi.status: HAVE_MORE_OUTPUT). Execute returns to DuckDB between replies, so
// everything needed to map a later reply back to the chunk's rows lives here.
struct ChunkExchange {
	bool active = false;
	// The shipped worker-input rows, and how many go in each call.
	std::shared_ptr<arrow::RecordBatch> ship;
	idx_t call_rows = 0;
	// The current call's rows [call_start, call_start + call_len) of `ship`, and the
	// first row no call has sent yet.
	idx_t call_start = 0;
	idx_t call_len = 0;
	idx_t next_call = 0;
	// The last reply said HAVE_MORE_OUTPUT: the next exchange is a continuation tick.
	bool worker_more = false;
	// Some reply of this chunk was continued. Its output is then never cached.
	bool continued = false;
	// ship row -> distinct-tuple index, when only the per-value misses were shipped.
	// Empty = identity (a ship row IS a distinct tuple, or an original row).
	std::vector<idx_t> ship_to_distinct;
	// Dedup grouping, for expanding each reply back over the original rows.
	InputDedup dd;
	bool needs_expand = false;
	// Per-value cache hits, emitted ahead of the first worker reply.
	std::shared_ptr<arrow::RecordBatch> cached;
	std::vector<int32_t> cached_parent;
	// Conditional revalidation, armed only when the chunk is a single call.
	std::shared_ptr<const VgiResultCacheEntry> reval_entry;
	// Per-value store: possible for this chunk, the sort-key blob per distinct tuple,
	// the hit mask, and how many stores this chunk has made (against the cap).
	bool pv_store_possible = false;
	std::vector<std::string> pv_blobs;
	std::vector<char> pv_hit;
	uint64_t pv_store_cap = 0;
	uint64_t pv_stored = 0;
	bool miss_recorded = false;
	bool capture_decided = false;

	idx_t ShipRows() const {
		return ship ? static_cast<idx_t>(ship->num_rows()) : 0;
	}
	bool HasMoreReplies() const {
		return active && (cached || worker_more || next_call < ShipRows());
	}
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
				// Execute reads one reply per input or tick it writes, so the wire is
				// already drained and this should see EOS on the first read — also when
				// a LIMIT stopped the query with output still pending: the worker drops
				// it when its input ends. Bounded anyway: a worker that keeps emitting
				// must not spin us forever.
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

	// The current input chunk's worker exchanges, while replies remain.
	ChunkExchange ex;

	// ---- Exchange-mode result cache (M2, per-input-chunk memoization) ----
	// Built once on the first chunk: the static key + whether caching is eligible.
	bool cache_key_built = false;
	bool cache_eligible = false;
	VgiResultCacheKey cache_static_key;
	std::string cache_catalog_name;
	// SHA-256 fingerprint of cache_static_key (input_hash empty) — the per-value arena
	// registry key. Computed once when the static key is built.
	std::string cache_static_fp;
	int64_t cache_default_ttl_seconds = 0;
	int64_t cache_revalidate_min_bytes = defaults::RESULT_CACHE_REVALIDATE_MIN_BYTES;
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
			    LateralViolation(bd)
			        .Set(error_key::kMetadataKey, "vgi_rpc.parent_row#b64")
			        .ExpectedActual(std::to_string(input_rows) + " rows", std::to_string(output_rows) + " rows"),
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
		throw IOException(LateralViolation(bd).Set(error_key::kActual, std::to_string(output_rows) + " rows"),
		                  "vgi_batch_lateral: implausible output row count %llu from worker '%s' function '%s'",
		                  (unsigned long long)output_rows, bd.worker_path(), bd.function_name);
	}
	if (raw.size() != output_rows * sizeof(int32_t)) {
		throw IOException(LateralViolation(bd)
		                      .Set(error_key::kMetadataKey, "vgi_rpc.parent_row#b64")
		                      .ExpectedActual(std::to_string(output_rows * sizeof(int32_t)) + " bytes",
		                                      std::to_string(raw.size()) + " bytes"),
		                  "vgi_batch_lateral: vgi_rpc.parent_row is %llu bytes for %llu output rows "
		                  "(expected %llu); worker '%s' function '%s'",
		                  (unsigned long long)raw.size(), (unsigned long long)output_rows,
		                  (unsigned long long)(output_rows * sizeof(int32_t)), bd.worker_path(), bd.function_name);
	}
	result.resize(output_rows);
	std::memcpy(result.data(), raw.data(), raw.size()); // LE int32[] — DuckDB targets are little-endian
	for (idx_t i = 0; i < output_rows; i++) {
		if (result[i] < 0 || static_cast<idx_t>(result[i]) >= input_rows) {
			throw IOException(LateralViolation(bd)
			                      .Set(error_key::kMetadataKey, "vgi_rpc.parent_row#b64")
			                      .ExpectedActual("[0, " + std::to_string(input_rows) + ")", std::to_string(result[i])),
			                  "vgi_batch_lateral: parent_row[%llu] = %d is out of range [0, %llu); "
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
	// NoBoundsCheck: `take` holds worker-output row positions built from worker_parent /
	// rows_by_d, always in [0, batch->num_rows()), so the kernel's bounds pass is wasted.
	auto res = arrow::compute::Take(arrow::Datum(batch), arrow::Datum(indices),
	                                arrow::compute::TakeOptions::NoBoundsCheck());
	if (!res.ok()) {
		throw IOException(ErrorInfo().Function(bd.function_name, "TABLE_IN_OUT").Worker(bd.worker_path()),
		                  "vgi_batch_lateral: worker '%s' function '%s' output Take (dedup expansion) failed: %s",
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
	auto sr = StoreExchangeMemoEntry(GetResultCache(ctx), state.capture_key, state.cache_cc, state.cache_catalog_name,
	                                 state.cache_default_ttl_seconds, state.capture_pending,
	                                 /*allow_disk=*/true);
	if (sr.stored) {
		GetResultCache(ctx).RecordExchangeStore();
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
		// operator_kind carries a fingerprint of the WORKER output schema (types[0..base_idx],
		// the worker columns before the projected outer columns) so a worker that changes its
		// output shape without bumping a version gets a DIFFERENT key — old-shaped cached bytes
		// can never be served into the new shape (critical across a disk restart). See shape_key.
		std::string op_kind = "lateral";
		for (idx_t c = 0; c < base_idx; c++) {
			op_kind += '\x1f';
			op_kind += types[c].ToString();
		}
		if (BuildExchangeCacheKeyStatic(client_context, bd, proj_key, state.cache_static_key,
		                                state.cache_catalog_name, catalog_version, reason, op_kind)) {
			state.cache_eligible = true;
			state.cache_static_fp = state.cache_static_key.Fingerprint();
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

	// End the current input chunk: commit its captured output (only when no reply was
	// continued — continued output is never cached) and clear its exchange state.
	auto finish_chunk = [&]() -> OperatorResultType {
		if (state.capturing && !state.ex.continued) {
			if (CommitCapture(state, bd, client_context)) {
				gstate.cache_stores.fetch_add(1, std::memory_order_relaxed);
			}
		}
		state.capturing = false;
		state.capture_pending.clear();
		state.ex = ChunkExchange();
		return OperatorResultType::NEED_MORE_INPUT;
	};

	// [per-value store] Persist each distinct tuple of the CURRENT call as its own
	// per-value entry (pre-stamp), so a future chunk with the same value serves without
	// the worker. Only for a call answered by one reply: that reply then holds each of
	// the call's tuples' whole output. A tuple with 0 output rows stores an empty batch
	// (a valid negative memo). `call_parent` is each output row's call-local input row.
	//
	// Gated on the LIVE advertisement (the worker asked for per-value memoization), not
	// on the probe: the probe needs the opt-in before the exchange and so is disarmed on
	// a function's very first exchange, but by now the worker has told us. Storing anyway
	// is what makes a single-chunk warm-up query populate the memo for the next query.
	auto store_per_value = [&](const std::shared_ptr<arrow::RecordBatch> &batch,
	                           const std::vector<int32_t> &call_parent) {
		auto &ex = state.ex;
		if (!ex.pv_store_possible || !state.cache_cc.per_value || !state.cache_cc.Cacheable() ||
		    ex.pv_blobs.empty()) {
			return;
		}
		std::vector<std::vector<int32_t>> rows_by_row(ex.call_len);
		for (idx_t m = 0; m < call_parent.size(); m++) {
			rows_by_row[static_cast<idx_t>(call_parent[m])].push_back(static_cast<int32_t>(m));
		}
		std::vector<int32_t> take_idx;
		std::vector<ArenaStoreSpec> specs;
		for (idx_t j = 0; j < ex.call_len; j++) {
			const idx_t ship_row = ex.call_start + j;
			const idx_t d = ex.ship_to_distinct.empty() ? ship_row : ex.ship_to_distinct[ship_row];
			if (!ex.pv_hit.empty() && ex.pv_hit[d]) {
				continue; // already cached (partial-hit chunk) — don't re-store
			}
			if (ex.pv_store_cap != 0 && ex.pv_stored >= ex.pv_store_cap) {
				break; // per-chunk store cap reached — leave the rest to recompute
			}
			for (int32_t r : rows_by_row[j]) {
				take_idx.push_back(r);
			}
			ArenaStoreSpec sp;
			sp.key = &ex.pv_blobs[d];
			sp.length = static_cast<int32_t>(rows_by_row[j].size());
			specs.push_back(sp);
			ex.pv_stored++;
		}
		if (specs.empty()) {
			return;
		}
		int64_t ttl = state.cache_cc.ttl_seconds.value_or(state.cache_default_ttl_seconds);
		if (ttl > VGI_CACHE_MAX_TTL_SECONDS) {
			ttl = VGI_CACHE_MAX_TTL_SECONDS;
		}
		auto expires = std::chrono::steady_clock::now() + std::chrono::seconds(ttl);
		auto arena = GetMemoArenaRegistry(client_context).GetOrCreate(state.cache_static_fp, batch->schema());
		if (!arena) {
			return;
		}
		ArenaValidator av;
		av.scope = state.cache_cc.scope;
		av.etag = state.cache_cc.etag;
		av.last_modified = state.cache_cc.last_modified;
		av.revalidatable = state.cache_cc.revalidatable;
		const int32_t vref = arena->ResolveValidator(av);
		for (auto &sp : specs) {
			sp.validator_ref = vref;
		}
		std::shared_ptr<arrow::RecordBatch> store_batch =
		    take_idx.empty() ? batch->Slice(0, 0) : TakeRecordBatch(batch, take_idx, bd);
		const int64_t delta = arena->Store(store_batch, specs, expires, /*never_expires=*/false);
		GetMemoArenaRegistry(client_context).NoteFootprintDelta(state.cache_static_fp, delta, arena.get());
		for (size_t j = 0; j < specs.size(); j++) {
			GetResultCache(client_context).RecordExchangeStore();
			gstate.cache_stores.fetch_add(1, std::memory_order_relaxed);
		}
	};

	// Produce the current chunk's next non-empty reply into `chunk`: its per-value
	// cached rows first, then each worker reply — a continuation of the current call
	// while the worker says it has more, else the chunk's next call. Ends the chunk
	// when none remain.
	auto emit_next_reply = [&]() -> OperatorResultType {
		auto &ex = state.ex;
		while (true) {
			std::shared_ptr<arrow::RecordBatch> batch;
			std::vector<int32_t> parent; // per output row: its distinct tuple (or original row)
			if (ex.cached) {
				batch = std::move(ex.cached);
				parent = std::move(ex.cached_parent);
				ex.cached.reset();
			} else if (ex.worker_more || ex.next_call < ex.ShipRows()) {
				const bool tick = ex.worker_more;
				if (tick) {
					VGI_LOG(client_context, "table_in_out.continue",
					        {{"conn", state.connection->GetConnIdHex()}, {"function_name", bd.function_name}});
					state.connection->WriteContinuationTick();
				} else {
					ex.call_start = ex.next_call;
					ex.call_len = MinValue<idx_t>(ex.call_rows, ex.ShipRows() - ex.call_start);
					ex.next_call += ex.call_len;
					auto call_input = ex.call_len == ex.ShipRows()
					                      ? ex.ship
					                      : ex.ship->Slice(static_cast<int64_t>(ex.call_start),
					                                       static_cast<int64_t>(ex.call_len));
					// Validators ride this unit's input batch (they were armed only for a
					// single-call chunk) and are cleared right after the exchange.
					if (ex.reval_entry) {
						state.connection->SetConditionalRequest(ex.reval_entry->etag, ex.reval_entry->last_modified);
					}
					VGI_LOG(client_context, "table_in_out.write_input",
					        {{"conn", state.connection->GetConnIdHex()},
					         {"worker_path", bd.worker_path()},
					         {"function_name", bd.function_name},
					         {"input_rows", std::to_string(call_input->num_rows())}});
					state.connection->WriteInputBatch(call_input);
				}
				batch = state.connection->ReadDataBatch();
				if (!batch) {
					// EOS mid-stream is a worker protocol violation: this operator never closes
					// the input writer (a map keeps exchanging), so the worker must answer every
					// input chunk and tick with a data batch. A silent FINISHED here would drop
					// the rest of the input; surface it. Drop the connection (never pool a
					// mid-stream worker).
					state.connection.reset();
					throw IOException(LateralViolation(bd).ExpectedActual("1 output batch per input chunk", "end-of-stream"),
					                  "vgi_batch_lateral: worker '%s' function '%s' returned end-of-stream "
					                  "mid-exchange (expected one output batch per input chunk)",
					                  bd.worker_path(), bd.function_name);
				}
				ex.worker_more = state.connection->GetLastMoreOutput();
				// A 304 stands for the chunk's whole stored output, so it can only be a
				// call's first and only reply: never one that says more, nor a tick's.
				if ((tick || ex.worker_more) && state.connection->GetLastCacheControl().not_modified) {
					state.connection.reset();
					throw IOException(LateralViolation(bd).ExpectedActual("not_modified only on an input's only reply",
					                                                      "not_modified on a continued reply"),
					                  "vgi_batch_lateral: worker '%s' function '%s' answered not_modified on a "
					                  "continued reply; a 304 must be an input's only reply",
					                  bd.worker_path(), bd.function_name);
				}

				// Conditional revalidation: clear the armed validators (so the next exchange
				// isn't a stray conditional request) and, on a 304 (0-row not_modified reply),
				// slide the stored entry's TTL and serve its cached POST-STAMP output instead of
				// the worker response. Otherwise the fresh response falls through to capture.
				if (ex.reval_entry) {
					state.connection->SetConditionalRequest("", "");
					auto reval_entry = std::move(ex.reval_entry);
					ex.reval_entry.reset();
					if (!ex.worker_more && batch->num_rows() == 0) {
						auto cc = state.connection->GetLastCacheControl();
						if (cc.not_modified) {
							SlideRevalidatedExchangeEntry(GetResultCache(client_context), *reval_entry, cc,
							                              state.cache_default_ttl_seconds, /*allow_disk=*/true);
							GetResultCache(client_context).RecordExchangeRevalidation(reval_entry->total_bytes);
							VGI_LOG(client_context, "result_cache.revalidate",
							        {{"function", bd.function_name},
							         {"key_hash", state.capture_key.HexDigest()},
							         {"outcome", "not_modified"}});
							state.ex = ChunkExchange();
							state.serving = reval_entry;
							state.serve_cursor = 0;
							return EmitServedSlice(state, state.serve_arrow_table, chunk);
						}
					}
				}

				// Reaching here = a fresh exchange for this chunk (not a cache hit, not a 304).
				if (state.cache_eligible && !ex.miss_recorded) {
					GetResultCache(client_context).RecordExchangeMiss();
					ex.miss_recorded = true;
				}
				// Latch the worker's cache-control advertisement off the first exchange output.
				if (state.cache_eligible && !state.cache_cc_latched) {
					state.cache_cc = state.connection->GetLastCacheControl();
					state.cache_cc_latched = true;
					if (state.cache_cc.per_value) {
						// Arm this scan's later chunks AND every later scan of this function.
						state.cache_pv_opt_in = true;
						GetResultCache(client_context).NotePerValueOptIn(state.cache_static_key);
					}
				}
				if (tick || ex.worker_more) {
					ex.continued = true; // continued output is never cached
					state.capturing = false;
					state.capture_pending.clear();
				}

				// Decode the reply's provenance against its call's rows, then map each call
				// row to its distinct tuple. Guard the 0-row case: DecodeParentRow requires
				// output_rows == input_rows when the worker sends no provenance, which a
				// whole-call 1->0 can't satisfy — a 0-row reply simply contributes no rows.
				std::vector<int32_t> call_parent;
				if (batch->num_rows() > 0) {
					call_parent = DecodeParentRow(state.connection->GetLastParentRowBytes(),
					                              static_cast<idx_t>(batch->num_rows()), ex.call_len, bd);
				}
				if (!tick && !ex.worker_more) {
					store_per_value(batch, call_parent);
				}
				parent.reserve(call_parent.size());
				for (auto p : call_parent) {
					const idx_t ship_row = ex.call_start + static_cast<idx_t>(p);
					parent.push_back(static_cast<int32_t>(ex.ship_to_distinct.empty() ? ship_row
					                                                                  : ex.ship_to_distinct[ship_row]));
				}
			} else {
				chunk.SetCardinality(0); // the chunk's remaining replies were all empty
				return finish_chunk();
			}
			if (batch->num_rows() == 0) {
				// An empty reply that more follow: return it as an empty HAVE_MORE_OUTPUT so
				// DuckDB (which checks for interrupts) drives the next tick, rather than ticking
				// here for as long as the worker keeps answering empty. The chunk's other calls
				// are bounded, so those keep looping.
				if (ex.worker_more) {
					chunk.SetCardinality(0);
					return OperatorResultType::HAVE_MORE_OUTPUT;
				}
				continue;
			}

			// Begin capturing the chunk's POST-STAMP output for the M2 per-chunk cache (if
			// the worker opted in). ALWAYS store it when eligible — including on a full
			// per-value hit. The two tiers are not redundant: an M2 serve is ONE decode per
			// input chunk, while a per-value serve of the same chunk is K decodes + a K-way
			// assembly (measured ~14x slower on `cached_double`). Per-value's job is
			// cross-CHUNK / cross-QUERY value reuse the whole-chunk key cannot see.
			if (!ex.capture_decided) {
				ex.capture_decided = true;
				state.capturing = state.cache_eligible && state.cache_cc.Cacheable() && !ex.continued;
				state.capture_pending.clear();
			}

			// EXPAND: each output row (parent d) fans out to every ORIGINAL row in that
			// tuple's group, and the output row is replicated (Arrow Take), so the drain +
			// stamp path runs unchanged over an expanded batch whose parent_index points at
			// ORIGINAL rows (each stamped with its OWN outer columns). Only when we reduced
			// (duplicates present); otherwise each distinct tuple IS one original row.
			if (ex.needs_expand) {
				std::vector<int32_t> take_idx;
				std::vector<int32_t> expanded;
				take_idx.reserve(parent.size());
				expanded.reserve(parent.size());
				for (idx_t m = 0; m < parent.size(); m++) {
					const auto grp = ex.dd.Group(static_cast<idx_t>(parent[m]));
					for (idx_t o : grp) {
						take_idx.push_back(static_cast<int32_t>(m));
						expanded.push_back(static_cast<int32_t>(o));
					}
				}
				batch = TakeRecordBatch(batch, take_idx, bd);
				state.parent_index = std::move(expanded);
			} else {
				state.parent_index = std::move(parent);
			}
			state.input_size_at_decode = input.size();

			// Type-confusion guard: the output batch's types must match what the worker
			// declared at bind (ArrowToDuckDB below reads buffers by the bind-time type).
			// Expected types come from bd.output_schema projected by the worker's column
			// ids. See vgi_batch_validation.hpp.
			if (!state.connection || !state.connection->IsCachedReplay()) {
				std::vector<int32_t> proj;
				if (projection_pushdown) {
					proj.reserve(worker_column_ids.size());
					for (auto c : worker_column_ids) {
						proj.push_back(static_cast<int32_t>(c));
					}
				}
				vgi::ValidateProjectedWireBatch(&client_context, *batch, bd.output_schema, proj, bd.worker_path(),
				                                bd.function_name);
			}

			LoadBatchIntoScanState(state.scan, batch);
			idx_t start = state.scan.chunk_offset; // 0 for a freshly loaded batch
			idx_t produced = ProduceOutputFromBatch(state.scan, bd.arrow_table, chunk, projection_pushdown);
			StampProjected(chunk, input, state.parent_index, projected_input, base_idx, start, produced);
			chunk.SetCardinality(produced);
			if (state.capturing) {
				state.capture_pending.push_back(DataChunkToArrow(client_context, chunk, state.serve_schema));
			}
			if (HasRemainingBatchData(state.scan) || ex.HasMoreReplies()) {
				return OperatorResultType::HAVE_MORE_OUTPUT;
			}
			return finish_chunk();
		}
	};

	// (A) Drain the remaining slices of the reply already loaded in `scan`. DuckDB
	// re-passes the SAME `input` chunk unchanged across HAVE_MORE_OUTPUT, so the held
	// parent_index still refers to the right rows.
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
		if (HasRemainingBatchData(state.scan) || state.ex.HasMoreReplies()) {
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}
		return finish_chunk();
	}

	// (C) The current chunk has more replies: a continuation of its current call, or
	// its next call.
	if (state.ex.HasMoreReplies()) {
		return emit_next_reply();
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
		auto reval = GetResultCache(client_context).LookupForRevalidation(state.capture_key, now);
		if (reval && reval->revalidatable && !reval->streams.empty() &&
		    reval->total_bytes >= state.cache_revalidate_min_bytes) {
			reval_entry = reval;
		} else {
			auto entry = GetResultCache(client_context).Lookup(state.capture_key, now);
			if (entry) {
				GetResultCache(client_context).RecordExchangeHit(entry->total_bytes);
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

	// [dedup] Ship only the DISTINCT worker-input tuples to the worker (unless disabled,
	// trivial, or the map is VOLATILE — a volatile map's per-row output varies for the
	// same input, so duplicate tuples must NOT be collapsed). Each reply is expanded back
	// over each tuple's original-row group (in emit_next_reply).
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
	// real reduction, only when there ARE duplicates); `needs_expand`: fan each reply
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
	// this scan's own first exchange. This gates the PROBE only — the STORE keys off the
	// live cache-control, so the very first exchange still memoizes what the worker
	// asked it to, and the next scan probes for it.
	if (!state.cache_pv_opt_in && state.cache_eligible) {
		state.cache_pv_opt_in = GetResultCache(client_context).HasPerValueOptIn(state.cache_static_key);
	}
	const bool pv_enabled = pv_setting && state.cache_pv_opt_in && dedup_active && state.cache_eligible &&
	                        !reval_entry;
	// Per-value memo probe against the columnar arena. pv_blobs is the raw sort-key blob
	// per distinct tuple (the arena slot key). Probe returns the cached rows (hits, ascending
	// distinct order, 1:N expanded) with a parent array (distinct index per row), a per-tuple
	// hit mask, and the aggregates for the M2 cc-latch.
	std::vector<std::string> pv_blobs;
	std::vector<char> pv_hit;
	std::shared_ptr<arrow::RecordBatch> pv_cached;
	std::vector<int32_t> pv_cached_parent; // distinct index d per pv_cached row
	int64_t pv_hit_count = 0;
	ArenaProbeResult pv_probe; // carries served_bytes + latch aggregates
	if (pv_enabled) {
		pv_blobs = InputRowSortKeys(client_context, worker_input);
		if (auto arena = GetMemoArenaRegistry(client_context).Get(state.cache_static_fp)) {
			pv_probe = arena->Probe(pv_blobs, std::chrono::steady_clock::now());
			pv_hit = std::move(pv_probe.hit);
			pv_cached = pv_probe.rows;
			pv_cached_parent = std::move(pv_probe.parent);
			pv_hit_count = pv_probe.num_hits;
		}
	}
	if (pv_hit.empty()) {
		pv_hit.assign(dd.k, 0);
	}
	const bool full_pv_hit = pv_enabled && dd.k > 0 && pv_hit_count == static_cast<int64_t>(dd.k);

	// Set up this chunk's exchanges; emit_next_reply runs them.
	auto &ex = state.ex;
	ex = ChunkExchange();
	ex.active = true;
	ex.dd = std::move(dd);
	ex.needs_expand = needs_expand;
	state.capturing = false;
	state.capture_pending.clear();

	if (full_pv_hit) {
		gstate.cache_hits.fetch_add(1, std::memory_order_relaxed); // full per-value hit (no worker)
		// Serve entirely from the arena: it already returned all rows (1:N expanded) with the
		// parent array (distinct index d per row). No worker.
		if (pv_probe.served_bytes) {
			GetResultCache(client_context).RecordExchangeHit(pv_probe.served_bytes);
		}
		// Latch cache-control off the served per-value slots for the coarse whole-chunk (M2)
		// store, when this operator state has not yet seen an exchange (cache_cc is normally
		// latched off a worker reply, and there is no worker reply on this path). B1: min
		// REMAINING lifetime across all hit slots. B2: validators only if all hit slots agree,
		// else freshness-only.
		if (!state.cache_cc_latched && pv_hit_count > 0) {
			VgiCacheControl cc;
			cc.present = true;
			cc.per_value = true; // reaching here means the worker advertised the opt-in
			cc.ttl_seconds =
			    pv_probe.latch_all_never_expires ? VGI_CACHE_MAX_TTL_SECONDS : pv_probe.latch_min_remaining_s;
			if (pv_probe.latch_validators_agree && pv_probe.latch_validator_ref >= 0) {
				if (auto arena = GetMemoArenaRegistry(client_context).Get(state.cache_static_fp)) {
					auto v = arena->GetValidator(pv_probe.latch_validator_ref);
					cc.scope = v.scope.empty() ? std::string(VGI_CACHE_SCOPE_CATALOG) : v.scope;
					cc.etag = v.etag;
					cc.last_modified = v.last_modified;
					cc.revalidatable = v.revalidatable;
				} else {
					cc.scope = VGI_CACHE_SCOPE_CATALOG;
				}
			} else {
				cc.scope = VGI_CACHE_SCOPE_CATALOG;
				cc.revalidatable = false; // freshness-only fallback (validators disagree)
			}
			state.cache_cc = cc;
			state.cache_cc_latched = true;
		}
		VGI_LOG(client_context, "result_cache.hit",
		        {{"function", bd.function_name}, {"key_hash", state.capture_key.HexDigest()}, {"tier", "per_value"}});
		if (pv_cached && pv_cached->num_rows() > 0) {
			ex.cached = std::move(pv_cached);
			ex.cached_parent = std::move(pv_cached_parent);
		}
		return emit_next_reply();
	}

	if (state.cache_eligible) {
		gstate.cache_misses.fetch_add(1, std::memory_order_relaxed); // this chunk hit the worker
	}
	// [partial per-value] Ship ONLY the distinct tuples with no cached entry; the cached
	// ones are emitted first, ahead of the worker's replies. So a chunk whose distinct set
	// overlaps a prior chunk/query recomputes just the NEW values (a full miss is the
	// empty-cached-prefix special case: miss_indices == the whole distinct set). pv off ⇒
	// ship the whole worker_input unchanged. `full_pv_hit` already handled the all-cached
	// case, so with pv on here miss_indices is guaranteed non-empty.
	std::vector<idx_t> miss_indices;
	if (pv_enabled) {
		for (idx_t d = 0; d < ex.dd.k; d++) {
			if (!pv_hit[d]) {
				miss_indices.push_back(d);
			}
		}
	}
	DataChunk miss_input;
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
	}
	DataChunk &ship = pv_enabled ? miss_input : worker_input;
	ex.ship = ConvertInputToArrow(client_context, ship, bd);
	// The function's declared input_rows_per_call splits the shipped rows into calls.
	ex.call_rows = ex.ShipRows();
	if (bd.input_rows_per_call > 0 && bd.input_rows_per_call < ex.call_rows) {
		ex.call_rows = bd.input_rows_per_call;
	}
	if (pv_enabled) {
		ex.ship_to_distinct = std::move(miss_indices);
		if (pv_cached && pv_cached->num_rows() > 0) {
			ex.cached = std::move(pv_cached);
			ex.cached_parent = std::move(pv_cached_parent);
		}
		// A partial hit still runs the worker (for the misses), so it stays a MISS for the
		// hit/miss counters (recorded above); this event surfaces the worker-input reduction.
		if (pv_hit_count > 0 && pv_hit_count < static_cast<int64_t>(ex.dd.k)) {
			VGI_LOG(client_context, "result_cache.partial_hit",
			        {{"function", bd.function_name},
			         {"key_hash", state.capture_key.HexDigest()},
			         {"reused_tuples", std::to_string(pv_hit_count)},
			         {"computed_tuples", std::to_string(ex.ship_to_distinct.size())}});
		}
	}
	// Conditional revalidation asks the worker about the chunk's whole output, so it
	// applies only when the chunk goes out as one call.
	if (reval_entry && ex.call_rows == ex.ShipRows()) {
		ex.reval_entry = std::move(reval_entry);
	}
	// Per-value store: the blobs key each distinct tuple. Built over the distinct set
	// now (worker_input is gone by the time a later reply arrives), unless this scan
	// already knows the worker did not ask for per-value memoization.
	ex.pv_store_possible = pv_setting && dedup_active && state.cache_eligible && !ex.reval_entry;
	if (ex.pv_store_possible && pv_blobs.empty() && (!state.cache_cc_latched || state.cache_cc.per_value)) {
		pv_blobs = InputRowSortKeys(client_context, worker_input);
	}
	ex.pv_blobs = std::move(pv_blobs);
	ex.pv_hit = std::move(pv_hit);
	// Cap new stores per chunk to bound fill on a high-cardinality input (0 = unlimited).
	ex.pv_store_cap = defaults::RESULT_CACHE_PER_VALUE_MAX_STORES_PER_CHUNK;
	{
		Value scv;
		if (client_context.TryGetCurrentSetting("vgi_result_cache_per_value_max_stores_per_chunk", scv) &&
		    !scv.IsNull()) {
			ex.pv_store_cap = scv.GetValue<uint64_t>();
		}
	}
	return emit_next_reply();
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
