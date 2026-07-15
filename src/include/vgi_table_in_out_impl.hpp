// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"

#include "vgi_arrow_utils.hpp"
#include "vgi_cache_control.hpp"        // VgiCacheControl (latched per substream)
#include "vgi_catalog_metadata.hpp"
#include "vgi_protocol.hpp" // BindResult
#include "vgi_result_cache.hpp"         // VgiResultCacheKey (exchange-mode memoization)
#include "vgi_ifunction_connection.hpp" // complete IFunctionConnection for unique_ptr member

namespace duckdb {

class DatabaseInstance;

namespace vgi {

// Forward declaration
class IFunctionConnection;

// ============================================================================
// Bind Data for Table-In-Out Functions
// ============================================================================

struct VgiTableInOutBindData : public TableFunctionData {
	VgiTableInOutBindData();
	~VgiTableInOutBindData() override;

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other) const override;

	// Connection parameters
	std::shared_ptr<VgiAttachParameters> attach_params;  // replaces worker_path, worker_debug, use_pool
	std::vector<uint8_t> attach_opaque_data;
	std::vector<uint8_t> transaction_opaque_data;
	std::string function_name;
	std::map<std::string, Value> settings;
	std::vector<vgi::VgiSecretRequirement> required_secrets;

	// Convenience accessors
	const std::string &worker_path() const { return attach_params->worker_path(); }
	bool worker_debug() const { return attach_params->worker_debug(); }
	bool use_pool() const { return attach_params->use_pool(); }
	const std::string &data_version_spec() const { return attach_params->data_version_spec(); }
	const std::string &implementation_version() const { return attach_params->implementation_version(); }

	// Arguments (excluding TABLE input)
	ArrowArguments arguments;

	// Output schema from bind handshake
	std::shared_ptr<arrow::Schema> output_schema;

	// Arrow C ABI schema and table schema for ArrowToDuckDB conversion
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;

	// Input schema (built from table input types/names)
	std::shared_ptr<arrow::Schema> input_schema;

	// Bind output retained for init phase. The init payload (BuildInitRequest)
	// carries bind_request_bytes + output_schema_bytes + opaque_data inline, so
	// the worker reconstructs bind context entirely from the init request — no
	// second on-wire bind needed.
	BindResult bind_result;

	// Worker capabilities
	int32_t max_processes = 1;
	int64_t cardinality_estimate = -1;

	// Buffered table function path (Sink+Source PhysicalOperator). When true,
	// the OptimizerExtension rewrites the LogicalGet to
	// LogicalVgiTableBufferingFunction; the streaming in_out_function /
	// in_out_function_final callbacks must never see a bind_data with this
	// flag set (loud-failure assertion at their entry).
	bool table_buffering = false;
	bool source_order_dependent = false;
	// Sink-side ordering knobs — only meaningful with table_buffering=true.
	// sink_order_dependent → ParallelSink=false (single-thread ingest).
	// requires_input_batch_index → operator declares
	// RequiredPartitionInfo()=BatchIndex(); per-chunk batch_index flows
	// through to the worker's process() params. Mutually exclusive.
	bool sink_order_dependent = false;
	bool requires_input_batch_index = false;
	// Worker-declared pushdown capabilities (echo of Meta.projection_pushdown /
	// Meta.filter_pushdown). The catalog set advertises these to DuckDB via
	// table_func.projection_pushdown / filter_pushdown for the buffered
	// branch. The rewriter reads these to know whether to capture column_ids
	// / table_filters from the LogicalGet (without the flag, DuckDB never
	// pushes — column_ids has only a virtual GetAnyColumn placeholder).
	bool projection_pushdown = false;
	bool filter_pushdown = false;
	// Phase A parallelism gate, DERIVED at bind from the registered finalize
	// callback (`input.table_function.in_out_function_final`), not from
	// func_info metadata. True (default) = no finalize = per-substream map =
	// fan out one worker per substream. False = finalize registered = keep a
	// single shared worker (per-substream finalize is unsound; DuckDB #18222).
	bool parallel_safe = true;
	// Whether the function registered an in_out_function_final (finalize) callback,
	// derived at bind from the actual TableFunction callback. Distinct from
	// parallel_safe: a finalize function IS parallel (per-substream finalize on its
	// own worker), but Execute must keep the substream connection open at input-EOS
	// so VgiTableInOutFinalize can reuse it (a no-finalize map releases it there).
	bool has_finalize = false;
	// Worker-declared function stability (DuckDB FunctionStability). Threaded so the
	// map operators (batched LATERAL / streaming) can refuse to DEDUP a VOLATILE map —
	// its per-row output legitimately varies for the same input, so collapsing
	// duplicate input tuples would be wrong. Unset ⇒ treat as CONSISTENT (the default).
	std::optional<FunctionStability> stability;

	// Blended ("UNNEST-style") table-in-out (Phase B). input_from_args: this is a
	// blended function (positional args are input columns). single_row_scan: this
	// particular bind is the childless call shape (literal f(52,13) or a
	// pure-varargs childless call) that DuckDB drives via PhysicalTableScan — a
	// SOURCE that re-feeds its cardinality-1 input chunk until the callback returns
	// a 0-row chunk. Execute uses the write-once -> CloseInputWriter -> drain-to-EOS
	// scan-mode branch for this shape. False for the streaming column/LATERAL shape
	// (PhysicalTableInOutFunction, which advances on NEED_MORE_INPUT).
	bool input_from_args = false;
	bool single_row_scan = false;
	// Declared DuckDB types of the per-row input columns (blended). One entry per
	// input column. Execute casts the incoming input DataChunk to these before
	// shipping it, so the worker always receives its declared arg types (the
	// literal shape otherwise delivers the constant's natural type). Empty for
	// classic (non-blended) table-in-out.
	std::vector<LogicalType> declared_input_types;
};

// ============================================================================
// Global State for Table-In-Out Functions
// ============================================================================

struct VgiTableInOutGlobalState : public GlobalTableFunctionState {
	explicit VgiTableInOutGlobalState(DatabaseInstance *db = nullptr);
	~VgiTableInOutGlobalState() override;

	// Primary connection for this execution
	std::unique_ptr<IFunctionConnection> connection;

	// Serializes the per-batch write→read exchange on `connection`. Under
	// UNION ALL, DuckDB feeds this operator from multiple source sub-pipelines
	// whose PipelineExecutors can run concurrently; MaxThreads()=1 serializes
	// only within a single pipeline, NOT across union pipelines. Without this
	// lock their WriteInputBatch/ReadDataBatch calls interleave on the one IPC
	// stream and desync the worker's schema-first reader. Held across each 1:1
	// write→read in VgiTableInOutFunction so the exchange is atomic.
	std::mutex exchange_mutex;

	// BindResult from the bind RPC sent at InitGlobal time. Held here so
	// that the later PerformInit (INPUT phase, called below at InitGlobal)
	// and PerformFinalizeInit (called from VgiTableInOutFunction at the
	// end of the input stream) can both reference the same bind.
	BindResult bind_result;

	// Global execution ID for multi-worker coordination
	std::vector<uint8_t> global_execution_id;

	// Whether finalize signal has been sent to the worker
	bool finalize_sent = false;

	// Set when the stream reaches a natural end (worker returned
	// FINISHED or Finalize drained). The destructor uses this to
	// decide between cancel-dispatch and plain connection drop.
	bool stream_finished = false;

	// Captured at InitGlobal from the ClientContext setting.
	bool cancel_enabled = true;

	// Most recent HTTP stream-state token seen on exchange responses.
	// Empty for subprocess. Used to address the right worker on cancel
	// when HTTP max_workers > 1.
	std::vector<uint8_t> last_state_token;

	// DatabaseInstance used to locate the per-instance cancel
	// dispatcher. nullptr is valid (cancel dispatch is skipped).
	DatabaseInstance *db = nullptr;

	// Pushdown state captured at ``VgiTableInOutInitGlobal`` from the
	// ``TableFunctionInitInput``. Threaded to ``local_state.column_ids``
	// at InitLocal so ``ProduceOutputFromBatch`` can drive
	// ``ArrowToDuckDB`` with ``arrow_scan_is_projected=true`` and remap
	// worker-original column indices to type info correctly. Mirrors
	// the streaming pure-table fields at
	// ``vgi_table_function_impl.hpp:305-323``.
	vector<column_t> column_ids;            // DuckDB-side column_ids (worker schema indices)
	std::vector<int32_t> projection_ids;    // int32 form sent on the InitRequest wire
	std::shared_ptr<arrow::Buffer> static_filter_bytes;
	std::vector<std::shared_ptr<arrow::Buffer>> join_keys_buffers;

	// Phase A: parallel-by-default. A streaming table-in/out function is a
	// per-substream map, so it fans out one worker per substream (see
	// VgiTableInOutFunction). A function that registers a finalize callback stays
	// single-worker (per-substream finalize is unsound — DuckDB #18222); that fact
	// is derived at bind from `input.table_function.in_out_function_final` (the
	// single source of truth — the actual registered callback) into
	// `bind_data.parallel_safe`, copied here at InitGlobal.
	bool parallel_safe = true;
	idx_t MaxThreads() const override {
		return parallel_safe ? GlobalTableFunctionState::MAX_THREADS : 1;
	}

	// Exchange-mode result cache (M1). Built once at InitGlobal for the parallel,
	// no-finalize streaming-map path (a pure per-input-batch function). When
	// cache_eligible, each exchange keys on cache_key + an ordered hash of its input
	// batch: a hit replays the cached output batch and skips the worker exchange; a
	// miss captures it (gated on the worker's vgi.cache.* advertisement). The
	// per-substream cc latch lives on the local state (each substream's own worker
	// advertises). Entries are shared process-wide via the cache singleton, so one
	// substream's store can serve another's identical input batch.
	bool cache_eligible = false;
	VgiResultCacheKey cache_key;             // static portion; input_hash set per exchange
	std::string cache_catalog_name;
	int64_t cache_default_ttl_seconds = 0;
	// Min stored-payload size before a stale revalidatable entry is conditionally
	// revalidated (below it, refetch instead of a conditional request).
	int64_t cache_revalidate_min_bytes = 262144;
};

// ============================================================================
// Local State for Table-In-Out Functions
// ============================================================================

struct VgiTableInOutLocalState : public ArrowScanLocalState {
	VgiTableInOutLocalState(unique_ptr<ArrowArrayWrapper> current_chunk, ClientContext &ctx)
	    : ArrowScanLocalState(std::move(current_chunk), ctx) {
	}
	~VgiTableInOutLocalState() override;

	// Phase A parallel path: this substream's OWN worker (one per
	// PipelineExecutor). Acquired lazily on first exchange in
	// VgiTableInOutFunction, released to the pool at end-of-stream. On early
	// termination the destructor drops it (unique_ptr) — a mid-stream worker is
	// never pooled. Null on the serial (finalize) path, which uses the shared
	// global connection instead.
	std::unique_ptr<IFunctionConnection> connection;

	// Phase A: stable, process-unique id for THIS substream, minted once at
	// InitLocal and carried on every InitRequest this substream's worker builds
	// (INPUT + FINALIZE) via IFunctionConnection::SetSubstreamId. It survives an
	// HTTP load balancer re-dispatching a substream's requests across backends,
	// so a finalize can key the substream's accumulated state even on a
	// different backend than the process() calls. Empty on the serial path.
	std::vector<uint8_t> substream_id;

	// Phase A (finalize functions): whether THIS substream has already sent its
	// FINALIZE init (PerformFinalizeInit) to its own worker. Per-substream — the
	// finalize is independent per substream, so the "init once" latch must live on
	// the local state, not the global one (which the serial path uses).
	bool finalize_sent = false;

	// Phase B (blended literal scan-mode): whether the single synthesized input row
	// has been written + the input writer closed. Single-thread-safe:
	// PhysicalTableScan::ParallelSource() is false for in-out functions, so the
	// literal scan runs on one local state.
	bool input_submitted = false;

	// Exchange-mode result cache (M1): this substream's latched worker cache-control
	// advertisement (read from the first exchange output batch). Once latched,
	// cache_cc.Cacheable() gates whether this substream stores its miss outputs.
	VgiCacheControl cache_cc;
	bool cache_cc_latched = false;
};

// ============================================================================
// Parameters for Bind Function
// ============================================================================

struct VgiTableInOutBindParams {
	std::shared_ptr<VgiAttachParameters> attach_params;  // replaces worker_path, worker_debug, use_pool
	std::string function_name;
	std::vector<uint8_t> attach_opaque_data;
	std::vector<uint8_t> transaction_opaque_data;
	std::map<std::string, Value> settings;
	std::vector<vgi::VgiSecretRequirement> required_secrets;

	// Worker-advertised Meta.max_workers (A3 serial opt-out). 1 = force serial
	// (single shared worker, MaxThreads()=1); 0/unset or >1 = parallel-by-default
	// per-substream fan-out. Threaded to bind_data.parallel_safe.
	int32_t max_workers = 0;

	// Worker-declared FunctionStability → threaded to bind_data.stability so the map
	// operators refuse to dedup a VOLATILE map. Unset ⇒ CONSISTENT.
	std::optional<FunctionStability> stability;

	// Blended ("UNNEST-style") table-in-out (Phase B): positional args ARE the
	// per-row input columns. When true, bind builds the worker input schema from
	// `positional_input_names` (the DECLARED arg names — the worker reads columns
	// by those names) + the DECLARED types (`positional_input_types` /
	// `varargs_input_type` — NOT the incoming input_table_types, so the worker
	// always receives its declared arg types regardless of call shape: a literal
	// otherwise delivers the constant's natural type, e.g. DECIMAL for 52.0, while
	// a column already casts to the signature). single_row_scan is set for the
	// childless (literal / pure-varargs) call so Execute uses the write-once
	// scan-mode + casts the synthesized input row to the declared types.
	bool input_from_args = false;
	std::vector<std::string> positional_input_names;
	std::vector<LogicalType> positional_input_types;
	LogicalType varargs_input_type;
	bool has_varargs = false;

	// Routes through to bind_data so the OptimizerExtension can recognize a
	// LogicalGet of a buffered table function and rewrite it.
	bool table_buffering = false;
	bool source_order_dependent = false;
	bool sink_order_dependent = false;
	bool requires_input_batch_index = false;
	// Pushdown capability flags echoed from Meta. The rewriter reads
	// bind_data->projection_pushdown to decide whether DuckDB's column_ids
	// represent a real projection (vs. the default GetAnyColumn placeholder).
	bool projection_pushdown = false;
	bool filter_pushdown = false;

	// Convenience accessors
	const std::string &worker_path() const { return attach_params->worker_path(); }
	bool worker_debug() const { return attach_params->worker_debug(); }
	bool use_pool() const { return attach_params->use_pool(); }
	const std::string &data_version_spec() const { return attach_params->data_version_spec(); }
	const std::string &implementation_version() const { return attach_params->implementation_version(); }
};

// ============================================================================
// Function Implementations
// ============================================================================

// Bind function for table-in-out functions
// Receives table input schema via input.input_table_types/input_table_names
unique_ptr<FunctionData> VgiTableInOutBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names,
                                            const VgiTableInOutBindParams &params);

// Init functions
unique_ptr<GlobalTableFunctionState> VgiTableInOutInitGlobal(ClientContext &context, TableFunctionInitInput &input);
unique_ptr<LocalTableFunctionState> VgiTableInOutInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                            GlobalTableFunctionState *global_state);

// In-out function - processes input chunks and produces output chunks
OperatorResultType VgiTableInOutFunction(ExecutionContext &context, TableFunctionInput &data,
                                          DataChunk &input, DataChunk &output);

// ============================================================================
// Exchange/scan helpers reused by the batched-lateral operator
// (vgi_lateral_batch_operator.cpp). Defined in vgi_table_in_out_impl.cpp.
// ============================================================================

// Mint a stable, process-unique per-substream id (8-byte salt || 8-byte counter).
std::vector<uint8_t> MintSubstreamId();

// Acquire + INPUT-init a blended function's worker from bind_data alone (no
// VgiTableInOutGlobalState); bind_data.bind_result is populated at bind.
// `projection_ids` (worker-original column indices) is threaded to PerformInit so a
// projection-pushdown function emits only the referenced columns; pass {} for none.
std::unique_ptr<IFunctionConnection>
AcquireBlendedInputConnection(ClientContext &context, const VgiTableInOutBindData &bind_data,
                              const std::vector<uint8_t> &substream_id,
                              const std::vector<int32_t> &projection_ids = {});

// Return a substream's worker to the pool at clean end-of-stream.
void ReleaseSubstreamConnection(std::unique_ptr<IFunctionConnection> &conn,
                                const VgiTableInOutBindData &bind_data, ClientContext &context);

// Convert an input DataChunk to the worker-input Arrow batch (casts to declared types).
std::shared_ptr<arrow::RecordBatch>
ConvertInputToArrow(ClientContext &context, DataChunk &input, const VgiTableInOutBindData &bind_data);

// Load a worker output Arrow batch into a scan local state for draining.
void LoadBatchIntoScanState(VgiTableInOutLocalState &local_state,
                            const std::shared_ptr<arrow::RecordBatch> &batch);

// Whether the scan local state has remaining rows to produce from its batch.
bool HasRemainingBatchData(const VgiTableInOutLocalState &local_state);

// Produce one DataChunk slice (<= STANDARD_VECTOR_SIZE) of worker output columns
// into `output`'s LEADING columns, leaving any trailing columns untouched.
idx_t ProduceOutputFromBatch(VgiTableInOutLocalState &local_state, const ArrowTableSchema &arrow_table,
                             DataChunk &output, bool projection_pushdown = false);

// Finalize function - called when all input has been processed
OperatorFinalizeResultType VgiTableInOutFinalize(ExecutionContext &context, TableFunctionInput &data, DataChunk &output);

} // namespace vgi
} // namespace duckdb
