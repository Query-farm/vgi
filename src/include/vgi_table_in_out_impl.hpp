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
#include "vgi_catalog_metadata.hpp"
#include "vgi_protocol.hpp" // BindResult
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

// Finalize function - called when all input has been processed
OperatorFinalizeResultType VgiTableInOutFinalize(ExecutionContext &context, TableFunctionInput &data, DataChunk &output);

} // namespace vgi
} // namespace duckdb
