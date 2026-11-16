// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"

#include "vgi_settings_defaults.hpp"
#include <memory>
#include <mutex>
#include <string>
#include <vector>


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

class IFunctionConnection;

// Bind Data for Table-In-Out Functions

struct VgiTableInOutBindData : public TableFunctionData {
	VgiTableInOutBindData();
	~VgiTableInOutBindData() override;

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other) const override;

	std::shared_ptr<VgiAttachParameters> attach_params;
	std::vector<uint8_t> attach_opaque_data;
	std::vector<uint8_t> transaction_opaque_data;
	std::string function_name;
	// Catalog schema that owns `function_name`; the worker resolves the pair
	// because one name may be registered in several schemas. Empty for
	// non-catalog call sites.
	std::string schema_name;
	std::map<std::string, Value> settings;
	std::vector<vgi::VgiSecretRequirement> required_secrets;
	// Sanitized union of metadata-declared and bind-discovered secret usage.
	bool secret_dependent = false;

	const std::string &worker_path() const { return attach_params->worker_path(); }
	bool worker_debug() const { return attach_params->worker_debug(); }
	bool use_pool() const { return attach_params->use_pool(); }
	const std::string &data_version_spec() const { return attach_params->data_version_spec(); }
	const std::string &implementation_version() const { return attach_params->implementation_version(); }

	// Arguments (excluding TABLE input)
	ArrowArguments arguments;

	VgiSchemaPtr output_schema;

	// For ArrowToDuckDB conversion of worker output.
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;

	VgiSchemaPtr input_schema;

	// Retained for init: the init request (BuildInitRequest) carries the bind
	// context inline, so the worker needs no second bind.
	BindResult bind_result;

	int32_t max_processes = 1;
	int64_t cardinality_estimate = -1;

	// Buffered table function (Sink+Source): VgiTableBufferingRewriter rewrites
	// the LogicalGet to LogicalVgiTableBufferingFunction, so the streaming
	// callbacks throw if they ever see this set.
	bool table_buffering = false;
	bool source_order_dependent = false;
	// Sink-side ordering knobs — only meaningful with table_buffering=true.
	// sink_order_dependent → ParallelSink=false (single-thread ingest).
	// requires_input_batch_index → operator declares
	// RequiredPartitionInfo()=BatchIndex(); per-chunk batch_index flows
	// through to the worker's process() params. Mutually exclusive.
	bool sink_order_dependent = false;
	bool requires_input_batch_index = false;
	// Worker-declared pushdown capabilities. Advertised to DuckDB for the buffered
	// path (streaming InOut advertises projection only; see
	// vgi_table_function_set.cpp). Without the flag DuckDB never pushes and
	// column_ids holds only a GetAnyColumn placeholder.
	bool projection_pushdown = false;
	bool filter_pushdown = false;
	// Expression-filter negotiation copied from FunctionInfo; the buffered
	// pushdown callback reads it here (this is not a VgiTableFunctionBindData).
	std::vector<std::string> filter_semantic_profiles;
	std::vector<VgiFilterFunctionCapability> additional_filter_functions;
	// True (default): one worker per substream. False only when the worker
	// declared max_workers = 1: a single shared worker, MaxThreads() = 1.
	bool parallel_safe = true;
	// Whether in_out_function_final is registered (derived from the actual
	// callback). A finalize function still runs per substream, each finalizing
	// on its own worker, so Execute keeps the substream connection open at
	// input-EOS for VgiTableInOutFinalize (a no-finalize map releases it there).
	bool has_finalize = false;
	// Worker-declared stability; a VOLATILE map's output varies for the same
	// input, so its duplicate input tuples must not be deduped. Unset = CONSISTENT.
	std::optional<FunctionStability> stability;

	// Blended ("UNNEST-style") table-in-out: input_from_args = positional args are
	// input columns. single_row_scan: this bind is the childless call shape
	// (literal f(52,13) or pure-varargs) that DuckDB drives via PhysicalTableScan,
	// which re-feeds its cardinality-1 input chunk until the callback returns 0
	// rows; Execute then uses its write-once scan mode. False for the column/LATERAL
	// shape (PhysicalTableInOutFunction, which advances on NEED_MORE_INPUT).
	bool input_from_args = false;
	bool single_row_scan = false;
	// Blended only: the most input rows one worker call receives; the batched-LATERAL
	// operator splits each input chunk into calls of at most this many rows. 0 = a
	// whole chunk per call.
	idx_t input_rows_per_call = 0;
	// Blended: declared type per input column; ConvertInputToArrow casts to them
	// (a literal otherwise arrives with its natural type). Empty otherwise.
	std::vector<LogicalType> declared_input_types;
};

// Global State for Table-In-Out Functions

struct VgiTableInOutGlobalState : public GlobalTableFunctionState {
	explicit VgiTableInOutGlobalState(DatabaseInstance *db = nullptr);
	~VgiTableInOutGlobalState() override;

	// The shared connection of the serial (max_workers = 1) path; null otherwise.
	std::unique_ptr<IFunctionConnection> connection;

	// Serial path: held across each write→read on `connection`. Under UNION ALL,
	// several source pipelines can feed this operator concurrently (MaxThreads()=1
	// serializes only within one pipeline), and interleaved calls would desync the
	// worker's schema-first reader.
	std::mutex exchange_mutex;

	// Copied from bind_data; used by PerformInit (INPUT) and PerformFinalizeInit.
	BindResult bind_result;

	std::vector<uint8_t> global_execution_id;

	// Serial path: whether the FINALIZE init has been sent.
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

	// Pushdown state captured at InitGlobal; column_ids is copied to each local
	// state so ProduceOutputFromBatch can run ArrowToDuckDB projected (see
	// VgiTableFunctionGlobalState::projection_ids for the pure-table analogue).
	vector<column_t> column_ids;            // DuckDB-side column_ids (worker schema indices)
	std::vector<int32_t> projection_ids;    // int32 form sent on the InitRequest wire
	VgiBufferPtr static_filter_bytes;
	std::vector<VgiBufferPtr> join_keys_buffers;

	// Copied from bind_data.parallel_safe (false only for max_workers = 1).
	bool parallel_safe = true;
	idx_t MaxThreads() const override {
		return parallel_safe ? GlobalTableFunctionState::MAX_THREADS : 1;
	}

	// Exchange-mode result cache, built at InitGlobal for the parallel no-finalize
	// map path only. Each exchange keys on cache_key + an ordered hash of its
	// input batch: a hit replays the stored output and skips the worker; a miss
	// stores it if the worker advertises vgi.cache.* (latched per substream on
	// the local state). Entries are process-wide, shared across substreams.
	bool cache_eligible = false;
	VgiResultCacheKey cache_key;             // static portion; input_hash set per exchange
	std::string cache_catalog_name;
	int64_t cache_default_ttl_seconds = 0;
	// Min stored-payload size before a stale revalidatable entry is conditionally
	// revalidated (below it, refetch instead of a conditional request).
	int64_t cache_revalidate_min_bytes = defaults::RESULT_CACHE_REVALIDATE_MIN_BYTES;
};

// Local State for Table-In-Out Functions

struct VgiTableInOutLocalState : public ArrowScanLocalState {
	VgiTableInOutLocalState(unique_ptr<ArrowArrayWrapper> current_chunk, ClientContext &ctx)
	    : ArrowScanLocalState(std::move(current_chunk), ctx) {
	}
	~VgiTableInOutLocalState() override;

	// Parallel path: this substream's own worker, acquired lazily on first
	// exchange and released to the pool at end-of-stream (or after FINALIZE).
	// On early termination the destructor drops it: a mid-stream worker is never
	// pooled. Null on the serial (max_workers = 1) path.
	std::unique_ptr<IFunctionConnection> connection;

	// Process-unique id for this substream, minted at InitLocal and sent on its
	// INPUT and FINALIZE init requests (SetSubstreamId), so a finalize can find
	// the substream's state even when an HTTP load balancer sends it to a
	// different backend than the process() calls.
	std::vector<uint8_t> substream_id;

	// Parallel path: whether this substream has sent its FINALIZE init (the
	// serial path uses the global latch).
	bool finalize_sent = false;

	// Blended literal scan mode: whether the synthesized input row was written, and
	// whether the input writer is closed (once the worker has no more output for
	// it). One local state only: PhysicalTableScan does not parallelize in-out
	// functions.
	bool input_submitted = false;
	bool input_closed = false;

	// Continued output: the last reply said HAVE_MORE_OUTPUT, so the next exchange on
	// this input is a continuation tick, not new input. Never set on the shared
	// (max_workers = 1) connection, which refuses continued output.
	bool more_pending = false;

	// This substream's worker cache-control, latched from its first exchange
	// output; Cacheable() gates storing miss outputs.
	VgiCacheControl cache_cc;
	bool cache_cc_latched = false;
};

// Parameters for Bind Function

struct VgiTableInOutBindParams {
	std::shared_ptr<VgiAttachParameters> attach_params;
	std::string function_name;
	// Owning catalog schema; see VgiTableInOutBindData::schema_name.
	std::string schema_name;
	std::vector<uint8_t> attach_opaque_data;
	std::vector<uint8_t> transaction_opaque_data;
	std::map<std::string, Value> settings;
	std::vector<vgi::VgiSecretRequirement> required_secrets;

	// Worker-advertised Meta.max_workers: 1 forces the serial path; anything else
	// keeps one worker per substream (no N-worker cap). -> bind_data.parallel_safe.
	int32_t max_workers = 0;

	// -> bind_data.stability.
	std::optional<FunctionStability> stability;

	// Blended: positional args are the per-row input columns. Bind builds the
	// worker input schema from the DECLARED names (the worker reads columns by
	// name) and DECLARED types (`positional_input_types` / `varargs_input_type`),
	// not input_table_types: a literal otherwise arrives with its natural type
	// (DECIMAL for 52.0).
	bool input_from_args = false;
	// Blended only: the most input rows per worker call (see FunctionInfo). Unset =
	// a whole chunk per call.
	std::optional<int64_t> input_rows_per_call;
	std::vector<std::string> positional_input_names;
	std::vector<LogicalType> positional_input_types;
	LogicalType varargs_input_type;
	bool has_varargs = false;

	// Copied to bind_data (see VgiTableInOutBindData).
	bool table_buffering = false;
	bool source_order_dependent = false;
	bool sink_order_dependent = false;
	bool requires_input_batch_index = false;
	bool projection_pushdown = false;
	bool filter_pushdown = false;
	std::vector<std::string> filter_semantic_profiles;
	std::vector<VgiFilterFunctionCapability> additional_filter_functions;

	const std::string &worker_path() const { return attach_params->worker_path(); }
	bool worker_debug() const { return attach_params->worker_debug(); }
	bool use_pool() const { return attach_params->use_pool(); }
	const std::string &data_version_spec() const { return attach_params->data_version_spec(); }
	const std::string &implementation_version() const { return attach_params->implementation_version(); }
};

// Function Implementations

// The table input schema arrives in input.input_table_types / input_table_names.
unique_ptr<FunctionData> VgiTableInOutBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<Identifier> &names,
                                            const VgiTableInOutBindParams &params);

unique_ptr<GlobalTableFunctionState> VgiTableInOutInitGlobal(ClientContext &context, TableFunctionInitInput &input);
unique_ptr<LocalTableFunctionState> VgiTableInOutInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                            GlobalTableFunctionState *global_state);

OperatorResultType VgiTableInOutFunction(ExecutionContext &context, TableFunctionInput &data,
                                          DataChunk &input, DataChunk &output);

// Exchange/scan helpers shared with the batched-lateral operator.

// A process-unique per-substream id (8-byte random salt || 8-byte counter).
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
VgiBatchPtr
ConvertInputToArrow(ClientContext &context, DataChunk &input, const VgiTableInOutBindData &bind_data);

// Load a worker output Arrow batch into a scan local state for draining.
void LoadBatchIntoScanState(VgiTableInOutLocalState &local_state,
                            const VgiBatchPtr &batch);

// Whether the scan local state has remaining rows to produce from its batch.
bool HasRemainingBatchData(const VgiTableInOutLocalState &local_state);

// Produce one slice (<= STANDARD_VECTOR_SIZE rows) of worker output into
// `output`'s LEADING columns, leaving trailing (LATERAL) columns untouched.
idx_t ProduceOutputFromBatch(VgiTableInOutLocalState &local_state, const ArrowTableSchema &arrow_table,
                             DataChunk &output, bool projection_pushdown = false);

// Called once all input has been processed.
OperatorFinalizeResultType VgiTableInOutFinalize(ExecutionContext &context, TableFunctionInput &data, DataChunk &output);

} // namespace vgi
} // namespace duckdb
