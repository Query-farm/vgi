#pragma once

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include "duckdb/function/aggregate_state.hpp"
#include "duckdb/function/function.hpp"

#include "vgi_arrow_utils.hpp"
#include "vgi_protocol.hpp"
#include "vgi_subprocess.hpp"
#include "vgi_worker_pool.hpp"

namespace duckdb {

class ClientContext;

namespace vgi {

// Parameters for connecting to a VGI worker
struct VgiAttachParameters {
	VgiAttachParameters(const std::string &worker_path, const std::string &catalog_name, bool worker_debug = false,
	                    bool use_pool = true)
	    : worker_path_(worker_path), catalog_name_(catalog_name), worker_debug_(worker_debug), use_pool_(use_pool) {
	}

	const std::string &worker_path() const {
		return worker_path_;
	}

	const std::string &catalog_name() const {
		return catalog_name_;
	}

	bool worker_debug() const {
		return worker_debug_;
	}

	bool use_pool() const {
		return use_pool_;
	}

private:
	std::string worker_path_;
	std::string catalog_name_;
	bool worker_debug_;
	bool use_pool_;
};

// ============================================================================
// Catalog Method Invocation API
// ============================================================================

// Catalog method names for type-safe invocation
// These correspond to methods in the Python CatalogInterface
enum class CatalogMethod {
	// Catalog-level methods
	Catalogs,                  // List available catalogs
	CatalogAttach,             // Attach to a catalog
	CatalogDetach,             // Detach from a catalog
	CatalogVersion,            // Get catalog version
	CatalogTransactionBegin,   // Begin a transaction
	CatalogTransactionCommit,  // Commit a transaction
	CatalogTransactionRollback, // Rollback a transaction

	// Schema methods
	Schemas,        // List schemas
	SchemaGet,      // Get schema info
	SchemaContents, // List schema contents (tables, views, functions)

	// Table methods
	TableGet,  // Get table info
	TableScan, // Scan table data (streaming)

	// View methods
	ViewGet, // Get view info

	// Function methods
	FunctionGet // Get function info
};

// Convert CatalogMethod to protocol string
const char *CatalogMethodToString(CatalogMethod method);

// Invoke a catalog method and return a single result batch.
// Use this for methods that return a single response (CatalogAttach, TableGet, Catalogs).
// Handles all the plumbing: spawns worker, sends invocation+args, reads result, waits for exit.
// Handles log message batches from the worker - exceptions are thrown, other levels are logged.
// Throws IOException on worker failure or timeout.
// Non-exception log messages are written to DuckDB's log via the provided context.
// If worker_debug is true, worker stderr is passed through to the terminal for debugging.
std::shared_ptr<arrow::RecordBatch> InvokeCatalogMethod(const std::string &worker_path, CatalogMethod method,
                                                        const std::shared_ptr<arrow::RecordBatch> &args,
                                                        ClientContext &context, bool worker_debug = false);

// Streaming interface for catalog methods that return multiple batches.
// Use this for methods like Schemas or TableScan that stream results.
// Handles log message batches from the worker - exceptions are thrown, other levels are logged.
class CatalogMethodStream {
public:
	// Start a streaming catalog method call
	// Non-exception log messages are written to DuckDB's log via the provided context.
	// If worker_debug is true, worker stderr is passed through to the terminal for debugging.
	CatalogMethodStream(const std::string &worker_path, CatalogMethod method,
	                    const std::shared_ptr<arrow::RecordBatch> &args, ClientContext &context,
	                    bool worker_debug = false);
	~CatalogMethodStream();

	// Read the next result batch. Returns nullptr at end of stream.
	// Handles log message batches internally (exceptions thrown, others logged).
	std::shared_ptr<arrow::RecordBatch> ReadNext();

	// Wait for the worker process to exit. Called automatically by destructor.
	// Returns exit status (0 = success).
	int Wait();

	// Check if stream has finished (ReadNext returned nullptr)
	bool IsFinished() const {
		return finished_;
	}

	// Get the subprocess (for access to stderr, pid, etc.)
	SubProcess &GetProcess() {
		return *proc_;
	}

	// Non-copyable
	CatalogMethodStream(const CatalogMethodStream &) = delete;
	CatalogMethodStream &operator=(const CatalogMethodStream &) = delete;

private:
	std::unique_ptr<SubProcess> proc_;
	ClientContext &context_;
	std::string worker_path_;
	bool finished_ = false;
};

// A setting exposed by a VGI worker
// Parsed from serialized Setting objects in CatalogAttachResult
struct VgiSetting {
	std::string name;                     // Setting name (e.g., "vgi_verbose_mode")
	std::string description;              // Human-readable description
	LogicalType type;                     // DuckDB logical type
	Value default_value;                  // Default value (may be NULL if required)
};

// Result of catalog_attach call
struct CatalogAttachResult {
	std::vector<uint8_t> attach_id;
	bool supports_transactions = false;
	bool supports_time_travel = false;
	bool catalog_version_frozen = false;
	int64_t catalog_version = 0;
	bool attach_id_required = false;
	std::string default_schema = "main";
	std::vector<VgiSetting> settings;     // Extension options exposed by this catalog
};

// Schema metadata from the worker
struct VgiSchemaInfo {
	std::string name;
	std::string comment;
	std::map<std::string, std::string> tags;
};

// Table metadata from the worker
struct VgiTableInfo {
	std::string name;
	std::string schema_name;
	std::shared_ptr<arrow::Schema> arrow_schema;
	std::string comment;
	std::map<std::string, std::string> tags;
	// Constraint indices
	std::vector<int> not_null_constraints;
	std::vector<std::vector<int>> unique_constraints;
	std::vector<std::string> check_constraints;
};

// View metadata from the worker
struct VgiViewInfo {
	std::string name;
	std::string schema_name;
	std::string definition; // SQL query defining the view
	std::string comment;
	std::map<std::string, std::string> tags;
};

// ============================================================================
// Function Metadata Enums and Parsing
// ============================================================================

// Type of function for DuckDB registration
// Wire format: lowercase string ("scalar", "table", "aggregate")
enum class VgiFunctionType {
	Scalar,    // Scalar function: one output per input row
	Table,     // Table function: returns a table (includes table_in_out)
	Aggregate  // Aggregate function: many inputs → one output
};

// Parse VgiFunctionType from wire format string
// Returns nullopt for unrecognized values
std::optional<VgiFunctionType> ParseVgiFunctionType(const std::string &value);

// Convert VgiFunctionType to string for error messages
std::string VgiFunctionTypeToString(VgiFunctionType type);

// Row order preservation behavior for table functions
// Wire format: uppercase enum name ("PRESERVES_ORDER", "NO_ORDER_GUARANTEE")
enum class VgiOrderPreservation {
	PreservesOrder,    // Output rows are in same order as input rows
	NoOrderGuarantee   // Output order is undefined (may be reordered)
};

// Parse VgiOrderPreservation from wire format string
// Returns nullopt for unrecognized values
std::optional<VgiOrderPreservation> ParseVgiOrderPreservation(const std::string &value);

// ============================================================================
// Function metadata from the worker (matches Python FunctionInfo)
struct VgiFunctionInfo {
	std::string name;
	std::string schema_name;
	VgiFunctionType function_type = VgiFunctionType::Scalar;
	std::string description;
	std::map<std::string, std::string> tags;

	// Arguments and output as deserialized Arrow schemas
	std::shared_ptr<arrow::Schema> arguments_schema;
	std::shared_ptr<arrow::Schema> output_schema;

	// Documentation fields
	std::vector<std::string> examples;    // SQL examples showing function usage
	std::vector<std::string> categories;  // Function categories for organization

	// Scalar function behavior fields (nullopt if not applicable)
	// Uses DuckDB's FunctionStability enum (CONSISTENT, VOLATILE, CONSISTENT_WITHIN_QUERY)
	std::optional<FunctionStability> stability;
	// Uses DuckDB's FunctionNullHandling enum (DEFAULT_NULL_HANDLING, SPECIAL_HANDLING)
	std::optional<FunctionNullHandling> null_handling;

	// Table function capabilities (nullopt if not applicable)
	std::optional<bool> projection_pushdown;
	std::optional<bool> filter_pushdown;
	std::optional<VgiOrderPreservation> order_preservation;
	std::optional<int32_t> max_workers;

	// Aggregate function fields - uses DuckDB's enums
	AggregateOrderDependent order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	AggregateDistinctDependent distinct_dependent = AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT;

	// Settings required by this function (must be set before invocation)
	std::vector<std::string> required_settings;
};

// Parse a VgiSetting from serialized bytes (Arrow IPC format)
// The bytes contain a single-row RecordBatch with: name, description, type, default_value
VgiSetting ParseVgiSetting(const std::vector<uint8_t> &bytes, const std::string &worker_path);

// Parse a CatalogAttachResult from an Arrow RecordBatch
CatalogAttachResult ParseCatalogAttachResult(const std::shared_ptr<arrow::RecordBatch> &batch,
                                             const std::string &worker_path);

// Parse a VgiSchemaInfo from an Arrow RecordBatch (single row)
VgiSchemaInfo ParseSchemaInfo(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &worker_path);

// Parse a VgiTableInfo from an Arrow RecordBatch (single row)
VgiTableInfo ParseTableInfo(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &worker_path);

// Parse multiple schemas from a batch (for schemas() call)
std::vector<VgiSchemaInfo> ParseSchemaList(const std::shared_ptr<arrow::RecordBatch> &batch,
                                           const std::string &worker_path);

// Parse a VgiFunctionInfo from an Arrow RecordBatch at a specific row
VgiFunctionInfo ParseFunctionInfo(const std::shared_ptr<arrow::RecordBatch> &batch, int64_t row_idx,
                                  const std::string &worker_path);

// Parse a VgiViewInfo from an Arrow RecordBatch (single row)
VgiViewInfo ParseViewInfo(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &worker_path);

// ============================================================================
// Function Invocation API
// ============================================================================

// Get function metadata from a VGI worker.
// Calls the "function_get" catalog method and parses the result.
VgiFunctionInfo GetFunctionInfo(const std::string &worker_path, const std::vector<uint8_t> &attach_id,
                                const std::string &schema_name, const std::string &function_name,
                                ClientContext &context, bool worker_debug = false);

// ============================================================================
// Connection Acquisition with Retry
// ============================================================================

// Forward declaration for FunctionConnection (defined below)
class FunctionConnection;

// Parameters for creating a FunctionConnection
// Used with AcquireAndBindConnection to handle pool acquire + stale connection retry
struct FunctionConnectionParams {
	std::string worker_path;
	std::string function_name;
	ArrowArguments arguments;
	std::vector<uint8_t> attach_id;
	std::vector<uint8_t> global_execution_id;  // Empty for primary workers
	bool worker_debug = false;
	std::map<std::string, std::string> settings;
	bool use_pool = true;
	std::string phase;  // For logging (e.g., "bind", "init_local_secondary")

	// Default constructor
	FunctionConnectionParams() = default;

	// Convenience constructor for common case
	FunctionConnectionParams(const std::string &worker_path, const std::string &function_name,
	                         const ArrowArguments &arguments, const std::vector<uint8_t> &attach_id,
	                         const std::vector<uint8_t> &global_execution_id, bool worker_debug,
	                         const std::map<std::string, std::string> &settings, bool use_pool,
	                         const std::string &phase)
	    : worker_path(worker_path), function_name(function_name), arguments(arguments), attach_id(attach_id),
	      global_execution_id(global_execution_id), worker_debug(worker_debug), settings(settings), use_pool(use_pool),
	      phase(phase) {
	}
};

// Result of AcquireAndBindConnection
struct AcquireAndBindResult {
	std::unique_ptr<FunctionConnection> connection;
	OutputSpecResult output_spec;
};

// Acquire a FunctionConnection and perform bind with automatic retry for stale pool connections.
//
// This helper encapsulates the common pattern of:
// 1. Try to acquire a pooled worker
// 2. Create FunctionConnection (from pool or fresh)
// 3. Attempt PerformBindFull()
// 4. If bind fails and connection was from pool, retry with fresh connection
//
// The retry handles the case where a pooled worker died while idle. If the
// fresh connection also fails, the exception propagates to the caller.
//
// Returns the successfully-bound connection and the OutputSpecResult.
// Throws IOException if bind fails (after retry if applicable).
AcquireAndBindResult AcquireAndBindConnection(ClientContext &context, const FunctionConnectionParams &params);

// ============================================================================
// Function Connection - Proper 6-Stream Protocol
// ============================================================================

// FunctionConnection implements the proper VGI function protocol with 6 streams:
// Stream 1: Invocation (client → worker)
// Stream 2: OutputSpec (worker → client) - contains output schema
// Stream 3: InitInput (client → worker)
// Stream 4: InitResult (worker → client)
// Stream 5: Input batches (client → worker) - not used for Table functions
// Stream 6: Output batches (worker → client)
//
// State machine with validation:
// - Created → bind_done_ (via PerformBind/PerformBindFull)
// - bind_done_ → init_done_ (via PerformInit or SkipInit)
// - init_done_ → data_finished_ (via ReadDataBatch returning nullptr)
//
// Validation rules:
// - PerformBindFull: Idempotent, returns cached result if already called
// - PerformInit/SkipInit: Throws if !bind_done_ or init_done_
// - ReadDataBatch: Throws if !init_done_, returns nullptr if data_finished_
//
// Usage:
// 1. During Bind: Call PerformBind() to get schema from OutputSpec
// 2. During Init Local: Call PerformInit() to complete handshake
// 3. During Scan: Call ReadDataBatch() to stream results
class FunctionConnection {
public:
	// Create connection parameters (does not spawn worker yet)
	// arguments: Arrow struct array with fields named positional_0, positional_1, etc.
	// global_execution_id: For secondary workers, pass the ID from the primary worker's InitResult
	// settings: Optional map of settings to pass to the worker (e.g., DuckDB pragmas)
	FunctionConnection(const std::string &worker_path, const std::string &function_name,
	                   const ArrowArguments &arguments, const std::vector<uint8_t> &attach_id, ClientContext &context,
	                   const std::vector<uint8_t> &global_execution_id = {}, bool worker_debug = false,
	                   const std::map<std::string, std::string> &settings = {});

	// Create connection using a pooled worker (skips spawning new subprocess)
	FunctionConnection(std::unique_ptr<PooledWorker> pooled_worker, const std::string &function_name,
	                   const ArrowArguments &arguments, const std::vector<uint8_t> &attach_id, ClientContext &context,
	                   const std::vector<uint8_t> &global_execution_id = {}, bool worker_debug = false,
	                   const std::map<std::string, std::string> &settings = {});

	~FunctionConnection();

	// Phase 1: Perform bind handshake (Streams 1-2)
	// Spawns worker, sends Invocation, reads OutputSpec
	// Returns the output schema and metadata
	// After this call, the worker is waiting for InitInput
	std::shared_ptr<arrow::Schema> PerformBind(int32_t &max_processes_out, int64_t &cardinality_estimate_out);

	// Phase 1 (full): Perform bind handshake and return full OutputSpec
	// Same as PerformBind but returns the complete OutputSpecResult including
	// invocation_id, active_features, and other metadata
	OutputSpecResult PerformBindFull();

	// Set the input schema for table-in-out functions
	// Must be called before PerformBindFull() for table-in-out functions
	// input_schema: Arrow schema describing the input data that will be sent via Stream 5
	void SetInputSchema(const std::shared_ptr<arrow::Schema> &input_schema);

	// Phase 2: Perform init handshake (Streams 3-4)
	// Sends InitInput, reads InitResult, then closes stdin (for Table functions)
	// After this call, the connection is ready to read data
	// projection_ids: optional list of column indices for projection pushdown
	// Returns InitResultData containing global_execution_identifier for multi-worker coordination
	InitResultData PerformInit(const std::vector<int32_t> &projection_ids = {});

	// Phase 2 (secondary worker): Skip init handshake for secondary workers
	// The Python worker's secondary worker path doesn't do InitInput/InitResult exchange,
	// it goes straight from OutputSpec to generating batches.
	// This method closes stdin and opens the data stream without the handshake.
	void SkipInit();

	// ========================================================================
	// Stream 5: Input Data (Table-In-Out Functions Only)
	// ========================================================================

	// Open Stream 5 for writing input data batches (table-in-out functions)
	// Must be called after PerformInit() and before WriteInputBatch()
	// Throws if called on a regular table function (no input schema)
	void OpenInputWriter();

	// Write an input batch to Stream 5 (table-in-out functions)
	// Must be called after OpenInputWriter()
	// The batch schema must match the input_schema set via SetInputSchema()
	void WriteInputBatch(const std::shared_ptr<arrow::RecordBatch> &batch);

	// Send a finalize signal to the worker via Stream 5 (table-in-out functions)
	// This sends an empty batch with type=FINALIZE metadata, signaling the worker
	// to run its finalize() method and output any remaining data
	// Must be called before CloseInputWriter() for aggregation functions
	void SendFinalize();

	// Close Stream 5 (signals end of input to the worker)
	// Must be called after all input batches have been written
	// After this, the worker will process remaining data and output results via Stream 6
	void CloseInputWriter();

	// Check if this is a table-in-out function (has input schema)
	bool IsTableInOut() const {
		return input_schema_ != nullptr;
	}

	// Phase 3: Read a data batch (Stream 6)
	// Returns nullptr when stream is exhausted OR when NEED_MORE_INPUT is received
	// After nullptr, check NeedsMoreInput() to determine which case
	std::shared_ptr<arrow::RecordBatch> ReadDataBatch();

	// Check if data stream is exhausted
	bool IsFinished() const {
		return data_finished_;
	}

	// Mark the data phase as finished (for scalar functions after sending finalize)
	// This allows the connection to be pooled without reading a FINISHED status
	void MarkDataFinished() {
		data_finished_ = true;
	}

	// Check if the worker requested more input (table-in-out functions only)
	// This is set when ReadDataBatch receives a batch with vgi.status = NEED_MORE_INPUT
	// After sending more input via WriteInputBatch(), call ClearNeedsMoreInput() and continue reading
	bool NeedsMoreInput() const {
		return needs_more_input_;
	}

	// Clear the needs_more_input flag after sending more input
	void ClearNeedsMoreInput() {
		needs_more_input_ = false;
	}

	// Get the worker process PID (returns -1 if not yet spawned)
	pid_t GetPid() const {
		return proc_ ? proc_->GetPid() : -1;
	}

	// Get the invocation ID as hex string (empty if bind not done)
	std::string GetInvocationIdHex() const;

	// Get the attach ID as hex string (empty if no attach_id)
	std::string GetAttachIdHex() const;

	// Wait for worker process to exit
	int Wait();

	// Check if this connection can be returned to the pool
	// Returns true if data finished and subprocess is still alive
	bool CanBePooled() const;

	// Release the subprocess for pooling (transfers ownership)
	// Returns nullptr if cannot be pooled
	// Stops stderr thread and releases subprocess ownership
	std::unique_ptr<PooledWorker> ReleaseForPooling();

	// Non-copyable and non-movable (contains reference)
	FunctionConnection(const FunctionConnection &) = delete;
	FunctionConnection &operator=(const FunctionConnection &) = delete;
	FunctionConnection(FunctionConnection &&) = delete;
	FunctionConnection &operator=(FunctionConnection &&) = delete;

private:
	std::string worker_path_;
	std::string function_name_;
	std::shared_ptr<arrow::DataType> arguments_type_;
	std::shared_ptr<arrow::Array> arguments_array_;
	std::vector<uint8_t> attach_id_;
	std::vector<uint8_t> global_execution_id_;
	ClientContext &context_;
	bool worker_debug_;
	std::map<std::string, std::string> settings_;

	// Worker process (created during bind)
	std::unique_ptr<SubProcess> proc_;

	// State tracking
	bool bind_done_ = false;
	bool init_done_ = false;
	bool data_finished_ = false;
	bool needs_more_input_ = false;  // Set when worker responds with NEED_MORE_INPUT status

	// Cached bind results (full OutputSpec)
	OutputSpecResult output_spec_;

	// Data stream reader (opened during PerformInit, used for ReadDataBatch)
	// The data phase uses a single long-lived IPC stream with multiple batches
	std::shared_ptr<arrow::io::InputStream> data_stream_;
	std::shared_ptr<arrow::ipc::RecordBatchStreamReader> data_reader_;

	// Input schema for table-in-out functions (nullptr for regular table functions)
	std::shared_ptr<arrow::Schema> input_schema_;

	// Input data writer (Stream 5) for table-in-out functions
	// Used to send input batches to the worker
	std::shared_ptr<arrow::ipc::RecordBatchWriter> input_writer_;
	bool input_writer_opened_ = false;
	bool input_writer_closed_ = false;
	bool finalize_sent_ = false;  // Set when SendFinalize() is called

	// Stderr reader thread - reads stderr and buffers lines for main thread to log
	std::thread stderr_thread_;
	std::atomic<bool> stderr_stop_{false};
	std::mutex stderr_mutex_;
	std::vector<std::string> stderr_lines_;
	int stderr_fd_ = -1; // Stderr fd, owned by this class after release from SubProcess

	// Start stderr reader thread (called after spawning worker)
	void StartStderrReader();
	// Stop stderr reader thread (called in destructor or before pooling)
	// If close_fd is true, also closes the stderr fd (use in destructor)
	// If false, keeps fd open for reuse (use when pooling)
	void StopStderrReader(bool close_fd = true);
	// Drain buffered stderr lines and log them via VGI_LOG (call from main thread)
	void DrainStderrLog();
};

} // namespace vgi

// Forward declarations for DuckDB types used in conversion
class ClientContext;
struct CreateTableInfo;

namespace vgi {

// Convert VgiTableInfo to DuckDB CreateTableInfo.
// This handles converting the Arrow schema to DuckDB column definitions.
CreateTableInfo CreateTableInfoFromVgiTable(ClientContext &context, const VgiTableInfo &table_info,
                                            const std::string &schema_name);

} // namespace vgi
} // namespace duckdb
