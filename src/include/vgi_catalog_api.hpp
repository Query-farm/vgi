#pragma once

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include "vgi_protocol.hpp"
#include "vgi_subprocess.hpp"

namespace duckdb {

class ClientContext;

namespace vgi {

// Parameters for connecting to a VGI worker
struct VgiAttachParameters {
	VgiAttachParameters(const std::string &worker_path, const std::string &catalog_name, bool worker_debug = false)
	    : worker_path_(worker_path), catalog_name_(catalog_name), worker_debug_(worker_debug) {
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

private:
	std::string worker_path_;
	std::string catalog_name_;
	bool worker_debug_;
};

// ============================================================================
// Log Message Handling
// ============================================================================

// Check if a batch contains a log message (zero rows with vgi.log_* custom metadata).
// If it's an EXCEPTION, throws IOException with the message, traceback, and worker context.
// For other log levels, logs to DuckDB if context is provided.
// Returns true if the batch was a log message, false otherwise.
bool HandleBatchLogMessage(const std::shared_ptr<arrow::RecordBatch> &batch,
                           const std::shared_ptr<arrow::KeyValueMetadata> &custom_metadata, ClientContext *context,
                           const std::string &worker_path, pid_t worker_pid = -1,
                           const std::string &invocation_id_hex = "");

// ============================================================================
// Catalog Method Invocation API
// ============================================================================

// Invoke a catalog method and return a single result batch.
// Use this for methods that return a single response (catalog_attach, table_get, catalogs).
// Handles all the plumbing: spawns worker, sends invocation+args, reads result, waits for exit.
// Handles log message batches from the worker - exceptions are thrown, other levels are logged.
// Throws IOException on worker failure or timeout.
// Non-exception log messages are written to DuckDB's log via the provided context.
// If worker_debug is true, worker stderr is passed through to the terminal for debugging.
std::shared_ptr<arrow::RecordBatch> InvokeCatalogMethod(const std::string &worker_path, const std::string &method_name,
                                                        const std::shared_ptr<arrow::RecordBatch> &args,
                                                        ClientContext &context, bool worker_debug = false);

// Streaming interface for catalog methods that return multiple batches.
// Use this for methods like "schemas" or "table_scan" that stream results.
// Handles log message batches from the worker - exceptions are thrown, other levels are logged.
class CatalogMethodStream {
public:
	// Start a streaming catalog method call
	// Non-exception log messages are written to DuckDB's log via the provided context.
	// If worker_debug is true, worker stderr is passed through to the terminal for debugging.
	CatalogMethodStream(const std::string &worker_path, const std::string &method_name,
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
	std::string method_name_;
	bool finished_ = false;
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

// Function parameter metadata
struct VgiFunctionParameter {
	std::string name;
	int32_t position;
	std::string arrow_type;    // Arrow type name (e.g., "int64", "utf8")
	std::string default_value; // JSON-encoded default value, empty if required
	std::string doc;
	bool is_varargs = false;
};

// Function metadata from the worker
struct VgiFunctionInfo {
	std::string name;
	std::string schema_name;
	std::string type;          // "table", "table_in_out", "scalar"
	std::string description;
	std::vector<VgiFunctionParameter> parameters;
	std::shared_ptr<arrow::Schema> return_schema;
	int64_t cardinality_estimate = -1;  // -1 means unknown
	int32_t max_workers = 1;
	std::map<std::string, std::string> tags;
};

// Parse a CatalogAttachResult from an Arrow RecordBatch
CatalogAttachResult ParseCatalogAttachResult(const std::shared_ptr<arrow::RecordBatch> &batch);

// Parse a VgiSchemaInfo from an Arrow RecordBatch (single row)
VgiSchemaInfo ParseSchemaInfo(const std::shared_ptr<arrow::RecordBatch> &batch);

// Parse a VgiTableInfo from an Arrow RecordBatch (single row)
VgiTableInfo ParseTableInfo(const std::shared_ptr<arrow::RecordBatch> &batch);

// Parse multiple schemas from a batch (for schemas() call)
std::vector<VgiSchemaInfo> ParseSchemaList(const std::shared_ptr<arrow::RecordBatch> &batch);

// Parse a VgiFunctionInfo from an Arrow RecordBatch (single row)
VgiFunctionInfo ParseFunctionInfo(const std::shared_ptr<arrow::RecordBatch> &batch);

// ============================================================================
// Function Invocation API
// ============================================================================

// Get function metadata from a VGI worker.
// Calls the "function_get" catalog method and parses the result.
VgiFunctionInfo GetFunctionInfo(const std::string &worker_path, const std::vector<uint8_t> &attach_id,
                                const std::string &schema_name, const std::string &function_name,
                                ClientContext &context, bool worker_debug = false);

// ============================================================================
// Function Connection - Proper 6-Stream Protocol
// ============================================================================

// Forward declaration for ArrowArguments (defined in vgi_arrow_utils.hpp)
struct ArrowArguments;

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
	FunctionConnection(const std::string &worker_path, const std::string &function_name,
	                   const ArrowArguments &arguments, const std::vector<uint8_t> &attach_id, ClientContext &context,
	                   const std::vector<uint8_t> &global_execution_id = {}, bool worker_debug = false);
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

	// Phase 3: Read a data batch (Stream 6)
	// Returns nullptr when stream is exhausted
	std::shared_ptr<arrow::RecordBatch> ReadDataBatch();

	// Check if data stream is exhausted
	bool IsFinished() const {
		return data_finished_;
	}

	// Get the worker process PID (returns -1 if not yet spawned)
	pid_t GetPid() const {
		return proc_ ? proc_->GetPid() : -1;
	}

	// Get the invocation ID as hex string (empty if bind not done)
	std::string GetInvocationIdHex() const;

	// Wait for worker process to exit
	int Wait();

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

	// Worker process (created during bind)
	std::unique_ptr<SubProcess> proc_;

	// State tracking
	bool bind_done_ = false;
	bool init_done_ = false;
	bool data_finished_ = false;

	// Cached bind results (full OutputSpec)
	OutputSpecResult output_spec_;

	// Data stream reader (opened during PerformInit, used for ReadDataBatch)
	// The data phase uses a single long-lived IPC stream with multiple batches
	std::shared_ptr<arrow::io::InputStream> data_stream_;
	std::shared_ptr<arrow::ipc::RecordBatchStreamReader> data_reader_;

	// Stderr reader thread - reads stderr and buffers lines for main thread to log
	std::thread stderr_thread_;
	std::atomic<bool> stderr_stop_{false};
	std::mutex stderr_mutex_;
	std::vector<std::string> stderr_lines_;

	// Start stderr reader thread (called after spawning worker)
	void StartStderrReader();
	// Stop stderr reader thread (called in destructor)
	void StopStderrReader();
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
