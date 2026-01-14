#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

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
// If it's an EXCEPTION, throws IOException with the message, traceback, and worker path.
// For other log levels, logs to DuckDB if context is provided.
// Returns true if the batch was a log message, false otherwise.
bool HandleBatchLogMessage(const std::shared_ptr<arrow::RecordBatch> &batch,
                           const std::shared_ptr<arrow::KeyValueMetadata> &custom_metadata, ClientContext *context,
                           const std::string &worker_path);

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

// Streaming interface for function invocation.
// Use this to invoke a table function and stream results.
class FunctionInvokeStream {
public:
	// Start a streaming function invocation
	// positional_args_json: JSON-encoded array of positional arguments
	// named_args: map of named argument name to JSON-encoded value
	FunctionInvokeStream(const std::string &worker_path, const std::vector<uint8_t> &attach_id,
	                     const std::string &schema_name, const std::string &function_name,
	                     const std::string &positional_args_json,
	                     const std::vector<std::pair<std::string, std::string>> &named_args,
	                     const std::vector<int32_t> &projection_ids, ClientContext &context,
	                     bool worker_debug = false);
	~FunctionInvokeStream();

	// Read the next result batch. Returns nullptr at end of stream.
	std::shared_ptr<arrow::RecordBatch> ReadNext();

	// Wait for the worker process to exit. Called automatically by destructor.
	int Wait();

	// Check if stream has finished
	bool IsFinished() const {
		return finished_;
	}

	// Non-copyable
	FunctionInvokeStream(const FunctionInvokeStream &) = delete;
	FunctionInvokeStream &operator=(const FunctionInvokeStream &) = delete;

private:
	std::unique_ptr<SubProcess> proc_;
	ClientContext &context_;
	std::string worker_path_;
	bool finished_ = false;
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
