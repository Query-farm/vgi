#include "vgi_catalog_api.hpp"

#include "duckdb.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "vgi_arrow_ipc.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_exception.hpp"
#include "vgi_logging.hpp"
#include "vgi_protocol.hpp"
#include "yyjson.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {
namespace vgi {

// ============================================================================
// Helper: Handle log messages from batch metadata
// ============================================================================

// Check if a batch contains a log message (zero rows with vgi.log_* metadata).
// If it's an EXCEPTION, throws IOException with the message, traceback, and worker context.
// For other log levels, logs to DuckDB if context is provided.
// Returns true if the batch was a log message, false otherwise.
bool HandleBatchLogMessage(const std::shared_ptr<arrow::RecordBatch> &batch,
                           const std::shared_ptr<arrow::KeyValueMetadata> &custom_metadata, ClientContext *context,
                           const std::string &worker_path, pid_t worker_pid, const std::string &invocation_id_hex) {
	if (!batch || batch->num_rows() != 0) {
		return false;
	}

	// Check for log metadata in the custom metadata (per-batch metadata from IPC)
	if (!custom_metadata) {
		return false;
	}

	// Look for vgi.log_level and vgi.log_message
	int level_idx = custom_metadata->FindKey("vgi.log_level");
	int message_idx = custom_metadata->FindKey("vgi.log_message");

	if (level_idx < 0 || message_idx < 0) {
		return false;
	}

	std::string log_level = custom_metadata->value(level_idx);
	std::string log_message = custom_metadata->value(message_idx);

	// Parse vgi.log_extra if present (contains traceback for exceptions)
	std::string traceback;
	std::string exception_type;
	int extra_idx = custom_metadata->FindKey("vgi.log_extra");
	if (extra_idx >= 0) {
		std::string extra_json = custom_metadata->value(extra_idx);
		auto doc = yyjson_read(extra_json.c_str(), extra_json.size(), 0);
		if (doc) {
			auto root = yyjson_doc_get_root(doc);
			if (root && yyjson_is_obj(root)) {
				auto tb_val = yyjson_obj_get(root, "traceback");
				if (tb_val && yyjson_is_str(tb_val)) {
					traceback = yyjson_get_str(tb_val);
				}
				auto type_val = yyjson_obj_get(root, "exception_type");
				if (type_val && yyjson_is_str(type_val)) {
					exception_type = yyjson_get_str(type_val);
				}
			}
			yyjson_doc_free(doc);
		}
	}

	// Handle based on log level
	if (log_level == "EXCEPTION") {
		// Construct error message with traceback (worker context is in extra_info)
		std::string full_message = "VGI Worker Exception: " + log_message;
		if (!traceback.empty()) {
			full_message += "\n" + traceback;
		}
		ThrowVgiIOException(full_message, worker_path, worker_pid, invocation_id_hex);
	}

	// For non-exception log levels, log to DuckDB if we have a context
	if (context) {
		// Create log info with the level and optional details
		vector<pair<string, string>> info;
		info.emplace_back("level", log_level);
		if (!exception_type.empty()) {
			info.emplace_back("exception_type", exception_type);
		}
		if (!traceback.empty()) {
			info.emplace_back("traceback", traceback);
		}

		VGI_LOG(*context, log_message, info);
	}

	return true;
}

// ============================================================================
// Helper: Send invocation and args to a worker process
// ============================================================================

static void SendInvocationAndArgs(SubProcess &proc, const std::string &method_name,
                                  const std::shared_ptr<arrow::RecordBatch> &args) {
	// Create and send the invocation
	auto invocation = CreateCatalogInvocation(method_name);
	auto invocation_bytes = SerializeRecordBatch(invocation);
	WriteAll(proc.GetStdinFd(), invocation_bytes->data(), invocation_bytes->size());

	// Send the arguments
	auto args_bytes = SerializeRecordBatch(args);
	WriteAll(proc.GetStdinFd(), args_bytes->data(), args_bytes->size());

	// Close stdin to signal end of input
	proc.CloseStdin();
}

// ============================================================================
// InvokeCatalogMethod - Single result batch
// ============================================================================

std::shared_ptr<arrow::RecordBatch> InvokeCatalogMethod(const std::string &worker_path, const std::string &method_name,
                                                        const std::shared_ptr<arrow::RecordBatch> &args,
                                                        ClientContext &context, bool worker_debug) {
	// Spawn the worker process
	SubProcess proc(worker_path, worker_debug);

	// Send invocation and args
	SendInvocationAndArgs(proc, method_name, args);

	// Read batches, handling log messages until we get actual data
	arrow::RecordBatchWithMetadata result;
	while (true) {
		try {
			result = ReadRecordBatch(proc.GetStdoutFd(), worker_path, proc.GetPid());
		} catch (const IOException &e) {
			// Check if the worker process died early (e.g., command not found)
			int exit_status = 0;
			if (proc.TryWait(&exit_status)) {
				if (exit_status == 127) {
					ThrowVgiIOException("VGI worker not found or not executable", worker_path, proc.GetPid(), "");
				} else if (exit_status == 126) {
					ThrowVgiIOException("VGI worker permission denied", worker_path, proc.GetPid(), "");
				} else if (exit_status != 0) {
					ThrowVgiIOException("VGI worker failed to start (exit code %d)", worker_path, proc.GetPid(), "",
					                    exit_status);
				}
			}
			// Re-throw original error if process didn't die early
			throw;
		}

		// Check for log messages (will throw on EXCEPTION)
		// If it was a log message, continue reading the next batch
		// Note: Catalog methods don't have invocation_id
		if (!HandleBatchLogMessage(result.batch, result.custom_metadata, &context, worker_path, proc.GetPid(), "")) {
			break;
		}
	}

	// Wait for the worker to exit and check status
	bool exited_normally = false;
	int exit_status = proc.Wait(&exited_normally);

	if (!exited_normally) {
		ThrowVgiIOException("VGI worker was killed by signal %d", worker_path, proc.GetPid(), "", exit_status);
	}
	if (exit_status != 0) {
		ThrowVgiIOException("VGI worker exited with status %d", worker_path, proc.GetPid(), "", exit_status);
	}

	return result.batch;
}

// ============================================================================
// CatalogMethodStream - Streaming results
// ============================================================================

CatalogMethodStream::CatalogMethodStream(const std::string &worker_path, const std::string &method_name,
                                         const std::shared_ptr<arrow::RecordBatch> &args, ClientContext &context,
                                         bool worker_debug)
    : proc_(std::make_unique<SubProcess>(worker_path, worker_debug)), context_(context), worker_path_(worker_path) {
	SendInvocationAndArgs(*proc_, method_name, args);
}

CatalogMethodStream::~CatalogMethodStream() {
	if (proc_) {
		proc_->Wait();
	}
}

std::shared_ptr<arrow::RecordBatch> CatalogMethodStream::ReadNext() {
	if (finished_ || !proc_) {
		return nullptr;
	}

	arrow::RecordBatchWithMetadata result;
	try {
		result = ReadRecordBatch(proc_->GetStdoutFd(), worker_path_, proc_->GetPid());
	} catch (const IOException &e) {
		// Check if the worker process died early (e.g., command not found)
		int exit_status = 0;
		if (proc_->TryWait(&exit_status)) {
			if (exit_status == 127) {
				ThrowVgiIOException("VGI worker not found or not executable", worker_path_, proc_->GetPid(), "");
			} else if (exit_status == 126) {
				ThrowVgiIOException("VGI worker permission denied", worker_path_, proc_->GetPid(), "");
			} else if (exit_status != 0) {
				ThrowVgiIOException("VGI worker failed to start (exit code %d)", worker_path_, proc_->GetPid(), "",
				                    exit_status);
			}
		}
		// Check if this is an EOF indicator (invalid IPC stream at end)
		// The Python worker may signal EOF by closing the pipe or writing
		// invalid data, which causes specific error messages.
		std::string error_msg = e.what();
		if (error_msg.find("Tried reading schema message") != std::string::npos ||
		    error_msg.find("was null or length 0") != std::string::npos) {
			finished_ = true;
			return nullptr;
		}
		// Re-throw other errors
		throw;
	}

	// Check for log messages (will throw on EXCEPTION)
	// Note: A zero-row batch with log metadata is a log message, not end of stream
	// Catalog methods don't have invocation_id
	if (HandleBatchLogMessage(result.batch, result.custom_metadata, &context_, worker_path_, proc_->GetPid(), "")) {
		// It was a log message - read the next batch
		return ReadNext();
	}

	// Empty batch or null signals end of stream (but only if not a log message)
	if (!result.batch || result.batch->num_rows() == 0) {
		finished_ = true;
		return nullptr;
	}

	return result.batch;
}

int CatalogMethodStream::Wait() {
	if (!proc_) {
		return 0;
	}
	return proc_->Wait();
}

// ============================================================================
// Result parsing helpers
// ============================================================================

namespace {

// Helper to get a string column value, returns empty string if null
std::string GetStringValue(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &column_name,
                           int64_t row_idx) {
	auto column = batch->GetColumnByName(column_name);
	if (!column) {
		return "";
	}
	auto string_array = std::dynamic_pointer_cast<arrow::StringArray>(column);
	if (!string_array || string_array->IsNull(row_idx)) {
		return "";
	}
	return string_array->GetString(row_idx);
}

// Helper to get a bool column value, returns false if null
bool GetBoolValue(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &column_name, int64_t row_idx) {
	auto column = batch->GetColumnByName(column_name);
	if (!column) {
		return false;
	}
	auto bool_array = std::dynamic_pointer_cast<arrow::BooleanArray>(column);
	if (!bool_array || bool_array->IsNull(row_idx)) {
		return false;
	}
	return bool_array->Value(row_idx);
}

// Helper to get an int64 column value, returns 0 if null
int64_t GetInt64Value(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &column_name,
                      int64_t row_idx) {
	auto column = batch->GetColumnByName(column_name);
	if (!column) {
		return 0;
	}
	auto int_array = std::dynamic_pointer_cast<arrow::Int64Array>(column);
	if (!int_array || int_array->IsNull(row_idx)) {
		return 0;
	}
	return int_array->Value(row_idx);
}

// Helper to get a binary column value, returns empty vector if null
std::vector<uint8_t> GetBinaryValue(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &column_name,
                                    int64_t row_idx) {
	auto column = batch->GetColumnByName(column_name);
	if (!column) {
		return {};
	}
	auto binary_array = std::dynamic_pointer_cast<arrow::BinaryArray>(column);
	if (!binary_array || binary_array->IsNull(row_idx)) {
		return {};
	}
	auto value = binary_array->GetView(row_idx);
	return std::vector<uint8_t>(reinterpret_cast<const uint8_t *>(value.data()),
	                            reinterpret_cast<const uint8_t *>(value.data()) + value.size());
}

// Helper to get a map<string, string> column value, returns empty map if null
std::map<std::string, std::string> GetStringMapValue(const std::shared_ptr<arrow::RecordBatch> &batch,
                                                     const std::string &column_name, int64_t row_idx) {
	std::map<std::string, std::string> result;
	auto column = batch->GetColumnByName(column_name);
	if (!column) {
		return result;
	}
	auto map_array = std::dynamic_pointer_cast<arrow::MapArray>(column);
	if (!map_array || map_array->IsNull(row_idx)) {
		return result;
	}

	// Get the offsets for this row's entries
	int64_t start = map_array->value_offset(row_idx);
	int64_t end = map_array->value_offset(row_idx + 1);

	// Get the key and value arrays
	auto keys = std::dynamic_pointer_cast<arrow::StringArray>(map_array->keys());
	auto values = std::dynamic_pointer_cast<arrow::StringArray>(map_array->items());

	if (!keys || !values) {
		return result;
	}

	for (int64_t i = start; i < end; i++) {
		if (!keys->IsNull(i) && !values->IsNull(i)) {
			result[keys->GetString(i)] = values->GetString(i);
		}
	}

	return result;
}

} // namespace

CatalogAttachResult ParseCatalogAttachResult(const std::shared_ptr<arrow::RecordBatch> &batch) {
	CatalogAttachResult result;

	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty response from catalog_attach");
	}

	result.attach_id = GetBinaryValue(batch, "attach_id", 0);
	result.supports_transactions = GetBoolValue(batch, "supports_transactions", 0);
	result.supports_time_travel = GetBoolValue(batch, "supports_time_travel", 0);
	result.catalog_version_frozen = GetBoolValue(batch, "catalog_version_frozen", 0);
	result.catalog_version = GetInt64Value(batch, "catalog_version", 0);
	result.attach_id_required = GetBoolValue(batch, "attach_id_required", 0);
	result.default_schema = GetStringValue(batch, "default_schema", 0);
	if (result.default_schema.empty()) {
		result.default_schema = "main";
	}

	return result;
}

VgiSchemaInfo ParseSchemaInfo(const std::shared_ptr<arrow::RecordBatch> &batch) {
	VgiSchemaInfo info;

	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty response from schema_get");
	}

	info.name = GetStringValue(batch, "name", 0);
	info.comment = GetStringValue(batch, "comment", 0);
	info.tags = GetStringMapValue(batch, "tags", 0);

	return info;
}

VgiTableInfo ParseTableInfo(const std::shared_ptr<arrow::RecordBatch> &batch) {
	VgiTableInfo info;

	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty response from table_get");
	}

	info.name = GetStringValue(batch, "name", 0);
	info.schema_name = GetStringValue(batch, "schema_name", 0);
	info.comment = GetStringValue(batch, "comment", 0);
	info.tags = GetStringMapValue(batch, "tags", 0);

	// Parse the columns field which contains a serialized Arrow schema
	auto columns_data = GetBinaryValue(batch, "columns", 0);
	info.arrow_schema = DeserializeSchema(columns_data);

	// TODO: Parse constraints if present

	return info;
}

std::vector<VgiSchemaInfo> ParseSchemaList(const std::shared_ptr<arrow::RecordBatch> &batch) {
	std::vector<VgiSchemaInfo> schemas;

	if (!batch || batch->num_rows() == 0) {
		return schemas;
	}

	for (int64_t i = 0; i < batch->num_rows(); i++) {
		VgiSchemaInfo info;
		info.name = GetStringValue(batch, "name", i);
		info.comment = GetStringValue(batch, "comment", i);
		info.tags = GetStringMapValue(batch, "tags", i);
		schemas.push_back(std::move(info));
	}

	return schemas;
}

// ============================================================================
// DuckDB type conversion
// ============================================================================

CreateTableInfo CreateTableInfoFromVgiTable(ClientContext &context, const VgiTableInfo &table_info,
                                            const std::string &schema_name) {
	CreateTableInfo create_info;
	create_info.table = table_info.name;
	create_info.schema = schema_name;

	if (table_info.arrow_schema) {
		ArrowSchemaToColumnList(context, table_info.arrow_schema, create_info.columns);
	}

	return create_info;
}

VgiFunctionInfo ParseFunctionInfo(const std::shared_ptr<arrow::RecordBatch> &batch) {
	VgiFunctionInfo info;

	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty response from function_get");
	}

	info.name = GetStringValue(batch, "name", 0);
	info.schema_name = GetStringValue(batch, "schema_name", 0);
	info.type = GetStringValue(batch, "type", 0);
	info.description = GetStringValue(batch, "description", 0);
	info.cardinality_estimate = GetInt64Value(batch, "cardinality_estimate", 0);
	info.max_workers = static_cast<int32_t>(GetInt64Value(batch, "max_workers", 0));
	if (info.max_workers <= 0) {
		info.max_workers = 1;
	}
	info.tags = GetStringMapValue(batch, "tags", 0);

	// Parse the return_schema field which contains a serialized Arrow schema
	auto schema_data = GetBinaryValue(batch, "return_schema", 0);
	if (!schema_data.empty()) {
		info.return_schema = DeserializeSchema(schema_data);
	}

	// TODO: Parse parameters if present

	return info;
}

// ============================================================================
// Function Invocation API
// ============================================================================

VgiFunctionInfo GetFunctionInfo(const std::string &worker_path, const std::vector<uint8_t> &attach_id,
                                const std::string &schema_name, const std::string &function_name,
                                ClientContext &context, bool worker_debug) {
	auto args = CreateFunctionGetArgs(attach_id, schema_name, function_name);
	auto result_batch = InvokeCatalogMethod(worker_path, "function_get", args, context, worker_debug);
	return ParseFunctionInfo(result_batch);
}

// ============================================================================
// FunctionInvokeStream implementation
// ============================================================================

namespace {

// Helper to send function invocation and args to a worker process
static void SendFunctionInvocationAndArgs(SubProcess &proc, const std::string &function_name,
                                          const std::shared_ptr<arrow::RecordBatch> &args) {
	// Create and send the invocation
	auto invocation = CreateFunctionInvocation(function_name);
	auto invocation_bytes = SerializeRecordBatch(invocation);
	WriteAll(proc.GetStdinFd(), invocation_bytes->data(), invocation_bytes->size());

	// Send the arguments
	auto args_bytes = SerializeRecordBatch(args);
	WriteAll(proc.GetStdinFd(), args_bytes->data(), args_bytes->size());

	// Close stdin to signal end of input
	proc.CloseStdin();
}

} // namespace

FunctionInvokeStream::FunctionInvokeStream(const std::string &worker_path, const std::vector<uint8_t> &attach_id,
                                           const std::string &schema_name, const std::string &function_name,
                                           const std::string &positional_args_json,
                                           const std::vector<std::pair<std::string, std::string>> &named_args,
                                           const std::vector<int32_t> &projection_ids, ClientContext &context,
                                           bool worker_debug)
    : proc_(std::make_unique<SubProcess>(worker_path, worker_debug)), context_(context), worker_path_(worker_path) {

	// Create args batch
	auto args = CreateFunctionInvokeArgs(attach_id, schema_name, function_name, positional_args_json, named_args,
	                                     projection_ids);

	// Send invocation and args
	SendFunctionInvocationAndArgs(*proc_, function_name, args);
}

FunctionInvokeStream::~FunctionInvokeStream() {
	if (proc_) {
		proc_->Wait();
	}
}

std::shared_ptr<arrow::RecordBatch> FunctionInvokeStream::ReadNext() {
	if (finished_ || !proc_) {
		return nullptr;
	}

	arrow::RecordBatchWithMetadata result;
	try {
		result = ReadRecordBatch(proc_->GetStdoutFd(), worker_path_, proc_->GetPid());
	} catch (const IOException &e) {
		// Check if the worker process died early (e.g., command not found)
		int exit_status = 0;
		if (proc_->TryWait(&exit_status)) {
			if (exit_status == 127) {
				ThrowVgiIOException("VGI worker not found or not executable", worker_path_, proc_->GetPid(), "");
			} else if (exit_status == 126) {
				ThrowVgiIOException("VGI worker permission denied", worker_path_, proc_->GetPid(), "");
			} else if (exit_status != 0) {
				ThrowVgiIOException("VGI worker failed to start (exit code %d)", worker_path_, proc_->GetPid(), "",
				                    exit_status);
			}
		}
		// Check if this is an EOF indicator
		std::string error_msg = e.what();
		if (error_msg.find("Tried reading schema message") != std::string::npos ||
		    error_msg.find("was null or length 0") != std::string::npos) {
			finished_ = true;
			return nullptr;
		}
		throw;
	}

	// Check for log messages (will throw on EXCEPTION)
	// FunctionInvokeStream doesn't track invocation_id
	if (HandleBatchLogMessage(result.batch, result.custom_metadata, &context_, worker_path_, proc_->GetPid(), "")) {
		// It was a log message - read the next batch
		return ReadNext();
	}

	// Empty batch or null signals end of stream
	if (!result.batch || result.batch->num_rows() == 0) {
		finished_ = true;
		return nullptr;
	}

	return result.batch;
}

int FunctionInvokeStream::Wait() {
	if (!proc_) {
		return 0;
	}
	return proc_->Wait();
}

// ============================================================================
// FunctionConnection - Proper 6-Stream Protocol Implementation
// ============================================================================

FunctionConnection::FunctionConnection(const std::string &worker_path, const std::string &function_name,
                                       const ArrowArguments &arguments, const std::vector<uint8_t> &attach_id,
                                       ClientContext &context, const std::vector<uint8_t> &global_execution_id,
                                       bool worker_debug)
    : worker_path_(worker_path), function_name_(function_name), arguments_type_(arguments.type),
      arguments_array_(arguments.array), attach_id_(attach_id), global_execution_id_(global_execution_id),
      context_(context), worker_debug_(worker_debug) {
}

FunctionConnection::~FunctionConnection() {
	// Terminate the subprocess first - this causes EOF on stderr which unblocks
	// the stderr reader thread. Without this, StopStderrReader() would block forever
	// waiting for the thread that's blocked on read().
	proc_.reset();
	// Now stop stderr reader thread (should exit quickly due to EOF)
	StopStderrReader();
	// Drain any remaining stderr (can't log without context, just discard)
	{
		std::lock_guard<std::mutex> lock(stderr_mutex_);
		stderr_lines_.clear();
	}
}

OutputSpecResult FunctionConnection::PerformBindFull() {
	if (bind_done_) {
		// Return cached results
		return output_spec_;
	}

	// Spawn the worker process
	proc_ = std::make_unique<SubProcess>(worker_path_, worker_debug_);

	// Start stderr reader thread to prevent pipe buffer from blocking worker
	StartStderrReader();

	// Log the invocation request
	int64_t num_args = arguments_array_ ? arguments_array_->length() : 0;
	VGI_LOG(context_, "function_connection.invoke",
	        {{"worker_path", worker_path_},
	         {"worker_pid", std::to_string(proc_->GetPid())},
	         {"function_name", function_name_},
	         {"num_args", std::to_string(num_args)}});

	// Stream 1: Send Invocation
	auto invocation =
	    CreateFunctionInvocationFull(function_name_, arguments_type_, arguments_array_, attach_id_, global_execution_id_);
	auto invocation_bytes = SerializeRecordBatch(invocation);
	WriteAll(proc_->GetStdinFd(), invocation_bytes->data(), invocation_bytes->size());

	// Stream 2: Read OutputSpec
	// Note: We don't have invocation_id yet (it comes in the OutputSpec response)
	arrow::RecordBatchWithMetadata output_spec_result;
	while (true) {
		try {
			output_spec_result = ReadRecordBatch(proc_->GetStdoutFd(), worker_path_, proc_->GetPid());
		} catch (const IOException &e) {
			// Check if the worker process died early (e.g., command not found)
			int exit_status = 0;
			if (proc_->TryWait(&exit_status)) {
				if (exit_status == 127) {
					ThrowVgiIOException("VGI worker not found or not executable", worker_path_, proc_->GetPid(), "");
				} else if (exit_status == 126) {
					ThrowVgiIOException("VGI worker permission denied", worker_path_, proc_->GetPid(), "");
				} else if (exit_status != 0) {
					ThrowVgiIOException("VGI worker failed to start (exit code %d)", worker_path_, proc_->GetPid(), "",
					                    exit_status);
				}
			}
			// Re-throw original error if process didn't die early
			throw;
		}
		// Handle log messages (throws on EXCEPTION)
		if (!HandleBatchLogMessage(output_spec_result.batch, output_spec_result.custom_metadata, &context_,
		                           worker_path_, proc_->GetPid(), "")) {
			break;
		}
	}

	// Parse and cache OutputSpec
	output_spec_ = ParseOutputSpec(output_spec_result.batch);
	bind_done_ = true;

	// Drain any buffered stderr from bind phase
	DrainStderrLog();

	return output_spec_;
}

std::shared_ptr<arrow::Schema> FunctionConnection::PerformBind(int32_t &max_processes_out,
                                                                int64_t &cardinality_estimate_out) {
	// Call PerformBindFull to do the actual work
	auto output_spec = PerformBindFull();

	// Return results via output parameters for backwards compatibility
	max_processes_out = output_spec.max_processes;
	cardinality_estimate_out = output_spec.cardinality_estimate;

	return output_spec.output_schema;
}

InitResultData FunctionConnection::PerformInit(const std::vector<int32_t> &projection_ids) {
	if (!bind_done_) {
		ThrowVgiIOException("FunctionConnection::PerformInit called before PerformBind", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetInvocationIdHex());
	}
	if (init_done_) {
		ThrowVgiIOException("FunctionConnection::PerformInit called twice", worker_path_, proc_ ? proc_->GetPid() : -1,
		                    GetInvocationIdHex());
	}

	// Stream 3: Send InitInput (TableFunctionInitInput with projection_ids)
	auto init_input = CreateInitInput(projection_ids);
	auto init_input_bytes = SerializeRecordBatch(init_input);
	WriteAll(proc_->GetStdinFd(), init_input_bytes->data(), init_input_bytes->size());

	// Stream 4: Read InitResult
	// The worker sends a single batch with global_execution_identifier
	arrow::RecordBatchWithMetadata init_result;
	while (true) {
		init_result = ReadRecordBatch(proc_->GetStdoutFd(), worker_path_, proc_->GetPid());
		// Handle log messages (throws on EXCEPTION)
		if (!HandleBatchLogMessage(init_result.batch, init_result.custom_metadata, &context_, worker_path_,
		                           proc_->GetPid(), GetInvocationIdHex())) {
			break;
		}
	}

	// Parse InitResult to get global_execution_identifier for multi-worker coordination
	auto init_data = ParseInitResult(init_result.batch);

	// For Table functions (no input), close stdin to signal no more input
	proc_->CloseStdin();

	// Stream 6: Open the data stream reader
	// Unlike handshake streams which are one batch each, the data phase uses
	// a single long-lived IPC stream with multiple batches
	data_stream_ = std::make_shared<FdInputStream>(proc_->GetStdoutFd());
	auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(data_stream_);
	if (!reader_result.ok()) {
		ThrowVgiIOException("Failed to open data stream: %s", worker_path_, proc_->GetPid(), GetInvocationIdHex(),
		                    reader_result.status().ToString());
	}
	data_reader_ = reader_result.ValueUnsafe();

	init_done_ = true;

	// Drain any buffered stderr from init phase
	DrainStderrLog();

	return init_data;
}

void FunctionConnection::SkipInit() {
	if (!bind_done_) {
		ThrowVgiIOException("FunctionConnection::SkipInit called before PerformBind", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetInvocationIdHex());
	}
	if (init_done_) {
		ThrowVgiIOException("FunctionConnection::SkipInit called after init already done", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetInvocationIdHex());
	}

	// For secondary workers, send InitInput but skip reading InitResult.
	// The Python worker reads InitInput but doesn't write InitResult for secondary workers,
	// going straight to the data stream.

	// Stream 3: Send InitInput (with empty projection_ids)
	auto init_input = CreateInitInput({});
	auto init_input_bytes = SerializeRecordBatch(init_input);
	WriteAll(proc_->GetStdinFd(), init_input_bytes->data(), init_input_bytes->size());

	// Skip Stream 4 (InitResult) - secondary workers don't send it

	// Close stdin to signal no more input
	proc_->CloseStdin();

	// Open the data stream reader
	data_stream_ = std::make_shared<FdInputStream>(proc_->GetStdoutFd());
	auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(data_stream_);
	if (!reader_result.ok()) {
		ThrowVgiIOException("Failed to open data stream: %s", worker_path_, proc_->GetPid(), GetInvocationIdHex(),
		                    reader_result.status().ToString());
	}
	data_reader_ = reader_result.ValueUnsafe();

	init_done_ = true;

	// Drain any buffered stderr from init phase
	DrainStderrLog();
}

std::shared_ptr<arrow::RecordBatch> FunctionConnection::ReadDataBatch() {
	if (!init_done_) {
		ThrowVgiIOException("FunctionConnection::ReadDataBatch called before PerformInit", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetInvocationIdHex());
	}
	if (data_finished_ || !data_reader_) {
		return nullptr;
	}

	// Drain any buffered stderr to logging
	DrainStderrLog();

	// Read from the persistent data stream reader
	arrow::RecordBatchWithMetadata result;
	auto read_result = data_reader_->ReadNext();
	if (!read_result.ok()) {
		// Check if this is an EOF indicator or actual error
		std::string error_msg = read_result.status().ToString();
		if (error_msg.find("end of stream") != std::string::npos ||
		    error_msg.find("EOF") != std::string::npos) {
			data_finished_ = true;
			return nullptr;
		}
		ThrowVgiIOException("Failed to read data batch: %s", worker_path_, proc_ ? proc_->GetPid() : -1,
		                    GetInvocationIdHex(), error_msg);
	}
	result = read_result.ValueUnsafe();

	// Null batch means end of stream
	if (!result.batch) {
		data_finished_ = true;
		return nullptr;
	}

	// Check for log messages (will throw on EXCEPTION)
	if (HandleBatchLogMessage(result.batch, result.custom_metadata, &context_, worker_path_, proc_->GetPid(),
	                          GetInvocationIdHex())) {
		// It was a log message - read the next batch
		return ReadDataBatch();
	}

	// Check vgi.status metadata for FINISHED
	if (result.custom_metadata) {
		int status_idx = result.custom_metadata->FindKey("vgi.status");
		if (status_idx >= 0) {
			std::string status = result.custom_metadata->value(status_idx);
			if (status == "FINISHED") {
				data_finished_ = true;
				// Still return the batch if it has data
				if (result.batch->num_rows() > 0) {
					return result.batch;
				}
				return nullptr;
			}
		}
	}

	// Empty batch signals end of stream
	if (result.batch->num_rows() == 0) {
		data_finished_ = true;
		return nullptr;
	}

	return result.batch;
}

std::string FunctionConnection::GetInvocationIdHex() const {
	if (!bind_done_ || output_spec_.invocation_id.empty()) {
		return "";
	}
	return BytesToHex(output_spec_.invocation_id);
}

int FunctionConnection::Wait() {
	if (!proc_) {
		return 0;
	}
	return proc_->Wait();
}

void FunctionConnection::StartStderrReader() {
	if (!proc_ || proc_->GetStderrFd() < 0) {
		return; // No stderr to read (passthrough mode or no process)
	}

	int stderr_fd = proc_->ReleaseStderrFd();

	stderr_thread_ = std::thread([this, stderr_fd]() {
		char buffer[4096];
		std::string line_buffer;

		while (!stderr_stop_.load(std::memory_order_relaxed)) {
			ssize_t bytes_read = read(stderr_fd, buffer, sizeof(buffer) - 1);
			if (bytes_read <= 0) {
				break; // EOF or error
			}
			buffer[bytes_read] = '\0';

			// Append to line buffer and process complete lines
			line_buffer.append(buffer, bytes_read);
			size_t pos;
			while ((pos = line_buffer.find('\n')) != std::string::npos) {
				std::string line = line_buffer.substr(0, pos);
				line_buffer.erase(0, pos + 1);

				// Trim trailing \r if present
				if (!line.empty() && line.back() == '\r') {
					line.pop_back();
				}
				if (!line.empty()) {
					std::lock_guard<std::mutex> lock(stderr_mutex_);
					stderr_lines_.push_back(std::move(line));
				}
			}
		}

		// Buffer any remaining partial line
		if (!line_buffer.empty()) {
			std::lock_guard<std::mutex> lock(stderr_mutex_);
			stderr_lines_.push_back(std::move(line_buffer));
		}

		close(stderr_fd);
	});
}

void FunctionConnection::StopStderrReader() {
	stderr_stop_.store(true, std::memory_order_relaxed);
	if (stderr_thread_.joinable()) {
		stderr_thread_.join();
	}
}

void FunctionConnection::DrainStderrLog() {
	std::vector<std::string> lines;
	{
		std::lock_guard<std::mutex> lock(stderr_mutex_);
		lines.swap(stderr_lines_);
	}

	pid_t worker_pid = proc_ ? proc_->GetPid() : -1;
	for (const auto &line : lines) {
		VGI_LOG(context_, "worker.stderr",
		        {{"worker_path", worker_path_},
		         {"worker_pid", std::to_string(worker_pid)},
		         {"message", line}});
	}
}

} // namespace vgi
} // namespace duckdb
