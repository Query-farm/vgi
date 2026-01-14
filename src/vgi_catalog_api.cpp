#include "vgi_catalog_api.hpp"

#include <arrow/c/bridge.h>

#include "duckdb.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "vgi_arrow_ipc.hpp"
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
// If it's an EXCEPTION, throws IOException with the message and traceback.
// For other log levels, logs to DuckDB if context is provided.
// Returns true if the batch was a log message, false otherwise.
bool HandleBatchLogMessage(const std::shared_ptr<arrow::RecordBatch> &batch,
                           const std::shared_ptr<arrow::KeyValueMetadata> &custom_metadata, ClientContext *context,
                           const std::string &worker_path) {
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
		// Construct error message with traceback and worker info
		std::string full_message = "VGI Worker Exception";
		if (!worker_path.empty()) {
			full_message += " [" + worker_path + "]";
		}
		full_message += ": " + log_message;
		if (!traceback.empty()) {
			full_message += "\n" + traceback;
		}
		throw IOException(full_message);
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

		DUCKDB_LOG(*context, VgiLogType, log_message, info);
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
		result = ReadRecordBatch(proc.GetStdoutFd());

		// Check for log messages (will throw on EXCEPTION)
		// If it was a log message, continue reading the next batch
		if (!HandleBatchLogMessage(result.batch, result.custom_metadata, &context, worker_path)) {
			break;
		}
	}

	// Wait for the worker to exit and check status
	bool exited_normally = false;
	int exit_status = proc.Wait(&exited_normally);

	if (!exited_normally) {
		throw IOException("VGI worker '%s' was killed by signal %d", worker_path, exit_status);
	}
	if (exit_status != 0) {
		throw IOException("VGI worker '%s' exited with status %d", worker_path, exit_status);
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
		result = ReadRecordBatch(proc_->GetStdoutFd());
	} catch (const IOException &e) {
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
	if (HandleBatchLogMessage(result.batch, result.custom_metadata, &context_, worker_path_)) {
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

	// TODO: Parse tags map if present

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
		auto &db_config = DBConfig::GetConfig(context);
		for (int i = 0; i < table_info.arrow_schema->num_fields(); i++) {
			auto &field = table_info.arrow_schema->field(i);

			// Export Arrow C++ field to C ABI format
			ArrowSchema c_schema;
			auto status = arrow::ExportField(*field, &c_schema);
			if (!status.ok()) {
				throw IOException("Failed to export Arrow field '%s': %s", field->name(), status.ToString());
			}

			// Use DuckDB's built-in Arrow type conversion
			auto arrow_type = ArrowType::GetArrowLogicalType(db_config, c_schema);
			arrow_type->ThrowIfInvalid();
			LogicalType duckdb_type = arrow_type->GetDuckType();

			// Release the C schema
			if (c_schema.release) {
				c_schema.release(&c_schema);
			}

			create_info.columns.AddColumn(ColumnDefinition(field->name(), duckdb_type));
		}
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
		result = ReadRecordBatch(proc_->GetStdoutFd());
	} catch (const IOException &e) {
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
	if (HandleBatchLogMessage(result.batch, result.custom_metadata, &context_, worker_path_)) {
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
                                       const std::string &positional_args_json,
                                       const std::vector<std::pair<std::string, std::string>> &named_args,
                                       const std::vector<uint8_t> &attach_id, ClientContext &context, bool worker_debug)
    : worker_path_(worker_path), function_name_(function_name), positional_args_json_(positional_args_json),
      named_args_(named_args), attach_id_(attach_id), context_(context), worker_debug_(worker_debug) {
}

FunctionConnection::~FunctionConnection() {
	if (proc_) {
		proc_->Wait();
	}
}

std::shared_ptr<arrow::Schema> FunctionConnection::PerformBind(int32_t &max_processes_out,
                                                                int64_t &cardinality_estimate_out) {
	if (bind_done_) {
		// Return cached results
		max_processes_out = max_processes_;
		cardinality_estimate_out = cardinality_estimate_;
		return output_schema_;
	}

	// Spawn the worker process
	proc_ = std::make_unique<SubProcess>(worker_path_, worker_debug_);

	// Stream 1: Send Invocation
	auto invocation =
	    CreateFunctionInvocationFull(function_name_, positional_args_json_, named_args_, attach_id_, {});
	auto invocation_bytes = SerializeRecordBatch(invocation);
	WriteAll(proc_->GetStdinFd(), invocation_bytes->data(), invocation_bytes->size());

	// Stream 2: Read OutputSpec
	arrow::RecordBatchWithMetadata output_spec_result;
	while (true) {
		output_spec_result = ReadRecordBatch(proc_->GetStdoutFd());
		// Handle log messages (throws on EXCEPTION)
		if (!HandleBatchLogMessage(output_spec_result.batch, output_spec_result.custom_metadata, &context_,
		                           worker_path_)) {
			break;
		}
	}

	// Parse OutputSpec
	auto output_spec = ParseOutputSpec(output_spec_result.batch);
	output_schema_ = output_spec.output_schema;
	max_processes_ = output_spec.max_processes;
	cardinality_estimate_ = output_spec.cardinality_estimate;

	// Return results
	max_processes_out = max_processes_;
	cardinality_estimate_out = cardinality_estimate_;
	bind_done_ = true;

	return output_schema_;
}

void FunctionConnection::PerformInit(const std::vector<int32_t> &projection_ids) {
	if (!bind_done_) {
		throw IOException("FunctionConnection::PerformInit called before PerformBind");
	}
	if (init_done_) {
		return; // Already initialized
	}

	// Stream 3: Send InitInput (TableFunctionInitInput with projection_ids)
	auto init_input = CreateInitInput(projection_ids);
	auto init_input_bytes = SerializeRecordBatch(init_input);
	WriteAll(proc_->GetStdinFd(), init_input_bytes->data(), init_input_bytes->size());

	// Stream 4: Read InitResult
	// The worker sends a single batch with global_execution_identifier
	arrow::RecordBatchWithMetadata init_result;
	while (true) {
		init_result = ReadRecordBatch(proc_->GetStdoutFd());
		// Handle log messages (throws on EXCEPTION)
		if (!HandleBatchLogMessage(init_result.batch, init_result.custom_metadata, &context_, worker_path_)) {
			break;
		}
	}

	// Parse InitResult (we don't need the global_execution_identifier for single-worker)
	auto init_data = ParseInitResult(init_result.batch);
	(void)init_data; // Suppress unused variable warning

	// For Table functions (no input), close stdin to signal no more input
	proc_->CloseStdin();

	// Stream 6: Open the data stream reader
	// Unlike handshake streams which are one batch each, the data phase uses
	// a single long-lived IPC stream with multiple batches
	data_stream_ = std::make_shared<FdInputStream>(proc_->GetStdoutFd());
	auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(data_stream_);
	if (!reader_result.ok()) {
		throw IOException("Failed to open data stream: " + reader_result.status().ToString());
	}
	data_reader_ = reader_result.ValueUnsafe();

	init_done_ = true;
}

std::shared_ptr<arrow::RecordBatch> FunctionConnection::ReadDataBatch() {
	if (!init_done_) {
		throw IOException("FunctionConnection::ReadDataBatch called before PerformInit");
	}
	if (data_finished_ || !data_reader_) {
		return nullptr;
	}

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
		throw IOException("Failed to read data batch: " + error_msg);
	}
	result = read_result.ValueUnsafe();

	// Null batch means end of stream
	if (!result.batch) {
		data_finished_ = true;
		return nullptr;
	}

	// Check for log messages (will throw on EXCEPTION)
	if (HandleBatchLogMessage(result.batch, result.custom_metadata, &context_, worker_path_)) {
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

int FunctionConnection::Wait() {
	if (!proc_) {
		return 0;
	}
	return proc_->Wait();
}

// Helper function for bind-only schema retrieval
std::shared_ptr<arrow::Schema> GetFunctionSchema(const std::string &worker_path, const std::string &function_name,
                                                  const std::string &positional_args_json,
                                                  const std::vector<std::pair<std::string, std::string>> &named_args,
                                                  const std::vector<uint8_t> &attach_id, ClientContext &context,
                                                  int32_t &max_processes_out, int64_t &cardinality_estimate_out,
                                                  bool worker_debug) {
	// Create a temporary connection just for bind
	FunctionConnection conn(worker_path, function_name, positional_args_json, named_args, attach_id, context,
	                        worker_debug);

	// Perform bind to get schema
	auto schema = conn.PerformBind(max_processes_out, cardinality_estimate_out);

	// Also perform init to let the worker complete the handshake
	// The worker expects InitInput before it will proceed
	conn.PerformInit();

	// Drain any data output from the worker (we don't need it, just schema)
	// This is necessary because the worker won't exit until we read its output
	while (conn.ReadDataBatch() != nullptr) {
		// Discard data batches
	}

	// Now the worker has finished and will exit when we destroy the connection

	return schema;
}

} // namespace vgi
} // namespace duckdb
