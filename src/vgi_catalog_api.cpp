#include "vgi_catalog_api.hpp"

#include <poll.h>

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
// Helper: Send invocation and args to a worker process
// ============================================================================

static void SendInvocationAndArgs(SubProcess &proc, const std::string &method_name,
                                  const std::shared_ptr<arrow::RecordBatch> &args) {
	// Create and send the invocation with protocol state metadata
	auto invocation = CreateCatalogInvocation(method_name);
	auto invocation_metadata = CreateProtocolStateMetadata(ProtocolState::INVOCATION);
	auto invocation_bytes = SerializeRecordBatch(invocation, invocation_metadata);
	WriteAll(proc.GetStdinFd(), invocation_bytes->data(), invocation_bytes->size());

	// Send the arguments with catalog_args protocol state
	auto args_metadata = CreateProtocolStateMetadata(ProtocolState::CATALOG_ARGS);
	auto args_bytes = SerializeRecordBatch(args, args_metadata);
	WriteAll(proc.GetStdinFd(), args_bytes->data(), args_bytes->size());

	// Close stdin to signal end of input
	proc.CloseStdin();
}

// ============================================================================
// Worker Error Handling Helper
// ============================================================================

// Check if a worker process exited with an error and throw appropriate exception.
// Returns true if the process has exited, false if still running.
// Throws VgiIOException for exit codes 127 (not found), 126 (permission denied), or other non-zero.
// The error_context parameter customizes the message for non-special exit codes:
// - "failed to start" for errors during read attempts
// - "exited with status" for EOF/null batch cases
static bool CheckWorkerExitStatus(SubProcess &proc, const std::string &worker_path, const std::string &error_context,
                                  const std::string &invocation_id_hex = "") {
	int exit_status = 0;
	if (!proc.TryWait(&exit_status)) {
		return false; // Process still running
	}

	if (exit_status == 127) {
		ThrowVgiIOException("VGI worker not found or not executable", worker_path, proc.GetPid(), invocation_id_hex);
	} else if (exit_status == 126) {
		ThrowVgiIOException("VGI worker permission denied", worker_path, proc.GetPid(), invocation_id_hex);
	} else if (exit_status != 0) {
		ThrowVgiIOException("VGI worker %s (exit code %d)", worker_path, proc.GetPid(), invocation_id_hex,
		                    error_context, exit_status);
	}

	return true; // Process exited normally (exit_status == 0)
}

// ============================================================================
// CatalogMethod enum to string conversion
// ============================================================================

// ============================================================================
// Enum parsing functions
// ============================================================================

std::optional<VgiFunctionType> ParseVgiFunctionType(const std::string &value) {
	if (value == "scalar") {
		return VgiFunctionType::Scalar;
	} else if (value == "table" || value == "table_in_out") {
		// Both "table" and "table_in_out" map to Table type
		return VgiFunctionType::Table;
	} else if (value == "aggregate") {
		return VgiFunctionType::Aggregate;
	}
	return std::nullopt;
}

std::string VgiFunctionTypeToString(VgiFunctionType type) {
	switch (type) {
	case VgiFunctionType::Scalar:
		return "scalar";
	case VgiFunctionType::Table:
		return "table";
	case VgiFunctionType::Aggregate:
		return "aggregate";
	default:
		return "unknown";
	}
}

// Parse FunctionStability from wire format (Python enum .name)
// Wire format: "CONSISTENT", "VOLATILE", "CONSISTENT_WITHIN_QUERY"
static std::optional<FunctionStability> ParseFunctionStability(const std::string &value) {
	if (value == "CONSISTENT") {
		return FunctionStability::CONSISTENT;
	} else if (value == "VOLATILE") {
		return FunctionStability::VOLATILE;
	} else if (value == "CONSISTENT_WITHIN_QUERY") {
		return FunctionStability::CONSISTENT_WITHIN_QUERY;
	}
	return std::nullopt;
}

// Parse FunctionNullHandling from wire format (Python enum .name)
// Wire format: "DEFAULT", "SPECIAL"
static std::optional<FunctionNullHandling> ParseFunctionNullHandling(const std::string &value) {
	if (value == "DEFAULT") {
		return FunctionNullHandling::DEFAULT_NULL_HANDLING;
	} else if (value == "SPECIAL") {
		return FunctionNullHandling::SPECIAL_HANDLING;
	}
	return std::nullopt;
}

std::optional<VgiOrderPreservation> ParseVgiOrderPreservation(const std::string &value) {
	if (value == "PRESERVES_ORDER") {
		return VgiOrderPreservation::PreservesOrder;
	} else if (value == "NO_ORDER_GUARANTEE") {
		return VgiOrderPreservation::NoOrderGuarantee;
	}
	return std::nullopt;
}

// Parse AggregateOrderDependent from wire format (Python enum .name)
// Wire format: "ORDER_DEPENDENT", "NOT_ORDER_DEPENDENT"
static std::optional<AggregateOrderDependent> ParseAggregateOrderDependent(const std::string &value) {
	if (value == "ORDER_DEPENDENT") {
		return AggregateOrderDependent::ORDER_DEPENDENT;
	} else if (value == "NOT_ORDER_DEPENDENT") {
		return AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	}
	return std::nullopt;
}

// Parse AggregateDistinctDependent from wire format (Python enum .name)
// Wire format: "DISTINCT_DEPENDENT", "NOT_DISTINCT_DEPENDENT"
static std::optional<AggregateDistinctDependent> ParseAggregateDistinctDependent(const std::string &value) {
	if (value == "DISTINCT_DEPENDENT") {
		return AggregateDistinctDependent::DISTINCT_DEPENDENT;
	} else if (value == "NOT_DISTINCT_DEPENDENT") {
		return AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT;
	}
	return std::nullopt;
}

// ============================================================================
// CatalogMethod enum to string conversion
// ============================================================================

const char *CatalogMethodToString(CatalogMethod method) {
	switch (method) {
	case CatalogMethod::Catalogs:
		return "catalogs";
	case CatalogMethod::CatalogAttach:
		return "catalog_attach";
	case CatalogMethod::CatalogDetach:
		return "catalog_detach";
	case CatalogMethod::CatalogVersion:
		return "catalog_version";
	case CatalogMethod::CatalogTransactionBegin:
		return "catalog_transaction_begin";
	case CatalogMethod::CatalogTransactionCommit:
		return "catalog_transaction_commit";
	case CatalogMethod::CatalogTransactionRollback:
		return "catalog_transaction_rollback";
	case CatalogMethod::Schemas:
		return "schemas";
	case CatalogMethod::SchemaGet:
		return "schema_get";
	case CatalogMethod::SchemaContents:
		return "schema_contents";
	case CatalogMethod::TableGet:
		return "table_get";
	case CatalogMethod::TableScan:
		return "table_scan";
	case CatalogMethod::TableScanFunctionGet:
		return "table_scan_function_get";
	case CatalogMethod::ViewGet:
		return "view_get";
	case CatalogMethod::FunctionGet:
		return "function_get";
	default:
		throw InternalException("Unknown CatalogMethod value");
	}
}

// ============================================================================
// InvokeCatalogMethod - Single result batch
// ============================================================================

std::shared_ptr<arrow::RecordBatch> InvokeCatalogMethod(const std::string &worker_path, CatalogMethod method,
                                                        const std::shared_ptr<arrow::RecordBatch> &args,
                                                        ClientContext &context, bool worker_debug) {
	const char *method_name = CatalogMethodToString(method);

	// Spawn the worker process
	SubProcess proc(worker_path, worker_debug);

	VGI_LOG(
	    context, "catalog_method.invoke",
	    {{"worker_path", worker_path}, {"worker_pid", std::to_string(proc.GetPid())}, {"method_name", method_name}});

	// Send invocation and args
	SendInvocationAndArgs(proc, method_name, args);

	// Read batches, handling log messages until we get actual data
	arrow::RecordBatchWithMetadata result;
	while (true) {
		try {
			result = ReadRecordBatch(proc.GetStdoutFd(), worker_path, proc.GetPid());
		} catch (const IOException &e) {
			// Check if the worker process died early (e.g., command not found)
			CheckWorkerExitStatus(proc, worker_path, "failed to start");
			// Re-throw original error if process didn't die early
			throw;
		}

		// Null batch from ReadRecordBatch means EOF (pipe closed)
		// Check if the worker failed before sending data
		if (!result.batch) {
			CheckWorkerExitStatus(proc, worker_path, "exited with status");
			ThrowVgiIOException("VGI worker closed connection without sending data", worker_path, proc.GetPid(), "");
		}

		// Check for log messages (will throw on EXCEPTION)
		// If it was a log message, continue reading the next batch
		// Note: Catalog methods don't have invocation_id or attach_id
		if (!HandleBatchLogMessage(result.batch, result.custom_metadata, &context, worker_path, proc.GetPid(), "", "",
		                           "")) {
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

CatalogMethodStream::CatalogMethodStream(const std::string &worker_path, CatalogMethod method,
                                         const std::shared_ptr<arrow::RecordBatch> &args, ClientContext &context,
                                         bool worker_debug)
    : proc_(std::make_unique<SubProcess>(worker_path, worker_debug)), context_(context), worker_path_(worker_path) {
	const char *method_name = CatalogMethodToString(method);
	VGI_LOG(
	    context_, "catalog_method.invoke",
	    {{"worker_path", worker_path_}, {"worker_pid", std::to_string(proc_->GetPid())}, {"method_name", method_name}});
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
		CheckWorkerExitStatus(*proc_, worker_path_, "failed to start");
		// Re-throw - ReadRecordBatch now returns null batch for EOF instead of throwing
		throw;
	}

	// Null batch from ReadRecordBatch means EOF (pipe closed)
	// Check if the worker failed before sending data
	if (!result.batch) {
		CheckWorkerExitStatus(*proc_, worker_path_, "exited with status");
		finished_ = true;
		return nullptr;
	}

	// Check for log messages (will throw on EXCEPTION)
	// Note: A zero-row batch with log metadata is a log message, not end of stream
	// Catalog methods don't have invocation_id or attach_id
	if (HandleBatchLogMessage(result.batch, result.custom_metadata, &context_, worker_path_, proc_->GetPid(), "", "",
	                          "")) {
		// It was a log message - read the next batch
		return ReadNext();
	}

	// Validate protocol state for catalog results
	ValidateProtocolState(result.custom_metadata, ProtocolState::CATALOG_RESULT, "catalog result", worker_path_,
	                      proc_->GetPid());

	// Empty batch signals end of stream
	if (result.batch->num_rows() == 0) {
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
// Result parsing using RecordBatchSingleRow
// ============================================================================

// Helper to deserialize an Arrow RecordBatch from bytes
static std::shared_ptr<arrow::RecordBatch> DeserializeRecordBatch(const std::vector<uint8_t> &bytes) {
	auto buffer = arrow::Buffer::Wrap(bytes.data(), bytes.size());
	auto buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);
	auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(buffer_reader);
	if (!reader_result.ok()) {
		throw IOException("Failed to open IPC stream: %s", reader_result.status().ToString());
	}
	auto reader = reader_result.ValueUnsafe();

	std::shared_ptr<arrow::RecordBatch> batch;
	auto status = reader->ReadNext(&batch);
	if (!status.ok()) {
		throw IOException("Failed to read batch: %s", status.ToString());
	}
	return batch;
}

// Helper to extract a single value from an Arrow array at given row index
// Uses DuckDB type from ArrowSchemaToDuckDBTypes and constructs Value appropriately
static Value ExtractArrowValue(const std::shared_ptr<arrow::Array> &array, int64_t row_idx,
                               const LogicalType &duck_type) {
	if (!array || array->IsNull(row_idx)) {
		return Value(duck_type);
	}

	// Extract value based on Arrow type and construct DuckDB Value
	switch (array->type()->id()) {
	case arrow::Type::BOOL: {
		auto typed = std::static_pointer_cast<arrow::BooleanArray>(array);
		return Value::BOOLEAN(typed->Value(row_idx));
	}
	case arrow::Type::INT8: {
		auto typed = std::static_pointer_cast<arrow::Int8Array>(array);
		return Value::TINYINT(typed->Value(row_idx));
	}
	case arrow::Type::INT16: {
		auto typed = std::static_pointer_cast<arrow::Int16Array>(array);
		return Value::SMALLINT(typed->Value(row_idx));
	}
	case arrow::Type::INT32: {
		auto typed = std::static_pointer_cast<arrow::Int32Array>(array);
		return Value::INTEGER(typed->Value(row_idx));
	}
	case arrow::Type::INT64: {
		auto typed = std::static_pointer_cast<arrow::Int64Array>(array);
		return Value::BIGINT(typed->Value(row_idx));
	}
	case arrow::Type::UINT8: {
		auto typed = std::static_pointer_cast<arrow::UInt8Array>(array);
		return Value::UTINYINT(typed->Value(row_idx));
	}
	case arrow::Type::UINT16: {
		auto typed = std::static_pointer_cast<arrow::UInt16Array>(array);
		return Value::USMALLINT(typed->Value(row_idx));
	}
	case arrow::Type::UINT32: {
		auto typed = std::static_pointer_cast<arrow::UInt32Array>(array);
		return Value::UINTEGER(typed->Value(row_idx));
	}
	case arrow::Type::UINT64: {
		auto typed = std::static_pointer_cast<arrow::UInt64Array>(array);
		return Value::UBIGINT(typed->Value(row_idx));
	}
	case arrow::Type::FLOAT: {
		auto typed = std::static_pointer_cast<arrow::FloatArray>(array);
		return Value::FLOAT(typed->Value(row_idx));
	}
	case arrow::Type::DOUBLE: {
		auto typed = std::static_pointer_cast<arrow::DoubleArray>(array);
		return Value::DOUBLE(typed->Value(row_idx));
	}
	case arrow::Type::STRING: {
		auto typed = std::static_pointer_cast<arrow::StringArray>(array);
		return Value(typed->GetString(row_idx));
	}
	case arrow::Type::LARGE_STRING: {
		auto typed = std::static_pointer_cast<arrow::LargeStringArray>(array);
		return Value(typed->GetString(row_idx));
	}
	case arrow::Type::BINARY: {
		auto typed = std::static_pointer_cast<arrow::BinaryArray>(array);
		auto view = typed->GetView(row_idx);
		return Value::BLOB(reinterpret_cast<const_data_ptr_t>(view.data()), view.size());
	}
	case arrow::Type::LARGE_BINARY: {
		auto typed = std::static_pointer_cast<arrow::LargeBinaryArray>(array);
		auto view = typed->GetView(row_idx);
		return Value::BLOB(reinterpret_cast<const_data_ptr_t>(view.data()), view.size());
	}
	default:
		// For complex/unsupported types, try to get string representation
		auto scalar_result = array->GetScalar(row_idx);
		if (scalar_result.ok()) {
			return Value(scalar_result.ValueUnsafe()->ToString());
		}
		return Value(duck_type);
	}
}

// Helper to convert Arrow RecordBatch to vector of DuckDB Values
// Uses DuckDB's ArrowSchemaToDuckDBTypes for proper type mapping
static vector<Value> ArrowBatchToValues(ClientContext &context, const std::shared_ptr<arrow::RecordBatch> &batch) {
	vector<Value> result;
	if (!batch || batch->num_rows() == 0) {
		return result;
	}

	// Use DuckDB's proper Arrow schema conversion to get correct types
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
	vector<LogicalType> types;
	vector<string> names;
	ArrowSchemaToDuckDBTypes(context, batch->schema(), c_schema, arrow_table, types, names);

	// Extract value from each column at row 0
	for (int64_t col_idx = 0; col_idx < batch->num_columns(); col_idx++) {
		auto array = batch->column(col_idx);
		auto &duck_type = types[col_idx];
		result.push_back(ExtractArrowValue(array, 0, duck_type));
	}

	return result;
}

VgiSetting ParseVgiSetting(const std::vector<uint8_t> &bytes, const std::string &worker_path) {
	// Deserialize the Setting RecordBatch
	auto batch = DeserializeRecordBatch(bytes);
	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty Setting batch from worker: %s", worker_path);
	}

	RecordBatchSingleRow row(batch, 0, "Setting", worker_path);

	VgiSetting setting;
	setting.name = row["name"].value_not_null<std::string>();
	setting.description = row["description"].value_not_null<std::string>();

	// Parse the type from the serialized schema bytes
	auto type_bytes = row["type"].value_not_null<std::vector<uint8_t>>();
	auto type_buffer = arrow::Buffer::Wrap(type_bytes.data(), type_bytes.size());
	auto type_stream = std::make_shared<arrow::io::BufferReader>(type_buffer);
	auto type_schema_result = arrow::ipc::ReadSchema(type_stream.get(), nullptr);
	if (!type_schema_result.ok()) {
		throw IOException("Failed to read type schema for setting '%s': %s", setting.name,
		                  type_schema_result.status().ToString());
	}
	auto type_schema = type_schema_result.ValueUnsafe();
	auto arrow_type = type_schema->field(0)->type();

	// We need a context to convert Arrow type to DuckDB type
	// For now, use a simple mapping for common types
	switch (arrow_type->id()) {
	case arrow::Type::BOOL:
		setting.type = LogicalType::BOOLEAN;
		break;
	case arrow::Type::INT32:
		setting.type = LogicalType::INTEGER;
		break;
	case arrow::Type::INT64:
		setting.type = LogicalType::BIGINT;
		break;
	case arrow::Type::FLOAT:
		setting.type = LogicalType::FLOAT;
		break;
	case arrow::Type::DOUBLE:
		setting.type = LogicalType::DOUBLE;
		break;
	case arrow::Type::STRING:
	case arrow::Type::LARGE_STRING:
		setting.type = LogicalType::VARCHAR;
		break;
	default:
		// Default to VARCHAR for unknown types
		setting.type = LogicalType::VARCHAR;
		break;
	}

	// Parse the default value if present
	auto default_bytes_opt = row["default_value"].as<std::vector<uint8_t>>();
	if (default_bytes_opt && !default_bytes_opt->empty()) {
		auto default_batch = DeserializeRecordBatch(*default_bytes_opt);
		if (default_batch && default_batch->num_rows() > 0 && default_batch->num_columns() > 0) {
			setting.default_value = ExtractArrowValue(default_batch->column(0), 0, setting.type);
		} else {
			setting.default_value = Value(setting.type);
		}
	} else {
		setting.default_value = Value(setting.type);
	}

	return setting;
}

CatalogAttachResult ParseCatalogAttachResult(const std::shared_ptr<arrow::RecordBatch> &batch,
                                             const std::string &worker_path) {
	CatalogAttachResult result;

	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty response from catalog_attach");
	}

	RecordBatchSingleRow row(batch, 0, "CatalogAttachResult", worker_path);
	result.attach_id = row["attach_id"].value_not_null<std::vector<uint8_t>>();
	result.supports_transactions = row["supports_transactions"].value_not_null<bool>();
	result.supports_time_travel = row["supports_time_travel"].value_not_null<bool>();
	result.catalog_version_frozen = row["catalog_version_frozen"].value_not_null<bool>();
	result.catalog_version = row["catalog_version"].value_not_null<int64_t>();
	result.attach_id_required = row["attach_id_required"].value_not_null<bool>();
	result.default_schema = row["default_schema"].value_not_null<std::string>();
	if (result.default_schema.empty()) {
		result.default_schema = "main";
	}

	// Parse settings list - each element is a serialized Setting
	auto settings_bytes = row["settings"].value_or(std::vector<std::vector<uint8_t>> {});
	for (const auto &setting_bytes : settings_bytes) {
		result.settings.push_back(ParseVgiSetting(setting_bytes, worker_path));
	}

	return result;
}

VgiSchemaInfo ParseSchemaInfo(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &worker_path) {
	VgiSchemaInfo info;

	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty response from schema_get");
	}

	RecordBatchSingleRow row(batch, 0, "SchemaInfo", worker_path);
	info.name = row["name"].value_not_null<std::string>();
	info.comment = row["comment"].value_or("");
	info.tags = row["tags"].value_not_null<std::map<std::string, std::string>>();

	return info;
}

VgiTableInfo ParseTableInfo(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &worker_path) {
	VgiTableInfo info;

	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty response from table_get");
	}

	RecordBatchSingleRow row(batch, 0, "TableInfo", worker_path);
	info.name = row["name"].value_not_null<std::string>();
	info.schema_name = row["schema_name"].value_not_null<std::string>();
	info.comment = row["comment"].value_or("");
	info.tags = row["tags"].value_not_null<std::map<std::string, std::string>>();

	// Parse the columns field which contains a serialized Arrow schema
	auto columns_data = row["columns"].value_not_null<std::vector<uint8_t>>();
	info.arrow_schema = DeserializeSchema(columns_data);

	// Parse constraints (non-nullable arrays per protocol)
	auto not_null = row["not_null_constraints"].value_not_null<std::vector<int32_t>>();
	info.not_null_constraints = std::vector<int>(not_null.begin(), not_null.end());
	auto unique = row["unique_constraints"].value_not_null<std::vector<std::vector<int32_t>>>();
	for (const auto &u : unique) {
		info.unique_constraints.push_back(std::vector<int>(u.begin(), u.end()));
	}
	info.check_constraints = row["check_constraints"].value_not_null<std::vector<std::string>>();

	return info;
}

std::vector<VgiSchemaInfo> ParseSchemaList(const std::shared_ptr<arrow::RecordBatch> &batch,
                                           const std::string &worker_path) {
	std::vector<VgiSchemaInfo> schemas;

	if (!batch || batch->num_rows() == 0) {
		return schemas;
	}

	for (int64_t i = 0; i < batch->num_rows(); i++) {
		RecordBatchSingleRow row(batch, i, "SchemaInfo", worker_path);
		VgiSchemaInfo info;
		info.name = row["name"].value_not_null<std::string>();
		info.comment = row["comment"].value_or("");
		info.tags = row["tags"].value_not_null<std::map<std::string, std::string>>();
		schemas.push_back(std::move(info));
	}

	return schemas;
}

VgiViewInfo ParseViewInfo(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &worker_path) {
	VgiViewInfo info;

	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty response from view_get");
	}

	RecordBatchSingleRow row(batch, 0, "ViewInfo", worker_path);
	info.name = row["name"].value_not_null<std::string>();
	info.schema_name = row["schema_name"].value_not_null<std::string>();
	info.definition = row["definition"].value_not_null<std::string>();
	info.comment = row["comment"].value_or("");
	info.tags = row["tags"].value_not_null<std::map<std::string, std::string>>();

	return info;
}

VgiScanFunctionResult ParseScanFunctionResult(ClientContext &context, const std::shared_ptr<arrow::RecordBatch> &batch,
                                               const std::string &worker_path) {
	VgiScanFunctionResult result;

	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty response from table_scan_function_get");
	}

	RecordBatchSingleRow row(batch, 0, "ScanFunctionResult", worker_path);

	// Get function_name (required, non-nullable)
	result.function_name = row["function_name"].value_not_null<std::string>();

	// Get required_extensions (required, non-nullable list<string>)
	result.required_extensions = row["required_extensions"].value_or(std::vector<std::string> {});

	// Get arguments as binary and deserialize the nested IPC batch
	auto arguments_bytes = row["arguments"].value_not_null<std::vector<uint8_t>>();
	if (!arguments_bytes.empty()) {
		auto arguments_batch = DeserializeRecordBatch(arguments_bytes);
		if (arguments_batch && arguments_batch->num_rows() > 0) {
			// Convert the entire batch to DuckDB Values using proper conversion
			auto values = ArrowBatchToValues(context, arguments_batch);
			auto &schema = arguments_batch->schema();

			// Map values to positional or named arguments based on field names
			for (int i = 0; i < schema->num_fields(); i++) {
				const auto &field_name = schema->field(i)->name();
				auto &duck_value = values[i];

				// Check if this is a positional argument (arg_0, arg_1, etc.)
				if (field_name.rfind("arg_", 0) == 0) {
					// Extract index from arg_N
					try {
						size_t idx = std::stoul(field_name.substr(4));
						// Ensure positional_arguments vector is large enough
						if (idx >= result.positional_arguments.size()) {
							result.positional_arguments.resize(idx + 1);
						}
						result.positional_arguments[idx] = duck_value;
					} catch (const std::exception &) {
						// If parsing fails, treat as named argument
						result.named_arguments[field_name] = duck_value;
					}
				} else {
					// Named argument
					result.named_arguments[field_name] = duck_value;
				}
			}
		}
	}

	return result;
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

VgiFunctionInfo ParseFunctionInfo(const std::shared_ptr<arrow::RecordBatch> &batch, int64_t row_idx,
                                  const std::string &worker_path) {
	VgiFunctionInfo info;

	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty response from function_get");
	}

	if (row_idx >= batch->num_rows()) {
		throw IOException("Row index %lld out of range (batch has %lld rows)", row_idx, batch->num_rows());
	}

	RecordBatchSingleRow row(batch, row_idx, "FunctionInfo", worker_path);

	// Required fields (non-nullable per protocol)
	info.name = row["name"].value_not_null<std::string>();
	info.schema_name = row["schema_name"].value_not_null<std::string>();
	info.tags = row["tags"].value_not_null<std::map<std::string, std::string>>();

	// Parse function_type as enum (required, non-nullable)
	auto function_type_str = row["function_type"].value_not_null<std::string>();
	auto function_type = ParseVgiFunctionType(function_type_str);
	if (!function_type) {
		throw IOException("VGI worker '%s' returned unknown function_type '%s' for function '%s'", worker_path,
		                  function_type_str, info.name);
	}
	info.function_type = *function_type;

	// Optional string field for description
	info.description = row["description"].value_or("");

	// Parse optional enum fields (nullable per protocol)
	auto stability_str = row["stability"].as<std::string>();
	if (stability_str) {
		info.stability = ParseFunctionStability(*stability_str);
	}

	auto null_handling_str = row["null_handling"].as<std::string>();
	if (null_handling_str) {
		info.null_handling = ParseFunctionNullHandling(*null_handling_str);
	}

	auto order_preservation_str = row["order_preservation"].as<std::string>();
	if (order_preservation_str) {
		info.order_preservation = ParseVgiOrderPreservation(*order_preservation_str);
	}

	// Documentation fields
	// examples is a list of structs with {sql, description, expected_output} - extract sql strings
	auto examples_col = batch->GetColumnByName("examples");
	if (examples_col) {
		auto list_array = std::dynamic_pointer_cast<arrow::ListArray>(examples_col);
		if (list_array && !list_array->IsNull(row_idx)) {
			auto start = list_array->value_offset(row_idx);
			auto end = list_array->value_offset(row_idx + 1);
			auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(list_array->values());
			if (struct_array) {
				auto sql_field = struct_array->GetFieldByName("sql");
				auto sql_array = std::dynamic_pointer_cast<arrow::StringArray>(sql_field);
				if (sql_array) {
					for (int64_t i = start; i < end; i++) {
						if (!sql_array->IsNull(i)) {
							info.examples.push_back(sql_array->GetString(i));
						}
					}
				}
			}
		}
	}
	// categories is a simple list of strings
	info.categories = row["categories"].value_or(std::vector<std::string> {});

	// Parse the arguments field which contains a serialized Arrow schema (non-nullable)
	auto args_data = row["arguments"].value_not_null<std::vector<uint8_t>>();
	if (!args_data.empty()) {
		info.arguments_schema = DeserializeSchema(args_data);
	}

	// Parse the output_schema field which contains a serialized Arrow schema (non-nullable)
	auto output_data = row["output_schema"].value_not_null<std::vector<uint8_t>>();
	if (!output_data.empty()) {
		info.output_schema = DeserializeSchema(output_data);
	}

	// Table function capabilities (nullable booleans, stored as optional)
	info.projection_pushdown = row["projection_pushdown"].as<bool>();
	info.filter_pushdown = row["filter_pushdown"].as<bool>();

	// max_workers (nullable int, stored as optional)
	info.max_workers = row["max_workers"].as<int32_t>();

	// Aggregate function fields (non-nullable with defaults)
	auto order_dependent_str = row["order_dependent"].value_or(std::string {"NOT_ORDER_DEPENDENT"});
	auto order_dependent = ParseAggregateOrderDependent(order_dependent_str);
	info.order_dependent = order_dependent.value_or(AggregateOrderDependent::NOT_ORDER_DEPENDENT);

	auto distinct_dependent_str = row["distinct_dependent"].value_or(std::string {"NOT_DISTINCT_DEPENDENT"});
	auto distinct_dependent = ParseAggregateDistinctDependent(distinct_dependent_str);
	info.distinct_dependent = distinct_dependent.value_or(AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT);

	// Required settings for this function (list of strings)
	info.required_settings = row["required_settings"].value_or(std::vector<std::string> {});

	return info;
}

// ============================================================================
// Function Invocation API
// ============================================================================

VgiFunctionInfo GetFunctionInfo(const std::string &worker_path, const std::vector<uint8_t> &attach_id,
                                const std::string &schema_name, const std::string &function_name,
                                ClientContext &context, bool worker_debug) {
	auto args = CreateFunctionGetArgs(attach_id, schema_name, function_name);
	auto result_batch = InvokeCatalogMethod(worker_path, CatalogMethod::FunctionGet, args, context, worker_debug);
	return ParseFunctionInfo(result_batch, 0, worker_path);
}

// ============================================================================
// Connection Acquisition with Retry
// ============================================================================

AcquireAndBindResult AcquireAndBindConnection(ClientContext &context, const FunctionConnectionParams &params) {
	std::unique_ptr<FunctionConnection> conn;
	OutputSpecResult output_spec;
	bool from_pool = false;

	// Lambda to create a fresh connection
	auto create_fresh_connection = [&]() {
		return make_uniq<FunctionConnection>(params.worker_path, params.function_name, params.arguments,
		                                     params.attach_id, context, params.global_execution_id, params.worker_debug,
		                                     params.settings);
	};

	// Lambda to attempt bind, returns true on success, false if retry needed
	auto try_bind = [&](bool is_retry) -> bool {
		try {
			output_spec = conn->PerformBindFull();
			return true; // Success
		} catch (const IOException &e) {
			if (!is_retry && from_pool) {
				// Pooled worker was stale, signal retry needed
				VGI_LOG(context, "worker_pool.stale",
				        {{"worker_path", params.worker_path},
				         {"worker_pid", std::to_string(conn->GetPid())},
				         {"error", e.what()},
				         {"phase", params.phase}});
				return false; // Trigger retry
			}
			throw; // Fresh connection or retry failed, propagate
		}
	};

	// Try pool first
	if (params.use_pool) {
		auto pooled = VgiWorkerPool::Instance().TryAcquire(params.worker_path);
		if (pooled) {
			auto pooled_pid = pooled->GetPid();
			conn = make_uniq<FunctionConnection>(std::move(pooled), params.function_name, params.arguments,
			                                     params.attach_id, context, params.global_execution_id,
			                                     params.worker_debug, params.settings);
			from_pool = true;
			VGI_LOG(context, "worker_pool.acquire",
			        {{"worker_path", params.worker_path},
			         {"worker_pid", std::to_string(pooled_pid)},
			         {"result", "hit"},
			         {"phase", params.phase}});
		}
	}

	// Create fresh if pool miss
	if (!conn) {
		conn = create_fresh_connection();
		if (params.use_pool) {
			VgiWorkerPool::Instance().RecordMiss(params.worker_path);
		}
		VGI_LOG(context, "worker_pool.acquire",
		        {{"worker_path", params.worker_path},
		         {"worker_pid", std::to_string(conn->GetPid())},
		         {"result", params.use_pool ? "miss" : "disabled"},
		         {"phase", params.phase}});
	}

	// Attempt bind with single retry for stale pool connections
	if (!try_bind(false)) {
		// Pooled worker was stale, retry with fresh
		conn = create_fresh_connection();
		VGI_LOG(context, "worker_pool.acquire",
		        {{"worker_path", params.worker_path},
		         {"worker_pid", std::to_string(conn->GetPid())},
		         {"result", "retry_after_stale"},
		         {"phase", params.phase}});
		try_bind(true); // Throws if fails, no more retries
	}

	return AcquireAndBindResult {std::move(conn), std::move(output_spec)};
}

// ============================================================================
// FunctionConnection - Proper 6-Stream Protocol Implementation
// ============================================================================

FunctionConnection::FunctionConnection(const std::string &worker_path, const std::string &function_name,
                                       const ArrowArguments &arguments, const std::vector<uint8_t> &attach_id,
                                       ClientContext &context, const std::vector<uint8_t> &global_execution_id,
                                       bool worker_debug, const std::map<std::string, std::string> &settings)
    : worker_path_(worker_path), function_name_(function_name), arguments_type_(arguments.type),
      arguments_array_(arguments.array), attach_id_(attach_id), global_execution_id_(global_execution_id),
      context_(context), worker_debug_(worker_debug), settings_(settings) {
}

FunctionConnection::FunctionConnection(std::unique_ptr<PooledWorker> pooled_worker, const std::string &function_name,
                                       const ArrowArguments &arguments, const std::vector<uint8_t> &attach_id,
                                       ClientContext &context, const std::vector<uint8_t> &global_execution_id,
                                       bool worker_debug, const std::map<std::string, std::string> &settings)
    : worker_path_(pooled_worker->GetWorkerPath()), function_name_(function_name), arguments_type_(arguments.type),
      arguments_array_(arguments.array), attach_id_(attach_id), global_execution_id_(global_execution_id),
      context_(context), worker_debug_(worker_debug), settings_(settings), proc_(pooled_worker->Release()),
      stderr_fd_(pooled_worker->ReleaseStderrFd()) {
	// Start stderr reader thread for the existing subprocess (reusing the fd)
	StartStderrReader();
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

	// Spawn the worker process (unless we already have one from the pool)
	if (!proc_) {
		proc_ = std::make_unique<SubProcess>(worker_path_, worker_debug_);
		// Start stderr reader thread to prevent pipe buffer from blocking worker
		StartStderrReader();
	}

	// Log the invocation request
	int64_t num_args = arguments_array_ ? arguments_array_->length() : 0;
	VGI_LOG(context_, "function_connection.invoke",
	        {{"worker_path", worker_path_},
	         {"worker_pid", std::to_string(proc_->GetPid())},
	         {"function_name", function_name_},
	         {"num_args", std::to_string(num_args)}});

	// Stream 1: Send Invocation with protocol state metadata
	// Pass input_schema for table-in-out functions (nullptr for regular table functions)
	auto invocation = CreateFunctionInvocationFull(function_name_, arguments_type_, arguments_array_, attach_id_,
	                                               global_execution_id_, settings_, input_schema_);
	auto invocation_metadata = CreateProtocolStateMetadata(ProtocolState::INVOCATION);
	auto invocation_bytes = SerializeRecordBatch(invocation, invocation_metadata);
	WriteAll(proc_->GetStdinFd(), invocation_bytes->data(), invocation_bytes->size());

	// Stream 2: Read OutputSpec
	// Note: We don't have invocation_id yet (it comes in the OutputSpec response)
	arrow::RecordBatchWithMetadata output_spec_result;
	while (true) {
		try {
			output_spec_result = ReadRecordBatch(proc_->GetStdoutFd(), worker_path_, proc_->GetPid());
		} catch (const IOException &e) {
			// Check if the worker process died early (e.g., command not found)
			CheckWorkerExitStatus(*proc_, worker_path_, "failed to start");
			// Re-throw original error if process didn't die early
			throw;
		}
		// Handle log messages (throws on EXCEPTION)
		// Note: invocation_id not yet available during bind phase
		if (!HandleBatchLogMessage(output_spec_result.batch, output_spec_result.custom_metadata, &context_,
		                           worker_path_, proc_->GetPid(), "", GetAttachIdHex(), "")) {
			break;
		}
	}

	// Validate protocol state and parse OutputSpec
	ValidateProtocolState(output_spec_result.custom_metadata, ProtocolState::BIND_RESULT, "OutputSpec", worker_path_,
	                      proc_->GetPid());
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

InitResultData FunctionConnection::PerformInit(const std::vector<int32_t> &projection_ids,
                                               std::shared_ptr<arrow::Buffer> pushdown_filters) {
	if (!bind_done_) {
		ThrowVgiIOException("FunctionConnection::PerformInit called before PerformBind", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetInvocationIdHex());
	}
	if (init_done_) {
		ThrowVgiIOException("FunctionConnection::PerformInit called twice", worker_path_, proc_ ? proc_->GetPid() : -1,
		                    GetInvocationIdHex());
	}

	// Stream 3: Send InitInput with protocol state
	// For table functions: includes projection_ids and pushdown_filters
	// For scalar/table-in-out functions: both fields are null
	auto init_input = CreateInitInput(projection_ids, pushdown_filters);
	auto init_input_metadata = CreateProtocolStateMetadata(ProtocolState::INIT_INPUT);
	auto init_input_bytes = SerializeRecordBatch(init_input, init_input_metadata);
	WriteAll(proc_->GetStdinFd(), init_input_bytes->data(), init_input_bytes->size());

	// Stream 4: Read InitResult
	// The worker sends a single batch with global_execution_identifier
	arrow::RecordBatchWithMetadata init_result;
	while (true) {
		init_result = ReadRecordBatch(proc_->GetStdoutFd(), worker_path_, proc_->GetPid());
		// Handle log messages (throws on EXCEPTION)
		if (!HandleBatchLogMessage(init_result.batch, init_result.custom_metadata, &context_, worker_path_,
		                           proc_->GetPid(), GetInvocationIdHex(), GetAttachIdHex(), "")) {
			break;
		}
	}

	// Validate protocol state and parse InitResult
	ValidateProtocolState(init_result.custom_metadata, ProtocolState::INIT_RESULT, "InitResult", worker_path_,
	                      proc_->GetPid());
	auto init_data = ParseInitResult(init_result.batch);

	// Note: We no longer close stdin here to allow connection pooling.
	// The worker will receive EOF when the process is actually terminated.

	// Stream 6: Open the data stream reader
	// For table-in-out functions, we defer opening the reader because the worker
	// won't write output until it receives input. Opening the reader now would
	// block waiting for the schema.
	if (!input_schema_) {
		// Regular table function - worker will start writing immediately
		data_stream_ = std::make_shared<FdInputStream>(proc_->GetStdoutFd());
		auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(data_stream_);
		if (!reader_result.ok()) {
			ThrowVgiIOException("Failed to open data stream: %s", worker_path_, proc_->GetPid(), GetInvocationIdHex(),
			                    reader_result.status().ToString());
		}
		data_reader_ = reader_result.ValueUnsafe();
	}
	// For table-in-out functions (input_schema_ set), data_reader_ will be opened
	// lazily in ReadDataBatch after we've sent at least one input batch

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

	// For secondary workers, skip the init phase entirely.
	// The Python worker gets init data from shared storage (via global_execution_identifier),
	// not from stdin. It does NOT read InitInput, so we must NOT send it.
	// If we send InitInput, it will sit in the pipe and cause a protocol error when
	// the worker loops back expecting a new Invocation.

	// Open the data stream reader to read output batches
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
	if (data_finished_) {
		return nullptr;
	}

	// Lazily open the data reader for table-in-out functions
	// We defer opening until we've sent at least one input batch because the worker
	// won't write output until it receives input
	if (!data_reader_) {
		if (!input_schema_) {
			// Regular table function should have had data_reader_ opened in PerformInit
			ThrowVgiIOException("FunctionConnection::ReadDataBatch data_reader_ is null", worker_path_,
			                    proc_ ? proc_->GetPid() : -1, GetInvocationIdHex());
		}
		// Table-in-out function - open the reader now
		data_stream_ = std::make_shared<FdInputStream>(proc_->GetStdoutFd());
		auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(data_stream_);
		if (!reader_result.ok()) {
			ThrowVgiIOException("Failed to open data stream: %s", worker_path_, proc_->GetPid(), GetInvocationIdHex(),
			                    reader_result.status().ToString());
		}
		data_reader_ = reader_result.ValueUnsafe();
	}

	// Drain any buffered stderr to logging
	DrainStderrLog();

	// Read from the persistent data stream reader
	arrow::RecordBatchWithMetadata result;
	auto read_result = data_reader_->ReadNext();
	if (!read_result.ok()) {
		auto status = read_result.status();
		// Invalid status from IPC reader typically indicates end-of-stream
		// (e.g., trying to read past the stream end marker)
		if (status.IsInvalid()) {
			data_finished_ = true;
			return nullptr;
		}
		ThrowVgiIOException("Failed to read data batch: %s", worker_path_, proc_ ? proc_->GetPid() : -1,
		                    GetInvocationIdHex(), status.ToString());
	}
	result = read_result.ValueUnsafe();

	// Null batch means end of stream
	if (!result.batch) {
		data_finished_ = true;
		return nullptr;
	}

	// Check for log messages (will throw on EXCEPTION)
	if (HandleBatchLogMessage(result.batch, result.custom_metadata, &context_, worker_path_, proc_->GetPid(),
	                          GetInvocationIdHex(), GetAttachIdHex(), "")) {
		// It was a log message - for table-in-out functions, we need to send a "continue" signal
		// because the Python generator is waiting for input to resume after yielding the log message.
		// The protocol says: "When a Message is yielded, an empty batch is sent with the message
		// in metadata, and the current input is re-sent."
		// We send an empty batch to resume the generator (the Python side discards it for log messages).
		if (IsTableInOut() && input_writer_opened_ && !input_writer_closed_) {
			// Create and send an empty input batch with the input schema
			std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
			for (int i = 0; i < input_schema_->num_fields(); i++) {
				auto builder_result = arrow::MakeBuilder(input_schema_->field(i)->type());
				if (!builder_result.ok()) {
					ThrowVgiIOException("Failed to create Arrow builder: %s", worker_path_, proc_->GetPid(),
					                    GetInvocationIdHex(), builder_result.status().ToString());
				}
				auto array_result = builder_result.ValueUnsafe()->Finish();
				if (!array_result.ok()) {
					ThrowVgiIOException("Failed to finish Arrow array: %s", worker_path_, proc_->GetPid(),
					                    GetInvocationIdHex(), array_result.status().ToString());
				}
				empty_arrays.push_back(array_result.ValueUnsafe());
			}
			auto empty_batch = arrow::RecordBatch::Make(input_schema_, 0, empty_arrays);
			auto write_status = input_writer_->WriteRecordBatch(*empty_batch);
			if (!write_status.ok()) {
				ThrowVgiIOException("Failed to write continue batch after log message: %s", worker_path_,
				                    proc_->GetPid(), GetInvocationIdHex(), write_status.ToString());
			}
		}
		// Read the next batch
		return ReadDataBatch();
	}

	// Validate protocol state for output data batches
	ValidateProtocolState(result.custom_metadata, ProtocolState::OUTPUT, "data batch", worker_path_,
	                      proc_ ? proc_->GetPid() : -1);

	// Check vgi.status metadata for FINISHED or NEED_MORE_INPUT
	if (result.custom_metadata) {
		int status_idx = result.custom_metadata->FindKey("vgi.status");
		if (status_idx >= 0) {
			std::string status = result.custom_metadata->value(status_idx);
			if (status == "FINISHED") {
				data_finished_ = true;
				// For table-in-out functions, close the input writer to signal the Python worker
				// that we're done. This allows the worker to close its output writer and the
				// drain loop below to complete.
				if (IsTableInOut() && input_writer_opened_ && !input_writer_closed_) {
					CloseInputWriter();
				}
				// Drain remaining stream (EOS marker) to clean up for potential pooling
				while (data_reader_) {
					auto drain_result = data_reader_->ReadNext();
					if (!drain_result.ok() || !drain_result.ValueUnsafe().batch) {
						break;
					}
				}
				// Still return the batch if it has data
				if (result.batch->num_rows() > 0) {
					return result.batch;
				}
				return nullptr;
			} else if (status == "NEED_MORE_INPUT") {
				// Table-in-out function needs more input data
				needs_more_input_ = true;
				// Return the batch if it has data, otherwise nullptr
				// The caller should check NeedsMoreInput() and provide more data
				if (result.batch->num_rows() > 0) {
					return result.batch;
				}
				return nullptr;
			} else if (status == "HAVE_MORE_OUTPUT") {
				// The worker has more output to produce.
				// During finalize phase (after SendFinalize()), we need to send a "continue" signal
				// because the Python generator is waiting for input to resume after yielding.
				// During data phase, DuckDB will call the operator again with the same input,
				// which serves as the continue signal - we don't send anything extra.
				if (IsTableInOut() && finalize_sent_ && input_writer_opened_ && !input_writer_closed_) {
					// Create and send an empty input batch with the input schema
					std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
					for (int i = 0; i < input_schema_->num_fields(); i++) {
						auto builder_result = arrow::MakeBuilder(input_schema_->field(i)->type());
						if (!builder_result.ok()) {
							ThrowVgiIOException("Failed to create Arrow builder: %s", worker_path_, proc_->GetPid(),
							                    GetInvocationIdHex(), builder_result.status().ToString());
						}
						auto array_result = builder_result.ValueUnsafe()->Finish();
						if (!array_result.ok()) {
							ThrowVgiIOException("Failed to finish Arrow array: %s", worker_path_, proc_->GetPid(),
							                    GetInvocationIdHex(), array_result.status().ToString());
						}
						empty_arrays.push_back(array_result.ValueUnsafe());
					}
					auto empty_batch = arrow::RecordBatch::Make(input_schema_, 0, empty_arrays);
					auto write_status = input_writer_->WriteRecordBatch(*empty_batch);
					if (!write_status.ok()) {
						ThrowVgiIOException("Failed to write continue batch for HAVE_MORE_OUTPUT: %s", worker_path_,
						                    proc_->GetPid(), GetInvocationIdHex(), write_status.ToString());
					}
				}
				// Return the batch - caller will call ReadDataBatch again
				return result.batch;
			}
		}
	}

	// Null or empty batch signals end of stream
	if (!result.batch || result.batch->num_rows() == 0) {
		data_finished_ = true;
		// For table-in-out functions, close the input writer to signal the Python worker
		// that we're done. This allows the worker to close its output writer and the
		// drain loop below to complete.
		if (IsTableInOut() && input_writer_opened_ && !input_writer_closed_) {
			CloseInputWriter();
		}
		// Drain remaining stream (EOS marker) to clean up for potential pooling
		while (data_reader_) {
			auto drain_result = data_reader_->ReadNext();
			if (!drain_result.ok() || !drain_result.ValueUnsafe().batch) {
				break;
			}
		}
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

std::string FunctionConnection::GetAttachIdHex() const {
	if (attach_id_.empty()) {
		return "";
	}
	return BytesToHex(attach_id_);
}

void FunctionConnection::SetInputSchema(const std::shared_ptr<arrow::Schema> &input_schema) {
	if (bind_done_) {
		ThrowVgiIOException("FunctionConnection::SetInputSchema called after bind", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetInvocationIdHex());
	}
	input_schema_ = input_schema;
}

void FunctionConnection::OpenInputWriter() {
	if (!init_done_) {
		ThrowVgiIOException("FunctionConnection::OpenInputWriter called before PerformInit", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetInvocationIdHex());
	}
	if (!input_schema_) {
		ThrowVgiIOException("FunctionConnection::OpenInputWriter called on non-table-in-out function", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetInvocationIdHex());
	}
	if (input_writer_opened_) {
		ThrowVgiIOException("FunctionConnection::OpenInputWriter called twice", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetInvocationIdHex());
	}

	// Create an IPC stream writer for stdin (Stream 5)
	auto sink = std::make_shared<FdOutputStream>(proc_->GetStdinFd());
	auto writer_result = arrow::ipc::MakeStreamWriter(sink, input_schema_);
	if (!writer_result.ok()) {
		ThrowVgiIOException("Failed to create input stream writer: %s", worker_path_, proc_ ? proc_->GetPid() : -1,
		                    GetInvocationIdHex(), writer_result.status().ToString());
	}
	input_writer_ = writer_result.ValueUnsafe();
	input_writer_opened_ = true;

	VGI_LOG(context_, "function_connection.input_writer_opened",
	        {{"worker_path", worker_path_},
	         {"worker_pid", std::to_string(proc_->GetPid())},
	         {"invocation_id", GetInvocationIdHex()},
	         {"function_name", function_name_},
	         {"input_schema_fields", std::to_string(input_schema_->num_fields())}});
}

void FunctionConnection::WriteInputBatch(const std::shared_ptr<arrow::RecordBatch> &batch) {
	if (!input_writer_opened_) {
		ThrowVgiIOException("FunctionConnection::WriteInputBatch called before OpenInputWriter", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetInvocationIdHex());
	}
	if (input_writer_closed_) {
		ThrowVgiIOException("FunctionConnection::WriteInputBatch called after CloseInputWriter", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetInvocationIdHex());
	}

	// Create protocol state metadata for data batches
	auto data_metadata = CreateProtocolStateMetadata(ProtocolState::DATA);

	// Write the batch with metadata
	auto write_status = input_writer_->WriteRecordBatch(*batch, data_metadata);
	if (!write_status.ok()) {
		ThrowVgiIOException("Failed to write input batch: %s", worker_path_, proc_ ? proc_->GetPid() : -1,
		                    GetInvocationIdHex(), write_status.ToString());
	}

	VGI_LOG(context_, "function_connection.input_batch_written",
	        {{"worker_path", worker_path_},
	         {"worker_pid", std::to_string(proc_->GetPid())},
	         {"invocation_id", GetInvocationIdHex()},
	         {"function_name", function_name_},
	         {"batch_rows", std::to_string(batch->num_rows())}});

	// Drain any buffered stderr
	DrainStderrLog();
}

void FunctionConnection::SendFinalize() {
	if (!input_writer_opened_) {
		ThrowVgiIOException("FunctionConnection::SendFinalize called before OpenInputWriter", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetInvocationIdHex());
	}
	if (input_writer_closed_) {
		ThrowVgiIOException("FunctionConnection::SendFinalize called after CloseInputWriter", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetInvocationIdHex());
	}

	// Create an empty batch with the input schema
	std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
	for (int i = 0; i < input_schema_->num_fields(); i++) {
		auto builder_result = arrow::MakeBuilder(input_schema_->field(i)->type());
		if (!builder_result.ok()) {
			ThrowVgiIOException("Failed to create Arrow builder for finalize: %s", worker_path_, proc_->GetPid(),
			                    GetInvocationIdHex(), builder_result.status().ToString());
		}
		auto array_result = builder_result.ValueUnsafe()->Finish();
		if (!array_result.ok()) {
			ThrowVgiIOException("Failed to finish Arrow array for finalize: %s", worker_path_, proc_->GetPid(),
			                    GetInvocationIdHex(), array_result.status().ToString());
		}
		empty_arrays.push_back(array_result.ValueUnsafe());
	}
	auto empty_batch = arrow::RecordBatch::Make(input_schema_, 0, empty_arrays);

	// Create metadata with protocol state DATA and type FINALIZE
	// Keys vector: [vgi.protocol_state, type]
	// Values vector: [data, FINALIZE]
	auto finalize_metadata = arrow::KeyValueMetadata::Make(std::vector<std::string> {PROTOCOL_STATE_KEY, "type"},
	                                                       std::vector<std::string> {ProtocolState::DATA, "FINALIZE"});

	// Write the finalize batch
	auto write_status = input_writer_->WriteRecordBatch(*empty_batch, finalize_metadata);
	if (!write_status.ok()) {
		ThrowVgiIOException("Failed to write finalize batch: %s", worker_path_, proc_->GetPid(), GetInvocationIdHex(),
		                    write_status.ToString());
	}

	finalize_sent_ = true;

	VGI_LOG(context_, "function_connection.finalize_sent",
	        {{"worker_path", worker_path_},
	         {"worker_pid", std::to_string(proc_->GetPid())},
	         {"invocation_id", GetInvocationIdHex()},
	         {"function_name", function_name_}});

	// Drain any buffered stderr
	DrainStderrLog();
}

void FunctionConnection::CloseInputWriter() {
	if (!input_writer_opened_) {
		ThrowVgiIOException("FunctionConnection::CloseInputWriter called before OpenInputWriter", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetInvocationIdHex());
	}
	if (input_writer_closed_) {
		// Already closed, no-op
		return;
	}

	// Close the IPC stream writer (sends end-of-stream marker)
	auto close_status = input_writer_->Close();
	if (!close_status.ok()) {
		ThrowVgiIOException("Failed to close input stream: %s", worker_path_, proc_ ? proc_->GetPid() : -1,
		                    GetInvocationIdHex(), close_status.ToString());
	}
	input_writer_.reset();
	input_writer_closed_ = true;

	VGI_LOG(context_, "function_connection.input_writer_closed",
	        {{"worker_path", worker_path_},
	         {"worker_pid", std::to_string(proc_->GetPid())},
	         {"invocation_id", GetInvocationIdHex()},
	         {"function_name", function_name_}});

	// Drain any buffered stderr
	DrainStderrLog();
}

int FunctionConnection::Wait() {
	if (!proc_) {
		return 0;
	}
	return proc_->Wait();
}

bool FunctionConnection::CanBePooled() const {
	// Can only pool if:
	// 1. Data phase completed (data_finished_ is true)
	// 2. Subprocess is still alive
	if (!data_finished_ || !proc_) {
		return false;
	}
	return !proc_->TryWait(); // TryWait returns true if process exited
}

std::unique_ptr<PooledWorker> FunctionConnection::ReleaseForPooling() {
	if (!CanBePooled()) {
		return nullptr;
	}

	// Stop stderr thread but keep the fd open for reuse
	StopStderrReader(false);

	// Create pooled worker with our subprocess and stderr fd
	auto pooled = std::make_unique<PooledWorker>(std::move(proc_), worker_path_, stderr_fd_);
	stderr_fd_ = -1; // Ownership transferred to pooled worker

	// Clear our state so destructor doesn't try to use proc_
	proc_.reset();

	return pooled;
}

void FunctionConnection::StartStderrReader() {
	// If we don't have an fd yet, try to get one from the subprocess
	if (stderr_fd_ < 0) {
		if (!proc_ || proc_->GetStderrFd() < 0) {
			return; // No stderr to read (passthrough mode or no process)
		}
		stderr_fd_ = proc_->ReleaseStderrFd();
	}

	// Reset stop flag for new thread
	stderr_stop_.store(false, std::memory_order_relaxed);

	stderr_thread_ = std::thread([this]() {
		char buffer[4096];
		std::string line_buffer;

		struct pollfd pfd;
		pfd.fd = stderr_fd_;
		pfd.events = POLLIN;

		while (!stderr_stop_.load(std::memory_order_relaxed)) {
			// Use poll() with timeout to allow periodic checking of stderr_stop_
			// This avoids blocking indefinitely on read() which could cause
			// the destructor to hang waiting for the thread to join
			int poll_result = poll(&pfd, 1, 100); // 100ms timeout
			if (poll_result < 0) {
				if (errno == EINTR) {
					continue; // Interrupted by signal, retry
				}
				break; // Error
			}
			if (poll_result == 0) {
				continue; // Timeout, check stop flag and poll again
			}

			// Data available or hangup - read what's available
			ssize_t bytes_read = read(stderr_fd_, buffer, sizeof(buffer) - 1);
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

		// Note: fd is NOT closed here - it's owned by the class and closed in StopStderrReader(true)
	});
}

void FunctionConnection::StopStderrReader(bool close_fd) {
	stderr_stop_.store(true, std::memory_order_relaxed);
	if (stderr_thread_.joinable()) {
		stderr_thread_.join();
	}
	if (close_fd && stderr_fd_ >= 0) {
		close(stderr_fd_);
		stderr_fd_ = -1;
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
		        {{"worker_path", worker_path_}, {"worker_pid", std::to_string(worker_pid)}, {"message", line}});
	}
}

} // namespace vgi
} // namespace duckdb
