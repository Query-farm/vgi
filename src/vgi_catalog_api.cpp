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
		ThrowVgiIOException("VGI worker %s (exit code %d)", worker_path, proc.GetPid(), invocation_id_hex, error_context,
		                    exit_status);
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

	VGI_LOG(context, "catalog_method.invoke",
	        {{"worker_path", worker_path},
	         {"worker_pid", std::to_string(proc.GetPid())},
	         {"method_name", method_name}});

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

CatalogMethodStream::CatalogMethodStream(const std::string &worker_path, CatalogMethod method,
                                         const std::shared_ptr<arrow::RecordBatch> &args, ClientContext &context,
                                         bool worker_debug)
    : proc_(std::make_unique<SubProcess>(worker_path, worker_debug)), context_(context), worker_path_(worker_path) {
	const char *method_name = CatalogMethodToString(method);
	VGI_LOG(context_, "catalog_method.invoke",
	        {{"worker_path", worker_path_},
	         {"worker_pid", std::to_string(proc_->GetPid())},
	         {"method_name", method_name}});
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
	// Catalog methods don't have invocation_id
	if (HandleBatchLogMessage(result.batch, result.custom_metadata, &context_, worker_path_, proc_->GetPid(), "")) {
		// It was a log message - read the next batch
		return ReadNext();
	}

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

	// Optional string field for comment
	info.comment = row["comment"].value_or("");

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
	info.categories = row["categories"].value_or(std::vector<std::string>{});

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
	auto order_dependent_str = row["order_dependent"].value_or(std::string{"NOT_ORDER_DEPENDENT"});
	auto order_dependent = ParseAggregateOrderDependent(order_dependent_str);
	info.order_dependent = order_dependent.value_or(AggregateOrderDependent::NOT_ORDER_DEPENDENT);

	auto distinct_dependent_str = row["distinct_dependent"].value_or(std::string{"NOT_DISTINCT_DEPENDENT"});
	auto distinct_dependent = ParseAggregateDistinctDependent(distinct_dependent_str);
	info.distinct_dependent = distinct_dependent.value_or(AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT);

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
			CheckWorkerExitStatus(*proc_, worker_path_, "failed to start");
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

	// Null or empty batch signals end of stream
	if (!result.batch || result.batch->num_rows() == 0) {
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

		struct pollfd pfd;
		pfd.fd = stderr_fd;
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
