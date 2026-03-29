#include "vgi_table_function_impl.hpp"

#include "vgi_catalog_api.hpp"
#include "vgi_exception.hpp"
#include "vgi_logging.hpp"
#include "vgi_protocol.hpp"

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/planner/table_filter.hpp"

#include "yyjson.hpp"

#include <arrow/c/bridge.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {
namespace vgi {

// ============================================================================
// Perform Bind - Common bind logic for VGI table functions
// ============================================================================

void PerformVgiTableFunctionBind(ClientContext &context, VgiTableFunctionBindData &bind_data,
                                 vector<LogicalType> &return_types, vector<string> &names) {
	// Log the invocation
	VGI_LOG(context, "table_function.bind",
	        {{"worker_path", bind_data.worker_path},
	         {"function_name", bind_data.function_name},
	         {"num_args", bind_data.arguments.array ? std::to_string(bind_data.arguments.array->length()) : "0"}});

	// Validate that arguments type is a struct (defensive check)
	D_ASSERT(!bind_data.arguments.type || bind_data.arguments.type->id() == arrow::Type::STRUCT);

	// Create connection to worker and perform bind handshake.
	// Connection is preserved in bind_data for reuse in InitGlobal.
	// Uses helper that handles pool acquire and stale connection retry.
	FunctionConnectionParams params(bind_data.worker_path, bind_data.function_name, bind_data.arguments,
	                                bind_data.attach_id, bind_data.transaction_id,
	                                {} /* primary worker, no global exec ID */,
	                                bind_data.worker_debug, bind_data.settings, bind_data.use_pool, "bind", "TABLE",
	                                bind_data.required_secrets);

	auto result = AcquireAndBindConnection(context, params);
	bind_data.bind_connection = std::move(result.connection);
	auto &bind_result = result.bind_result;

	// At bind time, max_processes is unknown (updated after init)
	bind_data.max_processes = 1;
	bind_data.cardinality_estimate = -1;

	// Cache bind request bytes for lazy cardinality RPC in the cardinality callback
	bind_data.bind_request_bytes = bind_result.bind_request_bytes;
	bind_data.bind_opaque_data = bind_result.opaque_data;

	// bind_connection is preserved for reuse in InitGlobal

	// Convert Arrow schema to DuckDB types using centralized utility
	try {
		ArrowSchemaToDuckDBTypes(context, bind_result.output_schema, bind_data.c_schema, bind_data.arrow_table,
		                         return_types, names);
	} catch (const std::exception &e) {
		throw IOException("Failed to convert output schema for function '%s': %s", bind_data.function_name, e.what());
	}

	// Strip row_id column from return_types/names so DuckDB's physical column list excludes it.
	// The full schema (including row_id) is retained in arrow_table/c_schema for ArrowToDuckDB.
	if (bind_data.rowid_worker_col_index >= 0) {
		auto idx = static_cast<size_t>(bind_data.rowid_worker_col_index);
		if (idx < return_types.size()) {
			return_types.erase(return_types.begin() + idx);
			names.erase(names.begin() + idx);
		}
	}

	bind_data.all_column_names = names;

	VGI_LOG(context, "table_function.bind_result",
	        {{"worker_path", bind_data.worker_path},
	         {"worker_pid", std::to_string(bind_data.bind_connection->GetPid())},
	         {"function_name", bind_data.function_name},
	         {"num_columns", std::to_string(bind_result.output_schema->num_fields())}});
}

// ============================================================================
// FilterSerializer - Builds JSON filter structure and collects values
// ============================================================================

namespace {

//! Convert ExpressionType to operator string for JSON
const char *ExpressionTypeToOp(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		return "eq";
	case ExpressionType::COMPARE_NOTEQUAL:
		return "ne";
	case ExpressionType::COMPARE_GREATERTHAN:
		return "gt";
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return "ge";
	case ExpressionType::COMPARE_LESSTHAN:
		return "lt";
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return "le";
	default:
		return "unknown";
	}
}

//! FilterSerializer walks the filter tree, builds JSON, and collects values
class FilterSerializer {
public:
	FilterSerializer(const string &worker_path) : doc_(yyjson_mut_doc_new(nullptr)), worker_path_(worker_path) {
	}

	~FilterSerializer() {
		if (doc_) {
			yyjson_mut_doc_free(doc_);
		}
	}

	//! Serialize a single filter for a column
	yyjson_mut_val *SerializeColumnFilter(idx_t column_index, const string &column_name, const TableFilter &filter) {
		auto obj = yyjson_mut_obj(doc_);
		yyjson_mut_obj_add_strcpy(doc_, obj, "column_name", column_name.c_str());
		yyjson_mut_obj_add_uint(doc_, obj, "column_index", column_index);

		SerializeFilterInto(obj, filter, column_index, column_name);
		return obj;
	}

	//! Get the collected values
	const vector<Value> &GetValues() const {
		return values_;
	}

	//! Get the collected value types
	const vector<LogicalType> &GetValueTypes() const {
		return value_types_;
	}

	//! Write the JSON to a string (caller must free with free())
	char *WriteJson(yyjson_mut_val *root) {
		return yyjson_mut_val_write(root, 0, nullptr);
	}

	//! Get the document for creating arrays
	yyjson_mut_doc *GetDoc() {
		return doc_;
	}

private:
	//! Serialize filter fields into an existing object
	//! column_index and column_name are passed through for child filters in conjunctions
	void SerializeFilterInto(yyjson_mut_val *obj, const TableFilter &filter, idx_t column_index,
	                         const string &column_name) {
		switch (filter.filter_type) {
		case TableFilterType::CONSTANT_COMPARISON: {
			auto &const_filter = filter.Cast<ConstantFilter>();
			yyjson_mut_obj_add_str(doc_, obj, "type", "constant");
			yyjson_mut_obj_add_str(doc_, obj, "op", ExpressionTypeToOp(const_filter.comparison_type));
			yyjson_mut_obj_add_uint(doc_, obj, "value_ref", AddValue(const_filter.constant));
			break;
		}
		case TableFilterType::IS_NULL: {
			yyjson_mut_obj_add_str(doc_, obj, "type", "is_null");
			break;
		}
		case TableFilterType::IS_NOT_NULL: {
			yyjson_mut_obj_add_str(doc_, obj, "type", "is_not_null");
			break;
		}
		case TableFilterType::IN_FILTER: {
			auto &in_filter = filter.Cast<InFilter>();
			yyjson_mut_obj_add_str(doc_, obj, "type", "in");
			yyjson_mut_obj_add_uint(doc_, obj, "value_ref", AddListValue(in_filter.values));
			break;
		}
		case TableFilterType::CONJUNCTION_AND: {
			auto &conj_filter = filter.Cast<ConjunctionAndFilter>();
			yyjson_mut_obj_add_str(doc_, obj, "type", "and");
			auto children = yyjson_mut_arr(doc_);
			for (auto &child : conj_filter.child_filters) {
				auto child_obj = yyjson_mut_obj(doc_);
				// Children inherit column_name and column_index from parent
				yyjson_mut_obj_add_strcpy(doc_, child_obj, "column_name", column_name.c_str());
				yyjson_mut_obj_add_uint(doc_, child_obj, "column_index", column_index);
				SerializeFilterInto(child_obj, *child, column_index, column_name);
				yyjson_mut_arr_append(children, child_obj);
			}
			yyjson_mut_obj_add_val(doc_, obj, "children", children);
			break;
		}
		case TableFilterType::CONJUNCTION_OR: {
			auto &conj_filter = filter.Cast<ConjunctionOrFilter>();
			yyjson_mut_obj_add_str(doc_, obj, "type", "or");
			auto children = yyjson_mut_arr(doc_);
			for (auto &child : conj_filter.child_filters) {
				auto child_obj = yyjson_mut_obj(doc_);
				// Children inherit column_name and column_index from parent
				yyjson_mut_obj_add_strcpy(doc_, child_obj, "column_name", column_name.c_str());
				yyjson_mut_obj_add_uint(doc_, child_obj, "column_index", column_index);
				SerializeFilterInto(child_obj, *child, column_index, column_name);
				yyjson_mut_arr_append(children, child_obj);
			}
			yyjson_mut_obj_add_val(doc_, obj, "children", children);
			break;
		}
		case TableFilterType::STRUCT_EXTRACT: {
			auto &struct_filter = filter.Cast<StructFilter>();
			yyjson_mut_obj_add_str(doc_, obj, "type", "struct");
			yyjson_mut_obj_add_uint(doc_, obj, "child_index", struct_filter.child_idx);
			yyjson_mut_obj_add_strcpy(doc_, obj, "child_name", struct_filter.child_name.c_str());
			auto child_filter_obj = yyjson_mut_obj(doc_);
			// Struct child filter inherits column info from parent struct column
			yyjson_mut_obj_add_strcpy(doc_, child_filter_obj, "column_name", column_name.c_str());
			yyjson_mut_obj_add_uint(doc_, child_filter_obj, "column_index", column_index);
			SerializeFilterInto(child_filter_obj, *struct_filter.child_filter, column_index, column_name);
			yyjson_mut_obj_add_val(doc_, obj, "child_filter", child_filter_obj);
			break;
		}
		case TableFilterType::DYNAMIC_FILTER: {
			throw InvalidInputException(
			    "VGI filter pushdown failed for worker '%s': DynamicFilter cannot be serialized because the filter "
			    "value mutates during query execution (e.g., TOP-N optimization)",
			    worker_path_);
		}
		case TableFilterType::EXPRESSION_FILTER: {
			throw InvalidInputException(
			    "VGI filter pushdown failed for worker '%s': ExpressionFilter cannot be serialized because it "
			    "contains an expression tree that may reference functions unavailable in the worker",
			    worker_path_);
		}
		case TableFilterType::BLOOM_FILTER: {
			throw InvalidInputException(
			    "VGI filter pushdown failed for worker '%s': BloomFilter cannot be serialized because it contains a "
			    "large binary buffer from join optimization",
			    worker_path_);
		}
		case TableFilterType::OPTIONAL_FILTER: {
			auto &optional_filter = filter.Cast<OptionalFilter>();
			if (optional_filter.child_filter) {
				SerializeFilterInto(obj, *optional_filter.child_filter, column_index, column_name);
			}
			break;
		}
		default: {
			throw InvalidInputException(
			    "VGI filter pushdown failed for worker '%s': unknown filter type %d cannot be serialized",
			    worker_path_, static_cast<int>(filter.filter_type));
		}
		}
	}

	//! Add a value and return its reference index
	idx_t AddValue(const Value &value) {
		idx_t ref = values_.size();
		values_.push_back(value);
		value_types_.push_back(value.type());
		return ref;
	}

	//! Add a list value from multiple values (for IN filters) and return its reference index
	idx_t AddListValue(const vector<Value> &list_values) {
		// All values in an IN filter have the same type
		auto element_type = list_values[0].type();
		auto list_type = LogicalType::LIST(element_type);
		auto list_value = Value::LIST(element_type, list_values);

		idx_t ref = values_.size();
		values_.push_back(list_value);
		value_types_.push_back(list_type);
		return ref;
	}

	yyjson_mut_doc *doc_;
	string worker_path_;
	vector<Value> values_;
	vector<LogicalType> value_types_;
};

} // anonymous namespace

// ============================================================================
// Serialize Filters - Convert TableFilterSet to Arrow IPC bytes for worker
// ============================================================================

std::shared_ptr<arrow::Buffer> VgiSerializeFilters(ClientContext &context, const vector<column_t> &column_ids,
                                                   optional_ptr<TableFilterSet> filters,
                                                   const vector<string> &column_names, const string &worker_path) {
	// Return nullptr if no filters
	if (!filters || filters->filters.empty()) {
		return nullptr;
	}

	// Build JSON filter structure and collect values
	FilterSerializer serializer(worker_path);
	auto filter_array = yyjson_mut_arr(serializer.GetDoc());

	for (auto &entry : filters->filters) {
		idx_t col_idx = entry.first;
		auto &filter = *entry.second;

		// DuckDB's TableFilterSet uses indices into the projected column list (column_ids).
		// Map through column_ids to get the original schema column name, but keep the
		// projected index as column_index since the worker output follows projection order.
		idx_t original_col_idx = col_idx < column_ids.size() ? column_ids[col_idx] : col_idx;
		string col_name =
		    original_col_idx < column_names.size() ? column_names[original_col_idx] : std::to_string(original_col_idx);

		if (filter.filter_type == TableFilterType::OPTIONAL_FILTER) {
			// Optional filters (e.g., DynamicFilter from TOP-N) may contain unserializable
			// children. Skip them rather than failing the entire filter set.
			try {
				auto filter_obj = serializer.SerializeColumnFilter(col_idx, col_name, filter);
				yyjson_mut_arr_append(filter_array, filter_obj);
			} catch (const InvalidInputException &) {
				continue;
			}
		} else {
			auto filter_obj = serializer.SerializeColumnFilter(col_idx, col_name, filter);
			yyjson_mut_arr_append(filter_array, filter_obj);
		}
	}

	// Write JSON string
	char *json_str = serializer.WriteJson(filter_array);
	if (!json_str) {
		throw IOException("Failed to serialize filters to JSON");
	}
	string filter_spec(json_str);
	free(json_str);

	// Build Arrow RecordBatch with filter_spec + value columns
	auto &values = serializer.GetValues();
	auto &value_types = serializer.GetValueTypes();

	// Build types and names: filter_spec (VARCHAR) + value columns
	vector<LogicalType> types;
	vector<string> names;
	types.push_back(LogicalType::VARCHAR);
	names.push_back("filter_spec");
	for (idx_t i = 0; i < value_types.size(); i++) {
		types.push_back(value_types[i]);
		names.push_back("_val_" + std::to_string(i));
	}

	// Create single-row DataChunk and populate
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), types);
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value(filter_spec));
	for (idx_t i = 0; i < values.size(); i++) {
		chunk.SetValue(i + 1, 0, values[i]);
	}

	// Convert to Arrow via ArrowAppender
	ClientProperties client_props = context.GetClientProperties();
	ArrowAppender appender(types, 1, client_props, ArrowTypeExtensionData::GetExtensionTypes(context, types));
	appender.Append(chunk, 0, 1, 1);
	ArrowArray arr = appender.Finalize();

	// Build Arrow schema via C API
	ArrowSchema c_schema;
	ArrowConverter::ToArrowSchema(&c_schema, types, names, client_props);

	// Import to Arrow C++ RecordBatch
	auto import_result = arrow::ImportRecordBatch(&arr, &c_schema);
	if (!import_result.ok()) {
		throw IOException("Failed to import filter RecordBatch: %s", import_result.status().ToString());
	}
	auto record_batch = import_result.ValueUnsafe();

	// Add version metadata to filter_spec field (field 0)
	// We need to rebuild the schema with the metadata
	auto filter_spec_field = record_batch->schema()->field(0);
	auto metadata = arrow::KeyValueMetadata::Make({"vgi_filter_version"}, {"1"});
	auto new_field = filter_spec_field->WithMetadata(metadata);

	// Build new schema with the updated field
	std::vector<std::shared_ptr<arrow::Field>> new_fields;
	new_fields.push_back(new_field);
	for (int i = 1; i < record_batch->schema()->num_fields(); i++) {
		new_fields.push_back(record_batch->schema()->field(i));
	}
	auto new_schema = arrow::schema(new_fields);

	// Create new RecordBatch with updated schema
	record_batch = arrow::RecordBatch::Make(new_schema, record_batch->num_rows(), record_batch->columns());

	// Serialize to Arrow IPC stream format (schema + record batch)
	auto buffer_result = arrow::io::BufferOutputStream::Create();
	if (!buffer_result.ok()) {
		throw IOException("Failed to create buffer for filter IPC: %s", buffer_result.status().ToString());
	}
	auto buffer_stream = buffer_result.ValueUnsafe();

	auto writer_result = arrow::ipc::MakeStreamWriter(buffer_stream, record_batch->schema());
	if (!writer_result.ok()) {
		throw IOException("Failed to create IPC stream writer: %s", writer_result.status().ToString());
	}
	auto writer = writer_result.ValueUnsafe();

	auto write_status = writer->WriteRecordBatch(*record_batch);
	if (!write_status.ok()) {
		throw IOException("Failed to write filter RecordBatch to IPC stream: %s", write_status.ToString());
	}

	auto close_status = writer->Close();
	if (!close_status.ok()) {
		throw IOException("Failed to close IPC stream writer: %s", close_status.ToString());
	}

	auto finish_result = buffer_stream->Finish();
	if (!finish_result.ok()) {
		throw IOException("Failed to finish filter IPC buffer: %s", finish_result.status().ToString());
	}

	return finish_result.ValueUnsafe();
}

// ============================================================================
// Init Global Function - Performs init handshake with existing connection
// ============================================================================

unique_ptr<GlobalTableFunctionState> VgiTableFunctionInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<VgiTableFunctionBindData>();

	// Extract projection IDs from input.column_ids for the worker.
	// Only send projection IDs when the function supports projection pushdown.
	// When unsupported, send empty list (meaning "all columns" in the protocol).
	std::vector<int32_t> projection_ids;
	if (bind_data.projection_pushdown) {
		projection_ids.reserve(input.column_ids.size());
		for (auto col_id : input.column_ids) {
			if (col_id == COLUMN_IDENTIFIER_ROW_ID && bind_data.rowid_worker_col_index >= 0) {
				// Map rowid request to the actual worker column index
				projection_ids.push_back(static_cast<int32_t>(bind_data.rowid_worker_col_index));
			} else if (bind_data.rowid_worker_col_index >= 0 &&
			           col_id != COLUMN_IDENTIFIER_ROW_ID &&
			           col_id >= static_cast<idx_t>(bind_data.rowid_worker_col_index)) {
				// Shift to account for the excluded row_id column in worker's schema
				projection_ids.push_back(static_cast<int32_t>(col_id + 1));
			} else {
				projection_ids.push_back(static_cast<int32_t>(col_id));
			}
		}
	}

	// Move connection from bind phase (bind already completed, skip redundant handshake)
	auto connection = std::move(bind_data.bind_connection);

	// Serialize the filters (returns nullptr if no filters or if serialization fails)
	std::shared_ptr<arrow::Buffer> filter_bytes;
	try {
		filter_bytes =
		    VgiSerializeFilters(context, input.column_ids, input.filters, bind_data.all_column_names, bind_data.worker_path);
		if (filter_bytes) {
			VGI_LOG(context, "table_function.filters_serialized",
			        {{"function_name", bind_data.function_name},
			         {"filter_bytes_size", std::to_string(filter_bytes->size())}});
		}
	} catch (const InvalidInputException &e) {
		// Filter contains unsupported types - skip pushdown, let DuckDB filter locally
		VGI_LOG(context, "table_function.filter_pushdown_skipped",
		        {{"function_name", bind_data.function_name},
		         {"reason", e.what()}});
		filter_bytes = nullptr;
	}

	// Perform init phase via vgi_rpc with projection and filter pushdown
	auto init_result = connection->PerformInit(projection_ids, filter_bytes);

	auto global_state = make_uniq<VgiTableFunctionGlobalState>();
	global_state->global_execution_id = std::move(init_result.execution_id);
	global_state->max_processes = static_cast<idx_t>(init_result.max_workers);
	global_state->primary_connection = std::move(connection);

	VGI_LOG(context, "table_function.init_global",
	        {{"worker_path", bind_data.worker_path},
	         {"worker_pid", std::to_string(global_state->primary_connection->GetPid())},
	         {"function_name", bind_data.function_name},
	         {"global_execution_id", BytesToHex(global_state->global_execution_id)},
	         {"max_processes", std::to_string(global_state->max_processes)},
	         {"num_projection_columns", std::to_string(projection_ids.size())}});

	return global_state;
}

// ============================================================================
// Init Local Function - Create local state for scanning
// ============================================================================

unique_ptr<LocalTableFunctionState> VgiTableFunctionInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                              GlobalTableFunctionState *global_state_p) {
	auto &bind_data = input.bind_data->Cast<VgiTableFunctionBindData>();
	auto &global_state = global_state_p->Cast<VgiTableFunctionGlobalState>();

	auto current_chunk = make_uniq<ArrowArrayWrapper>();
	auto local_state = make_uniq<VgiTableFunctionLocalState>(std::move(current_chunk), context.client,
	                                                         bind_data.use_pool, bind_data.worker_path);

	// Set column_ids for ArrowToDuckDB projection support if function supports it
	// This tells ArrowToDuckDB which columns to extract from the Arrow arrays
	if (bind_data.projection_pushdown) {
		local_state->column_ids = input.column_ids;
		// When a rowid column exists, remap column_ids to match the worker's full schema indices.
		// COLUMN_IDENTIFIER_ROW_ID maps to the actual worker column index.
		// Physical columns at or after the rowid position shift by +1.
		// This allows ArrowToDuckDB to look up the correct type info in arrow_convert_data
		// (which was built from the full worker schema including the rowid column).
		if (bind_data.rowid_worker_col_index >= 0) {
			for (auto &col_id : local_state->column_ids) {
				if (col_id == COLUMN_IDENTIFIER_ROW_ID) {
					col_id = static_cast<idx_t>(bind_data.rowid_worker_col_index);
				} else if (col_id >= static_cast<idx_t>(bind_data.rowid_worker_col_index)) {
					col_id += 1;
				}
			}
		}
	}

	// Try to claim the primary connection from global_state (thread-safe check-and-move)
	std::unique_ptr<IFunctionConnection> primary_connection;
	{
		std::lock_guard<std::mutex> lock(global_state.connection_mutex);
		if (global_state.primary_connection) {
			primary_connection = std::move(global_state.primary_connection);
		}
	}

	if (primary_connection) {
		// Primary worker: use connection from bind phase
		VGI_LOG(context.client, "table_function.init_local",
		        {{"worker_path", bind_data.worker_path},
		         {"worker_pid", std::to_string(primary_connection->GetPid())},
		         {"function_name", bind_data.function_name},
		         {"global_execution_id", BytesToHex(global_state.global_execution_id)},
		         {"worker_type", "primary"}});

		local_state->connection = std::move(primary_connection);
	} else {
		// Secondary worker: create new connection with global_execution_id
		// Uses helper that handles pool acquire and stale connection retry.
		// The global_execution_id is passed via FunctionConnectionParams, and
		// PerformInit includes it in the InitRequest automatically.
		FunctionConnectionParams params(bind_data.worker_path, bind_data.function_name, bind_data.arguments,
		                                bind_data.attach_id, bind_data.transaction_id,
		                                global_state.global_execution_id, bind_data.worker_debug,
		                                bind_data.settings, bind_data.use_pool, "init_local_secondary", "TABLE",
		                                bind_data.required_secrets);

		auto result = AcquireAndBindConnection(context.client, params);
		local_state->connection = std::move(result.connection);

		// Secondary workers do a normal PerformInit with execution_id set in InitRequest
		local_state->connection->PerformInit();

		VGI_LOG(context.client, "table_function.init_local",
		        {{"worker_path", bind_data.worker_path},
		         {"worker_pid", std::to_string(local_state->connection->GetPid())},
		         {"function_name", bind_data.function_name},
		         {"global_execution_id", BytesToHex(global_state.global_execution_id)},
		         {"worker_type", "secondary"}});
	}

	return local_state;
}

// ============================================================================
// Helper: Get next batch from worker and convert to Arrow C ABI
// ============================================================================

static bool GetNextBatch(ClientContext &context, const VgiTableFunctionBindData &bind_data,
                         VgiTableFunctionLocalState &local_state) {
	if (local_state.done) {
		return false;
	}

	// Read next Arrow C++ batch from connection
	auto worker_pid = local_state.connection->GetPid();
	auto arrow_batch = local_state.connection->ReadDataBatch();
	if (!arrow_batch) {
		local_state.done = true;
		VGI_LOG(context, "table_function.scan_complete",
		        {{"worker_path", bind_data.worker_path},
		         {"worker_pid", std::to_string(worker_pid)},
		         {"function_name", bind_data.function_name}});
		return false;
	}

	// Export batch to C ABI format using centralized utility
	auto chunk = make_uniq<ArrowArrayWrapper>();
	ExportRecordBatch(arrow_batch, *chunk);

	local_state.chunk = shared_ptr<ArrowArrayWrapper>(chunk.release());
	local_state.chunk_offset = 0;
	// Reset() clears owned_data in array_states, which is REQUIRED so that ArrowToDuckDB
	// will update owned_data to point to the new chunk. Without this, owned_data still
	// points to the previous chunk, and when that chunk is released, the data becomes invalid.
	local_state.Reset();

	VGI_LOG(context, "table_function.batch_received",
	        {{"worker_path", bind_data.worker_path},
	         {"worker_pid", std::to_string(local_state.connection->GetPid())},
	         {"function_name", bind_data.function_name},
	         {"batch_rows", std::to_string(arrow_batch->num_rows())}});

	return true;
}

// ============================================================================
// Async Prefetch Helpers
// ============================================================================

void VgiPrefetchTask::Execute() {
	try {
		local_state_.prefetch_batch_ = local_state_.connection->ReadDataBatch();
		local_state_.prefetch_state_.store(PrefetchState::READY);
	} catch (...) {
		local_state_.prefetch_exception_ = std::current_exception();
		local_state_.prefetch_state_.store(PrefetchState::ERROR);
	}
}

static void LaunchPrefetch(TableFunctionInput &input, VgiTableFunctionLocalState &local_state) {
	local_state.prefetch_state_.store(PrefetchState::IN_FLIGHT);
	vector<unique_ptr<AsyncTask>> tasks;
	tasks.push_back(make_uniq<VgiPrefetchTask>(local_state));
	input.async_result = AsyncResult(std::move(tasks));
}

static void ConsumePrefetchedBatch(ClientContext &context, const VgiTableFunctionBindData &bind_data,
                                   VgiTableFunctionLocalState &local_state) {
	auto arrow_batch = std::move(local_state.prefetch_batch_);
	local_state.prefetch_batch_.reset();
	local_state.prefetch_state_.store(PrefetchState::IDLE);

	if (!arrow_batch) {
		local_state.done = true;
		VGI_LOG(context, "table_function.scan_complete",
		        {{"worker_path", bind_data.worker_path},
		         {"worker_pid", std::to_string(local_state.connection->GetPid())},
		         {"function_name", bind_data.function_name}});
		return;
	}

	auto chunk = make_uniq<ArrowArrayWrapper>();
	ExportRecordBatch(arrow_batch, *chunk);

	local_state.chunk = shared_ptr<ArrowArrayWrapper>(chunk.release());
	local_state.chunk_offset = 0;
	local_state.Reset();

	VGI_LOG(context, "table_function.batch_received",
	        {{"worker_path", bind_data.worker_path},
	         {"worker_pid", std::to_string(local_state.connection->GetPid())},
	         {"function_name", bind_data.function_name},
	         {"batch_rows", std::to_string(arrow_batch->num_rows())}});
}

// ============================================================================
// Helper: Convert current batch slice to DuckDB output
// ============================================================================

static void ConvertCurrentBatch(const VgiTableFunctionBindData &bind_data,
                                VgiTableFunctionGlobalState &global_state,
                                VgiTableFunctionLocalState &local_state, DataChunk &output) {
	idx_t output_size =
	    MinValue<idx_t>(STANDARD_VECTOR_SIZE, local_state.chunk->arrow_array.length - local_state.chunk_offset);

	output.SetCardinality(output_size);

	if (output_size > 0) {
		idx_t rowid_col = (!bind_data.projection_pushdown && bind_data.rowid_worker_col_index >= 0)
		                      ? static_cast<idx_t>(bind_data.rowid_worker_col_index)
		                      : COLUMN_IDENTIFIER_ROW_ID;
		ArrowTableFunction::ArrowToDuckDB(local_state, bind_data.arrow_table.GetColumns(), output,
		                                  bind_data.projection_pushdown, rowid_col);
	}

	local_state.chunk_offset += output.size();
	global_state.rows_read.fetch_add(output.size(), std::memory_order_relaxed);
	output.Verify();
}

//! Check whether the current batch has been fully consumed
static bool BatchExhausted(const VgiTableFunctionLocalState &local_state) {
	return !local_state.chunk || !local_state.chunk->arrow_array.release ||
	       local_state.chunk_offset >= static_cast<idx_t>(local_state.chunk->arrow_array.length);
}

// ============================================================================
// Scan Function
// ============================================================================

void VgiTableFunctionScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<VgiTableFunctionBindData>();
	auto &global_state = input.global_state->Cast<VgiTableFunctionGlobalState>();
	auto &local_state = input.local_state->Cast<VgiTableFunctionLocalState>();

	// Determine whether async prefetch is enabled for this scan
	bool is_async = (input.results_execution_mode == AsyncResultsExecutionMode::TASK_EXECUTOR);
	if (is_async) {
		Value async_val;
		if (context.TryGetCurrentSetting("vgi_async_prefetch", async_val)) {
			is_async = async_val.GetValue<bool>();
		}
	}

	// Fast path: current batch still has rows to consume
	if (!BatchExhausted(local_state)) {
		ConvertCurrentBatch(bind_data, global_state, local_state, output);
		return;
	}

	// Need a new batch
	if (local_state.done) {
		output.SetCardinality(0);
		return;
	}

	// Synchronous fallback: inline blocking I/O (original behavior)
	if (!is_async) {
		while (BatchExhausted(local_state)) {
			if (!GetNextBatch(context, bind_data, local_state)) {
				output.SetCardinality(0);
				return;
			}
		}
		ConvertCurrentBatch(bind_data, global_state, local_state, output);
		return;
	}

	// --- Async path ---
	auto state = local_state.prefetch_state_.load();

	if (state == PrefetchState::ERROR) {
		local_state.prefetch_state_.store(PrefetchState::IDLE);
		std::rethrow_exception(local_state.prefetch_exception_);
	}

	if (state == PrefetchState::READY) {
		ConsumePrefetchedBatch(context, bind_data, local_state);
		if (local_state.done) {
			output.SetCardinality(0);
			return;
		}
		ConvertCurrentBatch(bind_data, global_state, local_state, output);
		// Guard: if DuckDB-side filtering empties the chunk, IMPLICIT + empty = FINISHED.
		// Signal HAVE_MORE_OUTPUT to prevent premature termination.
		if (output.size() == 0) {
			input.async_result = AsyncResultType::HAVE_MORE_OUTPUT;
		}
		return;
	}

	if (state == PrefetchState::IDLE) {
		if (local_state.first_scan_call_) {
			// First batch: fetch synchronously to avoid an extra BLOCKED round-trip
			local_state.first_scan_call_ = false;
			while (BatchExhausted(local_state)) {
				if (!GetNextBatch(context, bind_data, local_state)) {
					output.SetCardinality(0);
					return;
				}
			}
			ConvertCurrentBatch(bind_data, global_state, local_state, output);
			if (output.size() == 0) {
				input.async_result = AsyncResultType::HAVE_MORE_OUTPUT;
			}
			return;
		}
		// Subsequent batches: launch prefetch and return BLOCKED
		LaunchPrefetch(input, local_state);
		output.SetCardinality(0);
		return;
	}

	// IN_FLIGHT: should be impossible — DuckDB won't call scan while task is running
	D_ASSERT(state != PrefetchState::IN_FLIGHT);
	throw InternalException("VgiTableFunctionScan: unexpected prefetch state IN_FLIGHT");
}

// ============================================================================
// Cardinality Function - Returns row count estimate from bind
// ============================================================================

unique_ptr<NodeStatistics> VgiTableFunctionCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<VgiTableFunctionBindData>();

	// Lazy fetch: make a single table_function_cardinality RPC call on first invocation
	if (!bind_data.cardinality_fetched && !bind_data.bind_request_bytes.empty()) {
		bind_data.cardinality_fetched = true;
		try {
			auto result = InvokeTableFunctionCardinality(bind_data.worker_path, bind_data.bind_request_bytes,
			                                             bind_data.bind_opaque_data, context, bind_data.worker_debug,
			                                             bind_data.use_pool);
			bind_data.cardinality_estimate = result.estimate;
		} catch (const std::exception &e) {
			// Not critical — continue with unknown cardinality
			VGI_LOG(context, "table_function.cardinality_error",
			        {{"worker_path", bind_data.worker_path},
			         {"function_name", bind_data.function_name},
			         {"error", e.what()}});
		}
	}

	if (bind_data.cardinality_estimate >= 0) {
		VGI_LOG(context, "table_function.cardinality",
		        {{"worker_path", bind_data.worker_path},
		         {"function_name", bind_data.function_name},
		         {"cardinality_estimate", std::to_string(bind_data.cardinality_estimate)}});
		return make_uniq<NodeStatistics>(static_cast<idx_t>(bind_data.cardinality_estimate));
	}

	VGI_LOG(context, "table_function.cardinality",
	        {{"worker_path", bind_data.worker_path},
	         {"function_name", bind_data.function_name},
	         {"cardinality_estimate", "unknown"}});
	// No estimate available
	return make_uniq<NodeStatistics>();
}

// ============================================================================
// Progress Function - Returns scan progress as percentage
// ============================================================================

double VgiTableFunctionProgress(ClientContext &context, const FunctionData *bind_data_p,
                                const GlobalTableFunctionState *global_state_p) {
	auto &bind_data = bind_data_p->Cast<VgiTableFunctionBindData>();
	auto &global_state = global_state_p->Cast<VgiTableFunctionGlobalState>();

	if (bind_data.cardinality_estimate > 0) {
		idx_t rows_read = global_state.rows_read.load();
		double progress =
		    (static_cast<double>(rows_read) / static_cast<double>(bind_data.cardinality_estimate)) * 100.0;
		progress = MinValue(progress, 100.0);
		return progress;
	}

	// No estimate available
	return -1.0;
}

// ============================================================================
// ToString Function - Returns info for EXPLAIN output
// ============================================================================

InsertionOrderPreservingMap<string> VgiTableFunctionToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind_data = input.bind_data->Cast<VgiTableFunctionBindData>();
	result["Worker"] = bind_data.worker_path;
	result["Function"] = bind_data.function_name;
	return result;
}

// ============================================================================
// Virtual Column Callbacks for Row ID Support
// ============================================================================

BindInfo VgiTableScanGetBindInfo(const optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<VgiTableFunctionBindData>();
	if (bind_data.table_entry) {
		return BindInfo(const_cast<TableCatalogEntry &>(*bind_data.table_entry));
	}
	return BindInfo(ScanType::EXTERNAL);
}

virtual_column_map_t VgiTableScanGetVirtualColumns(ClientContext &context, optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<VgiTableFunctionBindData>();
	if (bind_data.rowid_worker_col_index < 0) {
		return {};
	}
	virtual_column_map_t result;
	result.insert({COLUMN_IDENTIFIER_ROW_ID, TableColumn("rowid", bind_data.rowid_type)});
	return result;
}

vector<column_t> VgiTableScanGetRowIdColumns(ClientContext &context, optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<VgiTableFunctionBindData>();
	if (bind_data.rowid_worker_col_index < 0) {
		return {};
	}
	return {COLUMN_IDENTIFIER_ROW_ID};
}

} // namespace vgi
} // namespace duckdb
