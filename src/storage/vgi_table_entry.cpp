#include "storage/vgi_table_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/table_storage_info.hpp"

#include "storage/vgi_catalog.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_function_connection.hpp"
#include "vgi_logging.hpp"
#include "vgi_worker_pool.hpp"

#include "duckdb/main/extension_helper.hpp"

namespace duckdb {

VgiTableEntry::VgiTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                             const vgi::VgiTableInfo &table_info)
    : TableCatalogEntry(catalog, schema, info), table_info_(table_info), catalog_(catalog) {
}

VgiTableEntry::~VgiTableEntry() = default;

unique_ptr<BaseStatistics> VgiTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	// No statistics available for VGI tables
	return nullptr;
}

// Bind data for VGI table scan
struct VgiTableScanBindData : public TableFunctionData {
	std::string worker_path;
	std::vector<uint8_t> attach_id;
	std::string schema_name;
	std::string table_name;
	std::vector<std::string> column_names;
	std::vector<LogicalType> column_types;
	bool worker_debug = false;

	// From InvokeCatalogTableScanFunctionGet: the actual function to call
	std::string scan_function_name;
	duckdb::vector<Value> scan_positional_args;
	std::map<std::string, Value> scan_named_args;

	// Worker pool settings
	bool use_pool = true;
	size_t max_pool_size = 0;

	// Arrow schema for type conversion
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
};

// Global state for VGI table scan
struct VgiTableScanGlobalState : public GlobalTableFunctionState {
	std::unique_ptr<vgi::FunctionConnection> connection;
	bool done = false;
	bool use_pool = false;
	size_t max_pool_size = 0;

	~VgiTableScanGlobalState() {
		if (use_pool && connection && connection->CanBePooled()) {
			auto pooled = connection->ReleaseForPooling();
			if (pooled) {
				vgi::VgiWorkerPool::Instance().Release(std::move(pooled), max_pool_size);
			}
		}
	}

	idx_t MaxThreads() const override {
		return 1;
	}
};

// Local state for VGI table scan - extends ArrowScanLocalState for Arrow conversion
struct VgiTableScanLocalState : public ArrowScanLocalState {
	explicit VgiTableScanLocalState(unique_ptr<ArrowArrayWrapper> current_chunk, ClientContext &ctx)
	    : ArrowScanLocalState(std::move(current_chunk), ctx) {
	}
};

// Bind function for VGI table scan - this is not used since we provide pre-populated bind data
static unique_ptr<FunctionData> VgiTableScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	throw InternalException("VgiTableScanBind should not be called directly");
}

// Global init function for VGI table scan
static unique_ptr<GlobalTableFunctionState> VgiTableScanInitGlobal(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<VgiTableScanBindData>();
	auto state = make_uniq<VgiTableScanGlobalState>();

	// Convert scan function arguments from DuckDB Values to Arrow
	vector<std::pair<string, Value>> named_args_vec;
	for (auto &[k, v] : bind_data.scan_named_args) {
		named_args_vec.emplace_back(k, v);
	}
	auto arrow_args = vgi::BuildArgumentsFromValues(context, bind_data.scan_positional_args, named_args_vec);

	// Build FunctionConnectionParams for AcquireAndBindConnection
	// Uses the scan function name (from table_scan_function_get), NOT the table name.
	vgi::FunctionConnectionParams params;
	params.worker_path = bind_data.worker_path;
	params.function_name = bind_data.scan_function_name;
	params.arguments = arrow_args;
	params.attach_id = bind_data.attach_id;
	params.worker_debug = bind_data.worker_debug;
	params.use_pool = bind_data.use_pool;
	params.function_type = "TABLE";
	params.phase = "table_scan";

	// Acquire connection (from pool or fresh) and perform bind with stale retry
	auto result = vgi::AcquireAndBindConnection(context, params);

	// Build projection_ids from column_ids for projection pushdown
	std::vector<int32_t> projection_ids;
	for (auto &col_id : input.column_ids) {
		projection_ids.push_back(static_cast<int32_t>(col_id));
	}

	// Perform init to start streaming
	result.connection->PerformInit(projection_ids);

	state->connection = std::move(result.connection);
	state->use_pool = bind_data.use_pool;
	state->max_pool_size = bind_data.max_pool_size;
	return state;
}

// Local init function for VGI table scan
static unique_ptr<LocalTableFunctionState> VgiTableScanInitLocal(ExecutionContext &context,
                                                                 TableFunctionInitInput &input,
                                                                 GlobalTableFunctionState *global_state_p) {
	auto current_chunk = make_uniq<ArrowArrayWrapper>();
	auto local_state = make_uniq<VgiTableScanLocalState>(std::move(current_chunk), context.client);

	// Set up column_ids for projection
	local_state->column_ids = input.column_ids;

	return local_state;
}

// Helper: Get next batch from stream and convert to Arrow C ABI
static bool GetNextBatch(const VgiTableScanBindData &bind_data, VgiTableScanGlobalState &global_state,
                         VgiTableScanLocalState &local_state) {
	if (global_state.done) {
		return false;
	}

	// Read next Arrow C++ batch from connection
	auto arrow_batch = global_state.connection->ReadDataBatch();
	if (!arrow_batch) {
		global_state.done = true;
		return false;
	}

	// Export batch to C ABI format
	auto chunk = make_uniq<ArrowArrayWrapper>();
	vgi::ExportRecordBatch(arrow_batch, *chunk);

	local_state.chunk = shared_ptr<ArrowArrayWrapper>(chunk.release());
	local_state.chunk_offset = 0;
	local_state.Reset();

	return true;
}

// Main scan function for VGI table scan
static void VgiTableScanFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<VgiTableScanBindData>();
	auto &global_state = data.global_state->Cast<VgiTableScanGlobalState>();
	auto &local_state = data.local_state->Cast<VgiTableScanLocalState>();

	// Get a batch if we don't have one or we've exhausted the current one
	while (!local_state.chunk || !local_state.chunk->arrow_array.release ||
	       local_state.chunk_offset >= static_cast<idx_t>(local_state.chunk->arrow_array.length)) {
		if (!GetNextBatch(bind_data, global_state, local_state)) {
			output.SetCardinality(0);
			return;
		}
	}

	// Calculate output size
	idx_t output_size =
	    MinValue<idx_t>(STANDARD_VECTOR_SIZE, local_state.chunk->arrow_array.length - local_state.chunk_offset);

	output.SetCardinality(output_size);

	// Convert Arrow data to DuckDB using ArrowTableFunction::ArrowToDuckDB
	if (output_size > 0) {
		ArrowTableFunction::ArrowToDuckDB(local_state, bind_data.arrow_table.GetColumns(), output, false);
	}

	local_state.chunk_offset += output.size();
	output.Verify();
}

TableFunction VgiTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();

	// Create bind data with table info
	auto scan_bind_data = make_uniq<VgiTableScanBindData>();
	scan_bind_data->worker_path = attach_params->worker_path();
	scan_bind_data->attach_id = attach_result->attach_id;
	scan_bind_data->schema_name = ParentSchema().name;
	scan_bind_data->table_name = name;
	scan_bind_data->worker_debug = attach_params->worker_debug();

	// Read pool settings
	Value pool_enabled_val;
	if (context.TryGetCurrentSetting("vgi_worker_pool_enabled", pool_enabled_val)) {
		scan_bind_data->use_pool = pool_enabled_val.GetValue<bool>();
	}
	scan_bind_data->max_pool_size = vgi::VgiWorkerPool::GetMaxPoolSize(context);

	// Ask the worker which function to call to scan this table
	auto scan_result = vgi::InvokeCatalogTableScanFunctionGet(
	    attach_params->worker_path(), attach_result->attach_id,
	    ParentSchema().name, name, context, "", "",
	    attach_params->worker_debug(), true);
	scan_bind_data->scan_function_name = scan_result.function_name;
	scan_bind_data->scan_positional_args = scan_result.positional_arguments;
	scan_bind_data->scan_named_args = scan_result.named_arguments;

	// Load any required extensions before scanning
	for (auto &ext : scan_result.required_extensions) {
		ExtensionHelper::TryAutoLoadExtension(context, ext);
	}

	// Get column info from the table
	vector<LogicalType> return_types;
	vector<string> names;
	for (auto &col : GetColumns().Logical()) {
		scan_bind_data->column_names.push_back(col.Name());
		scan_bind_data->column_types.push_back(col.Type());
		return_types.push_back(col.Type());
		names.push_back(col.Name());
	}

	// Convert Arrow schema to DuckDB types for ArrowToDuckDB conversion
	// The table_info_ contains the Arrow schema from the worker
	if (table_info_.arrow_schema) {
		try {
			vgi::ArrowSchemaToDuckDBTypes(context, table_info_.arrow_schema, scan_bind_data->c_schema,
			                              scan_bind_data->arrow_table, return_types, names);
		} catch (const std::exception &e) {
			throw IOException("Failed to convert schema for table '%s': %s", name, e.what());
		}
	}

	bind_data = std::move(scan_bind_data);

	// Create the table function with local init
	TableFunction func("vgi_table_scan", {}, VgiTableScanFunction, VgiTableScanBind, VgiTableScanInitGlobal,
	                   VgiTableScanInitLocal);
	// FIXME: this depends on the function info, but mostly
	// the function will likely already exist defined in the catalog
	// elsewhere.
	func.projection_pushdown = true;
	return func;
}

TableStorageInfo VgiTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo info;
	info.cardinality = 0; // Unknown
	return info;
}

} // namespace duckdb
