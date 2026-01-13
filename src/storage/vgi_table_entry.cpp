#include "storage/vgi_table_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/table_storage_info.hpp"

#include "storage/vgi_catalog.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_protocol.hpp"

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
};

// Global state for VGI table scan
struct VgiTableScanGlobalState : public GlobalTableFunctionState {
	std::unique_ptr<vgi::CatalogMethodStream> stream;

	idx_t MaxThreads() const override {
		return 1;
	}
};

// Bind function for VGI table scan - this is not used since we provide pre-populated bind data
static unique_ptr<FunctionData> VgiTableScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	throw InternalException("VgiTableScanBind should not be called directly");
}

// Init function for VGI table scan
static unique_ptr<GlobalTableFunctionState> VgiTableScanInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<VgiTableScanBindData>();
	auto state = make_uniq<VgiTableScanGlobalState>();

	// Create streaming method call for table_scan
	auto args = vgi::CreateTableGetArgs(bind_data.attach_id, bind_data.schema_name, bind_data.table_name);
	state->stream = make_uniq<vgi::CatalogMethodStream>(bind_data.worker_path, "table_scan", args, context, bind_data.worker_debug);

	return state;
}

// Main scan function for VGI table scan
static void VgiTableScanFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<VgiTableScanGlobalState>();

	if (!state.stream || state.stream->IsFinished()) {
		output.SetCardinality(0);
		return;
	}

	// Read next batch from stream
	auto batch = state.stream->ReadNext();

	if (!batch) {
		// End of stream
		output.SetCardinality(0);
		return;
	}

	// Convert Arrow batch to DuckDB DataChunk
	idx_t row_count = batch->num_rows();
	output.SetCardinality(row_count);

	for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
		auto &out_vec = output.data[col_idx];
		auto arrow_col = batch->column(col_idx);

		// Simple conversion for common types
		// TODO: Handle more types and nested structures
		switch (out_vec.GetType().id()) {
		case LogicalTypeId::VARCHAR: {
			auto string_array = std::static_pointer_cast<arrow::StringArray>(arrow_col);
			for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
				if (string_array->IsNull(row_idx)) {
					FlatVector::SetNull(out_vec, row_idx, true);
				} else {
					auto val = string_array->GetString(row_idx);
					FlatVector::GetData<string_t>(out_vec)[row_idx] =
					    StringVector::AddString(out_vec, val.data(), val.size());
				}
			}
			break;
		}
		case LogicalTypeId::BIGINT: {
			auto int_array = std::static_pointer_cast<arrow::Int64Array>(arrow_col);
			for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
				if (int_array->IsNull(row_idx)) {
					FlatVector::SetNull(out_vec, row_idx, true);
				} else {
					FlatVector::GetData<int64_t>(out_vec)[row_idx] = int_array->Value(row_idx);
				}
			}
			break;
		}
		case LogicalTypeId::INTEGER: {
			auto int_array = std::static_pointer_cast<arrow::Int32Array>(arrow_col);
			for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
				if (int_array->IsNull(row_idx)) {
					FlatVector::SetNull(out_vec, row_idx, true);
				} else {
					FlatVector::GetData<int32_t>(out_vec)[row_idx] = int_array->Value(row_idx);
				}
			}
			break;
		}
		case LogicalTypeId::DOUBLE: {
			auto double_array = std::static_pointer_cast<arrow::DoubleArray>(arrow_col);
			for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
				if (double_array->IsNull(row_idx)) {
					FlatVector::SetNull(out_vec, row_idx, true);
				} else {
					FlatVector::GetData<double>(out_vec)[row_idx] = double_array->Value(row_idx);
				}
			}
			break;
		}
		case LogicalTypeId::BOOLEAN: {
			auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(arrow_col);
			for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
				if (bool_array->IsNull(row_idx)) {
					FlatVector::SetNull(out_vec, row_idx, true);
				} else {
					FlatVector::GetData<bool>(out_vec)[row_idx] = bool_array->Value(row_idx);
				}
			}
			break;
		}
		default:
			// For unsupported types, set to NULL
			for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
				FlatVector::SetNull(out_vec, row_idx, true);
			}
			break;
		}
	}
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

	// Get column info from the table
	for (auto &col : GetColumns().Logical()) {
		scan_bind_data->column_names.push_back(col.Name());
		scan_bind_data->column_types.push_back(col.Type());
	}

	bind_data = std::move(scan_bind_data);

	// Create the table function
	TableFunction func("vgi_table_scan", {}, VgiTableScanFunction, VgiTableScanBind, VgiTableScanInit);
	return func;
}

TableStorageInfo VgiTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo info;
	info.cardinality = 0; // Unknown
	return info;
}

} // namespace duckdb
