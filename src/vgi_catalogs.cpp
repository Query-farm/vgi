#include "vgi_catalogs.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_logging.hpp"
#include "vgi_transport.hpp"

#include <string>
#include <vector>

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/extension_helper.hpp"

namespace duckdb {

namespace {

// ============================================================================
// Bind Data
// ============================================================================

struct VgiCatalogsBindData : public TableFunctionData {
	std::string worker_path;
};

// ============================================================================
// Global State
// ============================================================================

struct VgiCatalogsGlobalState : public GlobalTableFunctionState {
	std::vector<std::string> catalogs;
	idx_t current_idx = 0;
	bool done = false;

	idx_t MaxThreads() const override {
		return 1;
	}
};

// ============================================================================
// Bind Function
// ============================================================================

static unique_ptr<FunctionData> VgiCatalogsBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<VgiCatalogsBindData>();

	bind_data->worker_path = input.inputs[0].GetValue<string>();

	// HTTP transport: require httpfs for POST support
	if (vgi::IsHttpTransport(bind_data->worker_path)) {
		auto &db = DatabaseInstance::GetDatabase(context);
		try {
			ExtensionHelper::TryAutoLoadExtension(db, "httpfs");
		} catch (...) {
			// ignore auto-load errors, check below
		}
		if (!db.ExtensionIsLoaded("httpfs")) {
			throw BinderException("VGI HTTP transport requires the httpfs extension. "
			                      "Install it with: INSTALL httpfs; LOAD httpfs;");
		}
	}

	VGI_LOG(context, "vgi_catalogs.bind", {{"worker_path", bind_data->worker_path}});

	// Return type is a single column "catalog" of type VARCHAR
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("catalog");

	return bind_data;
}

// ============================================================================
// Init Global Function
// ============================================================================

static unique_ptr<GlobalTableFunctionState> VgiCatalogsInitGlobal(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<VgiCatalogsBindData>();
	auto state = make_uniq<VgiCatalogsGlobalState>();

	// Invoke catalog_catalogs via RPC
	state->catalogs = vgi::InvokeCatalogCatalogs(bind_data.worker_path, context);

	VGI_LOG(context, "vgi_catalogs.init",
	        {{"worker_path", bind_data.worker_path}, {"num_catalogs", std::to_string(state->catalogs.size())}});

	return state;
}

// ============================================================================
// Scan Function
// ============================================================================

static void VgiCatalogsScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<VgiCatalogsGlobalState>();

	if (state.done) {
		output.SetCardinality(0);
		return;
	}

	idx_t count = 0;
	idx_t max_count = STANDARD_VECTOR_SIZE;

	while (state.current_idx < state.catalogs.size() && count < max_count) {
		output.data[0].SetValue(count, Value(state.catalogs[state.current_idx]));
		count++;
		state.current_idx++;
	}

	if (state.current_idx >= state.catalogs.size()) {
		state.done = true;
	}

	output.SetCardinality(count);
}

// ============================================================================
// ToString Function - Returns info for EXPLAIN output
// ============================================================================

static InsertionOrderPreservingMap<string> VgiCatalogsToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind_data = input.bind_data->Cast<VgiCatalogsBindData>();
	result["Worker"] = bind_data.worker_path;
	return result;
}

} // anonymous namespace

// ============================================================================
// Registration
// ============================================================================

void RegisterVgiCatalogsFunction(ExtensionLoader &loader) {
	TableFunction func("vgi_catalogs", {LogicalType::VARCHAR}, VgiCatalogsScan, VgiCatalogsBind, VgiCatalogsInitGlobal);

	// Enable EXPLAIN output
	func.to_string = VgiCatalogsToString;

	loader.RegisterFunction(func);
}

} // namespace duckdb
