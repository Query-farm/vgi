#include "vgi_catalogs.hpp"
#include "vgi_arrow_ipc.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_exception.hpp"
#include "vgi_logging.hpp"
#include "vgi_protocol.hpp"

#include <chrono>
#include <string>
#include <vector>

#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/logging/logging.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

namespace {

// Call the VGI worker to get catalog names
std::vector<std::string> GetCatalogsFromWorker(ClientContext &context, const std::string &worker_path) {
	auto start_time = std::chrono::steady_clock::now();

	VGI_LOG(context, "catalog.start", {{"worker_path", worker_path}, {"method", "catalogs"}});

	try {
		// Invoke the catalogs method
		auto args = vgi::CreateEmptyArgsBatch();
		auto result_batch = vgi::InvokeCatalogMethod(worker_path, "catalogs", args, context);

		// Extract catalog names from the "value" column
		auto catalogs = vgi::ExtractStringColumn(result_batch, "value");

		auto end_time = std::chrono::steady_clock::now();
		auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

		VGI_LOG(context, "catalog.complete",
		           {{"worker_path", worker_path},
		            {"method", "catalogs"},
		            {"num_catalogs", std::to_string(catalogs.size())},
		            {"duration_ms", std::to_string(duration_ms)}});

		return catalogs;
	} catch (const IOException &) {
		// Let IOException propagate - it already has good context
		throw;
	} catch (const Exception &e) {
		// Wrap other DuckDB exceptions with worker context
		vgi::ThrowVgiIOException("VGI worker failed: %s", worker_path, -1, "", e.what());
	} catch (const std::exception &e) {
		// Wrap std::exception with worker context
		vgi::ThrowVgiIOException("VGI worker failed: %s", worker_path, -1, "", e.what());
	}
}

// Bind data for the vgi_catalogs function
struct VgiCatalogsBindData : public TableFunctionData {
	std::string worker_path;
};

// Global state for the vgi_catalogs function
struct VgiCatalogsGlobalState : public GlobalTableFunctionState {
	std::vector<std::string> catalogs;
	idx_t current_idx = 0;
	bool done = false;

	idx_t MaxThreads() const override {
		return 1;
	}
};

// Bind function - sets up return types and extracts worker path
unique_ptr<FunctionData> VgiCatalogsBind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<VgiCatalogsBindData>();

	// Get the worker path from the first argument
	bind_data->worker_path = input.inputs[0].ToString();

	// Return type is a single column "catalog" of type VARCHAR
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("catalog");

	return bind_data;
}

// Init function - spawns worker and gets catalog list
unique_ptr<GlobalTableFunctionState> VgiCatalogsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<VgiCatalogsBindData>();
	auto state = make_uniq<VgiCatalogsGlobalState>();

	// Get catalogs from the worker
	state->catalogs = GetCatalogsFromWorker(context, bind_data.worker_path);

	return state;
}

// Main function - outputs catalog rows
void VgiCatalogsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<VgiCatalogsGlobalState>();

	if (state.done) {
		output.SetCardinality(0);
		return;
	}

	idx_t count = 0;
	auto max_count = STANDARD_VECTOR_SIZE;

	while (state.current_idx < state.catalogs.size() && count < max_count) {
		auto &catalog_name = state.catalogs[state.current_idx];
		output.data[0].SetValue(count, Value(catalog_name));
		count++;
		state.current_idx++;
	}

	if (state.current_idx >= state.catalogs.size()) {
		state.done = true;
	}

	output.SetCardinality(count);
}

} // anonymous namespace

void RegisterVgiCatalogsFunction(ExtensionLoader &loader) {
	auto vgi_catalogs_function = TableFunction("vgi_catalogs", {LogicalType::VARCHAR}, VgiCatalogsFunction,
	                                           VgiCatalogsBind, VgiCatalogsInit);

	loader.RegisterFunction(vgi_catalogs_function);
}

} // namespace duckdb
