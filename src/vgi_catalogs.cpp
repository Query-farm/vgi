// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
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

struct VgiCatalogsBindData : public TableFunctionData {
	std::string worker_path;
};

struct VgiCatalogsGlobalState : public GlobalTableFunctionState {
	std::vector<vgi::VgiCatalogInfo> catalogs;
	idx_t current_idx = 0;
	bool done = false;

	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> VgiCatalogsBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<VgiCatalogsBindData>();

	bind_data->worker_path = input.inputs[0].GetValue<string>();

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

	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("catalog");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("implementation_version");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("data_version_spec");

	// Attach-time options declared by this catalog (for pre-attach discovery).
	// Each element describes one option; type/default rendered as VARCHAR for display.
	child_list_t<LogicalType> option_fields;
	option_fields.emplace_back("name", LogicalType::VARCHAR);
	option_fields.emplace_back("description", LogicalType::VARCHAR);
	option_fields.emplace_back("type", LogicalType::VARCHAR);
	option_fields.emplace_back("default_value", LogicalType::VARCHAR);
	return_types.push_back(LogicalType::LIST(LogicalType::STRUCT(std::move(option_fields))));
	names.push_back("attach_options");

	// Published data versions, newest-first. Empty when the worker has no
	// release history. See CatalogDataVersionRelease in vgi-python.
	child_list_t<LogicalType> release_fields;
	release_fields.emplace_back("version", LogicalType::VARCHAR);
	release_fields.emplace_back("released_at", LogicalType::TIMESTAMP_TZ);
	release_fields.emplace_back("summary", LogicalType::VARCHAR);
	release_fields.emplace_back("notes_url", LogicalType::VARCHAR);
	return_types.push_back(LogicalType::LIST(LogicalType::STRUCT(std::move(release_fields))));
	names.push_back("releases");

	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("source_url");

	return bind_data;
}

static unique_ptr<GlobalTableFunctionState> VgiCatalogsInitGlobal(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<VgiCatalogsBindData>();
	auto state = make_uniq<VgiCatalogsGlobalState>();

	state->catalogs = vgi::InvokeCatalogs(bind_data.worker_path, context);

	VGI_LOG(context, "vgi_catalogs.init",
	        {{"worker_path", bind_data.worker_path}, {"num_catalogs", std::to_string(state->catalogs.size())}});

	return state;
}

static void VgiCatalogsScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<VgiCatalogsGlobalState>();

	if (state.done) {
		output.SetCardinality(0);
		return;
	}

	idx_t count = 0;
	idx_t max_count = STANDARD_VECTOR_SIZE;

	while (state.current_idx < state.catalogs.size() && count < max_count) {
		const auto &info = state.catalogs[state.current_idx];
		output.data[0].SetValue(count, Value(info.name));
		output.data[1].SetValue(count, Value(info.implementation_version));
		output.data[2].SetValue(count, Value(info.data_version_spec));

		// Build LIST(STRUCT(...)) of attach options.
		child_list_t<LogicalType> struct_child_types;
		struct_child_types.emplace_back("name", LogicalType::VARCHAR);
		struct_child_types.emplace_back("description", LogicalType::VARCHAR);
		struct_child_types.emplace_back("type", LogicalType::VARCHAR);
		struct_child_types.emplace_back("default_value", LogicalType::VARCHAR);
		auto struct_type = LogicalType::STRUCT(struct_child_types);

		std::vector<Value> option_values;
		option_values.reserve(info.attach_option_specs.size());
		for (const auto &spec : info.attach_option_specs) {
			child_list_t<Value> struct_values;
			struct_values.emplace_back("name", Value(spec.name));
			struct_values.emplace_back("description", Value(spec.description));
			struct_values.emplace_back("type", Value(spec.type.ToString()));
			struct_values.emplace_back("default_value",
			                           spec.default_value.IsNull() ? Value(LogicalType::VARCHAR)
			                                                       : Value(spec.default_value.ToString()));
			option_values.push_back(Value::STRUCT(std::move(struct_values)));
		}
		output.data[3].SetValue(count, Value::LIST(struct_type, std::move(option_values)));

		// Build LIST(STRUCT(...)) of releases.
		child_list_t<LogicalType> release_child_types;
		release_child_types.emplace_back("version", LogicalType::VARCHAR);
		release_child_types.emplace_back("released_at", LogicalType::TIMESTAMP_TZ);
		release_child_types.emplace_back("summary", LogicalType::VARCHAR);
		release_child_types.emplace_back("notes_url", LogicalType::VARCHAR);
		auto release_struct_type = LogicalType::STRUCT(release_child_types);

		std::vector<Value> release_values;
		release_values.reserve(info.releases.size());
		for (const auto &release : info.releases) {
			child_list_t<Value> struct_values;
			struct_values.emplace_back("version", Value(release.version));
			struct_values.emplace_back("released_at",
			                           Value::TIMESTAMPTZ(timestamp_tz_t(release.released_at_us)));
			struct_values.emplace_back("summary", Value(release.summary));
			struct_values.emplace_back("notes_url",
			                           release.notes_url.empty() ? Value(LogicalType::VARCHAR)
			                                                     : Value(release.notes_url));
			release_values.push_back(Value::STRUCT(std::move(struct_values)));
		}
		output.data[4].SetValue(count, Value::LIST(release_struct_type, std::move(release_values)));

		output.data[5].SetValue(count, info.source_url.empty() ? Value(LogicalType::VARCHAR)
		                                                      : Value(info.source_url));

		count++;
		state.current_idx++;
	}

	if (state.current_idx >= state.catalogs.size()) {
		state.done = true;
	}

	output.SetCardinality(count);
}

static InsertionOrderPreservingMap<string> VgiCatalogsToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind_data = input.bind_data->Cast<VgiCatalogsBindData>();
	result["Worker"] = bind_data.worker_path;
	return result;
}

} // anonymous namespace

void RegisterVgiCatalogsFunction(ExtensionLoader &loader) {
	TableFunction func("vgi_catalogs", {LogicalType::VARCHAR}, VgiCatalogsScan, VgiCatalogsBind, VgiCatalogsInitGlobal);

	// Enable EXPLAIN output
	func.to_string = VgiCatalogsToString;

	loader.RegisterFunction(func);
}

} // namespace duckdb
