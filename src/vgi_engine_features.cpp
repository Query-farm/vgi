// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// vgi_engine_features(): the engine-dependent VGI capabilities this build was
// compiled with (Haybarn vs upstream DuckDB; see vgi_engine_compat.hpp).
#include "vgi_engine_features.hpp"

#include <vector>

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "vgi_engine_compat.hpp"
#include "vgi_function_docs.hpp"

namespace duckdb {

namespace {

struct EngineFeature {
	const char *name;
	bool available;
	const char *reference;
	const char *without;
};

const std::vector<EngineFeature> &EngineFeatures() {
	static const std::vector<EngineFeature> features = {
	    {"haybarn_engine", VGI_ENGINE_HAYBARN, "duckdb/haybarn.h",
	     "built against upstream DuckDB; the rows below describe what that changes"},
	    {"http_cancellation", VGI_ENGINE_HAS_HTTP_CANCELLATION, "Haybarn HTTPParams::interrupt_context",
	     "an interrupted query waits for its in-flight HTTP request to finish or time out"},
	    {"http_params_clone", VGI_ENGINE_HAS_HTTP_PARAMS_CLONE, "duckdb/duckdb#28088",
	     "HTTP requests share the catalog's cached parameters instead of a per-request copy"},
	    {"delete_returning_virtual_columns", VGI_ENGINE_HAS_DELETE_RETURNING_VIRTUAL_COLUMNS, "duckdb/duckdb#28089",
	     "DELETE ... RETURNING is refused before any row is deleted"},
	    {"runtime_filter_pushdown", VGI_ENGINE_HAS_RUNTIME_FILTER_PUSHDOWN_FLAG, "duckdb/duckdb#28090",
	     "a hash join that picks a Bloom or prefix-range filter pushes no min/max bounds to a VGI scan"},
	    {"correlated_named_args", VGI_ENGINE_HAS_CORRELATED_NAMED_ARGS, "duckdb/duckdb#27833",
	     "named arguments are rejected in correlated calls such as FROM t, LATERAL f(t.x, opt := 1)"},
	};
	return features;
}

struct VgiEngineFeaturesState : public GlobalTableFunctionState {
	idx_t offset = 0;
};

unique_ptr<FunctionData> VgiEngineFeaturesBind(ClientContext &, TableFunctionBindInput &,
                                               vector<LogicalType> &return_types, vector<Identifier> &names) {
	names = {"feature", "available", "reference", "when_unavailable"};
	return_types = {LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::VARCHAR, LogicalType::VARCHAR};
	return make_uniq<TableFunctionData>();
}

unique_ptr<GlobalTableFunctionState> VgiEngineFeaturesInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<VgiEngineFeaturesState>();
}

void VgiEngineFeaturesScan(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<VgiEngineFeaturesState>();
	const auto &features = EngineFeatures();
	idx_t count = 0;
	while (state.offset < features.size() && count < STANDARD_VECTOR_SIZE) {
		const auto &f = features[state.offset++];
		output.data[0].SetValue(count, Value(f.name));
		output.data[1].SetValue(count, Value::BOOLEAN(f.available));
		output.data[2].SetValue(count, Value(f.reference));
		output.data[3].SetValue(count, f.available ? Value(LogicalType::VARCHAR) : Value(f.without));
		count++;
	}
	output.SetCardinality(count);
}

} // anonymous namespace

void RegisterVgiEngineFeaturesFunction(ExtensionLoader &loader) {
	TableFunction func("vgi_engine_features", {}, VgiEngineFeaturesScan, VgiEngineFeaturesBind,
	                   VgiEngineFeaturesInit);
	CreateTableFunctionInfo info(func);
	info.descriptions.push_back(vgi::MakeFunctionDescription(
	    "List the engine-dependent VGI capabilities this build was compiled with: one row per feature with "
	    "whether it is available, the engine change it relies on, and what differs when it is not. Every "
	    "feature is available on Haybarn; builds against upstream DuckDB lack the ones whose engine changes "
	    "have not been released there.",
	    {}, {}, {"SELECT feature, available FROM vgi_engine_features();"}));
	loader.RegisterFunction(std::move(info));
}

} // namespace duckdb
