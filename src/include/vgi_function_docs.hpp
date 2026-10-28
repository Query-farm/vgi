// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/function/function.hpp"

#include <utility>

namespace duckdb {
namespace vgi {

// Parameter names belong to the signature in DuckDB 2.0; descriptions alone
// do not populate duckdb_functions() or the names supplied to bind callbacks.
inline void SetVgiFunctionArgumentNames(SimpleFunction &function, const vector<string> &names) {
	auto &signature = function.GetSignature();
	for (idx_t i = 0; i < names.size(); i++) {
		signature.GetParameter(i).SetName(Identifier(names[i]));
	}
}

// Optional named options are **kwargs entries: omitting an option leaves it
// absent in TableFunctionBindInput::named_parameters.
inline void AddVgiFunctionOption(SimpleFunction &function, Identifier name, LogicalType type) {
	auto &signature = function.GetSignature();
	auto configure = [&](TypedKwargs &options) { options.Add(std::move(name), std::move(type)); };
	if (signature.GetTypedKwargs()) {
		signature.ExtendTypedKwargs(configure);
	} else {
		signature.WithTypedKwargs("options", configure);
	}
}

// Build a FunctionDescription so the extension's built-in functions carry
// compiled-in documentation surfaced through duckdb_functions()
// (description, parameter names/types, examples). Worker/catalog-loaded
// functions populate this from worker metadata; these helpers do the same
// for the extension's own diagnostic / entry-point functions.
inline FunctionDescription MakeFunctionDescription(string description, vector<string> parameter_names = {},
                                                   vector<LogicalType> parameter_types = {},
                                                   vector<string> examples = {}) {
	FunctionDescription desc;
	desc.description = std::move(description);
	desc.parameter_names = std::move(parameter_names);
	desc.parameter_types = std::move(parameter_types);
	desc.examples = std::move(examples);
	desc.categories.emplace_back("vgi");
	return desc;
}

} // namespace vgi
} // namespace duckdb
