// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_function_info.hpp" // FunctionDescription

#include "vgi_catalog_metadata.hpp"

namespace duckdb {

class Catalog;
class ClientContext;

namespace vgi {

struct VgiAttachParameters;

// Where a batch of VGI functions is registered, plus the state their binds use:
//   * per-catalog — entries live in the VGI catalog's own schema and die with
//     it, so they may hold a Catalog& captured at registration.
//   * global (system.main, from CatalogAttachResult.global_functions) — DuckDB
//     cannot unregister a function, so these outlive DETACH; they hold only the
//     attach alias and re-resolve the catalog at every bind.
struct VgiFunctionRegistrationTarget {
	//! Owning VGI catalog. Null ⇒ this is a global registration.
	optional_ptr<Catalog> catalog;
	//! Attach alias — the bind-time resolution key for a global registration,
	//! and the name used in error messages.
	std::string catalog_name;
	std::shared_ptr<VgiAttachParameters> attach_params;
	std::vector<uint8_t> attach_opaque_data;
	std::vector<std::string> setting_names;
	//! Schema the entries are created in. Empty for a global registration,
	//! where each function's own `schema_name` (its dispatch key) is used.
	std::string schema_name;

	bool IsGlobal() const {
		return !catalog;
	}
	//! Dispatch schema for one function: the registration schema when set,
	//! otherwise the schema the worker declared the function in.
	const std::string &DispatchSchema(const VgiFunctionInfo &func_info) const {
		return schema_name.empty() ? func_info.schema_name : schema_name;
	}
};

// Live connection state behind a globally-published function, re-read from the
// attached catalog at every bind.
struct VgiGlobalBinding {
	optional_ptr<Catalog> catalog;
	std::shared_ptr<VgiAttachParameters> attach_params;
	std::vector<uint8_t> attach_opaque_data;
	std::vector<std::string> setting_names;
};

// Resolve the live state behind a global registration: a re-ATTACH of the
// alias refreshes the connection, and a call after DETACH (or with the alias
// now a non-VGI catalog) throws InvalidInputException telling the user to
// re-ATTACH, rather than reconnecting to a catalog the user detached.
VgiGlobalBinding ResolveVgiGlobalBinding(ClientContext &context, const std::string &catalog_name,
                                         const std::string &function_name);

// Connection state a bind should use for a registered VGI function: what
// registration captured for a catalog-scoped entry, freshly re-resolved from
// the live catalog for a global one.
VgiGlobalBinding ResolveVgiFunctionBinding(ClientContext &context, const VgiFunctionRegistrationTarget &target,
                                           const std::string &function_name);

// Function-set builders shared by per-catalog and global registration.
// `registered_name` is what users type (bare name, or `<prefix>_<name>` when
// global); dispatch always uses func_info.name + the target's DispatchSchema.
ScalarFunctionSet BuildVgiScalarFunctionSet(ClientContext &context, const std::string &registered_name,
                                            const std::vector<VgiFunctionInfo> &overloads,
                                            const VgiFunctionRegistrationTarget &target,
                                            vector<FunctionDescription> &descriptions);

AggregateFunctionSet BuildVgiAggregateFunctionSet(ClientContext &context, const std::string &registered_name,
                                                  const std::vector<VgiFunctionInfo> &overloads,
                                                  const VgiFunctionRegistrationTarget &target,
                                                  vector<FunctionDescription> &descriptions);

TableFunctionSet BuildVgiTableFunctionSet(ClientContext &context, const std::string &registered_name,
                                          const std::vector<VgiFunctionInfo> &overloads,
                                          const VgiFunctionRegistrationTarget &target,
                                          vector<FunctionDescription> &descriptions);

// Lower-cased `<prefix>_<name>`, or the bare name when the prefix is empty.
std::string VgiGlobalFunctionName(const std::string &prefix, const std::string &function_name);

} // namespace vgi
} // namespace duckdb
