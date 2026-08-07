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

// ============================================================================
// Where a batch of VGI functions is being registered, plus the connection state
// its binds should use.
//
// Two registration targets exist:
//   * per-catalog — entries live in the VGI catalog's own schema and die with
//     it, so they can hold a Catalog& captured at registration.
//   * global (system.main) — entries published by
//     `CatalogAttachResult.global_functions`. DuckDB has no API to unregister a
//     function, so these outlive DETACH; they hold only the attach *alias* and
//     re-resolve the live catalog at every bind (see ResolveVgiGlobalBinding).
// ============================================================================
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

// Resolve the live state behind a global registration. Because a global entry
// persists for the process lifetime, re-ATTACHing the alias transparently
// refreshes the connection (fresh attach_opaque_data / auth), and calling one
// after DETACH throws an InvalidInputException that names the function and
// tells the user to re-ATTACH — rather than silently reconnecting to a catalog
// the user detached.
VgiGlobalBinding ResolveVgiGlobalBinding(ClientContext &context, const std::string &catalog_name,
                                         const std::string &function_name);

// Connection state a bind should use for a registered VGI function: what
// registration captured for a catalog-scoped entry, freshly re-resolved from
// the live catalog for a global one.
VgiGlobalBinding ResolveVgiFunctionBinding(ClientContext &context, const VgiFunctionRegistrationTarget &target,
                                           const std::string &function_name);

// ============================================================================
// Function-set builders, shared by the per-catalog registration
// (storage/vgi_*_function_set.cpp) and the global registration in
// vgi_extension.cpp. `registered_name` is the name users type — the bare
// function name for a catalog entry, `<prefix>_<name>` for a global one — while
// dispatch always uses func_info.name + the target's dispatch schema.
// ============================================================================
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

// Apply a catalog's `global_function_prefix` to a worker-declared function
// name, yielding the globally visible (lowercased) name. An empty prefix
// publishes the bare name.
std::string VgiGlobalFunctionName(const std::string &prefix, const std::string &function_name);

} // namespace vgi
} // namespace duckdb
