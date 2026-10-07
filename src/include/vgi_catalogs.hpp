// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <memory>
#include <string>
#include <unordered_set>

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "vgi_location_policy.hpp"

namespace duckdb {

// Register the vgi_catalogs table function
// Usage: SELECT * FROM vgi_catalogs('/path/to/vgi-worker')
// Returns a table with a single column "catalog" containing catalog names from the VGI worker
void RegisterVgiCatalogsFunction(ExtensionLoader &loader);

namespace vgi {

class CatalogAuth;
struct IrohClientConfig;

// A worker LOCATION addressed before any ATTACH (vgi_catalogs(),
// vgi_protocols()), with the auth and Iroh configuration an ATTACH of the same
// LOCATION and options would use.
struct VgiDiscoveryTarget {
	std::string worker_path;
	std::shared_ptr<CatalogAuth> auth;
	std::shared_ptr<IrohClientConfig> iroh;
};

// Bind-time handling shared by the discovery table functions: location policy,
// bearer_token / oauth_* (redacted in place), iroh_* and the httpfs check.
// `function_name` names the caller in Iroh errors; named parameters in `skip`
// (lower-case) belong to the caller and are left alone.
VgiDiscoveryTarget BindDiscoveryTarget(ClientContext &context, std::string worker_path,
                                       named_parameter_map_t &named_parameters, const char *function_name,
                                       LocationEntryPoint entry, const std::unordered_set<std::string> &skip = {});

// Register the named parameters BindDiscoveryTarget understands.
void AddDiscoveryNamedParameters(TableFunction &func);

} // namespace vgi

} // namespace duckdb
