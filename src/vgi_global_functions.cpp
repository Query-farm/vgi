// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Bind-time resolution for functions a worker asked the client to publish into
// DuckDB's global function namespace (system.main). See docs/global_functions.md.
#include "vgi_global_functions.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"

#include "storage/vgi_catalog.hpp"
#include "vgi_attach_parameters.hpp"

namespace duckdb {
namespace vgi {

std::string VgiGlobalFunctionName(const std::string &prefix, const std::string &function_name) {
	if (prefix.empty()) {
		return StringUtil::Lower(function_name);
	}
	return StringUtil::Lower(prefix + "_" + function_name);
}

VgiGlobalBinding ResolveVgiGlobalBinding(ClientContext &context, const std::string &catalog_name,
                                         const std::string &function_name) {
	// A global registration is keyed on the attach alias only. DuckDB has no
	// API to unregister a function, so the entry survives DETACH — resolving
	// live here is what makes a re-ATTACH refresh the connection and a DETACH
	// produce a clear error instead of a stale-worker call.
	auto db = DatabaseManager::Get(context).GetDatabase(context, catalog_name);
	if (!db) {
		throw InvalidInputException(
		    "VGI function '%s' was published globally by catalog '%s', which is no longer attached. "
		    "Global registrations persist for the life of the process (DuckDB cannot unregister a "
		    "function), so re-ATTACH '%s' to call it again.",
		    function_name, catalog_name, catalog_name);
	}
	auto &catalog = db->GetCatalog();
	auto *vgi_catalog = dynamic_cast<VgiCatalog *>(&catalog);
	if (!vgi_catalog) {
		throw InvalidInputException(
		    "VGI function '%s' was published globally by catalog '%s', but '%s' is now attached as a "
		    "'%s' catalog. Re-ATTACH it as a VGI catalog to call this function.",
		    function_name, catalog_name, catalog_name, catalog.GetCatalogType());
	}

	const auto &attach_params = vgi_catalog->attach_parameters();
	const auto &attach_result = vgi_catalog->attach_result();
	if (!attach_params || !attach_result) {
		throw InvalidInputException("VGI catalog '%s' has no attach state; cannot call globally-published "
		                            "function '%s'",
		                            catalog_name, function_name);
	}

	VgiGlobalBinding binding;
	binding.catalog = vgi_catalog;
	binding.attach_params = attach_params;
	binding.attach_opaque_data = attach_result->attach_opaque_data;
	binding.setting_names.reserve(attach_result->settings.size());
	for (const auto &setting : attach_result->settings) {
		binding.setting_names.push_back(setting.name);
	}
	return binding;
}

VgiGlobalBinding ResolveVgiFunctionBinding(ClientContext &context, const VgiFunctionRegistrationTarget &target,
                                           const std::string &function_name) {
	if (target.IsGlobal()) {
		return ResolveVgiGlobalBinding(context, target.catalog_name, function_name);
	}
	// Catalog-scoped entry: it lives in the VGI catalog's own schema and dies
	// with it, so what registration captured is still valid.
	VgiGlobalBinding binding;
	binding.catalog = target.catalog;
	binding.attach_params = target.attach_params;
	binding.attach_opaque_data = target.attach_opaque_data;
	binding.setting_names = target.setting_names;
	return binding;
}

} // namespace vgi
} // namespace duckdb
