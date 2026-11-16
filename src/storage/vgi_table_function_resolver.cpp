// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "storage/vgi_table_function_resolver.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"

#include "storage/vgi_catalog.hpp"
#include "storage/vgi_schema_entry.hpp"
#include "vgi_exception.hpp"
#include "vgi_logging.hpp"

#include <vector>

namespace duckdb {
namespace vgi {

namespace {

void AddCandidate(std::vector<std::string> &schemas, const std::string &schema) {
	if (schema.empty()) {
		return;
	}
	for (const auto &existing : schemas) {
		if (existing == schema) {
			return;
		}
	}
	schemas.push_back(schema);
}

optional_ptr<TableFunctionCatalogEntry> LookupVisible(ClientContext &context, Catalog &catalog,
                                                      const std::string &schema, const std::string &function_name) {
	// Keep this exact lookup: see the worker-schema rule in ResolveVgiTableFunction.
	return catalog.GetEntry<TableFunctionCatalogEntry>(context, Identifier(schema), Identifier(function_name),
	                                                   OnEntryNotFound::RETURN_NULL);
}

optional_ptr<TableFunctionCatalogEntry> LookupHidden(ClientContext &context, Catalog &catalog,
                                                     const std::string &schema, const std::string &function_name) {
	auto schema_entry = catalog.GetSchema(context, Identifier(schema), OnEntryNotFound::RETURN_NULL);
	if (!schema_entry) {
		return nullptr;
	}
	return schema_entry->Cast<VgiSchemaEntry>().GetHiddenTableFunction(context, function_name);
}

} // namespace

VgiResolvedTableFunction ResolveVgiTableFunction(ClientContext &context, const VgiTableFunctionRequest &request) {
	D_ASSERT(request.catalog);
	auto &catalog = *request.catalog;
	VgiResolvedTableFunction result;

	// Visible order: a worker-claimed third schema, the table's, the default.
	// A worker-claimed schema equal to the table's or the default must go through
	// that same lookup, never a separate call: a separate lookup (even with an
	// identical name) was observed to disable filter/projection pushdown on a
	// later bind (cache/filter_pushdown_keys.test); cause not understood.
	const bool worker_schema_is_third_schema = !request.worker_schema.empty() &&
	                                           request.worker_schema != request.table_schema &&
	                                           request.worker_schema != request.default_schema;
	std::vector<std::string> schemas;
	if (worker_schema_is_third_schema) {
		AddCandidate(schemas, request.worker_schema);
	}
	AddCandidate(schemas, request.table_schema);
	AddCandidate(schemas, request.default_schema);

	for (const auto &schema : schemas) {
		if (auto entry = LookupVisible(context, catalog, schema, request.function_name)) {
			result.entry = entry;
			result.schema_name = schema;
			return result;
		}
	}
	// Hidden sets before built-ins; format readers skip them (see header).
	if (!request.format_reader) {
		for (const auto &schema : schemas) {
			if (auto entry = LookupHidden(context, catalog, schema, request.function_name)) {
				result.entry = entry;
				result.schema_name = schema;
				result.hidden = true;
				return result;
			}
		}
	}
	if (request.allow_system_catalog || request.format_reader) {
		// DuckDB built-ins a worker delegates to (read_parquet, iceberg_scan, the
		// readers of format branches, ...).
		EntryLookupInfo lookup(CatalogType::TABLE_FUNCTION_ENTRY, QualifiedName(Identifier(request.function_name)));
		auto sys_entry = Catalog::GetEntry(context, SYSTEM_CATALOG, DEFAULT_SCHEMA, lookup, OnEntryNotFound::RETURN_NULL);
		if (sys_entry) {
			result.entry = &sys_entry->Cast<TableFunctionCatalogEntry>();
			result.from_system_catalog = true;
			return result;
		}
	}

	auto &vgi_catalog = catalog.Cast<VgiCatalog>();
	const auto &attach_params = vgi_catalog.attach_parameters();
	const auto worker_path = attach_params ? attach_params->worker_path() : std::string();
	const auto catalog_name = catalog.GetName().GetIdentifierName();
	std::string searched;
	for (const auto &schema : schemas) {
		searched += searched.empty() ? "'" + schema + "'" : ", '" + schema + "'";
	}
	throw BinderException(
	    ErrorInfo(error_subtype::kProtocolViolation)
	        .Worker(worker_path)
	        .Set(error_key::kCatalog, catalog_name)
	        .Set(error_key::kTable, request.table_schema + "." + request.table_name)
	        .Set(error_key::kFunctionName, request.function_name),
	    request.format_reader
	        ? StringUtil::Format("VGI table '%s.%s.%s' (worker '%s') reads through %s function '%s', which is "
	                             "neither a DuckDB table function nor listed by the worker. Check that any required "
	                             "extensions are loaded.",
	                             catalog_name, request.table_schema, request.table_name,
	                             RedactCredentials(worker_path), request.role, request.function_name)
	        : StringUtil::Format(
	              "VGI table '%s.%s.%s' names %s function '%s', but worker '%s' does not list it (searched "
	              "schema(s) %s%s). The worker must list every function its tables name in "
	              "catalog_schema_contents_functions, with visibility 'table_only' to keep it hidden.%s",
	              catalog_name, request.table_schema, request.table_name, request.role, request.function_name,
	              RedactCredentials(worker_path), searched, request.allow_system_catalog ? " and the system catalog" : "",
	              request.allow_system_catalog ? " If it is a DuckDB function, check that any required extensions "
	                                             "are loaded."
	                                           : ""));
}

} // namespace vgi
} // namespace duckdb
