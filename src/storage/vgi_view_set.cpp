#include "storage/vgi_view_set.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parser.hpp"

#include "storage/vgi_catalog.hpp"
#include "storage/vgi_schema_entry.hpp"
#include "vgi_catalog_api.hpp"

namespace duckdb {

VgiViewSet::VgiViewSet(Catalog &catalog, VgiSchemaEntry &schema) : VgiCatalogSet(catalog), schema_(schema) {
}

void VgiViewSet::LoadEntries(ClientContext &context) {
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();

	if (!attach_params || !attach_result) {
		return;
	}

	// Call catalog_schema_contents_views via RPC
	auto views = vgi::InvokeCatalogSchemaContentsViews(attach_params->worker_path(), attach_result->attach_id,
	                                                   schema_.name, context, attach_params->worker_debug());

	for (auto &view_info : views) {
		CreateViewInfo info;
		info.view_name = view_info.name;
		info.sql = view_info.definition;

		// Parse the SQL to get the select statement
		try {
			Parser parser;
			parser.ParseQuery(view_info.definition);
			if (!parser.statements.empty() && parser.statements[0]->type == StatementType::SELECT_STATEMENT) {
				info.query = unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));
			}
		} catch (...) {
			// If parsing fails, we still create the view but without a parsed query
		}

		auto view_entry = make_uniq<ViewCatalogEntry>(catalog_, schema_, info);
		CreateEntry(std::move(view_entry));
	}
}

} // namespace duckdb
