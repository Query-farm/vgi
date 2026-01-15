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
#include "vgi_protocol.hpp"

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

	// Call schema_contents with type filter "view"
	auto args = vgi::CreateSchemaContentsArgs(attach_result->attach_id, schema_.name, "view");
	vgi::CatalogMethodStream stream(attach_params->worker_path(), "schema_contents", args, context,
	                                attach_params->worker_debug());

	// Read all view batches
	while (true) {
		auto batch = stream.ReadNext();
		if (!batch) {
			break;
		}

		// Parse each row in the batch as a view
		for (int64_t i = 0; i < batch->num_rows(); i++) {
			// Get string values for each column
			auto name_col = batch->GetColumnByName("name");
			auto def_col = batch->GetColumnByName("definition");

			if (!name_col || !def_col) {
				continue;
			}

			auto name_array = std::dynamic_pointer_cast<arrow::StringArray>(name_col);
			auto def_array = std::dynamic_pointer_cast<arrow::StringArray>(def_col);

			if (!name_array || name_array->IsNull(i) || !def_array || def_array->IsNull(i)) {
				continue;
			}

			std::string view_name = name_array->GetString(i);
			std::string definition = def_array->GetString(i);

			// Create a ViewCatalogEntry
			// Parse the SQL definition to get the SELECT statement
			CreateViewInfo info;
			info.view_name = view_name;
			info.sql = definition;

			// Parse the SQL to get the select statement
			try {
				Parser parser;
				parser.ParseQuery(definition);
				if (!parser.statements.empty() && parser.statements[0]->type == StatementType::SELECT_STATEMENT) {
					info.query = unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));
				}
			} catch (...) {
				// If parsing fails, we still create the view but without a parsed query
				// This allows the view to show up in catalogs even if the SQL is not parseable
			}

			auto view_entry = make_uniq<ViewCatalogEntry>(catalog_, schema_, info);
			CreateEntry(std::move(view_entry));
		}
	}

}

} // namespace duckdb
