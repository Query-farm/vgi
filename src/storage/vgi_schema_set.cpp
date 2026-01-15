#include "storage/vgi_schema_set.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

#include "storage/vgi_catalog.hpp"
#include "storage/vgi_schema_entry.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_protocol.hpp"

namespace duckdb {

VgiSchemaSet::VgiSchemaSet(Catalog &catalog) : VgiCatalogSet(catalog) {
}

std::string VgiSchemaSet::GetDefaultSchema(ClientContext &context) {
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_result = vgi_catalog.attach_result();

	if (attach_result) {
		return attach_result->default_schema;
	}
	return "main";
}

void VgiSchemaSet::LoadEntries(ClientContext &context) {
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();

	if (!attach_params || !attach_result) {
		return;
	}

	// Call "schemas" method via streaming API
	auto worker_path = attach_params->worker_path();
	auto args = vgi::CreateCatalogArgsWithAttachId(attach_result->attach_id);
	vgi::CatalogMethodStream stream(worker_path, "schemas", args, context, attach_params->worker_debug());

	// Read schema entries from the stream
	while (auto result_batch = stream.ReadNext()) {
		// Parse schema list from this batch
		auto schema_list = vgi::ParseSchemaList(result_batch, worker_path);

		// Create schema entries
		for (auto &schema_info : schema_list) {
			CreateSchemaInfo info;
			info.schema = schema_info.name;

			auto schema_entry = make_uniq<VgiSchemaEntry>(catalog_, info, schema_info);
			CreateEntry(std::move(schema_entry));
		}
	}

	// Wait for worker to exit
	stream.Wait();
}

} // namespace duckdb
