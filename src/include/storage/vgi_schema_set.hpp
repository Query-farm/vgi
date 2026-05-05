#pragma once

#include "storage/vgi_catalog_set.hpp"
#include "vgi_catalog_api.hpp"

namespace duckdb {

class ClientContext;
struct CreateSchemaInfo;

class VgiSchemaSet : public VgiCatalogSet {
public:
	explicit VgiSchemaSet(Catalog &catalog);

	// Get the default schema name
	std::string GetDefaultSchema(ClientContext &context);

protected:
	void LoadEntries(ClientContext &context) override;

	std::string CacheKindName() const override {
		return "schema";
	}
};

} // namespace duckdb
