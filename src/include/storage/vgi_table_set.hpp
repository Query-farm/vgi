#pragma once

#include "duckdb/common/types.hpp"
#include "storage/vgi_catalog_set.hpp"
#include "vgi_catalog_api.hpp"

namespace duckdb {

class VgiSchemaEntry;

class VgiTableSet : public VgiCatalogSet {
public:
	VgiTableSet(Catalog &catalog, VgiSchemaEntry &schema);

	// Override GetEntry to do on-demand loading
	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const std::string &name);

protected:
	void LoadEntries(ClientContext &context) override;

private:
	VgiSchemaEntry &schema_;
};

} // namespace duckdb
