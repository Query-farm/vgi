#pragma once

#include "duckdb/common/types.hpp"
#include "storage/vgi_catalog_set.hpp"
#include "vgi_catalog_api.hpp"

namespace duckdb {

class VgiSchemaEntry;

class VgiIndexSet : public VgiCatalogSet {
public:
	VgiIndexSet(Catalog &catalog, VgiSchemaEntry &schema);

	// Override GetEntry to do on-demand single-index loading
	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const std::string &name);

protected:
	void LoadEntries(ClientContext &context, const std::lock_guard<std::mutex> &_load_lock) override;

	std::string CacheKindName() const override {
		return "index";
	}

private:
	VgiSchemaEntry &schema_;
};

} // namespace duckdb
