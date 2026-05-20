// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "duckdb/common/types.hpp"
#include "storage/vgi_catalog_set.hpp"
#include "vgi_catalog_api.hpp"

namespace duckdb {

class VgiSchemaEntry;

class VgiViewSet : public VgiCatalogSet {
public:
	VgiViewSet(Catalog &catalog, VgiSchemaEntry &schema);

	// Override GetEntry to do on-demand single-view loading
	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const std::string &name);

protected:
	void LoadEntries(ClientContext &context, const std::lock_guard<std::mutex> &_load_lock) override;

	std::string CacheKindName() const override {
		return "view";
	}

private:
	VgiSchemaEntry &schema_;
};

} // namespace duckdb
