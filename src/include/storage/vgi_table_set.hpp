// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "storage/vgi_catalog_set.hpp"
#include "vgi_catalog_metadata.hpp"

namespace duckdb {

class VgiSchemaEntry;

class VgiTableSet : public VgiCatalogSet {
public:
	VgiTableSet(Catalog &catalog, VgiSchemaEntry &schema);

	// Override GetEntry to do on-demand loading
	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const std::string &name);

	// Override GetEntry with EntryLookupInfo to handle AT clause for time travel
	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const EntryLookupInfo &lookup_info);

protected:
	void LoadEntries(ClientContext &context,
	                  const std::lock_guard<std::mutex> &_load_lock) override;

	std::string CacheKindName() const override {
		return "table";
	}

private:
	VgiSchemaEntry &schema_;
};

} // namespace duckdb
