// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "duckdb/common/types.hpp"
#include "storage/vgi_catalog_set.hpp"
#include "vgi_catalog_metadata.hpp"

namespace duckdb {

class VgiSchemaEntry;

class VgiTableFunctionSet : public VgiCatalogSet {
public:
	VgiTableFunctionSet(Catalog &catalog, VgiSchemaEntry &schema);

protected:
	void LoadEntries(ClientContext &context, const std::lock_guard<std::mutex> &_load_lock) override;

	std::string CacheKindName() const override {
		return "table_function";
	}

private:
	VgiSchemaEntry &schema_;
};

} // namespace duckdb
