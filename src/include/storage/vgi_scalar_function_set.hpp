// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "duckdb/common/types.hpp"
#include "storage/vgi_catalog_set.hpp"
#include "vgi_catalog_api.hpp"

namespace duckdb {

class VgiSchemaEntry;

class VgiScalarFunctionSet : public VgiCatalogSet {
public:
	VgiScalarFunctionSet(Catalog &catalog, VgiSchemaEntry &schema);

protected:
	void LoadEntries(ClientContext &context, const std::lock_guard<std::mutex> &_load_lock) override;

	std::string CacheKindName() const override {
		return "scalar_function";
	}

private:
	VgiSchemaEntry &schema_;
};

} // namespace duckdb
