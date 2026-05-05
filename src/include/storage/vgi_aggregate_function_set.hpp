#pragma once

#include "duckdb/common/types.hpp"
#include "storage/vgi_catalog_set.hpp"
#include "vgi_catalog_api.hpp"

namespace duckdb {

class VgiSchemaEntry;

class VgiAggregateFunctionSet : public VgiCatalogSet {
public:
	VgiAggregateFunctionSet(Catalog &catalog, VgiSchemaEntry &schema);

protected:
	void LoadEntries(ClientContext &context) override;

	std::string CacheKindName() const override {
		return "aggregate_function";
	}

private:
	VgiSchemaEntry &schema_;
};

} // namespace duckdb
