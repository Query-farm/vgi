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

private:
	VgiSchemaEntry &schema_;
};

} // namespace duckdb
