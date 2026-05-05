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
	void LoadEntries(ClientContext &context) override;

	std::string CacheKindName() const override {
		return "scalar_function";
	}

private:
	VgiSchemaEntry &schema_;
};

} // namespace duckdb
