#pragma once

#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/common/types.hpp"
#include "storage/vgi_catalog_set.hpp"
#include "vgi_catalog_api.hpp"

namespace duckdb {

class VgiSchemaEntry;

class VgiMacroSet : public VgiCatalogSet {
public:
	VgiMacroSet(Catalog &catalog, VgiSchemaEntry &schema, CatalogType macro_type);

protected:
	void LoadEntries(ClientContext &context) override;

private:
	VgiSchemaEntry &schema_;
	CatalogType macro_catalog_type_;  // MACRO_ENTRY or TABLE_MACRO_ENTRY
};

} // namespace duckdb
