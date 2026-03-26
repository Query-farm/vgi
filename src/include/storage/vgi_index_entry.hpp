#pragma once

#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "vgi_catalog_api.hpp"

namespace duckdb {

class VgiIndexEntry : public IndexCatalogEntry {
public:
	VgiIndexEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateIndexInfo &info,
	              const vgi::VgiIndexInfo &index_info);
	~VgiIndexEntry() override;

	string GetSchemaName() const override;
	string GetTableName() const override;

	const vgi::VgiIndexInfo &GetIndexInfo() const {
		return index_info_;
	}

private:
	vgi::VgiIndexInfo index_info_;
};

} // namespace duckdb
