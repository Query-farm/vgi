#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "vgi_catalog_api.hpp"

namespace duckdb {

class VgiCatalog;
class VgiSchemaEntry;

class VgiTableEntry : public TableCatalogEntry {
public:
	VgiTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
	              const vgi::VgiTableInfo &table_info);
	~VgiTableEntry() override;

public:
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

	TableStorageInfo GetStorageInfo(ClientContext &context) override;

	const vgi::VgiTableInfo &GetTableInfo() const {
		return table_info_;
	}

	Catalog &GetCatalog() const {
		return catalog_;
	}

private:
	vgi::VgiTableInfo table_info_;
	Catalog &catalog_;
};

} // namespace duckdb
