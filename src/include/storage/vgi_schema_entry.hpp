#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "storage/vgi_aggregate_function_set.hpp"
#include "storage/vgi_macro_set.hpp"
#include "storage/vgi_scalar_function_set.hpp"
#include "storage/vgi_table_function_set.hpp"
#include "storage/vgi_table_set.hpp"
#include "storage/vgi_view_set.hpp"
#include "vgi_catalog_api.hpp"

namespace duckdb {

class VgiCatalog;

class VgiSchemaEntry : public SchemaCatalogEntry {
public:
	VgiSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, const vgi::VgiSchemaInfo &schema_info);
	~VgiSchemaEntry() override;

public:
	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
	                                       TableCatalogEntry &table) override;
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
	                                               CreateTableFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
	                                              CreateCopyFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
	                                                CreatePragmaFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override;
	optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;

	void Alter(CatalogTransaction transaction, AlterInfo &info) override;
	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void DropEntry(ClientContext &context, DropInfo &info) override;

	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;

	const vgi::VgiSchemaInfo &GetSchemaInfo() const {
		return schema_info_;
	}

private:
	VgiCatalogSet &GetCatalogSet(CatalogType type);

	vgi::VgiSchemaInfo schema_info_;
	VgiTableSet tables_;
	VgiViewSet views_;
	VgiScalarFunctionSet scalar_functions_;
	VgiAggregateFunctionSet aggregate_functions_;
	VgiTableFunctionSet table_functions_;
	VgiMacroSet scalar_macros_;
	VgiMacroSet table_macros_;
};

} // namespace duckdb
