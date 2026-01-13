#include "storage/vgi_schema_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"

#include "storage/vgi_catalog.hpp"
#include "storage/vgi_table_entry.hpp"

namespace duckdb {

VgiSchemaEntry::VgiSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, const vgi::VgiSchemaInfo &schema_info)
    : SchemaCatalogEntry(catalog, info), schema_info_(schema_info), tables_(catalog, *this) {
}

VgiSchemaEntry::~VgiSchemaEntry() = default;

optional_ptr<CatalogEntry> VgiSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	throw BinderException("VGI catalogs are read-only");
}

optional_ptr<CatalogEntry> VgiSchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
	throw BinderException("VGI catalogs are read-only");
}

optional_ptr<CatalogEntry> VgiSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                       TableCatalogEntry &table) {
	throw BinderException("VGI catalogs are read-only");
}

optional_ptr<CatalogEntry> VgiSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	throw BinderException("VGI catalogs are read-only");
}

optional_ptr<CatalogEntry> VgiSchemaEntry::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
	throw BinderException("VGI catalogs are read-only");
}

optional_ptr<CatalogEntry> VgiSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                               CreateTableFunctionInfo &info) {
	throw BinderException("VGI catalogs are read-only");
}

optional_ptr<CatalogEntry> VgiSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                              CreateCopyFunctionInfo &info) {
	throw BinderException("VGI catalogs are read-only");
}

optional_ptr<CatalogEntry> VgiSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                CreatePragmaFunctionInfo &info) {
	throw BinderException("VGI catalogs are read-only");
}

optional_ptr<CatalogEntry> VgiSchemaEntry::CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) {
	throw BinderException("VGI catalogs are read-only");
}

optional_ptr<CatalogEntry> VgiSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw BinderException("VGI catalogs are read-only");
}

void VgiSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	throw BinderException("VGI catalogs are read-only");
}

void VgiSchemaEntry::Scan(ClientContext &context, CatalogType type,
                          const std::function<void(CatalogEntry &)> &callback) {
	if (type == CatalogType::TABLE_ENTRY) {
		tables_.Scan(context, callback);
	}
	// Other types (functions, etc.) not yet implemented
}

void VgiSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw InternalException("VgiSchemaEntry::Scan without context not supported");
}

void VgiSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	throw BinderException("VGI catalogs are read-only");
}

optional_ptr<CatalogEntry> VgiSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                       const EntryLookupInfo &lookup_info) {
	auto type = lookup_info.GetCatalogType();
	if (type == CatalogType::TABLE_ENTRY) {
		return tables_.GetEntry(transaction.GetContext(), lookup_info.GetEntryName());
	}
	// Other types (functions, etc.) not yet implemented
	return nullptr;
}

VgiCatalogSet &VgiSchemaEntry::GetCatalogSet(CatalogType type) {
	if (type == CatalogType::TABLE_ENTRY) {
		return tables_;
	}
	throw InternalException("Unsupported catalog type for VgiSchemaEntry");
}

} // namespace duckdb
