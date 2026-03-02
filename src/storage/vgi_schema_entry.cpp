#include "storage/vgi_schema_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"

#include "storage/vgi_catalog.hpp"
#include "storage/vgi_table_entry.hpp"

namespace duckdb {

VgiSchemaEntry::VgiSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, const vgi::VgiSchemaInfo &schema_info)
    : SchemaCatalogEntry(catalog, info), schema_info_(schema_info), tables_(catalog, *this), views_(catalog, *this),
      scalar_functions_(catalog, *this), table_functions_(catalog, *this),
      scalar_macros_(catalog, *this, CatalogType::MACRO_ENTRY),
      table_macros_(catalog, *this, CatalogType::TABLE_MACRO_ENTRY) {
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
	switch (type) {
	case CatalogType::TABLE_ENTRY:
		tables_.Scan(context, callback);
		break;
	case CatalogType::VIEW_ENTRY:
		views_.Scan(context, callback);
		break;
	case CatalogType::SCALAR_FUNCTION_ENTRY:
		// DuckDB's duckdb_functions() only scans SCALAR_FUNCTION_ENTRY, not MACRO_ENTRY.
		// In DuckDB's default catalog, macros and functions share the same set.
		// We need to include scalar macros here so they appear in duckdb_functions().
		scalar_functions_.Scan(context, callback);
		scalar_macros_.Scan(context, callback);
		break;
	case CatalogType::TABLE_FUNCTION_ENTRY:
		// Same pattern: table macros must be included in TABLE_FUNCTION_ENTRY scan.
		table_functions_.Scan(context, callback);
		table_macros_.Scan(context, callback);
		break;
	case CatalogType::MACRO_ENTRY:
		scalar_macros_.Scan(context, callback);
		break;
	case CatalogType::TABLE_MACRO_ENTRY:
		table_macros_.Scan(context, callback);
		break;
	default:
		// Other types not yet implemented
		break;
	}
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
	auto &entry_name = lookup_info.GetEntryName();
	auto &context = transaction.GetContext();

	switch (type) {
	case CatalogType::TABLE_ENTRY: {
		// DuckDB resolves FROM clauses as TABLE_ENTRY; fall back to views
		auto result = tables_.GetEntry(context, entry_name);
		if (!result) {
			result = views_.GetEntry(context, entry_name);
		}
		return result;
	}
	case CatalogType::VIEW_ENTRY:
		return views_.GetEntry(context, entry_name);
	case CatalogType::SCALAR_FUNCTION_ENTRY: {
		// DuckDB looks up scalar macros via SCALAR_FUNCTION_ENTRY (they share a set
		// in the default catalog). Fall back to scalar macros.
		auto result = scalar_functions_.GetEntry(context, entry_name);
		if (!result) {
			result = scalar_macros_.GetEntry(context, entry_name);
		}
		return result;
	}
	case CatalogType::TABLE_FUNCTION_ENTRY: {
		// Same pattern: table macros are looked up via TABLE_FUNCTION_ENTRY.
		auto result = table_functions_.GetEntry(context, entry_name);
		if (!result) {
			result = table_macros_.GetEntry(context, entry_name);
		}
		return result;
	}
	case CatalogType::MACRO_ENTRY:
		return scalar_macros_.GetEntry(context, entry_name);
	case CatalogType::TABLE_MACRO_ENTRY:
		return table_macros_.GetEntry(context, entry_name);
	default:
		// Other types not yet implemented
		return nullptr;
	}
}

VgiCatalogSet &VgiSchemaEntry::GetCatalogSet(CatalogType type) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
		return tables_;
	case CatalogType::VIEW_ENTRY:
		return views_;
	case CatalogType::SCALAR_FUNCTION_ENTRY:
		return scalar_functions_;
	case CatalogType::TABLE_FUNCTION_ENTRY:
		return table_functions_;
	case CatalogType::MACRO_ENTRY:
		return scalar_macros_;
	case CatalogType::TABLE_MACRO_ENTRY:
		return table_macros_;
	default:
		throw InternalException("Unsupported catalog type for VgiSchemaEntry");
	}
}

} // namespace duckdb
