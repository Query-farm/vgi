#include "storage/vgi_catalog.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

#include "storage/vgi_schema_entry.hpp"

namespace duckdb {

VgiCatalog::VgiCatalog(AttachedDatabase &db_p, const std::string &internal_name, AccessMode access_mode,
                       std::shared_ptr<vgi::VgiAttachParameters> attach_params,
                       std::shared_ptr<vgi::CatalogAttachResult> attach_result)
    : Catalog(db_p), access_mode_(access_mode), attach_parameters_(std::move(attach_params)),
      attach_result_(std::move(attach_result)), internal_name_(internal_name), schemas(*this) {
}

VgiCatalog::~VgiCatalog() = default;

void VgiCatalog::Initialize(bool load_builtin) {
	// Nothing to do - schemas are loaded lazily
}

std::string VgiCatalog::GetDefaultSchema() const {
	if (attach_result_) {
		return attach_result_->default_schema;
	}
	return "main";
}

optional_ptr<CatalogEntry> VgiCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	throw BinderException("VGI catalogs are read-only");
}

void VgiCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	schemas.Scan(context, [&](CatalogEntry &entry) { callback(entry.Cast<SchemaCatalogEntry>()); });
}

optional_ptr<SchemaCatalogEntry> VgiCatalog::LookupSchema(CatalogTransaction transaction,
                                                          const EntryLookupInfo &schema_lookup,
                                                          OnEntryNotFound if_not_found) {
	auto entry = schemas.GetEntry(transaction.GetContext(), schema_lookup.GetEntryName());
	if (entry) {
		return &entry->Cast<SchemaCatalogEntry>();
	}
	if (if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
		throw CatalogException("Schema '%s' not found in catalog '%s'", schema_lookup.GetEntryName(), GetName());
	}
	return nullptr;
}

PhysicalOperator &VgiCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalCreateTable &op, PhysicalOperator &plan) {
	throw BinderException("VGI catalogs are read-only");
}

PhysicalOperator &VgiCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                         optional_ptr<PhysicalOperator> plan) {
	throw BinderException("VGI catalogs are read-only");
}

PhysicalOperator &VgiCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                         PhysicalOperator &plan) {
	throw BinderException("VGI catalogs are read-only");
}

PhysicalOperator &VgiCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                         PhysicalOperator &plan) {
	throw BinderException("VGI catalogs are read-only");
}

unique_ptr<LogicalOperator> VgiCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
                                                        unique_ptr<LogicalOperator> plan) {
	throw BinderException("VGI catalogs are read-only");
}

DatabaseSize VgiCatalog::GetDatabaseSize(ClientContext &context) {
	DatabaseSize size;
	size.free_blocks = 0;
	size.total_blocks = 0;
	size.used_blocks = 0;
	size.wal_size = 0;
	size.block_size = 0;
	return size;
}

bool VgiCatalog::InMemory() {
	return true;
}

std::string VgiCatalog::GetDBPath() {
	return attach_parameters_ ? attach_parameters_->worker_path() : "";
}

void VgiCatalog::ClearCache() {
	schemas.ClearEntries();
}

void VgiCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	throw BinderException("VGI catalogs are read-only");
}

} // namespace duckdb
