#include "storage/vgi_catalog.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

#include "storage/vgi_physical_write.hpp"
#include "storage/vgi_schema_entry.hpp"
#include "storage/vgi_table_entry.hpp"

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
	auto &table = op.table.Cast<VgiTableEntry>();
	if (!table.GetTableInfo().supports_insert) {
		throw BinderException("Table '%s' does not support INSERT", table.name);
	}
	// ON CONFLICT requires MERGE INTO support (DuckDB rewrites ON CONFLICT as MERGE INTO
	// internally). VgiCatalog::PlanMergeInto is not yet implemented — once it is, the
	// on_conflict_info fields (action_type, on_conflict_filter, etc.) are already plumbed
	// through to VgiPhysicalInsert and passed to the worker via write_options.
	if (op.on_conflict_info.action_type != OnConflictAction::THROW) {
		throw BinderException("ON CONFLICT is not yet supported for VGI tables (requires MERGE INTO support)");
	}
	// Use DuckDB's built-in default resolution: insert a PhysicalProjection child
	// that evaluates default expressions for omitted columns. This means Sink
	// always receives full-width rows with defaults already filled in.
	if (plan && !op.column_index_map.empty()) {
		plan = planner.ResolveDefaultsProjection(op, *plan);
	}
	auto &insert = planner.Make<VgiPhysicalInsert>(op, table, op.return_chunk, op.on_conflict_info.action_type,
	                                               std::move(op.on_conflict_info.on_conflict_filter));
	if (plan) {
		insert.children.push_back(*plan);
	}
	return insert;
}

PhysicalOperator &VgiCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                         PhysicalOperator &plan) {
	auto &table = op.table.Cast<VgiTableEntry>();
	if (!table.GetTableInfo().supports_delete) {
		throw BinderException("Table '%s' does not support DELETE", table.name);
	}
	auto &bound_ref = op.expressions[0]->Cast<BoundReferenceExpression>();
	auto &del = planner.Make<VgiPhysicalDelete>(op, table, op.return_chunk, bound_ref.index);
	del.children.push_back(plan);
	return del;
}

PhysicalOperator &VgiCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                         PhysicalOperator &plan) {
	auto &table = op.table.Cast<VgiTableEntry>();
	if (!table.GetTableInfo().supports_update) {
		throw BinderException("Table '%s' does not support UPDATE", table.name);
	}
	auto &upd = planner.Make<VgiPhysicalUpdate>(op, table, op.return_chunk);
	upd.children.push_back(plan);
	return upd;
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
