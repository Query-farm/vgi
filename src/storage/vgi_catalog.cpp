// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "storage/vgi_catalog.hpp"
#include "vgi_result_cache.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/execution/operator/persistent/physical_merge_into.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

#include "storage/vgi_physical_write.hpp"
#include "storage/vgi_schema_entry.hpp"
#include "storage/vgi_table_entry.hpp"
#include "storage/vgi_transaction.hpp"
#include "vgi_catalog_rpc.hpp"
#include "vgi_companion_catalogs.hpp"
#include "vgi_logging.hpp"
#include "vgi_oauth.hpp" // BuildCatalogIdentityScope (per-identity disk flush)

namespace duckdb {

VgiCatalog::VgiCatalog(AttachedDatabase &db_p, const std::string &internal_name, AccessMode access_mode,
                       std::shared_ptr<vgi::VgiAttachParameters> attach_params,
                       std::shared_ptr<vgi::CatalogAttachResult> attach_result,
                       VgiObjectCounts eager_load_thresholds)
    : Catalog(db_p), access_mode_(access_mode), attach_parameters_(std::move(attach_params)),
      attach_result_(std::move(attach_result)), internal_name_(internal_name),
      eager_load_thresholds_(eager_load_thresholds), schemas(*this) {
	if (attach_result_) {
		last_known_catalog_version_.store(attach_result_->catalog_version);
	}
}

VgiCatalog::~VgiCatalog() = default;

// Release the companion catalogs this VGI attach referenced, so the federation
// is reversible. Refcount-aware: a companion shared with another still-attached
// VGI catalog is NOT detached until the last referencer releases it (see
// ReleaseCompanionCatalogs → VgiStorageExtension::ReleaseCompanions). Called by
// AttachedDatabase::OnDetach AFTER DetachInternal removed the parent
// (databases_lock not held) — nested DetachDatabase is safe here.
void VgiCatalog::OnDetach(ClientContext &context) {
	if (companion_catalogs_.empty()) {
		return;
	}
	vgi::ReleaseCompanionCatalogs(context, companion_catalogs_);
	companion_catalogs_.clear();
}

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
	if (access_mode_ == AccessMode::READ_ONLY) {
		throw BinderException("Cannot CREATE SCHEMA in read-only VGI catalog '%s'", GetName());
	}

	auto &context = transaction.GetContext();
	if (!attach_result_) {
		throw IOException("VGI CREATE SCHEMA: catalog '%s' has no attach_result_", GetName());
	}
	auto &vgi_tx = VgiTransaction::Get(context, *this);

	auto on_conflict = vgi::MapOnConflict(info.on_conflict);

	vgi::CatalogRpcContext rpc_ctx{attach_parameters_, attach_result_->attach_opaque_data, vgi_tx.GetTransactionOpaqueData()};
	vgi::InvokeCatalogSchemaCreate(rpc_ctx, info.schema, on_conflict, context);

	// Invalidate schema cache so re-fetch picks up the new schema. Use the
	// deferred path so any bound query holding a CatalogEntry* doesn't
	// dangle.
	ClearCache(/*force=*/false);
	return nullptr;
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
	// CREATE TABLE AS SELECT: create table at execution time (inside the transaction),
	// then insert the SELECT results. Follows the Airport extension pattern.
	auto &insert = planner.Make<VgiPhysicalInsert>(op, op.schema, std::move(op.info));
	insert.children.push_back(plan);
	return insert;
}

PhysicalOperator &VgiCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                         optional_ptr<PhysicalOperator> plan) {
	auto &table = op.table.Cast<VgiTableEntry>();
	if (!table.GetTableInfo().supports_insert) {
		throw BinderException("Table '%s' does not support INSERT", table.name);
	}
	if (op.return_chunk && !table.GetTableInfo().supports_returning) {
		throw BinderException("Table '%s' does not support RETURNING on INSERT", table.name);
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
	if (op.return_chunk && !table.GetTableInfo().supports_returning) {
		throw BinderException("Table '%s' does not support RETURNING on DELETE", table.name);
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
	if (op.return_chunk && !table.GetTableInfo().supports_returning) {
		throw BinderException("Table '%s' does not support RETURNING on UPDATE", table.name);
	}
	auto &upd = planner.Make<VgiPhysicalUpdate>(op, table, op.return_chunk);
	upd.children.push_back(plan);
	return upd;
}

// Helper: plan a single merge action using VGI physical operators
static unique_ptr<MergeIntoOperator> VgiPlanMergeIntoAction(ClientContext &context, LogicalMergeInto &op,
                                                             PhysicalPlanGenerator &planner,
                                                             BoundMergeIntoAction &action) {
	auto result = make_uniq<MergeIntoOperator>();
	result->action_type = action.action_type;
	result->condition = std::move(action.condition);

	auto &table = op.table.Cast<VgiTableEntry>();

	switch (action.action_type) {
	case MergeActionType::MERGE_UPDATE: {
		if (!table.GetTableInfo().supports_update) {
			throw BinderException("Table '%s' does not support UPDATE", table.name);
		}
		auto &upd = planner.Make<VgiPhysicalUpdate>(op, table, op.return_chunk,
		                                             std::move(action.columns), std::move(action.expressions));
		result->op = upd;
		break;
	}
	case MergeActionType::MERGE_DELETE: {
		if (!table.GetTableInfo().supports_delete) {
			throw BinderException("Table '%s' does not support DELETE", table.name);
		}
		auto &del = planner.Make<VgiPhysicalDelete>(op, table, op.return_chunk, op.row_id_start);
		result->op = del;
		break;
	}
	case MergeActionType::MERGE_INSERT: {
		if (!table.GetTableInfo().supports_insert) {
			throw BinderException("Table '%s' does not support INSERT", table.name);
		}
		auto &ins = planner.Make<VgiPhysicalInsert>(op, table, op.return_chunk);
		if (!action.column_index_map.empty()) {
			vector<unique_ptr<Expression>> new_expressions;
			for (auto &col : op.table.GetColumns().Physical()) {
				auto storage_idx = col.StorageOid();
				auto mapped_index = action.column_index_map[col.Physical()];
				if (mapped_index == DConstants::INVALID_INDEX) {
					new_expressions.push_back(op.bound_defaults[storage_idx]->Copy());
				} else {
					new_expressions.push_back(std::move(action.expressions[mapped_index]));
				}
			}
			action.expressions = std::move(new_expressions);
		}
		result->expressions = std::move(action.expressions);
		result->op = ins;
		break;
	}
	case MergeActionType::MERGE_ERROR:
		result->expressions = std::move(action.expressions);
		break;
	case MergeActionType::MERGE_DO_NOTHING:
		break;
	default:
		throw InternalException("Unsupported merge action");
	}
	return result;
}

PhysicalOperator &VgiCatalog::PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner,
                                             LogicalMergeInto &op, PhysicalOperator &plan) {
	auto &table_for_returning = op.table.Cast<VgiTableEntry>();

	// Refuse MERGE on multi-branch tables, regardless of which clauses the
	// user specified (WHEN MATCHED / WHEN NOT MATCHED). Refusing here catches
	// the "MERGE WHEN NOT MATCHED THEN INSERT only" edge case that would
	// otherwise slip through (since the INSERT sub-operator's refusal helper
	// permits writable multi-branch). MERGE on multi-branch tables remains
	// unsupported until concrete customer requirements for cross-arm
	// semantics arrive — see docs/multi_branch.md.
	//
	// Short-circuits on the cheap multi-branch hint to avoid an RPC on every
	// MERGE bind to a single-branch VGI table.
	if (!table_for_returning.IsKnownSingleBranchNoAT()) {
		auto branches_result =
		    table_for_returning.FetchScanBranches(context, /*at_unit=*/"", /*at_value=*/"");
		if (branches_result.branches.size() > 1) {
			throw BinderException(
			    "MERGE is not supported on multi-branch VGI table '%s.%s' (%d branches). "
			    "Issue the MERGE directly against the writable arm's underlying VGI table "
			    "(declare it as a single-branch VGI table for write access). "
			    "(MERGE on multi-branch tables is not supported pending concrete "
			    "customer requirements for cross-arm semantics — see "
			    "docs/multi_branch.md.)",
			    table_for_returning.ParentSchema().name, table_for_returning.name,
			    static_cast<int>(branches_result.branches.size()));
		}
	}

	if (op.return_chunk && !table_for_returning.GetTableInfo().supports_returning) {
		throw BinderException("Table '%s' does not support RETURNING on MERGE",
		                      table_for_returning.name);
	}
	map<MergeActionCondition, vector<unique_ptr<MergeIntoOperator>>> actions;

	idx_t append_count = 0;
	for (auto &entry : op.actions) {
		vector<unique_ptr<MergeIntoOperator>> planned_actions;
		for (auto &action : entry.second) {
			if (action->action_type == MergeActionType::MERGE_INSERT) {
				append_count++;
			}
			if (action->action_type == MergeActionType::MERGE_UPDATE && action->update_is_del_and_insert) {
				append_count++;
			}
			planned_actions.push_back(VgiPlanMergeIntoAction(context, op, planner, *action));
		}
		actions.emplace(entry.first, std::move(planned_actions));
	}

	bool parallel = append_count <= 1 && !op.return_chunk;

	auto &result = planner.Make<PhysicalMergeInto>(op.types, std::move(actions), op.row_id_start, op.source_marker,
	                                               parallel, op.return_chunk);
	result.children.push_back(plan);
	return result;
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

void VgiCatalog::ClearCache(bool force) {
	// Drop this catalog's result-cache entries too (both the deferred and
	// forced paths, and the version-bump caller). Result entries are keyed by
	// the catalog's ATTACH identity, so a version bump / DDL / manual clear all
	// invalidate them here.
	if (attach_parameters_) {
		// identity_scope (catalog name + auth fingerprint) locates the per-identity
		// disk shard so we drop ONLY this identity's on-disk entries.
		auto identity = vgi::BuildCatalogIdentityScope(attach_parameters_->catalog_name(),
		                                               attach_parameters_->auth());
		vgi::VgiResultCache::Instance().FlushCatalog(attach_parameters_->catalog_name(), identity);
	}
	auto harvested = schemas.HarvestEntries();
	if (force) {
		// User-facing vgi_clear_cache(): purge the graveyard too. Bound
		// prepared statements may now dangle — caller's choice.
		std::lock_guard<std::mutex> lk(graveyard_mutex_);
		deferred_dropped_entries_.clear();
		graveyard_drops_since_last_log_ = 0;
		// `harvested` falls out of scope here and is destroyed.
		return;
	}
	AbsorbDroppedEntries(std::move(harvested));
}

void VgiCatalog::AbsorbDroppedEntry(unique_ptr<CatalogEntry> entry) {
	if (!entry) {
		return;
	}
	std::lock_guard<std::mutex> lk(graveyard_mutex_);
	deferred_dropped_entries_.push_back(std::move(entry));
	while (deferred_dropped_entries_.size() > kGraveyardLimit) {
		deferred_dropped_entries_.erase(deferred_dropped_entries_.begin());
		graveyard_drops_since_last_log_++;
	}
}

void VgiCatalog::AbsorbDroppedEntries(std::vector<unique_ptr<CatalogEntry>> entries) {
	if (entries.empty()) {
		return;
	}
	std::lock_guard<std::mutex> lk(graveyard_mutex_);
	for (auto &entry : entries) {
		if (entry) {
			deferred_dropped_entries_.push_back(std::move(entry));
		}
	}
	while (deferred_dropped_entries_.size() > kGraveyardLimit) {
		deferred_dropped_entries_.erase(deferred_dropped_entries_.begin());
		graveyard_drops_since_last_log_++;
	}
}

bool VgiCatalog::CheckAndInvalidateCache(ClientContext &context, const std::vector<uint8_t> &transaction_opaque_data) {
	// No attach_result_ means we never completed catalog_attach (e.g., the
	// constructor allows it for in-progress / restored states). With no
	// attach_opaque_data we can't address the worker, so skip the probe.
	if (!attach_result_) {
		VGI_LOG(context, "catalog.invalidate.skip", {{"reason", "no_attach_result"}});
		return false;
	}
	// Frozen catalogs never change metadata — skip version check entirely
	if (attach_result_->catalog_version_frozen) {
		VGI_LOG(context, "catalog.invalidate.skip", {{"reason", "frozen"}});
		return false;
	}

	// Query current version from the worker
	vgi::CatalogRpcContext rpc_ctx{attach_parameters_, attach_result_->attach_opaque_data, transaction_opaque_data};
	int64_t current_version = vgi::InvokeCatalogVersion(rpc_ctx, context);
	int64_t last_version = last_known_catalog_version_.load();

	// Version 0 means "unimplemented" or "unknown" — always clear for safety
	// (preserves existing behavior for workers that don't support catalog_version)
	if (current_version == 0) {
		VGI_LOG(context, "catalog.invalidate",
		        {{"current_version", std::to_string(current_version)},
		         {"last_version", std::to_string(last_version)},
		         {"action", "clear_unknown"}});
		ClearCache();
		return true;
	}

	// Compare with last known version
	if (current_version != last_version) {
		VGI_LOG(context, "catalog.invalidate",
		        {{"current_version", std::to_string(current_version)},
		         {"last_version", std::to_string(last_version)},
		         {"action", "clear_changed"}});
		last_known_catalog_version_.store(current_version);
		ClearCache();
		return true;
	}

	VGI_LOG(context, "catalog.invalidate",
	        {{"current_version", std::to_string(current_version)},
	         {"last_version", std::to_string(last_version)},
	         {"action", "noop"}});
	return false;
}

void VgiCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	if (access_mode_ == AccessMode::READ_ONLY) {
		throw BinderException("Cannot DROP SCHEMA in read-only VGI catalog '%s'", GetName());
	}

	bool ignore_not_found = (info.if_not_found == OnEntryNotFound::RETURN_NULL);
	bool cascade = (info.cascade);

	if (!attach_result_) {
		throw IOException("VGI DROP SCHEMA: catalog '%s' has no attach_result_", GetName());
	}
	auto &vgi_tx = VgiTransaction::Get(context, *this);

	vgi::CatalogRpcContext rpc_ctx{attach_parameters_, attach_result_->attach_opaque_data, vgi_tx.GetTransactionOpaqueData()};
	vgi::InvokeCatalogSchemaDrop(rpc_ctx, info.name, ignore_not_found, cascade, context);

	// Invalidate schema cache via the deferred path so any bound query
	// holding a CatalogEntry* doesn't dangle.
	ClearCache(/*force=*/false);
}

} // namespace duckdb
