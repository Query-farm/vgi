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

static int WriteResultModeRank(const string &mode) {
	if (mode == "count") {
		return 0;
	}
	if (mode == "rows") {
		return 1;
	}
	if (mode == "changes") {
		return 2;
	}
	throw InternalException("Unknown validated VGI write result mode '%s'", mode);
}

static void RequireWriteResultMode(const VgiTableEntry &table, const string &operation, bool return_chunk) {
	const auto requested = return_chunk ? "rows" : "count";
	const auto &modes = table.GetTableInfo().write_result_modes;
	auto entry = modes.find(operation);
	if (entry == modes.end()) {
		throw BinderException("Table '%s' does not support %s", table.name, StringUtil::Upper(operation));
	}
	if (WriteResultModeRank(entry->second) < WriteResultModeRank(requested)) {
		throw BinderException("Table '%s' does not support result mode '%s' for %s (maximum is '%s')", table.name,
		                      requested, StringUtil::Upper(operation), entry->second);
	}
}

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
	// The catalog lifetime pin is no longer needed. Running/pooled subprocesses
	// carry independent leases, so cleanup remains unable to race live code.
	attach_parameters_->ReleaseWorkerArtifactAnchor();
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
	schemas.Scan(context, [&](CatalogEntry &entry) {
		// A schema seeded from catalog_contents decodes its SchemaInfo lazily;
		// scans (duckdb_schemas() & co.) read its comment and tags.
		auto &schema = entry.Cast<VgiSchemaEntry>();
		schema.EnsureSchemaInfo();
		callback(schema);
	});
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
	RequireWriteResultMode(table, "insert", op.return_chunk);
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
	RequireWriteResultMode(table, "delete", op.return_chunk);
	auto &bound_ref = op.expressions[0]->Cast<BoundReferenceExpression>();
	auto &del = planner.Make<VgiPhysicalDelete>(op, table, op.return_chunk, bound_ref.index);
	del.children.push_back(plan);
	return del;
}

PhysicalOperator &VgiCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                         PhysicalOperator &plan) {
	auto &table = op.table.Cast<VgiTableEntry>();
	RequireWriteResultMode(table, "update", op.return_chunk);
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
		RequireWriteResultMode(table, "update", op.return_chunk);
		auto &upd = planner.Make<VgiPhysicalUpdate>(op, table, op.return_chunk,
		                                             std::move(action.columns), std::move(action.expressions));
		result->op = upd;
		break;
	}
	case MergeActionType::MERGE_DELETE: {
		RequireWriteResultMode(table, "delete", op.return_chunk);
		auto &del = planner.Make<VgiPhysicalDelete>(op, table, op.return_chunk, op.row_id_start);
		result->op = del;
		break;
	}
	case MergeActionType::MERGE_INSERT: {
		RequireWriteResultMode(table, "insert", op.return_chunk);
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
		vgi::GetResultCache(GetDatabase()).FlushCatalog(attach_parameters_->catalog_name(), identity);
	}
	{
		// A revalidation snapshot not yet taken predates whatever this clear is
		// for (DDL, a version bump, vgi_clear_cache()); the reload fetches anew.
		std::lock_guard<std::mutex> lk(contents_mutex_);
		pending_contents_.reset();
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

	vgi::CatalogRpcContext rpc_ctx{attach_parameters_, attach_result_->attach_opaque_data, transaction_opaque_data};

	// Conditional revalidation replaces the catalog_version poll when the
	// caches were built from a catalog_contents snapshot that carried an etag.
	std::optional<std::string> etag;
	{
		std::lock_guard<std::mutex> lk(contents_mutex_);
		etag = contents_etag_;
	}
	if (etag && attach_result_->supports_catalog_contents && vgi::UseCatalogContents(context)) {
		if (!schemas.Loaded()) {
			// Nothing is cached (cleared by DDL / vgi_clear_cache()): the next
			// schema-set load fetches a fresh snapshot anyway, so a conditional
			// call now could only cost a second catalog_contents.
			VGI_LOG(context, "catalog.invalidate.skip", {{"reason", "not_loaded"}});
			return false;
		}
		if (auto cleared = RevalidateContents(context, rpc_ctx, *etag)) {
			return *cleared;
		}
	}

	// Query current version from the worker
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

bool VgiCatalog::AdoptCatalogVersion(int64_t version) {
	// Versions are monotonic within a session: keep the larger of the two, and
	// report a snapshot older than what is already known.
	int64_t known = last_known_catalog_version_.load(std::memory_order_acquire);
	while (version >= known) {
		if (version == known ||
		    last_known_catalog_version_.compare_exchange_weak(known, version, std::memory_order_acq_rel)) {
			return true;
		}
	}
	return false;
}

bool VgiCatalog::ReloadUsesCatalogContents() {
	if (attach_result_->catalog_version_frozen || GetKnownCatalogVersion() != 0) {
		return true;
	}
	std::lock_guard<std::mutex> lk(contents_mutex_);
	return contents_etag_.has_value();
}

std::shared_ptr<vgi::VgiCatalogContents> VgiCatalog::TakeCatalogContents(ClientContext &context,
                                                                         const vgi::CatalogRpcContext &rpc_ctx) {
	if (!attach_result_ || !attach_result_->supports_catalog_contents) {
		return nullptr;
	}
	std::shared_ptr<vgi::VgiCatalogContents> pending;
	bool reload;
	{
		std::lock_guard<std::mutex> lk(contents_mutex_);
		pending = std::move(pending_contents_);
		reload = contents_loaded_once_;
		contents_loaded_once_ = true;
	}
	const auto forget_etag = [&]() {
		// Caches about to be built without catalog_contents have no etag:
		// the next transaction start polls catalog_version instead.
		std::lock_guard<std::mutex> lk(contents_mutex_);
		contents_etag_.reset();
	};
	if (!vgi::UseCatalogContents(context)) {
		forget_etag();
		return nullptr;
	}
	if (pending) {
		VGI_LOG(context, "catalog.contents",
		        {{"outcome", "loaded"},
		         {"source", "revalidation"},
		         {"schemas", std::to_string(pending->schemas.size())},
		         {"catalog_version", std::to_string(pending->catalog_version)}});
		return pending;
	}
	// Version-0 rule: a non-frozen worker reporting version 0 without an etag
	// has its cache cleared at every transaction start; reloading the whole
	// catalog each time would download it per statement, so reload lazily.
	if (reload && !ReloadUsesCatalogContents()) {
		VGI_LOG(context, "catalog.contents", {{"outcome", "skipped"}, {"reason", "unversioned_reload"}});
		return nullptr;
	}
	// Version adoption: a snapshot at least as new as the known version is
	// current, and its version becomes the known one (so the next
	// transaction-start check does not clear it as "changed"). An older one
	// (a lagging replica / reordered response) is retried once, then the
	// per-schema RPCs are used. A snapshot version of 0 means "unknown", as
	// for catalog_version, and is accepted without adopting it.
	for (int attempt = 1; attempt <= 2; attempt++) {
		std::shared_ptr<vgi::VgiCatalogContents> snapshot;
		try {
			snapshot = std::make_shared<vgi::VgiCatalogContents>(vgi::InvokeCatalogContents(rpc_ctx, context));
		} catch (std::exception &e) {
			VGI_LOG(context, "catalog.contents", {{"outcome", "fallback"}, {"error_message", e.what()}});
			forget_etag();
			return nullptr;
		}
		const int64_t known = GetKnownCatalogVersion();
		if (snapshot->catalog_version == 0 || AdoptCatalogVersion(snapshot->catalog_version)) {
			{
				std::lock_guard<std::mutex> lk(contents_mutex_);
				contents_etag_ = snapshot->etag;
			}
			VGI_LOG(context, "catalog.contents",
			        {{"outcome", "loaded"},
			         {"schemas", std::to_string(snapshot->schemas.size())},
			         {"catalog_version", std::to_string(snapshot->catalog_version)},
			         {"etag", snapshot->etag.value_or("")}});
			return snapshot;
		}
		VGI_LOG(context, "catalog.contents",
		        {{"outcome", "stale"},
		         {"attempt", std::to_string(attempt)},
		         {"catalog_version", std::to_string(snapshot->catalog_version)},
		         {"known_version", std::to_string(known)}});
	}
	VGI_LOG(context, "catalog.contents", {{"outcome", "fallback"}, {"error_message", "snapshot older than known catalog_version"}});
	forget_etag();
	return nullptr;
}

std::optional<bool> VgiCatalog::RevalidateContents(ClientContext &context, const vgi::CatalogRpcContext &rpc_ctx,
                                                   const std::string &etag) {
	std::shared_ptr<vgi::VgiCatalogContents> snapshot;
	try {
		snapshot = std::make_shared<vgi::VgiCatalogContents>(vgi::InvokeCatalogContents(rpc_ctx, context, etag));
	} catch (std::exception &e) {
		VGI_LOG(context, "catalog.invalidate",
		        {{"via", "catalog_contents"}, {"action", "revalidate_failed"}, {"error_message", e.what()}});
		std::lock_guard<std::mutex> lk(contents_mutex_);
		contents_etag_.reset();
		return std::nullopt;
	}
	const int64_t last_version = GetKnownCatalogVersion();
	const auto log = [&](const char *action) {
		VGI_LOG(context, "catalog.invalidate",
		        {{"via", "catalog_contents"},
		         {"current_version", std::to_string(snapshot->catalog_version)},
		         {"last_version", std::to_string(last_version)},
		         {"etag", snapshot->etag.value_or("")},
		         {"action", action}});
	};
	if (snapshot->not_modified) {
		// Same contents: keep every cache. The version may still have moved
		// (e.g. a DDL that was undone); record it.
		if (snapshot->catalog_version != 0) {
			AdoptCatalogVersion(snapshot->catalog_version);
		}
		log("not_modified");
		return false;
	}
	// The catalog changed (or the worker stopped revalidating). Replace the
	// snapshot: clear the sets, then hand the fresh contents to the next
	// schema-set load. An older snapshot (version went backwards) is not
	// reused; the reload fetches its own, under the adoption rule.
	const bool current = snapshot->catalog_version == 0 || AdoptCatalogVersion(snapshot->catalog_version);
	log(current ? "clear_modified" : "clear_stale");
	ClearCache();
	std::lock_guard<std::mutex> lk(contents_mutex_);
	contents_etag_ = current ? snapshot->etag : std::nullopt;
	if (current) {
		pending_contents_ = std::move(snapshot);
	}
	return true;
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
