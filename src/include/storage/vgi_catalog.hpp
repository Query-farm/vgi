// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <atomic>
#include <mutex>
#include <vector>

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/enums/access_mode.hpp"

#include "storage/vgi_object_counts.hpp"
#include "storage/vgi_schema_set.hpp"
#include "vgi_catalog_api.hpp"

namespace duckdb {

class VgiSchemaEntry;

class VgiCatalog : public Catalog {
public:
	VgiCatalog(AttachedDatabase &db_p, const std::string &internal_name, AccessMode access_mode,
	           std::shared_ptr<vgi::VgiAttachParameters> attach_params,
	           std::shared_ptr<vgi::CatalogAttachResult> attach_result,
	           VgiObjectCounts eager_load_thresholds);
	~VgiCatalog() override;

public:
	void Initialize(bool load_builtin) override;

	std::string GetCatalogType() override {
		return "vgi";
	}

	std::string GetDefaultSchema() const override;

	bool SupportsTimeTravel() const override {
		return attach_result_ && attach_result_->supports_time_travel;
	}

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;

	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;

	// These operations are not supported for read-only VGI catalogs
	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner, LogicalMergeInto &op,
	                                PhysicalOperator &plan) override;

	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;

	bool InMemory() override;
	std::string GetDBPath() override;

	/// Default behaviour (force=false): move the cached schema entries into
	/// a deferred-drop graveyard so any raw CatalogEntry* pointers held by
	/// bound prepared statements remain valid until the catalog is
	/// destroyed. The graveyard is bounded; once it fills the oldest entries
	/// are dropped (logged).
	///
	/// Force-clear (force=true): purge the graveyard along with the live
	/// entries. Used by the explicit user-facing `vgi_clear_cache()`
	/// table function — the user accepts that bound queries may break.
	void ClearCache(bool force = false);

	/// Push entries that a *child* set just harvested into the graveyard.
	/// Called from VgiSchemaEntry's DDL paths so a single dropped/altered
	/// table or view doesn't yank a CatalogEntry* pointer that a bound
	/// query is still holding. Bounded by kGraveyardLimit.
	void AbsorbDroppedEntry(unique_ptr<CatalogEntry> entry);
	void AbsorbDroppedEntries(std::vector<unique_ptr<CatalogEntry>> entries);

	/// Check the worker's catalog version; clear cache only if it has changed.
	/// No-op if catalog_version_frozen is true.
	/// Returns true if cache was cleared.
	bool CheckAndInvalidateCache(ClientContext &context, const std::vector<uint8_t> &transaction_opaque_data);

	const std::string &internal_name() const {
		return internal_name_;
	}

	const std::shared_ptr<vgi::VgiAttachParameters> &attach_parameters() const {
		return attach_parameters_;
	}

	const std::shared_ptr<vgi::CatalogAttachResult> &attach_result() const {
		return attach_result_;
	}

	AccessMode GetAccessMode() const {
		return access_mode_;
	}

	const VgiObjectCounts &EagerLoadThresholds() const {
		return eager_load_thresholds_;
	}

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;

private:
	AccessMode access_mode_;
	std::shared_ptr<vgi::VgiAttachParameters> attach_parameters_;
	std::shared_ptr<vgi::CatalogAttachResult> attach_result_;
	std::string internal_name_;
	std::string default_schema_;
	VgiObjectCounts eager_load_thresholds_;
	VgiSchemaSet schemas;

	/// Last known catalog version from the worker. Initialized from attach_result.
	/// Atomic since it can be read/written from concurrent transactions.
	std::atomic<int64_t> last_known_catalog_version_{0};

	/// Deferred-drop graveyard. Each ClearCache() (non-force) moves the
	/// schema set's entries here so any bound query holding a raw pointer
	/// stays alive. Bounded so a long-lived catalog with frequent DDL
	/// doesn't grow forever — when the bound is hit we drop the oldest
	/// entries (with a warning log on the next ClearCache).
	mutable std::mutex graveyard_mutex_;
	std::vector<unique_ptr<CatalogEntry>> deferred_dropped_entries_;
	size_t graveyard_drops_since_last_log_ = 0;
	static constexpr size_t kGraveyardLimit = 1024;
};

} // namespace duckdb
