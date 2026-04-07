#pragma once

#include <atomic>

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/enums/access_mode.hpp"

#include "storage/vgi_schema_set.hpp"
#include "vgi_catalog_api.hpp"

namespace duckdb {

class VgiSchemaEntry;

class VgiCatalog : public Catalog {
public:
	VgiCatalog(AttachedDatabase &db_p, const std::string &internal_name, AccessMode access_mode,
	           std::shared_ptr<vgi::VgiAttachParameters> attach_params,
	           std::shared_ptr<vgi::CatalogAttachResult> attach_result);
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

	void ClearCache();

	/// Check the worker's catalog version; clear cache only if it has changed.
	/// No-op if catalog_version_frozen is true.
	/// Returns true if cache was cleared.
	bool CheckAndInvalidateCache(ClientContext &context, const std::vector<uint8_t> &transaction_id);

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

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;

private:
	AccessMode access_mode_;
	std::shared_ptr<vgi::VgiAttachParameters> attach_parameters_;
	std::shared_ptr<vgi::CatalogAttachResult> attach_result_;
	std::string internal_name_;
	std::string default_schema_;
	VgiSchemaSet schemas;

	/// Last known catalog version from the worker. Initialized from attach_result.
	/// Atomic since it can be read/written from concurrent transactions.
	std::atomic<int64_t> last_known_catalog_version_{0};
};

} // namespace duckdb
