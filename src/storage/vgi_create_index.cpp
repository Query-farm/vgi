// © Copyright 2026 Query Farm LLC - https://query.farm
#include "storage/vgi_catalog.hpp"
#include "vgi_index_contract.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {
namespace {
class VgiPhysicalCreateIndex final : public PhysicalOperator {
public:
	VgiPhysicalCreateIndex(PhysicalPlan &plan, unique_ptr<CreateIndexInfo> definition)
	    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, 1),
	      info(std::move(definition)) {
	}
	bool IsSource() const override { return true; }
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &, OperatorSourceInput &) const override {
		auto &catalog = Catalog::GetCatalog(context.client, info->catalog);
		catalog.CreateIndex(context.client, *info);
		return SourceResultType::FINISHED;
	}
	string GetName() const override { return "VGI_CREATE_INDEX"; }
private:
	unique_ptr<CreateIndexInfo> info;
};
class VgiLogicalCreateIndex final : public LogicalExtensionOperator {
public:
	explicit VgiLogicalCreateIndex(unique_ptr<CreateIndexInfo> definition) : info(std::move(definition)) {
	}
	void ResolveTypes() override { types = {LogicalType::BIGINT}; }
	bool SupportSerialization() const override { return false; }
	PhysicalOperator &CreatePlan(ClientContext &, PhysicalPlanGenerator &planner) override {
		return planner.Make<VgiPhysicalCreateIndex>(std::move(info));
	}
	string GetName() const override { return "VGI_CREATE_INDEX"; }
	string GetExtensionName() const override { return "vgi_create_index"; }
private:
	unique_ptr<CreateIndexInfo> info;
};
} // namespace

unique_ptr<LogicalOperator> VgiCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
    TableCatalogEntry &table, unique_ptr<LogicalOperator>) {
	if (GetAccessMode() == AccessMode::READ_ONLY) throw BinderException("Cannot CREATE INDEX in read-only VGI catalog");
	auto &info = stmt.info->Cast<CreateIndexInfo>();
	vgi::RequireNativeIndexCapabilities(*attach_result(), vgi::VGI_INDEX_CREATE,
	    info.constraint_type == IndexConstraintType::UNIQUE);
	const auto &collation = Settings::Get<DefaultCollationSetting>(binder.context);
	if (!collation.empty() && !StringUtil::CIEquals(collation, "binary"))
		throw BinderException("VGI raw index keys require binary default collation");
	// Validation is effect-free. Only the physical operator dispatches CREATE,
	// so PREPARE and EXPLAIN never publish an index or build its rows locally.
	vgi::EncodeNativeIndexDefinition(info, table);
	info.catalog = GetName();
	info.schema = table.ParentSchema().name;
	info.table = table.name;
	return make_uniq<VgiLogicalCreateIndex>(unique_ptr_cast<CreateInfo, CreateIndexInfo>(std::move(stmt.info)));
}
} // namespace duckdb
