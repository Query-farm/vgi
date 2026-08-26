// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <mutex>
#include <optional>

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "storage/vgi_aggregate_function_set.hpp"
#include "storage/vgi_macro_set.hpp"
#include "storage/vgi_object_counts.hpp"
#include "storage/vgi_scalar_function_set.hpp"
#include "storage/vgi_table_function_set.hpp"
#include "storage/vgi_table_set.hpp"
#include "storage/vgi_view_set.hpp"
#include "vgi_catalog_metadata.hpp"

namespace duckdb {

class VgiCatalog;

namespace vgi {
struct CatalogRpcContext;
}

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

	// Estimated per-kind populations from the worker. Built once at
	// construction from ``schema_info_.estimated_object_count`` (a
	// map<string,int64>). Read by VgiCatalogSet::ResolveEagerLoadParamsLocked
	// to pick the eager-vs-lazy load policy for each child set without
	// re-walking the wire-format map.
	const VgiObjectCounts &GetEstimatedCounts() const {
		return estimated_counts_;
	}

	// Schema-lifetime macro discovery cache. Both macro catalog sets share the
	// same immutable metadata/name snapshot, avoiding duplicate function and
	// opposite-macro inventory RPCs. The schema mutex never reaches back into a
	// child set, so concurrent lazy loads cannot form a child/schema lock cycle.
	std::vector<vgi::VgiMacroInfo> GetMacroInventory(const vgi::CatalogRpcContext &rpc_ctx, CatalogType macro_type,
	                                                 ClientContext &context);
	std::vector<vgi::VgiFunctionInfo> GetFunctionInventory(const vgi::CatalogRpcContext &rpc_ctx,
	                                                       CatalogType function_type, ClientContext &context);
	std::shared_ptr<const case_insensitive_set_t> GetMacroCallableNames(const vgi::CatalogRpcContext &rpc_ctx,
	                                                                    bool trust_empty_kinds, ClientContext &context);

private:
	VgiCatalogSet &GetCatalogSet(CatalogType type);

	vgi::VgiSchemaInfo schema_info_;
	VgiObjectCounts estimated_counts_;
	VgiTableSet tables_;
	VgiViewSet views_;
	VgiScalarFunctionSet scalar_functions_;
	VgiAggregateFunctionSet aggregate_functions_;
	VgiTableFunctionSet table_functions_;
	VgiMacroSet scalar_macros_;
	VgiMacroSet table_macros_;
	std::mutex macro_discovery_mutex_;
	std::optional<std::vector<vgi::VgiMacroInfo>> scalar_macro_inventory_;
	std::optional<std::vector<vgi::VgiMacroInfo>> table_macro_inventory_;
	std::optional<std::vector<vgi::VgiFunctionInfo>> scalar_function_inventory_;
	std::optional<std::vector<vgi::VgiFunctionInfo>> aggregate_function_inventory_;
	std::optional<std::vector<vgi::VgiFunctionInfo>> table_function_inventory_;
	std::shared_ptr<const case_insensitive_set_t> macro_callable_names_;
	// True after every function kind has been inspected for macro dependencies.
	// Estimated zero counts are deliberately not trusted at this correctness
	// boundary.
	bool macro_callable_names_complete_ = false;
};

} // namespace duckdb
