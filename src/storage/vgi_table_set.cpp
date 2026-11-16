// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_constraint_contract.hpp"
#include "vgi_column_expression.hpp"
#include "storage/vgi_table_set.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"

#include "storage/vgi_catalog.hpp"
#include "storage/vgi_schema_entry.hpp"
#include "storage/vgi_table_entry.hpp"
#include "storage/vgi_transaction.hpp"
#include "vgi_catalog_rpc.hpp"
#include "vgi_logging.hpp"

#include <chrono>

namespace duckdb {

VgiTableSet::VgiTableSet(Catalog &catalog, VgiSchemaEntry &schema) : VgiCatalogSet(catalog, &schema), schema_(schema) {
}

void VgiTableSet::LoadEntries(ClientContext &context, const std::lock_guard<std::mutex> & /*_load_lock*/) {
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();

	if (!attach_params || !attach_result) {
		return;
	}

	// entry_lock_ is NOT held during the RPC; the caller's load_lock_
	// serializes concurrent loaders.
	auto &vgi_tx_load = VgiTransaction::Get(context, catalog_);
	vgi::CatalogRpcContext rpc_ctx {attach_params, attach_result->attach_opaque_data,
	                                vgi_tx_load.GetTransactionOpaqueData()};
	rpc_ctx.entity_kind = "schema";
	rpc_ctx.entity_qualifier = schema_.name.GetIdentifierName();
	auto tables = schema_.TakeTables(rpc_ctx, context);

	// Take entry_lock_ per entry, not across the loop.
	for (auto &table_info : tables) {
		vgi::RequireNativeConstraintRead(*attach_result, table_info);
		auto create_info = vgi::CreateTableInfoFromVgiTable(context, table_info, schema_.name.GetIdentifierName());
		auto table_entry = make_uniq<VgiTableEntry>(catalog_, schema_, create_info, table_info);
		{
			std::lock_guard<std::mutex> entry_lk(entry_lock_);
			CreateEntryLocked(std::move(table_entry));
		}
	}
}

// On-demand load of one table. entry_lock_ is held only to inspect and to
// publish, never across the worker RPC. Every VgiCatalogSet mutation bumps
// generation_; publishing only if it is unchanged keeps a table dropped during
// the RPC from being resurrected.
optional_ptr<CatalogEntry> VgiTableSet::GetEntry(ClientContext &context, const std::string &name) {
	auto rpc_start = std::chrono::steady_clock::now();
	uint64_t pre_gen;
	std::string qualifier = schema_.name.GetIdentifierName() + "." + name;
	bool eager_path = false;
	// Read settings OUTSIDE entry_lock_: DuckDB's setting registry and log
	// manager have their own mutexes, so calling them under it risks lock-order
	// inversion.
	const bool trust_empty_kinds = ReadTrustEmptyKinds(context);

	optional_ptr<CatalogEntry> cached_result = nullptr;
	const char *fast_outcome = nullptr;
	{
		std::lock_guard<std::mutex> lock(entry_lock_);
		auto it = GetEntries().find(name);
		if (it != GetEntries().end()) {
			cached_result = it->second.get();
			fast_outcome = "hit";
		} else if (IsKnownMissingLocked(name)) {
			fast_outcome = "negative_hit";
		} else if (ShouldBypassRpcLocked(context, trust_empty_kinds)) {
			is_loaded_ = true;
			fast_outcome = "kind_empty";
		} else {
			eager_path = ShouldEagerLoadLocked();
			pre_gen = generation_.load(std::memory_order_acquire);
		}
	}
	if (fast_outcome) {
		VGI_LOG(context, "catalog.entry_cache",
		        {{"set_kind", CacheKindName()},
		         {"name", name},
		         {"qualifier", qualifier},
		         {"outcome", fast_outcome},
		         {"triggered_load", "false"}});
		return cached_result;
	}

	if (eager_path) {
		// Same bulk fetch as LoadEntries(), but publishing only if generation_
		// is unchanged, so concurrent DDL cannot resurrect a dropped table.
		auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
		auto &attach_params = vgi_catalog.attach_parameters();
		auto &attach_result = vgi_catalog.attach_result();
		if (attach_params && attach_result) {
			auto &vgi_tx = VgiTransaction::Get(context, catalog_);
			vgi::CatalogRpcContext rpc_ctx {attach_params, attach_result->attach_opaque_data,
			                                vgi_tx.GetTransactionOpaqueData()};
			rpc_ctx.entity_kind = "schema";
			rpc_ctx.entity_qualifier = schema_.name.GetIdentifierName();
			auto tables = schema_.TakeTables(rpc_ctx, context);

			// Build entries OUTSIDE entry_lock_: CreateTableInfoFromVgiTable
			// may resolve types that re-enter the catalog.
			std::vector<unique_ptr<CatalogEntry>> built_entries;
			built_entries.reserve(tables.size());
			for (auto &table_info : tables) {
				vgi::RequireNativeConstraintRead(*attach_result, table_info);
				auto create_info =
				    vgi::CreateTableInfoFromVgiTable(context, table_info, schema_.name.GetIdentifierName());
				built_entries.push_back(make_uniq<VgiTableEntry>(catalog_, schema_, create_info, table_info));
			}
			{
				std::lock_guard<std::mutex> lock(entry_lock_);
				if (generation_.load(std::memory_order_acquire) == pre_gen && !is_loaded_) {
					for (auto &e : built_entries) {
						CreateEntryLocked(std::move(e));
					}
					is_loaded_ = true;
				}
			}
			optional_ptr<CatalogEntry> eager_result = nullptr;
			{
				std::lock_guard<std::mutex> find_lock(entry_lock_);
				auto it = GetEntries().find(name);
				if (it != GetEntries().end()) {
					eager_result = it->second.get();
				} else {
					// Not in the bulk listing: fall through to a per-name RPC.
					pre_gen = generation_.load(std::memory_order_acquire);
				}
			}
			if (eager_result) {
				VGI_LOG(context, "catalog.entry_cache",
				        {{"set_kind", CacheKindName()},
				         {"name", name},
				         {"qualifier", qualifier},
				         {"outcome", "miss_loaded"},
				         {"triggered_load", "true"},
				         {"loaded_reason", "below_threshold"}});
				return eager_result;
			}
		}
	}

	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();

	if (!attach_params || !attach_result) {
		VGI_LOG(context, "catalog.entry_cache",
		        {{"set_kind", CacheKindName()}, {"name", name}, {"qualifier", qualifier}, {"outcome", "not_attached"}});
		return nullptr;
	}

	auto &vgi_tx = VgiTransaction::Get(context, catalog_);
	vgi::CatalogRpcContext rpc_ctx {attach_params, attach_result->attach_opaque_data,
	                                vgi_tx.GetTransactionOpaqueData()};
	rpc_ctx.entity_kind = "table";
	rpc_ctx.entity_qualifier = qualifier;
	auto table_info_opt = vgi::InvokeCatalogTableGet(rpc_ctx, schema_.name.GetIdentifierName(), name, context);

	auto rpc_end = std::chrono::steady_clock::now();
	double rpc_ms = std::chrono::duration<double, std::milli>(rpc_end - rpc_start).count();

	if (!table_info_opt) {
		{
			std::lock_guard<std::mutex> lock(entry_lock_);
			CacheMissingEntryLocked(name, pre_gen);
		}
		VGI_LOG(context, "catalog.entry_cache",
		        {{"set_kind", CacheKindName()},
		         {"name", name},
		         {"qualifier", qualifier},
		         {"outcome", "not_found"},
		         {"duration_ms", std::to_string(rpc_ms)}});
		return nullptr;
	}

	auto &table_info = *table_info_opt;
	vgi::RequireNativeConstraintRead(*catalog_.Cast<VgiCatalog>().attach_result(), table_info);
	auto create_info = vgi::CreateTableInfoFromVgiTable(context, table_info, schema_.name.GetIdentifierName());
	auto table_entry = make_uniq<VgiTableEntry>(catalog_, schema_, create_info, table_info);

	// Log only after releasing entry_lock_ (see settings note above).
	optional_ptr<CatalogEntry> pub_result = nullptr;
	const char *pub_outcome = nullptr;
	{
		std::lock_guard<std::mutex> lock(entry_lock_);
		if (generation_.load(std::memory_order_acquire) != pre_gen) {
			pub_outcome = "generation_raced";
		} else {
			auto it = GetEntries().find(name);
			if (it != GetEntries().end()) {
				pub_result = it->second.get();
				pub_outcome = "concurrent_published";
			} else {
				pub_result = table_entry.get();
				missing_entries_.erase(name);
				GetEntries()[name] = std::move(table_entry);
				generation_.fetch_add(1, std::memory_order_release);
				pub_outcome = "rpc_fetched";
			}
		}
	}
	VGI_LOG(context, "catalog.entry_cache",
	        {{"set_kind", CacheKindName()},
	         {"name", name},
	         {"qualifier", qualifier},
	         {"outcome", pub_outcome},
	         {"duration_ms", std::to_string(rpc_ms)}});
	return pub_result;
}

optional_ptr<CatalogEntry> VgiTableSet::GetEntry(ClientContext &context, const EntryLookupInfo &lookup_info) {
	auto at = lookup_info.GetAtClause();
	auto &entry_name = lookup_info.GetEntryName();

	if (!at) {
		return GetEntry(context, entry_name);
	}

	// No entry_lock_: AT-clause entries live on the per-context
	// VgiTransaction (point_in_time_entries), not in the shared set.
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();

	if (!attach_params || !attach_result) {
		return nullptr;
	}

	auto at_unit = at->Unit().GetIdentifierName();
	auto at_raw = at->GetValue();
	if (at_raw.IsNull()) {
		// Value::ToString() would send the literal text "NULL" as the version.
		throw BinderException("Time travel AT clause (%s) value must not be NULL", at_unit);
	}
	auto at_value = at_raw.ToString();

	auto rpc_start = std::chrono::steady_clock::now();
	auto &vgi_tx_at = VgiTransaction::Get(context, catalog_);
	vgi::CatalogRpcContext rpc_ctx {attach_params, attach_result->attach_opaque_data,
	                                vgi_tx_at.GetTransactionOpaqueData()};
	rpc_ctx.entity_kind = "table";
	rpc_ctx.entity_qualifier = schema_.name.GetIdentifierName() + "." + entry_name;
	auto table_info_opt =
	    vgi::InvokeCatalogTableGet(rpc_ctx, schema_.name.GetIdentifierName(), entry_name, context, at_unit, at_value);
	auto rpc_end = std::chrono::steady_clock::now();
	double rpc_ms = std::chrono::duration<double, std::milli>(rpc_end - rpc_start).count();

	if (!table_info_opt) {
		VGI_LOG(context, "catalog.entry_cache",
		        {{"set_kind", CacheKindName()},
		         {"name", entry_name},
		         {"qualifier", rpc_ctx.entity_qualifier},
		         {"outcome", "at_clause_not_found"},
		         {"at_unit", at_unit},
		         {"at_value", at_value},
		         {"duration_ms", std::to_string(rpc_ms)}});
		return nullptr;
	}

	auto &table_info = *table_info_opt;
	vgi::RequireNativeConstraintRead(*catalog_.Cast<VgiCatalog>().attach_result(), table_info);
	auto create_info = vgi::CreateTableInfoFromVgiTable(context, table_info, schema_.name.GetIdentifierName());
	auto table_entry = make_uniq<VgiTableEntry>(catalog_, schema_, create_info, table_info);

	// Owned by the transaction: lives for the query, freed at transaction end.
	auto &transaction = VgiTransaction::Get(context, catalog_);
	auto &result = transaction.point_in_time_entries.emplace_back(std::move(table_entry));
	VGI_LOG(context, "catalog.entry_cache",
	        {{"set_kind", CacheKindName()},
	         {"name", entry_name},
	         {"qualifier", rpc_ctx.entity_qualifier},
	         {"outcome", "at_clause_rpc"},
	         {"at_unit", at_unit},
	         {"at_value", at_value},
	         {"duration_ms", std::to_string(rpc_ms)}});
	return result;
}

} // namespace duckdb
