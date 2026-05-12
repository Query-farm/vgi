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
#include "vgi_catalog_api.hpp"
#include "vgi_logging.hpp"

#include <chrono>

namespace duckdb {

VgiTableSet::VgiTableSet(Catalog &catalog, VgiSchemaEntry &schema) : VgiCatalogSet(catalog, &schema), schema_(schema) {
}

void VgiTableSet::LoadEntries(ClientContext &context,
                               const std::lock_guard<std::mutex> &/*_load_lock*/) {
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();

	if (!attach_params || !attach_result) {
		return;
	}

	// Call catalog_schema_contents_tables via RPC — entry_lock_ is NOT held
	// here. load_lock_ (held by caller) serializes concurrent loaders.
	auto &vgi_tx_load = VgiTransaction::Get(context, catalog_);
	vgi::CatalogRpcContext rpc_ctx{attach_params, attach_result->attach_id, vgi_tx_load.GetTransactionId()};
	rpc_ctx.entity_kind = "schema";
	rpc_ctx.entity_qualifier = schema_.name;
	auto tables = vgi::InvokeCatalogSchemaContentsTables(rpc_ctx, schema_.name, context);

	// Publish each entry under entry_lock_ briefly (per-entry, not whole loop).
	// Mirrors UnityCatalog UCTableSet::LoadEntries.
	for (auto &table_info : tables) {
		auto create_info = vgi::CreateTableInfoFromVgiTable(context, table_info, schema_.name);
		auto table_entry = make_uniq<VgiTableEntry>(catalog_, schema_, create_info, table_info);
		{
			std::lock_guard<std::mutex> entry_lk(entry_lock_);
			CreateEntryLocked(std::move(table_entry));
		}
	}
}

// Override GetEntry to do on-demand loading for individual tables.
//
// Pattern (mirrors VgiTableEntry::GetStatistics' two-phase fetch): hold the
// mutex only for state inspection, drop it during the worker RPC, then
// re-acquire to publish. The generation counter on VgiCatalogSet is bumped
// by every mutation; a concurrent DropEntry / ClearEntries / HarvestEntries
// during our RPC moves it forward, and we re-check on the way out so a
// dropped table can't be silently resurrected as a "ghost" entry.
optional_ptr<CatalogEntry> VgiTableSet::GetEntry(ClientContext &context, const std::string &name) {
	auto rpc_start = std::chrono::steady_clock::now();
	uint64_t pre_gen;
	std::string qualifier = schema_.name + "." + name;
	bool eager_path = false;
	// Pre-read settings OUTSIDE entry_lock_ — DuckDB's setting registry +
	// log manager both have their own mutexes; holding entry_lock_ across
	// calls into them risks lock-order inversion (WASM hang trace evidence).
	const bool trust_empty_kinds = ReadTrustEmptyKinds(context);

	optional_ptr<CatalogEntry> cached_result = nullptr;
	const char *fast_outcome = nullptr;
	{
		std::lock_guard<std::mutex> lock(entry_lock_);
		auto it = GetEntries().find(name);
		if (it != GetEntries().end()) {
			cached_result = it->second.get();
			fast_outcome = "hit";
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
		// Bulk fetch outside the lock. LoadEntries() is implemented to do
		// the RPC + CreateEntryLocked() under the assumption it's already
		// holding entry_lock_; here we re-implement that with explicit
		// generation-check publish so multi-thread DDL doesn't resurrect
		// a dropped table.
		auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
		auto &attach_params = vgi_catalog.attach_parameters();
		auto &attach_result = vgi_catalog.attach_result();
		if (attach_params && attach_result) {
			auto &vgi_tx = VgiTransaction::Get(context, catalog_);
			vgi::CatalogRpcContext rpc_ctx{attach_params, attach_result->attach_id, vgi_tx.GetTransactionId()};
			rpc_ctx.entity_kind = "schema";
			rpc_ctx.entity_qualifier = schema_.name;
			auto tables = vgi::InvokeCatalogSchemaContentsTables(rpc_ctx, schema_.name, context);

			// Build entries OUTSIDE entry_lock_ (CreateTableInfoFromVgiTable
			// may resolve types that re-enter the catalog). Publish each
			// under entry_lock_ briefly. Mirrors UnityCatalog UCTableSet.
			std::vector<unique_ptr<CatalogEntry>> built_entries;
			built_entries.reserve(tables.size());
			for (auto &table_info : tables) {
				auto create_info = vgi::CreateTableInfoFromVgiTable(context, table_info, schema_.name);
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
					// Bulk listing didn't include this name — fall through
					// to a per-name single-entry RPC.
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

	// Load this specific table from the worker (no lock held — long RPC).
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();

	if (!attach_params || !attach_result) {
		VGI_LOG(context, "catalog.entry_cache",
		        {{"set_kind", CacheKindName()},
		         {"name", name},
		         {"qualifier", qualifier},
		         {"outcome", "not_attached"}});
		return nullptr;
	}

	auto &vgi_tx = VgiTransaction::Get(context, catalog_);
	vgi::CatalogRpcContext rpc_ctx{attach_params, attach_result->attach_id, vgi_tx.GetTransactionId()};
	rpc_ctx.entity_kind = "table";
	rpc_ctx.entity_qualifier = qualifier;
	auto table_info_opt = vgi::InvokeCatalogTableGet(rpc_ctx, schema_.name, name, context);

	auto rpc_end = std::chrono::steady_clock::now();
	double rpc_ms = std::chrono::duration<double, std::milli>(rpc_end - rpc_start).count();

	if (!table_info_opt) {
		VGI_LOG(context, "catalog.entry_cache",
		        {{"set_kind", CacheKindName()},
		         {"name", name},
		         {"qualifier", qualifier},
		         {"outcome", "not_found"},
		         {"duration_ms", std::to_string(rpc_ms)}});
		return nullptr;
	}

	auto &table_info = *table_info_opt;
	auto create_info = vgi::CreateTableInfoFromVgiTable(context, table_info, schema_.name);
	auto table_entry = make_uniq<VgiTableEntry>(catalog_, schema_, create_info, table_info);

	// Publish under entry_lock_ briefly; log AFTER releasing.
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

	// If no AT clause, delegate to the existing name-based GetEntry (which holds its own lock)
	if (!at) {
		return GetEntry(context, entry_name);
	}

	// AT clause path: no entry_lock_ needed because point_in_time_entries
	// lives on the per-context VgiTransaction, not the shared table set.

	// AT clause present: fetch version-specific table info from worker
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();

	if (!attach_params || !attach_result) {
		return nullptr;
	}

	auto at_unit = at->Unit();
	auto at_raw = at->GetValue();
	if (at_raw.IsNull()) {
		// AT (... => NULL) cannot reach the worker as a meaningful version
		// string — Value::ToString() on a NULL Value emits the literal text
		// "NULL", which the worker then tries to parse as a version and
		// fails with "invalid literal for int(): 'NULL'". Reject at bind.
		throw BinderException("Time travel AT clause (%s) value must not be NULL", at_unit);
	}
	auto at_value = at_raw.ToString();

	// Call catalog_table_get with AT params via RPC
	auto rpc_start = std::chrono::steady_clock::now();
	auto &vgi_tx_at = VgiTransaction::Get(context, catalog_);
	vgi::CatalogRpcContext rpc_ctx{attach_params, attach_result->attach_id, vgi_tx_at.GetTransactionId()};
	rpc_ctx.entity_kind = "table";
	rpc_ctx.entity_qualifier = schema_.name + "." + entry_name;
	auto table_info_opt = vgi::InvokeCatalogTableGet(rpc_ctx, schema_.name, entry_name, context,
	                                                  at_unit, at_value);
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
	auto create_info = vgi::CreateTableInfoFromVgiTable(context, table_info, schema_.name);
	auto table_entry = make_uniq<VgiTableEntry>(catalog_, schema_, create_info, table_info);

	// Store on the transaction so it stays alive for the query lifetime
	// and gets cleaned up when the transaction ends.
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
