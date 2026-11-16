// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_settings_defaults.hpp"
#include <atomic>
#include <functional>
#include <mutex>
#include <string>

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {

class Catalog;
class ClientContext;
class DatabaseInstance;
class VgiSchemaEntry;

// Base class for managing a set of catalog entries with lazy loading
class VgiCatalogSet {
public:
	// `schema` is nullptr for sets that live directly on the catalog (e.g.
	// VgiSchemaSet). Per-kind eager-load parameters (keyed by the virtual
	// CacheKindName()) are resolved lazily on first use, never in the constructor.
	VgiCatalogSet(Catalog &catalog, VgiSchemaEntry *schema);
	virtual ~VgiCatalogSet() = default;

	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const std::string &name);

	// Add an entry; the caller must hold entry_lock_. LoadEntries() runs without
	// it, so implementations take entry_lock_ briefly around these calls. First
	// wins: an existing same-named entry is kept (see the definition).
	void CreateEntryLocked(unique_ptr<CatalogEntry> entry);

	void DropEntry(const std::string &name);

	// Hidden entries (protocol 3.3 "table_only" functions). Built by the same
	// LoadEntries() but kept in a separate map that GetEntry() and Scan() never
	// read, so they are absent from duckdb_functions()/autocomplete and SQL cannot
	// call them, even fully qualified. Only GetHiddenEntry() reaches them, to
	// resolve the functions a table descriptor names (vgi_table_function_resolver.hpp).
	// AddHiddenEntry() takes entry_lock_ itself.
	void AddHiddenEntry(unique_ptr<CatalogEntry> entry);
	// The hidden entry called `name`, loading the set if necessary. Unlike
	// GetEntry(), a worker's zero estimated count for this kind does not skip
	// the load: the count describes what the worker lists publicly, and a
	// table naming a function is evidence that one exists.
	optional_ptr<CatalogEntry> GetHiddenEntry(ClientContext &context, const std::string &name);

	void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);

	// Destroys the cached entries in place: any raw CatalogEntry* held by a bound
	// query/prepared statement dangles. Prefer HarvestEntries() where the entries
	// must outlive the clear.
	void ClearEntries();

	// Move all cached entries out and reset is_loaded_. Callers hand them to
	// VgiCatalog's deferred-drop graveyard, which keeps pointers held by bound
	// queries alive (bounded: kGraveyardLimit, oldest evicted first) until the
	// catalog is destroyed or vgi_clear_cache() purges it.
	std::vector<unique_ptr<CatalogEntry>> HarvestEntries();

	// Move a single named entry out and mark the listing incomplete; returns
	// nullptr if the entry isn't cached. Schema-level DDL keeps sibling
	// entries alive while forcing scans to reload the changed listing.
	unique_ptr<CatalogEntry> HarvestEntry(const std::string &name);

	// Bumped on every mutation of the set. Callers that drop the lock for an RPC
	// capture it before and re-check after, so concurrent DDL is detected and a
	// dropped entry is not resurrected (see VgiTableSet::GetEntry).
	uint64_t Generation() const {
		return generation_.load(std::memory_order_acquire);
	}

	Catalog &GetCatalog() {
		return catalog_;
	}

	// Whether the set currently holds a loaded listing (takes entry_lock_).
	bool Loaded() {
		std::lock_guard<std::mutex> lock(entry_lock_);
		return is_loaded_;
	}

	// After local DDL that adds an entry of this kind: clears the zero-count
	// emptiness signal so the next GetEntry() does not short-circuit. Takes
	// entry_lock_ itself; called from VgiSchemaEntry DDL.
	void MarkPopulated();

protected:
	// Load entries from the remote source. The caller holds `load_lock_`
	// (`_load_lock` is the proof token), so loads are single-flight. The RPC MUST
	// run outside `entry_lock_`: take `entry_lock_` only briefly to insert via
	// CreateEntryLocked. Holding it across the RPC deadlocks re-entrant catalog
	// walks (observed as WASM hangs).
	virtual void LoadEntries(ClientContext &context,
	                          const std::lock_guard<std::mutex> &_load_lock) = 0;

	// Single-flight load: the first caller runs LoadEntries under load_lock_;
	// concurrent callers wait on load_lock_ (without entry_lock_) and then see
	// is_loaded_. `honor_empty_hint` false ignores the worker's zero estimated
	// count (see GetHiddenEntry).
	void EnsureLoaded(ClientContext &context, bool honor_empty_hint = true);

	std::mutex load_lock_;

	// Kind tag ("table", "view", "scalar_function", "macro", ...): the `set_kind`
	// of catalog.entry_cache events and the key for per-kind estimated counts,
	// eager-load thresholds and catalog_contents seeds.
	virtual std::string CacheKindName() const {
		return "";
	}

	bool IsLoaded() const {
		return is_loaded_;
	}

	void MarkLoaded() {
		is_loaded_ = true;
	}

	case_insensitive_map_t<unique_ptr<CatalogEntry>> &GetEntries() {
		return entries_;
	}

	// True when a subclass GetEntry() should bulk-load via LoadEntries() rather
	// than issue a single-entry RPC: not yet loaded, and either a catalog_contents
	// seed is present or estimated_count_ <= threshold_. Caller holds entry_lock_.
	bool ShouldEagerLoadLocked();

	// True when all RPCs for this kind should be skipped: the worker asserted
	// estimated_object_count[kind] == 0, there is no seed, and
	// vgi_trust_empty_kinds (default true) trusts it. Caller holds entry_lock_.
	// Read the setting with ReadTrustEmptyKinds() BEFORE taking the lock: DuckDB's
	// settings registry has its own mutex (lock-order inversion).
	bool ShouldBypassRpcLocked(ClientContext &context, bool trust_empty_kinds);

	// Must be called WITHOUT entry_lock_ held (see ShouldBypassRpcLocked).
	static bool ReadTrustEmptyKinds(ClientContext &context);

	// Resolve estimated_count_ and threshold_ once, keyed by CacheKindName().
	void ResolveEagerLoadParamsLocked();

	// Explicit per-name RPC misses only, for workers without transactions.
	// Caller must hold entry_lock_. A miss may be published only if no
	// invalidation or positive insertion occurred while the RPC was in flight.
	bool IsKnownMissingLocked(const std::string &name) const;
	void CacheMissingEntryLocked(const std::string &name, uint64_t expected_generation);

protected:
	Catalog &catalog_;
	VgiSchemaEntry *schema_entry_ = nullptr;
	case_insensitive_map_t<unique_ptr<CatalogEntry>> entries_;
	// Hidden ("table_only") entries; see AddHiddenEntry(). Guarded by entry_lock_.
	case_insensitive_map_t<unique_ptr<CatalogEntry>> hidden_entries_;
	case_insensitive_set_t missing_entries_;
	bool is_loaded_ = false;
	// True while is_loaded_ came from the zero-count bypass rather than a real
	// LoadEntries() (see GetHiddenEntry). Guarded by entry_lock_.
	bool loaded_by_empty_hint_ = false;
	std::mutex entry_lock_;
	std::atomic<uint64_t> generation_{0};

	// Eager-load gate, resolved lazily by ResolveEagerLoadParamsLocked so the hot
	// path does no map lookups. estimated_count_ is the worker's reported
	// population for this kind; threshold_ is the catalog's per-kind cutoff.
	int64_t estimated_count_ = 1;
	int64_t threshold_ = vgi::defaults::EAGER_LOAD_THRESHOLD;
	bool eager_load_resolved_ = false;
};

} // namespace duckdb
