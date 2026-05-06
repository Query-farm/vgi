#pragma once

#include <atomic>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

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
	// `schema` may be nullptr for sets that live directly on the catalog
	// (e.g. VgiSchemaSet). When non-null, the base class lazily resolves
	// per-kind eager-load parameters from the schema's
	// estimated_object_count map and the catalog's eager-load thresholds —
	// keyed by CacheKindName(). Resolution happens on first cache miss
	// inside GetEntry(), so the virtual CacheKindName() override is safely
	// observable (not callable from a constructor).
	VgiCatalogSet(Catalog &catalog, VgiSchemaEntry *schema);
	virtual ~VgiCatalogSet() = default;

	// Get an entry by name, loading if necessary
	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const std::string &name);

	// Create a new entry and add it to the set.
	// PRECONDITION: Caller must hold entry_lock_ (called from LoadEntries under lock).
	void CreateEntryLocked(unique_ptr<CatalogEntry> entry);

	// Drop an entry from the set
	void DropEntry(const std::string &name);

	// Scan all entries in the set
	void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);

	// Clear all cached entries — destroys the unique_ptrs in place. Any
	// outstanding raw CatalogEntry* held by a bound query/prepared
	// statement dangles after this call. Prefer HarvestEntries() at
	// callsites where the entries should outlive the clear.
	void ClearEntries();

	// Move all cached entries out of the set into the returned vector and
	// reset is_loaded_. Used by VgiCatalog's deferred-drop graveyard to
	// preserve entry pointers held by bound queries until either the
	// catalog is destroyed or vgi_clear_cache() is called explicitly.
	std::vector<unique_ptr<CatalogEntry>> HarvestEntries();

	// Move a single named entry out of the set; returns nullptr if the
	// entry isn't currently cached. Used by schema-level DDL paths that
	// need to invalidate one entry without disturbing siblings.
	unique_ptr<CatalogEntry> HarvestEntry(const std::string &name);

	// Generation counter — bumped every time the entry set is mutated
	// (CreateEntryLocked / DropEntry / ClearEntries / HarvestEntries /
	// HarvestEntry). Callers that drop the lock to do an RPC can capture
	// this before, then re-check after to detect concurrent DDL on the
	// same set and avoid resurrecting a dropped entry. See
	// VgiTableSet::GetEntry for the canonical use.
	uint64_t Generation() const {
		return generation_.load(std::memory_order_acquire);
	}

	Catalog &GetCatalog() {
		return catalog_;
	}

	// Defensive flip after local DDL that adds an entry of this kind. Clears
	// the cached zero-count emptiness signal so the next GetEntry() does not
	// short-circuit. Public because it's called from VgiSchemaEntry's DDL
	// paths. See implementation comment for the asymmetric-flip rationale.
	void MarkPopulatedLocked();

protected:
	// Override to load entries from the remote source
	virtual void LoadEntries(ClientContext &context) = 0;

	// Short tag identifying this set's contents for instrumentation logs
	// (e.g. "table", "schema", "view", "function", "macro", "index"). The
	// base returns empty so subclasses opt in; the catalog.entry_cache event
	// uses this as the `set_kind` field.
	virtual std::string CacheKindName() const {
		return "";
	}

	// Check if entries have been loaded
	bool IsLoaded() const {
		return is_loaded_;
	}

	// Mark entries as loaded
	void MarkLoaded() {
		is_loaded_ = true;
	}

	// Get the entries map (for subclasses)
	case_insensitive_map_t<unique_ptr<CatalogEntry>> &GetEntries() {
		return entries_;
	}

	// Eager-load gate. Returns true when GetEntry() should call LoadEntries()
	// once instead of falling through to a single-entry RPC override:
	// estimated_count_ <= threshold_ and the set is not yet loaded. Caller
	// must hold entry_lock_.
	bool ShouldEagerLoadLocked();

	// Zero-count RPC bypass. Returns true when the bulk + per-name RPCs for
	// this kind should be skipped entirely because the worker has asserted
	// estimated_object_count[kind] == 0 AND the user trusts the assertion
	// (vgi_trust_empty_kinds setting, default true). Caller must hold
	// entry_lock_. Reads the trust setting per call so SET takes effect
	// immediately. Triggers ResolveEagerLoadParamsLocked exactly once.
	bool ShouldBypassRpcLocked(ClientContext &context);

	// Resolve estimated_count_ and threshold_ once from the catalog/schema,
	// keyed by CacheKindName(). Idempotent. Called from ShouldEagerLoadLocked.
	void ResolveEagerLoadParamsLocked();

protected:
	Catalog &catalog_;
	VgiSchemaEntry *schema_entry_ = nullptr;
	case_insensitive_map_t<unique_ptr<CatalogEntry>> entries_;
	bool is_loaded_ = false;
	std::mutex entry_lock_;
	std::atomic<uint64_t> generation_{0};

	// Eager-load gate. estimated_count_ is the worker's reported population
	// for this set's kind (defaulted to 1 when absent). threshold_ is the
	// catalog-snapshotted cutoff (default 1000). Both are read on the hot
	// path — no string lookups, no map traversals — but resolved lazily
	// once via ResolveEagerLoadParamsLocked the first time it matters.
	int64_t estimated_count_ = 1;
	int64_t threshold_ = 1000;
	bool eager_load_resolved_ = false;
};

} // namespace duckdb
