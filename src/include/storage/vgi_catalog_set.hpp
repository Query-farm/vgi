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

// Base class for managing a set of catalog entries with lazy loading
class VgiCatalogSet {
public:
	explicit VgiCatalogSet(Catalog &catalog);
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

protected:
	Catalog &catalog_;
	case_insensitive_map_t<unique_ptr<CatalogEntry>> entries_;
	bool is_loaded_ = false;
	std::mutex entry_lock_;
	std::atomic<uint64_t> generation_{0};
};

} // namespace duckdb
