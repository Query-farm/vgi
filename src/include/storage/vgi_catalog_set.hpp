#pragma once

#include <functional>
#include <mutex>
#include <string>

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

	// Clear all cached entries
	void ClearEntries();

	Catalog &GetCatalog() {
		return catalog_;
	}

protected:
	// Override to load entries from the remote source
	virtual void LoadEntries(ClientContext &context) = 0;

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
};

} // namespace duckdb
