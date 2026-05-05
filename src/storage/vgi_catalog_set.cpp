#include "storage/vgi_catalog_set.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "storage/vgi_catalog.hpp"
#include "storage/vgi_object_counts.hpp"
#include "storage/vgi_schema_entry.hpp"
#include "vgi_logging.hpp"

namespace duckdb {

VgiCatalogSet::VgiCatalogSet(Catalog &catalog, VgiSchemaEntry *schema) : catalog_(catalog), schema_entry_(schema) {
}

void VgiCatalogSet::ResolveEagerLoadParamsLocked() {
	if (eager_load_resolved_) {
		return;
	}
	auto kind = CacheKindName();
	threshold_ = ObjectCountFor(catalog_.Cast<VgiCatalog>().EagerLoadThresholds(), kind);
	if (schema_entry_) {
		estimated_count_ = ObjectCountFor(schema_entry_->GetEstimatedCounts(), kind);
	}
	// else: schema_entry_ is null (e.g. VgiSchemaSet) — leave estimated_count_
	// at its default of 1 so any threshold >= 1 trivially eager-loads. Sets
	// without a single-entry override would already eager-load on first
	// GetEntry anyway, so this is just for symmetry.
	eager_load_resolved_ = true;
}

bool VgiCatalogSet::ShouldEagerLoadLocked() {
	if (is_loaded_) {
		return false;
	}
	ResolveEagerLoadParamsLocked();
	return estimated_count_ <= threshold_;
}

optional_ptr<CatalogEntry> VgiCatalogSet::GetEntry(ClientContext &context, const std::string &name) {
	std::lock_guard<std::mutex> lock(entry_lock_);

	// Check if we have the entry cached
	auto it = entries_.find(name);
	if (it != entries_.end()) {
		VGI_LOG(context, "catalog.entry_cache",
		        {{"set_kind", CacheKindName()},
		         {"name", name},
		         {"outcome", "hit"},
		         {"triggered_load", "false"}});
		return it->second.get();
	}

	// Load entries if not yet loaded. Sets without a single-entry override
	// (this base class) always do a full LoadEntries on first miss, so the
	// threshold doesn't gate them — but we still record loaded_reason for
	// log symmetry with the threshold-driven overrides in VgiTableSet /
	// VgiViewSet.
	bool triggered_load = false;
	if (!is_loaded_) {
		triggered_load = true;
		LoadEntries(context);
		is_loaded_ = true;
	}

	// Check again after loading
	it = entries_.find(name);
	if (it != entries_.end()) {
		VGI_LOG(context, "catalog.entry_cache",
		        {{"set_kind", CacheKindName()},
		         {"name", name},
		         {"outcome", "miss_loaded"},
		         {"triggered_load", triggered_load ? "true" : "false"},
		         {"loaded_reason", "first_miss"}});
		return it->second.get();
	}

	VGI_LOG(context, "catalog.entry_cache",
	        {{"set_kind", CacheKindName()},
	         {"name", name},
	         {"outcome", "miss_not_found"},
	         {"triggered_load", triggered_load ? "true" : "false"}});
	return nullptr;
}

void VgiCatalogSet::CreateEntryLocked(unique_ptr<CatalogEntry> entry) {
	// Called from LoadEntries while entry_lock_ is held by GetEntry/Scan.
	//
	// Skip if a same-named entry already exists. DuckDB's binder retains raw
	// CatalogEntry* values across the bind, and a re-entrant Scan triggered
	// from inside that bind (e.g. Catalog::GetAllSchemas walking sibling
	// catalogs) would otherwise destroy entries the bind is still using —
	// see ColumnIsGenerated → ColumnList::GetColumn(0) UAF that surfaces as
	// "Logical column index 0 out of range". Refresh goes through
	// ClearEntries() / vgi_clear_cache() at safe points instead.
	if (entries_.find(entry->name) != entries_.end()) {
		return;
	}
	entries_[entry->name] = std::move(entry);
	generation_.fetch_add(1, std::memory_order_release);
}

void VgiCatalogSet::DropEntry(const std::string &name) {
	std::lock_guard<std::mutex> lock(entry_lock_);
	if (entries_.erase(name) > 0) {
		generation_.fetch_add(1, std::memory_order_release);
	}
}

void VgiCatalogSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	std::lock_guard<std::mutex> lock(entry_lock_);

	// Load entries if not yet loaded
	if (!is_loaded_) {
		LoadEntries(context);
		is_loaded_ = true;
	}

	for (auto &entry : entries_) {
		callback(*entry.second);
	}
}

void VgiCatalogSet::ClearEntries() {
	std::lock_guard<std::mutex> lock(entry_lock_);
	if (!entries_.empty() || is_loaded_) {
		generation_.fetch_add(1, std::memory_order_release);
	}
	entries_.clear();
	is_loaded_ = false;
}

std::vector<unique_ptr<CatalogEntry>> VgiCatalogSet::HarvestEntries() {
	std::lock_guard<std::mutex> lock(entry_lock_);
	std::vector<unique_ptr<CatalogEntry>> harvested;
	harvested.reserve(entries_.size());
	for (auto &kv : entries_) {
		if (kv.second) {
			harvested.push_back(std::move(kv.second));
		}
	}
	entries_.clear();
	is_loaded_ = false;
	generation_.fetch_add(1, std::memory_order_release);
	return harvested;
}

unique_ptr<CatalogEntry> VgiCatalogSet::HarvestEntry(const std::string &name) {
	std::lock_guard<std::mutex> lock(entry_lock_);
	auto it = entries_.find(name);
	if (it == entries_.end()) {
		return nullptr;
	}
	auto entry = std::move(it->second);
	entries_.erase(it);
	generation_.fetch_add(1, std::memory_order_release);
	return entry;
}

} // namespace duckdb
