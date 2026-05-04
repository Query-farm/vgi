#include "storage/vgi_catalog_set.hpp"

#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

VgiCatalogSet::VgiCatalogSet(Catalog &catalog) : catalog_(catalog) {
}

optional_ptr<CatalogEntry> VgiCatalogSet::GetEntry(ClientContext &context, const std::string &name) {
	std::lock_guard<std::mutex> lock(entry_lock_);

	// Check if we have the entry cached
	auto it = entries_.find(name);
	if (it != entries_.end()) {
		return it->second.get();
	}

	// Load entries if not yet loaded
	if (!is_loaded_) {
		LoadEntries(context);
		is_loaded_ = true;
	}

	// Check again after loading
	it = entries_.find(name);
	if (it != entries_.end()) {
		return it->second.get();
	}

	return nullptr;
}

void VgiCatalogSet::CreateEntryLocked(unique_ptr<CatalogEntry> entry) {
	// Called from LoadEntries while entry_lock_ is held by GetEntry/Scan.
	entries_[entry->name] = std::move(entry);
}

void VgiCatalogSet::DropEntry(const std::string &name) {
	std::lock_guard<std::mutex> lock(entry_lock_);
	entries_.erase(name);
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
	return harvested;
}

} // namespace duckdb
