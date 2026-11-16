// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
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
	// Without a schema (VgiSchemaSet) estimated_count_ stays 1, so any
	// threshold >= 1 eager-loads.
	eager_load_resolved_ = true;
}

bool VgiCatalogSet::ShouldEagerLoadLocked() {
	if (is_loaded_) {
		return false;
	}
	// A catalog_contents seed is already in memory: loading it costs no RPC,
	// so always take the bulk path rather than per-name single-entry RPCs.
	if (schema_entry_ && schema_entry_->HasSeed(CacheKindName())) {
		return true;
	}
	ResolveEagerLoadParamsLocked();
	return estimated_count_ <= threshold_;
}

bool VgiCatalogSet::ReadTrustEmptyKinds(ClientContext &context) {
	Value trust_val;
	if (context.TryGetCurrentSetting("vgi_trust_empty_kinds", trust_val) && !trust_val.IsNull()) {
		return trust_val.GetValue<bool>();
	}
	return true; // default
}

bool VgiCatalogSet::ShouldBypassRpcLocked(ClientContext &context, bool trust_empty_kinds) {
	if (is_loaded_) {
		return false;
	}
	// A catalog_contents seed is authoritative and free to load; no need to
	// consult (and decode) the schema's estimated counts.
	if (schema_entry_ && schema_entry_->HasSeed(CacheKindName())) {
		return false;
	}
	ResolveEagerLoadParamsLocked();
	if (estimated_count_ != 0) {
		return false;
	}
	return trust_empty_kinds;
}

void VgiCatalogSet::MarkPopulated() {
	std::lock_guard<std::mutex> lock(entry_lock_);
	// Keep eager_load_resolved_ true: re-resolving would re-read the immutable
	// schema counts and restore estimated_count_ = 0, undoing the flip.
	estimated_count_ = 1;
	eager_load_resolved_ = true;
}

bool VgiCatalogSet::IsKnownMissingLocked(const std::string &name) const {
	auto &attach_result = catalog_.Cast<VgiCatalog>().attach_result();
	return attach_result && !attach_result->supports_transactions &&
	       missing_entries_.find(name) != missing_entries_.end();
}

void VgiCatalogSet::CacheMissingEntryLocked(const std::string &name, uint64_t expected_generation) {
	auto &attach_result = catalog_.Cast<VgiCatalog>().attach_result();
	// Transaction-local absence must never become shared catalog state.
	if (!attach_result || attach_result->supports_transactions ||
	    generation_.load(std::memory_order_acquire) != expected_generation || entries_.find(name) != entries_.end()) {
		return;
	}
	// Bound memory for arbitrary lookup names. Eviction only costs another RPC.
	static constexpr idx_t MAX_MISSING_ENTRIES = 1024;
	if (missing_entries_.size() >= MAX_MISSING_ENTRIES) {
		missing_entries_.clear();
	}
	missing_entries_.insert(name);
}

void VgiCatalogSet::EnsureLoaded(ClientContext &context, bool honor_empty_hint) {
	// Read outside any lock (see ReadTrustEmptyKinds).
	const bool trust_empty_kinds = honor_empty_hint && ReadTrustEmptyKinds(context);
	// A load the zero-count hint skipped is not a load for a caller that
	// ignores the hint.
	auto loaded = [&]() {
		return is_loaded_ && (honor_empty_hint || !loaded_by_empty_hint_);
	};
	{
		std::lock_guard<std::mutex> entry_lk(entry_lock_);
		if (loaded()) {
			return;
		}
		if (ShouldBypassRpcLocked(context, trust_empty_kinds)) {
			is_loaded_ = true;
			loaded_by_empty_hint_ = true;
			return;
		}
	}
	// Holding load_lock_ does not block cache reads, which need only entry_lock_.
	std::lock_guard<std::mutex> load_lk(load_lock_);
	// Another thread may have just finished the load.
	{
		std::lock_guard<std::mutex> entry_lk(entry_lock_);
		if (loaded()) {
			return;
		}
	}
	LoadEntries(context, load_lk);
	{
		std::lock_guard<std::mutex> entry_lk(entry_lock_);
		is_loaded_ = true;
		loaded_by_empty_hint_ = false;
	}
}

optional_ptr<CatalogEntry> VgiCatalogSet::GetEntry(ClientContext &context, const std::string &name) {
	// Read outside entry_lock_ (see ReadTrustEmptyKinds).
	const bool trust_empty_kinds = ReadTrustEmptyKinds(context);

	// Log only after releasing entry_lock_: DuckDB's log manager has its own
	// mutex (same inversion risk).
	optional_ptr<CatalogEntry> result = nullptr;
	const char *outcome = nullptr;
	{
		std::lock_guard<std::mutex> lock(entry_lock_);
		auto it = entries_.find(name);
		if (it != entries_.end()) {
			result = it->second.get();
			outcome = "hit";
		} else if (ShouldBypassRpcLocked(context, trust_empty_kinds)) {
			is_loaded_ = true;
			loaded_by_empty_hint_ = true;
			outcome = "kind_empty";
		}
	}
	if (outcome) {
		VGI_LOG(context, "catalog.entry_cache",
		        {{"set_kind", CacheKindName()}, {"name", name}, {"outcome", outcome}, {"triggered_load", "false"}});
		return result;
	}

	bool was_loaded_before;
	{
		std::lock_guard<std::mutex> lock(entry_lock_);
		was_loaded_before = is_loaded_;
	}
	EnsureLoaded(context);

	{
		std::lock_guard<std::mutex> lock(entry_lock_);
		auto it = entries_.find(name);
		if (it != entries_.end()) {
			result = it->second.get();
			outcome = "miss_loaded";
		} else {
			outcome = "miss_not_found";
		}
	}
	if (std::string(outcome) == "miss_loaded") {
		VGI_LOG(context, "catalog.entry_cache",
		        {{"set_kind", CacheKindName()},
		         {"name", name},
		         {"outcome", outcome},
		         {"triggered_load", !was_loaded_before ? "true" : "false"},
		         {"loaded_reason", "first_miss"}});
		return result;
	}
	VGI_LOG(context, "catalog.entry_cache",
	        {{"set_kind", CacheKindName()},
	         {"name", name},
	         {"outcome", outcome},
	         {"triggered_load", !was_loaded_before ? "true" : "false"}});
	return nullptr;
}

void VgiCatalogSet::AddHiddenEntry(unique_ptr<CatalogEntry> entry) {
	std::lock_guard<std::mutex> lock(entry_lock_);
	// Same first-wins rule as CreateEntryLocked: a bound statement may hold a
	// raw pointer to the existing entry.
	auto key = entry->name.GetIdentifierName();
	if (hidden_entries_.find(key) != hidden_entries_.end()) {
		return;
	}
	hidden_entries_[key] = std::move(entry);
	generation_.fetch_add(1, std::memory_order_release);
}

optional_ptr<CatalogEntry> VgiCatalogSet::GetHiddenEntry(ClientContext &context, const std::string &name) {
	{
		std::lock_guard<std::mutex> lock(entry_lock_);
		auto it = hidden_entries_.find(name);
		if (it != hidden_entries_.end()) {
			return it->second.get();
		}
	}
	EnsureLoaded(context, /*honor_empty_hint=*/false);
	optional_ptr<CatalogEntry> result;
	{
		std::lock_guard<std::mutex> lock(entry_lock_);
		auto it = hidden_entries_.find(name);
		if (it != hidden_entries_.end()) {
			result = it->second.get();
		}
	}
	VGI_LOG(context, "catalog.entry_cache",
	        {{"set_kind", CacheKindName()},
	         {"name", name},
	         {"outcome", result ? "hidden_hit" : "hidden_not_found"},
	         {"triggered_load", "false"}});
	return result;
}

void VgiCatalogSet::CreateEntryLocked(unique_ptr<CatalogEntry> entry) {
	// First wins: DuckDB's binder retains raw CatalogEntry* across the bind, and
	// a re-entrant load from inside it (e.g. Catalog::GetAllSchemas walking
	// sibling catalogs) would otherwise destroy entries still in use (a UAF).
	// Refresh goes through ClearEntries()/HarvestEntries() at safe points.
	missing_entries_.erase(entry->name.GetIdentifierName());
	if (entries_.find(entry->name.GetIdentifierName()) != entries_.end()) {
		return;
	}
	entries_[entry->name.GetIdentifierName()] = std::move(entry);
	generation_.fetch_add(1, std::memory_order_release);
}

void VgiCatalogSet::DropEntry(const std::string &name) {
	std::lock_guard<std::mutex> lock(entry_lock_);
	entries_.erase(name);
	missing_entries_.clear();
	// Even an uncached name may have a lookup in flight.
	generation_.fetch_add(1, std::memory_order_release);
}

void VgiCatalogSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	EnsureLoaded(context);

	// The callback runs outside entry_lock_: it may re-enter the catalog.
	std::vector<CatalogEntry *> snapshot;
	{
		std::lock_guard<std::mutex> lock(entry_lock_);
		snapshot.reserve(entries_.size());
		for (auto &kv : entries_) {
			snapshot.push_back(kv.second.get());
		}
	}
	for (auto *e : snapshot) {
		callback(*e);
	}
}

void VgiCatalogSet::ClearEntries() {
	if (schema_entry_) {
		schema_entry_->DiscardSeed(CacheKindName());
	}
	std::lock_guard<std::mutex> lock(entry_lock_);
	generation_.fetch_add(1, std::memory_order_release);
	entries_.clear();
	hidden_entries_.clear();
	missing_entries_.clear();
	is_loaded_ = false;
	loaded_by_empty_hint_ = false;
}

std::vector<unique_ptr<CatalogEntry>> VgiCatalogSet::HarvestEntries() {
	// A harvest invalidates the set (DDL / cache clear): a catalog_contents
	// seed it has not taken yet is now stale, so the reload must not use it.
	// The seed mutex is a leaf, taken before entry_lock_.
	if (schema_entry_) {
		schema_entry_->DiscardSeed(CacheKindName());
	}
	std::lock_guard<std::mutex> lock(entry_lock_);
	std::vector<unique_ptr<CatalogEntry>> harvested;
	harvested.reserve(entries_.size());
	for (auto &kv : entries_) {
		if (kv.second) {
			harvested.push_back(std::move(kv.second));
		}
	}
	for (auto &kv : hidden_entries_) {
		if (kv.second) {
			harvested.push_back(std::move(kv.second));
		}
	}
	entries_.clear();
	hidden_entries_.clear();
	missing_entries_.clear();
	is_loaded_ = false;
	loaded_by_empty_hint_ = false;
	generation_.fetch_add(1, std::memory_order_release);
	return harvested;
}

unique_ptr<CatalogEntry> VgiCatalogSet::HarvestEntry(const std::string &name) {
	if (schema_entry_) {
		schema_entry_->DiscardSeed(CacheKindName());
	}
	std::lock_guard<std::mutex> lock(entry_lock_);
	// A removed cache entry may still exist remotely with changed metadata.
	// The listing is incomplete until reloaded: otherwise catalog scans omit
	// an altered table until an individual lookup happens to fetch it again.
	// Invalidate in-flight loads even when this entry is not cached yet.
	is_loaded_ = false;
	// A rename invalidates the old name here, but its destination may have
	// been cached as missing. Clear all misses, even if the old name is absent.
	missing_entries_.clear();
	generation_.fetch_add(1, std::memory_order_release);
	auto it = entries_.find(name);
	if (it == entries_.end()) {
		return nullptr;
	}
	auto entry = std::move(it->second);
	entries_.erase(it);
	return entry;
}

} // namespace duckdb
