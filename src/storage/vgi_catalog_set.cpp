#include "storage/vgi_catalog_set.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "storage/vgi_catalog.hpp"
#include "storage/vgi_object_counts.hpp"
#include "storage/vgi_schema_entry.hpp"
#include "vgi_logging.hpp"

// Side-channel diag bumps for threading-deadlock diagnostics. Mirrors the
// http_wasm.cc. On native it's a weak symbol that resolves to null; we no-op.
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

// Helper: read the trust_empty_kinds setting WITHOUT holding entry_lock_.
// Called outside the lock so DuckDB's setting-registry mutex never sits
// inside our entry_lock_ critical section (lock-order inversion risk).
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
	ResolveEagerLoadParamsLocked();
	if (estimated_count_ != 0) {
		return false;
	}
	return trust_empty_kinds;
}

// Backwards-compatible overload that reads the setting itself.
// PREFERRED: callers should read the setting outside the lock and pass it in.
bool VgiCatalogSet::ShouldBypassRpcLocked(ClientContext &context) {
	return ShouldBypassRpcLocked(context, ReadTrustEmptyKinds(context));
}

void VgiCatalogSet::MarkPopulatedLocked() {
	std::lock_guard<std::mutex> lock(entry_lock_);
	// Clobber the count to a non-zero value so ShouldBypassRpcLocked stops
	// firing. We do NOT reset eager_load_resolved_: re-resolution would
	// re-read GetEstimatedCounts() from the immutable schema_info_ and
	// re-set estimated_count_=0, undoing the flip. The asymmetry is
	// deliberate.
	estimated_count_ = 1;
	eager_load_resolved_ = true;
}

// Serialize concurrent loads. First caller fires LoadEntries; later callers
// wait on load_lock_ (no entry_lock_ held during wait), see is_loaded_=true
// after the first finishes, and skip the load. No RPC duplication.
//
// Mirrors UnityCatalog UCTableSet::EnsureLoaded (~/Development/unity_catalog).
void VgiCatalogSet::EnsureLoaded(ClientContext &context) {
	// Pre-read setting outside any lock — avoids lock-order inversion with
	// DuckDB's setting registry.
	const bool trust_empty_kinds = ReadTrustEmptyKinds(context);
	// Check is_loaded_ briefly under entry_lock_ — quick path when already loaded.
	{
		std::lock_guard<std::mutex> entry_lk(entry_lock_);
		if (is_loaded_) {
			return;
		}
		if (ShouldBypassRpcLocked(context, trust_empty_kinds)) {
			is_loaded_ = true;
			return;
		}
	}
	// Serialize loads. Holding load_lock_ does NOT block GetEntry's cache
	// reads (they only need entry_lock_). LoadEntries fires its RPC outside
	// entry_lock_ and acquires entry_lock_ briefly per-entry insertion.
	std::lock_guard<std::mutex> load_lk(load_lock_);
	// Re-check under load_lock_ — another thread may have just finished.
	{
		std::lock_guard<std::mutex> entry_lk(entry_lock_);
		if (is_loaded_) {
			return;
		}
	}
	LoadEntries(context, load_lk);
	{
		std::lock_guard<std::mutex> entry_lk(entry_lock_);
		is_loaded_ = true;
	}
}

optional_ptr<CatalogEntry> VgiCatalogSet::GetEntry(ClientContext &context, const std::string &name) {
	// Pre-read setting OUTSIDE entry_lock_ — DuckDB's setting registry has
	// its own mutex; calling into it while holding entry_lock_ creates a
	// lock-order inversion that has shown up in WASM hang traces.
	const bool trust_empty_kinds = ReadTrustEmptyKinds(context);

	// Fast path: cache hit (or bypass). Hold entry_lock_ only for the map
	// lookup + the bypass-flag write. Capture the outcome/result, then LOG
	// AFTER releasing the lock — VGI_LOG goes through DuckDB's log manager
	// which has its own mutex, same inversion risk.
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
			outcome = "kind_empty";
		}
	}
	if (outcome) {
		VGI_LOG(context, "catalog.entry_cache",
		        {{"set_kind", CacheKindName()},
		         {"name", name},
		         {"outcome", outcome},
		         {"triggered_load", "false"}});
		return result;
	}

	// Cache miss — trigger load (single-flight via load_lock_). LoadEntries
	// runs WITHOUT entry_lock_ held; re-entrant binder walks are now safe.
	bool was_loaded_before;
	{
		std::lock_guard<std::mutex> lock(entry_lock_);
		was_loaded_before = is_loaded_;
	}
	EnsureLoaded(context);

	// Re-check cache under entry_lock_, capture result + outcome, log outside.
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
	// Trigger load (single-flight, no entry_lock_ held during RPC).
	EnsureLoaded(context);

	// Snapshot entries under entry_lock_ briefly; invoke callback OUTSIDE
	// the lock to avoid holding entry_lock_ while user code runs (which
	// could re-enter the catalog and deadlock on the same lock).
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
