// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// VgiResultCache — process-wide cache of complete table-function results.
//
// When a worker advertises `vgi.cache.*` metadata on the first batch of a
// result, the client caches the entire (multi-batch) result and serves
// identical future calls from the cache, skipping the worker round-trip.
// Milestone 1 is the in-memory tier only; the disk tier (milestone 2) reuses
// the same key/entry/substream types and is added later.
//
// This header is Arrow-free: Arrow types appear only behind `shared_ptr`
// members / forward declarations (header-hygiene rule — see CLAUDE.md). The
// real Arrow includes live in vgi_result_cache.cpp.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "vgi_cache_control.hpp" // vgi.cache.* constants + VgiCacheControl + ParseVgiCacheControl

namespace arrow {
class Buffer;
class KeyValueMetadata;
} // namespace arrow

namespace duckdb {

class ExtensionLoader;
class FileHandle; // full type only in the .cpp (streaming-capture writer)

namespace vgi {

// Register the vgi_result_cache() / vgi_result_cache_flush() /
// vgi_result_cache_reap() diagnostics.
void RegisterVgiResultCacheFunction(ExtensionLoader &loader);
void RegisterVgiResultCacheFlushFunction(ExtensionLoader &loader);
void RegisterVgiResultCacheReapFunction(ExtensionLoader &loader);
void RegisterVgiResultCacheStatsFunction(ExtensionLoader &loader);

// ============================================================================
// Cached storage — per-batch bytes, per-thread substreams, whole-result entry
// ============================================================================
// A single cached output batch: its IPC-stream bytes (self-contained: schema +
// batch + custom_metadata, produced by SerializeRecordBatch) plus the replayed
// per-batch metadata the scan needs (`vgi_batch_index` / `vgi_partition_values`).
struct VgiCachedBatch {
	std::shared_ptr<arrow::Buffer> ipc;   // serialized IPC stream for this one batch
	int64_t rows = 0;
	// vgi_meta to replay on serve so InstallBatch sees identical values:
	uint64_t batch_index = 0;             // DConstants::INVALID_INDEX sentinel set by producer
	bool has_batch_index = false;
	std::string partition_values_bytes;   // raw IPC bytes ("" = none)
	// [S8] Disk-backed streaming: when `ipc == nullptr` and `disk_ipc_offset >= 0`
	// this batch's IPC bytes live at [disk_ipc_offset, +disk_ipc_length) inside the
	// blob file. CachedReplayConnection positioned-reads just this batch on demand
	// so a >RAM disk result never materializes whole. `disk_path` is on the entry.
	int64_t disk_ipc_offset = -1;
	int64_t disk_ipc_length = 0;
};

// One capture thread's ordered substream (many batches). In v1 (memory tier)
// the batches live in RAM; the disk tier (M2) adds a blob_digest variant.
struct CachedStream {
	std::vector<VgiCachedBatch> batches;
	int64_t bytes = 0;
	int64_t rows = 0;
};

// The cache key. All fields are equality-matched; Hash() is only a bucket.
// Binary components (arguments/settings/projection) are held as byte-strings.
struct VgiResultCacheKey {
	std::string identity_scope;        // catalog_name + auth principal fingerprint
	std::string worker_path;
	std::string function_name;
	std::string canonical_arguments;   // canonical (sorted) serialization — NOT raw IPC framing
	std::string canonical_settings;    // canonical (sorted) serialization
	std::string attach_options;        // canonical ATTACH options (excl. secret tokens)
	std::string projection;            // serialized input.column_ids (v1: in the key)
	std::string attached_data_version;
	std::string implementation_version;
	int64_t catalog_version = 0;
	std::string at_unit;
	std::string at_value;
	// Static pushdown key components (M3). Empty when the pushdown is absent.
	std::string filter_bytes;          // serialized static table_filters (+ join-key IN sets)
	std::string order_by_hint;         // column/direction/null_order/row_limit
	std::string sample_hint;           // sample_percentage/seed
	std::string transaction_id;        // empty unless scope=transaction
	// EXCHANGE-mode input identity. Empty for producer-mode table-function entries
	// (so their keys are byte-identical to before this field existed). Set for
	// table-in-out / correlated-LATERAL / buffered entries whose output depends on
	// input data: a per-input-batch/-chunk hash (ordered IPC for streaming maps;
	// order-independent sorted-multiset for LATERAL; additive whole-input fold for
	// buffered). See vgi_exchange_cache_key.hpp.
	std::string input_hash;

	bool operator==(const VgiResultCacheKey &o) const;
	// 64-bit bucket hash over all fields.
	uint64_t Hash() const;
	// Stable hex digest for diagnostics / logs (not the equality key). 64-bit.
	std::string HexDigest() const;
	// Strong (SHA-256) collision-resistant digest of ALL key fields, used as the
	// on-disk ref filename and re-verified on load so a 64-bit HexDigest bucket
	// collision can never cross-serve. 64 lowercase hex chars, or "" if SHA-256
	// is unavailable (caller then skips the disk tier).
	std::string Fingerprint() const;
};

struct VgiResultCacheKeyHash {
	size_t operator()(const VgiResultCacheKey &k) const {
		return static_cast<size_t>(k.Hash());
	}
};

// A complete cached result: the ordered set of per-thread substreams plus
// freshness / scope / validator metadata.
struct VgiResultCacheEntry {
	VgiResultCacheKey key;
	std::vector<CachedStream> streams;
	int64_t total_bytes = 0;
	int64_t rows = 0;
	std::chrono::steady_clock::time_point stored_at;
	std::chrono::steady_clock::time_point expires_at; // stored_at + ttl (or from expires)
	bool never_expires = false;                       // frozen / at-pinned snapshots only
	std::string scope = VGI_CACHE_SCOPE_CATALOG;
	std::string etag;
	std::string last_modified;
	bool revalidatable = false;
	// A short catalog-name label for FlushCatalog scoping + diagnostics (the
	// identity_scope in the key is a fingerprint; this is the human name).
	std::string catalog_name;
	uint64_t hits = 0;
	// [S8] Disk-backed streaming entry: `disk_backed` set, `streams` hold TOC-only
	// VgiCachedBatch (disk_ipc_offset/length, no in-RAM `ipc`), and `disk_path` is
	// the blob file the replay connection positioned-reads. Such entries are served
	// transiently and NOT inserted into the memory LRU (they'd otherwise defeat the
	// point — the whole reason to stream is that they're larger than RAM).
	bool disk_backed = false;
	std::string disk_path;

	bool IsStale(std::chrono::steady_clock::time_point now) const {
		return !never_expires && now >= expires_at;
	}
};

// ============================================================================
// VgiCaptureDiskWriter — streams a capture straight to the disk tier
// ============================================================================
// When the disk tier is enabled, a MISS captures batches by APPENDING each one
// directly to a temp blob file (never buffering the whole result in RAM), hashing
// incrementally so the finished object stays content-addressed. RAM per capture is
// one batch, whatever the result size — a 2 GB result caches at ~flat memory rather
// than the ~2× (substream buffers + serialized blob string) the RAM path costs.
// Pimpl'd so the FileHandle / mbedtls SHA state stay out of this header.
class VgiCaptureDiskWriter {
public:
	~VgiCaptureDiskWriter();
	// Append one batch record (thread-safe; serializes writers). `ipc_data`/`ipc_len`
	// are the bytes written VERBATIM (already codec-compressed by the caller, or raw
	// when the codec is "none"); `logical_len` is the uncompressed IPC size, summed
	// separately so the ref's `bytes=` stays logical (drives the materialize-vs-stream
	// threshold). Returns false if the running ON-DISK (compressed) blob size would
	// exceed the per-entry disk budget — the caller then aborts + keeps streaming.
	bool AppendBatch(bool has_batch_index, uint64_t batch_index, int64_t rows,
	                 const std::string &pv_bytes, const uint8_t *ipc_data, int64_t ipc_len,
	                 int64_t logical_len);
	// The resolved codec ("none"/"zstd"/"lz4") + level this writer compresses with —
	// so a caller holding pre-serialized (uncompressed) bytes can transcode them to
	// match before AppendBatch (the spill-drain path).
	const std::string &CodecName() const;
	uint64_t Level() const;
	// Observability: how many distinct producer threads (local states) fed this
	// capture — the streaming analogue of substream count.
	void RegisterProducer();
	int64_t Rows() const;
	int64_t Bytes() const;
	int64_t Producers() const;

private:
	friend class VgiResultCache;
	VgiCaptureDiskWriter();
	struct Impl;
	std::unique_ptr<Impl> impl_;
};

// ============================================================================
// VgiResultCache — leaked process-wide singleton (mirrors VgiWorkerPool)
// ============================================================================
class VgiResultCache {
public:
	struct Settings {
		uint64_t max_bytes = 268435456;       // 256 MB in-memory global cap
		uint64_t max_entry_bytes = 67108864;  // 64 MB per-entry cap
		uint64_t max_entries = 131072;        // entry-count cap (0 = unlimited) [S5]
		uint64_t max_inflight_bytes = 268435456; // global in-flight capture budget [S6]
		std::string disk_dir;                 // empty = disk tier off
		uint64_t disk_max_bytes = 0;          // 0 = disk tier off
		uint64_t disk_reap_interval_seconds = 60; // disk reaper cadence [S7]
		// Disk-tier on-write compression (Arrow built-in IPC codec; memory tier is
		// never compressed). "zstd" default (level 1), "lz4", or "none".
		std::string disk_compression = "zstd";
		uint64_t disk_compression_level = 1; // zstd only; ignored for lz4/none
		// [S9] File-count cap for EXCHANGE-mode disk entries. Per-input-batch/-chunk
		// memos are tiny but numerous (one per input chunk), so keying the disk
		// decision on payload size would wrongly exclude a small-but-EXPENSIVE result
		// (the case a warm disk cache helps most). Instead every exchange memo may go
		// to disk, but the reaper LRU-evicts oldest exchange refs above this count so
		// per-chunk fan-out can't spray unbounded .vrc/.ref files. Scoped to exchange
		// refs (input_hash present) so a memo flood never evicts a large producer
		// entry. 0 = unbounded. Default 100k. (Loose store only; the packed store
		// below bounds file count structurally.)
		uint64_t exchange_disk_max_refs = 100000;
		// --- Packed small-entry disk backend (git-style loose-vs-packed split) ---
		// Master switch: route SMALL entries into append-only per-process pack files +
		// a rebuildable index instead of a loose .vrc/.ref pair each, so thousands of
		// tiny per-chunk memos cost a few files. Large entries stay loose. Default ON
		// (the disk tier itself is opt-in, so this only takes effect once a disk dir is
		// configured); the loose store is still used for large entries.
		bool pack = true;
		// Route threshold: entries with total_bytes below this pack; at/above go loose.
		uint64_t pack_max_entry_bytes = 262144; // 256 KB
		// Roll to a new pack file past this size (bounds one compaction unit).
		uint64_t pack_target_bytes = 67108864; // 64 MB
		// Compact an OWN pack when this fraction of its bytes is dead (0..100, percent,
		// integer so it folds into the settings signature cleanly). Default 50%.
		uint64_t pack_compaction_dead_pct = 50;
	};

	// Snapshot row for the vgi_result_cache() diagnostic.
	struct EntryInfo {
		std::string catalog_name;
		std::string function_name;
		std::string key_hex;
		std::string scope;
		std::string attached_data_version;
		std::string implementation_version;
		int64_t catalog_version = 0;
		std::string at_unit;
		std::string at_value;
		int64_t num_batches = 0;
		int64_t num_substreams = 0; // capture-thread count (>1 proves parallel capture)
		int64_t num_rows = 0;
		int64_t total_bytes = 0;
		int64_t age_seconds = 0;
		int64_t ttl_seconds = 0;   // -1 = never expires
		bool stale = false;
		std::string tier = "memory";
		std::string etag;
		std::string last_modified;
		bool revalidatable = false;
		uint64_t hits = 0;
		// Disk-tier on-write compression codec ("none"/"zstd"/"lz4"). Always "none"
		// for memory-tier rows (the memory tier is never compressed).
		std::string codec = "none";
	};

	// Aggregate counters for diagnostics / debugging.
	struct Counters {
		uint64_t hits = 0;
		uint64_t misses = 0;
		uint64_t inserts = 0;
		uint64_t evictions_lru = 0;
		uint64_t evictions_ttl = 0;
		uint64_t capture_aborts = 0;
		// Exchange-mode (table-in-out / LATERAL / buffered) sub-counters. The
		// hits/misses above are producer+exchange combined (both call Lookup); these
		// isolate the exchange paths so an operator can measure whether the input-keyed
		// cache earns its keep. A hit rate = (hits+revalidations)/(hits+revalidations+
		// misses); bytes_served is the payload NOT recomputed/transferred on hits.
		uint64_t exchange_hits = 0;
		uint64_t exchange_misses = 0;
		uint64_t exchange_stores = 0;
		uint64_t exchange_revalidations = 0;
		uint64_t exchange_bytes_served = 0;
	};

	// Exchange-mode metric recorders (the exchange operators call these at their
	// hit / miss / store / 304-revalidation decision points).
	void RecordExchangeHit(int64_t bytes_served);
	void RecordExchangeMiss();
	void RecordExchangeStore();
	void RecordExchangeRevalidation(int64_t bytes_served);

	static VgiResultCache &Instance();

	// Push process-global caps + disk-tier config (takes the lock + evicts-to-fit).
	void Configure(const Settings &settings);

	// [S1] Cheap per-query config sync: compares a signature of `settings` to the
	// last-applied one via an atomic; only calls Configure() (lock + evict) when
	// they actually differ. The steady-state hot path — every scan's InitGlobal
	// — is then lock-free instead of contending the global mutex on every query.
	void ConfigureIfChanged(const Settings &settings);

	// [S6] Reserve `bytes` of in-flight capture budget against a process-global
	// ceiling (max_inflight_bytes). Returns false when the reservation would
	// exceed it — the caller then aborts that capture to uncached, bounding total
	// concurrent-capture RAM regardless of query concurrency. Released via
	// ReleaseInflightCapture. `Insert`-committed bytes are separate (the memory
	// LRU cap); this bounds the *transient* capture buffers.
	bool TryReserveInflightCapture(int64_t bytes);
	void ReleaseInflightCapture(int64_t bytes);

	// Look up a fresh entry. Returns nullptr on miss or if the entry is stale
	// (a stale entry is dropped). On a hit, splices the entry to MRU, bumps its
	// hit counter, and returns a shared_ptr<const Entry> the caller can serve
	// from without holding the cache lock.
	std::shared_ptr<const VgiResultCacheEntry> Lookup(const VgiResultCacheKey &key,
	                                                   std::chrono::steady_clock::time_point now);

	// Batched memory-tier lookup for the per-VALUE dedup path: one lock acquisition for
	// all `keys` instead of K individual Lookup() calls (a mutex storm at 2048 keys/chunk).
	// Returns a vector parallel to `keys`; each element is the fresh entry or nullptr
	// (miss / stale, stale dropped). Memory tier only — per-value entries are tiny and the
	// disk tier's per-key file I/O would defeat the batching (they still spill to the packed
	// tier via Insert). Splices hits to MRU + bumps hit counters like Lookup.
	std::vector<std::shared_ptr<const VgiResultCacheEntry>>
	LookupBatch(const std::vector<VgiResultCacheKey> &keys, std::chrono::steady_clock::time_point now);

	// Conditional-revalidation lookup (M6). Like Lookup but returns a STALE
	// entry (past expires_at) *iff* it is revalidatable, WITHOUT dropping it —
	// the caller sends a conditional request and either slides the TTL (on a
	// not_modified reply) or replaces the entry (on fresh data). Returns nullptr
	// when there is no entry, the entry is still fresh (use Lookup), or it is
	// not revalidatable. Probes the disk tier on an in-memory miss.
	std::shared_ptr<const VgiResultCacheEntry>
	LookupForRevalidation(const VgiResultCacheKey &key, std::chrono::steady_clock::time_point now);

	// Insert (or replace) an entry. Rejects entries larger than max_entry_bytes;
	// evicts from LRU until the new entry fits under max_bytes. Returns false if
	// the entry was rejected (too large).
	// `allow_disk=false` keeps the entry memory-only (skips PersistToDisk) — used for
	// transaction-scoped entries, whose txn-id key is ephemeral + process-local.
	bool Insert(std::shared_ptr<VgiResultCacheEntry> entry, bool allow_disk = true);

	// Streaming disk capture (RAM-flat): open a temp blob under `key`'s identity
	// shard; nullptr if the disk tier is unavailable. Batches are appended via the
	// returned writer; CommitStreamingCapture finalizes (hash→rename→ref) using
	// `meta` for the ref fields (scope/etag/expiry/rows/bytes/catalog). The entry is
	// disk-only — future lookups discover it via the ref (adopted into memory on a
	// small serve). AbortStreamingCapture deletes the temp on a non-commit.
	std::shared_ptr<VgiCaptureDiskWriter> BeginStreamingCapture(const VgiResultCacheKey &key,
	                                                            const std::string &dir, uint64_t disk_max,
	                                                            const std::string &codec, uint64_t level);
	bool CommitStreamingCapture(VgiCaptureDiskWriter &writer, const VgiResultCacheEntry &meta);
	void AbortStreamingCapture(VgiCaptureDiskWriter &writer);

	// Invalidation. `identity_scope` (catalog_name + auth fingerprint, from
	// BuildCatalogIdentityScope) locates the per-identity disk shard to remove;
	// pass "" to skip the disk tier (memory-only flush by catalog label).
	size_t FlushAll();
	size_t FlushCatalog(const std::string &catalog_name, const std::string &identity_scope = "");

	// Synchronous, time-injectable reap of ONE pass over both tiers — the same
	// work the background thread does each tick, but driven on the caller's
	// thread with `now + advance_seconds` so cleanup is testable reproducibly
	// (no sleep, no clock dependency). `advance_seconds` simulates time passing:
	// any entry whose freshness lapses within the window is reaped. Returns the
	// per-tier removal counts. Exposed via `vgi_result_cache_reap()`.
	struct ReapStats {
		size_t memory_reaped = 0;    // in-memory entries dropped (stale, non-revalidatable)
		size_t disk_refs_removed = 0; // on-disk refs unlinked (expired + byte-cap)
	};
	ReapStats ReapNow(int64_t advance_seconds);

	// Background TTL reaping tick target + diagnostics.
	std::vector<EntryInfo> Snapshot() const;
	// Walk the on-disk tier's refs (if a disk dir is configured) into EntryInfo rows
	// with tier="disk" — for `vgi_result_cache(include_disk := true)` so spilled/
	// disk-only entries (invisible to the in-memory Snapshot) are observable. O(refs).
	std::vector<EntryInfo> SnapshotDisk() const;
	Counters GetCounters() const;
	// Bump the capture-abort counter (a capture streamed to DuckDB but was not cached
	// — entry too large, inflight budget exhausted, disk budget, not-cacheable, …).
	void NoteCaptureAbort() {
		capture_aborts_.fetch_add(1, std::memory_order_relaxed);
	}

private:
	VgiResultCache();
	~VgiResultCache();
	VgiResultCache(const VgiResultCache &) = delete;
	VgiResultCache &operator=(const VgiResultCache &) = delete;

	void CleanupThread();
	// Reaps stale non-revalidatable entries; returns how many were removed.
	size_t ReapStaleLocked(std::chrono::steady_clock::time_point now);
	void EvictToFitLocked(int64_t incoming_bytes);

	// --- Disk tier (M2): content-addressed store, cross-process ---
	// NOTE: `disk_dir_` / `disk_max_bytes_` are guarded by `mutex_` — Configure()
	// rewrites them per-query, so they must never be read off-lock. Callers
	// snapshot them under the lock and pass the snapshot into the disk methods
	// below (which then do file I/O lock-free). MUST hold `mutex_` to call.
	bool DiskEnabledLocked() const {
		return !disk_dir_.empty() && disk_max_bytes_ > 0;
	}
	// Persist a committed entry to the content-addressed disk store (best-effort;
	// failures are swallowed — the memory tier still holds it).
	void PersistToDisk(const VgiResultCacheEntry &entry, const std::string &dir, uint64_t max_bytes,
	                   const std::string &codec, uint64_t level);
	// Try to load an entry for `key` from disk (cross-process / cross-restart).
	// Returns null on miss/stale/corruption. Wall-clock TTL. Fully materializes
	// the payload — used only for SMALL entries (≤ max_entry_bytes) that are then
	// adopted into the memory LRU.
	std::shared_ptr<VgiResultCacheEntry> LoadFromDisk(const VgiResultCacheKey &key,
	                                                  const std::string &dir);
	// [S8] Lazy loader for LARGE entries: reads the ref + the blob header + a
	// per-batch TOC only (seeks PAST each batch's IPC payload, never reads it), and
	// returns a disk-backed entry whose batches carry (disk_ipc_offset, length) so
	// CachedReplayConnection streams them one at a time. RAM stays flat regardless
	// of result size. Returns null on miss/stale/corruption. `ref_bytes` is the
	// entry size read from the ref (the streaming-vs-materialize decision input).
	std::shared_ptr<VgiResultCacheEntry> LoadFromDiskStreaming(const VgiResultCacheKey &key,
	                                                           const std::string &dir);
	// Cheap peek: read ONLY the ref and return the entry's stored `bytes` (or -1
	// if there is no ref). Lets Lookup choose materialize-vs-stream without a TOC
	// build for small entries. Returns -1 (miss) when the ref is absent/invalid.
	int64_t PeekDiskRefBytes(const VgiResultCacheKey &key, const std::string &dir);
	// Reap the disk store against wall-clock `now_unix` (injectable for tests):
	// unlink expired refs, evict oldest-mtime while over max_bytes, LRU-evict oldest
	// EXCHANGE refs while over max_refs (0 = unbounded), then sweep orphan objects past
	// the grace window. Returns the count of refs removed.
	size_t ReapDisk(const std::string &dir, uint64_t max_bytes, uint64_t max_refs, int64_t now_unix);
	// Remove a whole per-identity disk shard (invalidation).
	void FlushCatalogDisk(const std::string &identity_scope, const std::string &dir);

	// [S9 packed] Append-only per-process pack backend for SMALL entries (git-style
	// loose-vs-packed split). Pimpl'd: the per-shard index / writer state stays in the
	// .cpp. Constructed lazily on first packed Persist. Its own internal mutex guards
	// shard state, so it is NOT under `mutex_` (file I/O must not hold the cache lock).
	struct PackStore;
	std::unique_ptr<PackStore> pack_store_;
	std::mutex pack_store_init_mutex_; // guards lazy construction of pack_store_
	PackStore &EnsurePackStore();
	// Guarded read of the (maybe-null) pack store — used by the reaper / flush paths
	// that must NOT construct it, without racing a concurrent EnsurePackStore.
	PackStore *PackStoreIfExists();

	std::string disk_dir_;      // guarded by mutex_
	uint64_t disk_max_bytes_ = 0; // guarded by mutex_
	// Disk-tier compression config (guarded by mutex_; snapshotted under the lock
	// and passed into the disk-write methods, like disk_dir_ / disk_max_bytes_).
	std::string disk_compression_ = "zstd"; // guarded by mutex_
	uint64_t disk_compression_level_ = 1;   // guarded by mutex_

	mutable std::mutex mutex_;
	// MRU-ordered list of entries; front = most recently used.
	std::list<std::shared_ptr<VgiResultCacheEntry>> lru_;
	std::unordered_map<VgiResultCacheKey,
	                   std::list<std::shared_ptr<VgiResultCacheEntry>>::iterator,
	                   VgiResultCacheKeyHash>
	    index_;
	int64_t total_bytes_ = 0;
	Settings settings_;

	// Atomic so the diagnostic path can read without the big lock.
	std::atomic<uint64_t> hits_ {0};
	std::atomic<uint64_t> misses_ {0};
	std::atomic<uint64_t> inserts_ {0};
	std::atomic<uint64_t> evictions_lru_ {0};
	std::atomic<uint64_t> evictions_ttl_ {0};
	std::atomic<uint64_t> capture_aborts_ {0};
	std::atomic<uint64_t> exchange_hits_ {0};
	std::atomic<uint64_t> exchange_misses_ {0};
	std::atomic<uint64_t> exchange_stores_ {0};
	std::atomic<uint64_t> exchange_revalidations_ {0};
	std::atomic<uint64_t> exchange_bytes_served_ {0};

	// [S1] Signature of the last-applied Settings; ConfigureIfChanged skips the
	// lock + evict when the current query's settings match this.
	std::atomic<uint64_t> config_signature_ {0};
	// [S6] Process-global sum of bytes reserved by in-flight captures.
	std::atomic<int64_t> inflight_capture_bytes_ {0};

	std::thread cleanup_thread_;
	std::atomic<bool> shutdown_ {false};
	std::condition_variable cleanup_cv_;
	std::mutex cleanup_mutex_;
};

} // namespace vgi
} // namespace duckdb
