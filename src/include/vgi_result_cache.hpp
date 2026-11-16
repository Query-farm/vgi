// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"

// VgiResultCache — database-owned cache of complete table-function results.
// When a worker advertises `vgi.cache.*` metadata on the first batch of a
// result, the client caches the entire (multi-batch) result in memory and/or on
// disk and serves identical future calls without a worker round-trip.
// See docs/result_cache.md.

#include <atomic>
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
#include <unordered_set>
#include <vector>

#include "vgi_settings_defaults.hpp"
#include "vgi_cache_control.hpp" // vgi.cache.* constants + VgiCacheControl + ParseVgiCacheControl

namespace duckdb {

class ExtensionLoader;
class ClientContext;
class DatabaseInstance;
class FileHandle; // full type only in the .cpp (streaming-capture writer)

namespace vgi {

class VgiResultCache;
VgiResultCache &GetResultCache(ClientContext &context);
VgiResultCache &GetResultCache(DatabaseInstance &db);

// Register the vgi_result_cache() / _flush() / _reap() / _stats() diagnostics.
void RegisterVgiResultCacheFunction(ExtensionLoader &loader);
void RegisterVgiResultCacheFlushFunction(ExtensionLoader &loader);
void RegisterVgiResultCacheReapFunction(ExtensionLoader &loader);
void RegisterVgiResultCacheStatsFunction(ExtensionLoader &loader);

// A uint64 counter that is atomic but stays copyable, so VgiResultCacheEntry keeps
// its implicit copy constructor (used by the revalidation shallow-copy paths).
// `entry->hits` is incremented under mutex_ but read off-lock by Snapshot().
struct CopyableAtomicU64 {
	std::atomic<uint64_t> v {0};
	CopyableAtomicU64() = default;
	CopyableAtomicU64(const CopyableAtomicU64 &o) : v(o.v.load(std::memory_order_relaxed)) {}
	CopyableAtomicU64 &operator=(const CopyableAtomicU64 &o) {
		v.store(o.v.load(std::memory_order_relaxed), std::memory_order_relaxed);
		return *this;
	}
	uint64_t operator++(int) { return v.fetch_add(1, std::memory_order_relaxed); }
	operator uint64_t() const { return v.load(std::memory_order_relaxed); }
};

// A single cached output batch: its IPC-stream bytes (self-contained: schema +
// batch + custom_metadata, produced by SerializeRecordBatch) plus the replayed
// per-batch metadata the scan needs (`vgi_batch_index` / `vgi_partition_values`).
struct VgiCachedBatch {
	VgiBufferPtr ipc;   // serialized IPC stream for this one batch
	int64_t rows = 0;
	// vgi_meta to replay on serve so InstallBatch sees identical values:
	uint64_t batch_index = 0;             // DConstants::INVALID_INDEX sentinel set by producer
	bool has_batch_index = false;
	std::string partition_values_bytes;   // raw IPC bytes ("" = none)
	// Disk-backed streaming: when `ipc == nullptr` and `disk_ipc_offset >= 0` this
	// batch's IPC bytes live at [disk_ipc_offset, +disk_ipc_length) in the entry's
	// `disk_path` blob; CachedReplayConnection positioned-reads them on demand so a
	// larger-than-RAM result never materializes whole.
	int64_t disk_ipc_offset = -1;
	int64_t disk_ipc_length = 0;
};

// One capture thread's ordered substream (many batches).
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
	// Owning catalog schema: a function name is unique only within a schema, so
	// same-named functions in different schemas must not share an entry. Empty
	// when the bind carries no schema. Mirrors BindRequest.schema_name.
	std::string schema_name;
	std::string canonical_arguments;   // canonical (sorted) serialization — NOT raw IPC framing
	std::string canonical_settings;    // canonical (sorted) serialization
	std::string attach_options;        // canonical ATTACH options (excl. secret tokens)
	std::string projection;            // serialized input.column_ids
	std::string attached_data_version;
	std::string implementation_version;
	int64_t catalog_version = 0;
	std::string at_unit;
	std::string at_value;
	// Static pushdown key components. Empty when the pushdown is absent.
	std::string filter_bytes;          // serialized static table_filters (+ join-key IN sets)
	std::string order_by_hint;         // column/direction/null_order/row_limit
	std::string sample_hint;           // sample_percentage/seed
	std::string transaction_id;        // empty unless scope=transaction
	// EXCHANGE-mode input identity. Empty for producer-mode entries. Set for
	// table-in-out / correlated-LATERAL / buffered entries whose output depends on
	// input data: a per-input-batch/-chunk hash (ordered IPC for streaming maps;
	// order-independent sorted-multiset for LATERAL; additive whole-input fold for
	// buffered). See vgi_exchange_cache_key.hpp.
	std::string input_hash;
	// EXCHANGE-mode SHAPE discriminator. Empty for producer-mode / per-partition
	// entries. Set to the operator kind ("scalar" / "lateral" / "stream" / "buffered";
	// scalar and lateral also append their output types) so entries produced by
	// different operators over the same function never cross-serve — otherwise a
	// function reachable both as scalar and as LATERAL (no const args, no projection
	// pushdown) could produce byte-identical keys.
	std::string shape_key;
	// SECRET identity: ComputeSecretCacheFingerprint of the secrets the bind sent
	// to the worker. Empty for a function that uses no secret. A secret-dependent
	// result is keyed on it so a rotated, re-scoped or dropped secret MISSES
	// instead of serving what the old credential produced. A fingerprint only —
	// secret values never enter a key. Kept separate from identity_scope because
	// that one also names the disk shard and scopes FlushCatalog to a principal.
	std::string secret_scope;

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
	CopyableAtomicU64 hits; // atomic (incremented under mutex_, read off-lock by Snapshot)
	// Per-partition producer entry: a human-readable label of the partition-value
	// tuple ("country=US", or "country=US, year=2020" for multi-column), empty for
	// non-partition entries. Diagnostics only (the real discriminator is the "p:"
	// input_hash). See docs/result_cache.md "Per-Partition Result Cache".
	std::string partition_label;
	// Disk-backed streaming entry: `streams` hold TOC-only VgiCachedBatch
	// (disk_ipc_offset/length, no in-RAM `ipc`) and `disk_path` is the blob the
	// replay connection positioned-reads. Served transiently and never inserted
	// into the memory LRU — such entries are streamed because they exceed RAM caps.
	bool disk_backed = false;
	std::string disk_path;

	bool IsStale(std::chrono::steady_clock::time_point now) const {
		return !never_expires && now >= expires_at;
	}
};

// Streams a capture straight to the disk tier: a MISS appends each batch to a temp
// blob file, hashing incrementally so the finished object is content-addressed.
// RAM per capture is one batch regardless of result size. Pimpl'd so the
// FileHandle / mbedtls SHA state stay out of this header.
class VgiCaptureDiskWriter {
public:
	~VgiCaptureDiskWriter();
	// Append one batch record (thread-safe; serializes writers). `ipc_data`/`ipc_len`
	// are written verbatim (already codec-compressed by the caller); `logical_len` is
	// the uncompressed IPC size, summed separately so the ref's `bytes=` stays logical
	// (drives the materialize-vs-stream threshold). Returns false if the on-disk blob
	// would exceed the per-entry disk budget — the caller then aborts the capture.
	bool AppendBatch(bool has_batch_index, uint64_t batch_index, int64_t rows,
	                 const std::string &pv_bytes, const uint8_t *ipc_data, int64_t ipc_len,
	                 int64_t logical_len);
	// The resolved codec ("none"/"zstd"/"lz4") + level this writer compresses with —
	// so a caller holding pre-serialized (uncompressed) bytes can transcode them to
	// match before AppendBatch (the spill-drain path).
	const std::string &CodecName() const;
	uint64_t Level() const;
	// Count of producer threads (local states) feeding this capture — the streaming
	// analogue of substream count, for diagnostics.
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

class VgiResultCache {
public:
	struct Settings {
		uint64_t max_bytes = defaults::RESULT_CACHE_MAX_BYTES;       // 256 MB in-memory global cap
		uint64_t max_entry_bytes = defaults::RESULT_CACHE_MAX_ENTRY_BYTES;  // 64 MB per-entry cap
		uint64_t max_entries = defaults::RESULT_CACHE_MAX_ENTRIES;        // entry-count cap (0 = unlimited)
		uint64_t max_inflight_bytes = defaults::RESULT_CACHE_MAX_INFLIGHT_BYTES; // global in-flight capture budget
		std::string disk_dir = defaults::RESULT_CACHE_DIR;                 // empty = disk tier off
		uint64_t disk_max_bytes = defaults::RESULT_CACHE_DISK_MAX_BYTES;          // 0 = disk tier off
		uint64_t disk_reap_interval_seconds = defaults::RESULT_CACHE_DISK_REAP_INTERVAL_SECONDS; // disk reaper cadence
		// Disk-tier on-write compression (Arrow IPC buffer codec; the memory tier is
		// never compressed). "zstd" default (level 1), "lz4", or "none".
		std::string disk_compression = defaults::RESULT_CACHE_DISK_COMPRESSION;
		uint64_t disk_compression_level = defaults::RESULT_CACHE_DISK_COMPRESSION_LEVEL; // zstd only; ignored for lz4/none
		// File-count cap for EXCHANGE-mode loose disk entries. Per-chunk memos are tiny
		// but numerous and may be expensive, so they are not excluded by size; instead
		// the reaper LRU-evicts the oldest exchange refs (input_hash present) above this
		// count, so a memo flood never evicts a large producer entry. 0 = unbounded.
		// The packed store bounds its file count structurally.
		uint64_t exchange_disk_max_refs = defaults::RESULT_CACHE_EXCHANGE_DISK_MAX_REFS;
		// Packed small-entry disk backend: route small exchange entries into append-only
		// per-process pack files + a rebuildable index instead of a loose .vrc/.ref
		// pair each. Only takes effect once the (opt-in) disk tier is configured.
		bool pack = defaults::RESULT_CACHE_PACK;
		// Route threshold: entries with total_bytes below this pack; at/above go loose.
		uint64_t pack_max_entry_bytes = defaults::RESULT_CACHE_PACK_MAX_ENTRY_BYTES; // 256 KB
		// Roll to a new pack file past this size (bounds one compaction unit).
		uint64_t pack_target_bytes = defaults::RESULT_CACHE_PACK_TARGET_BYTES; // 64 MB
		// Compact an own pack when this percentage (0..100) of its bytes is dead.
		uint64_t pack_compaction_dead_pct = defaults::RESULT_CACHE_PACK_COMPACTION_DEAD_PCT;
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
		// Per-partition producer entry label ("country=US"); empty for others.
		std::string partition_label;
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
		// isolate the exchange paths. bytes_served is the payload not recomputed on hits.
		uint64_t exchange_hits = 0;
		uint64_t exchange_misses = 0;
		uint64_t exchange_stores = 0;
		uint64_t exchange_revalidations = 0;
		uint64_t exchange_bytes_served = 0;
		// Per-partition producer sub-counters. A partition_hit serves a `=`/`IN` scan
		// entirely from N per-partition entries without the worker; a partition_miss is
		// a scan that could enumerate its partition set but at least one entry was cold
		// (fell through to the worker); a partition_store counts each per-partition entry
		// committed by a split capture.
		uint64_t partition_hits = 0;
		uint64_t partition_misses = 0;
		uint64_t partition_stores = 0;
	};

	// Exchange-mode metric recorders (the exchange operators call these at their
	// hit / miss / store / 304-revalidation decision points).
	void RecordExchangeHit(int64_t bytes_served);
	void RecordExchangeMiss();
	void RecordExchangeStore();
	void RecordExchangeRevalidation(int64_t bytes_served);

	// Per-partition producer metric recorders (the producer scan calls these at its
	// partition serve / miss decision points; the split commit calls RecordPartitionStore
	// once per committed partition entry).
	void RecordPartitionHit();
	void RecordPartitionMiss();
	void RecordPartitionStore();

	// `vgi.cache.per_value` advertisement registry. The opt-in rides on an output
	// batch, so it is only observable after an exchange; a scan whose whole input is
	// one chunk has no earlier exchange, so the advertisement is remembered per
	// function identity for the process. It is a hint, not a correctness input (a
	// false positive costs one missed probe), and monotonic: never invalidated, and
	// not cleared by a flush — it describes the function, not cached bytes.
	void NotePerValueOptIn(const VgiResultCacheKey &static_key);
	bool HasPerValueOptIn(const VgiResultCacheKey &static_key);

	// Standalone benchmark/test compatibility; production uses GetResultCache(context).
	static VgiResultCache &Instance();

	// Push database-wide caps + disk-tier config (takes the lock + evicts-to-fit).
	void Configure(const Settings &settings);

	// Per-query config sync: compares a signature of `settings` to the last-applied
	// one (atomic) and calls Configure() only when they differ, so the hot path
	// (every scan's InitGlobal) does not take the global mutex.
	void ConfigureIfChanged(const Settings &settings);

	// Reserve `bytes` of transient capture-buffer budget against max_inflight_bytes.
	// Returns false when it would be exceeded — the caller then abandons caching that
	// capture. Released via ReleaseInflightCapture. Committed bytes are bounded
	// separately by the memory LRU cap.
	bool TryReserveInflightCapture(int64_t bytes);
	void ReleaseInflightCapture(int64_t bytes);

	// Look up a fresh entry. Returns nullptr on miss or if the entry is stale
	// (a stale entry is dropped). On a hit, splices the entry to MRU, bumps its
	// hit counter, and returns a shared_ptr<const Entry> the caller can serve
	// from without holding the cache lock. `served_tier`, if given, is set on a
	// hit to the tier that served it: "memory", "disk" or "disk_streaming".
	std::shared_ptr<const VgiResultCacheEntry> Lookup(const VgiResultCacheKey &key,
	                                                   std::chrono::steady_clock::time_point now,
	                                                   const char **served_tier = nullptr);

	// Batched memory-tier lookup for the per-value dedup path: one lock acquisition
	// for all `keys`. Returns a vector parallel to `keys` (fresh entry or nullptr;
	// stale entries dropped). Memory tier only — per-key disk I/O would defeat the
	// batching. Splices hits to MRU and bumps hit counters like Lookup.
	std::vector<std::shared_ptr<const VgiResultCacheEntry>>
	LookupBatch(const std::vector<VgiResultCacheKey> &keys, std::chrono::steady_clock::time_point now);

	// Conditional-revalidation lookup. Like Lookup but returns a STALE
	// entry (past expires_at) *iff* it is revalidatable, WITHOUT dropping it —
	// the caller sends a conditional request and either slides the TTL (on a
	// not_modified reply) or replaces the entry (on fresh data). Returns nullptr
	// when there is no entry, the entry is still fresh (use Lookup), or it is
	// not revalidatable. Probes the disk tier on an in-memory miss.
	std::shared_ptr<const VgiResultCacheEntry>
	LookupForRevalidation(const VgiResultCacheKey &key, std::chrono::steady_clock::time_point now);

	// Insert (or replace) an entry. Rejects entries larger than max_entry_bytes;
	// retains only entries that fit max_bytes, evicting older entries as needed.
	// Eligible entries can still persist to disk when they exceed max_bytes.
	// Returns false when neither tier accepts the entry.
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

	// One synchronous reap pass over both tiers (what the background thread does),
	// run at `now + advance_seconds` so tests can simulate elapsed time without
	// sleeping. Exposed via `vgi_result_cache_reap()`.
	struct ReapStats {
		size_t memory_reaped = 0;    // in-memory entries dropped (stale, non-revalidatable)
		size_t disk_refs_removed = 0; // on-disk refs unlinked (expired + byte-cap)
	};
	ReapStats ReapNow(int64_t advance_seconds);

	// In-memory entries, for vgi_result_cache().
	std::vector<EntryInfo> Snapshot() const;
	// On-disk refs as tier="disk" rows, for `vgi_result_cache(include_disk := true)`.
	// O(refs).
	std::vector<EntryInfo> SnapshotDisk() const;
	Counters GetCounters() const;
	// Count a capture that streamed to DuckDB but was not cached.
	void NoteCaptureAbort() {
		capture_aborts_.fetch_add(1, std::memory_order_relaxed);
	}

public:
	VgiResultCache();
	~VgiResultCache();

private:
	VgiResultCache(const VgiResultCache &) = delete;
	VgiResultCache &operator=(const VgiResultCache &) = delete;

	void CleanupThread();
	// Reaps stale non-revalidatable entries; returns how many were removed.
	size_t ReapStaleLocked(std::chrono::steady_clock::time_point now);
	void EvictToFitLocked(int64_t incoming_bytes, bool admitting_entry = false);

	// --- Disk tier: content-addressed store, cross-process ---
	// `disk_dir_` / `disk_max_bytes_` are guarded by `mutex_` (Configure() rewrites
	// them). Callers snapshot them under the lock and pass the snapshot into the disk
	// methods below, which do file I/O lock-free. Caller must hold `mutex_`.
	bool DiskEnabledLocked() const {
		return !disk_dir_.empty() && disk_max_bytes_ > 0;
	}
	// Persist a committed entry to the content-addressed disk store (best-effort;
	// failures are swallowed — the memory tier still holds it).
	void PersistToDisk(const VgiResultCacheEntry &entry, const std::string &dir, uint64_t max_bytes,
	                   const std::string &codec, uint64_t level);
	// Load and fully materialize an entry from disk (wall-clock TTL). Used only for
	// small entries (≤ max_entry_bytes), which are then adopted into the memory LRU.
	// Returns null on miss/stale/corruption.
	std::shared_ptr<VgiResultCacheEntry> LoadFromDisk(const VgiResultCacheKey &key,
	                                                  const std::string &dir);
	// Lazy loader for large entries: reads the ref and a per-batch TOC only (seeking
	// past each IPC payload) and returns a disk-backed entry whose batches carry
	// (disk_ipc_offset, length) for CachedReplayConnection to stream one at a time.
	// Returns null on miss/stale/corruption.
	std::shared_ptr<VgiResultCacheEntry> LoadFromDiskStreaming(const VgiResultCacheKey &key,
	                                                           const std::string &dir);
	// Read only the ref and return the entry's stored `bytes`, or -1 when the ref is
	// absent/invalid. Lets Lookup choose materialize-vs-stream cheaply.
	int64_t PeekDiskRefBytes(const VgiResultCacheKey &key, const std::string &dir);
	// Reap the disk store against wall-clock `now_unix` (injectable for tests):
	// unlink expired refs, evict oldest-mtime while over max_bytes, LRU-evict oldest
	// EXCHANGE refs while over max_refs (0 = unbounded), then sweep orphan objects past
	// the grace window. Returns the count of refs removed.
	size_t ReapDisk(const std::string &dir, uint64_t max_bytes, uint64_t max_refs, int64_t now_unix);
	// Remove a whole per-identity disk shard (invalidation).
	void FlushCatalogDisk(const std::string &identity_scope, const std::string &dir);

	// Append-only per-process pack backend for small entries. Constructed lazily on
	// first packed persist. Its own internal mutex guards shard state; it is NOT
	// under `mutex_` (file I/O must not hold the cache lock).
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

	// `vgi.cache.per_value` advertisements, keyed by function identity (see
	// NotePerValueOptIn). Its own mutex: probed on a hot path and never together with
	// the entry index, so it must not queue behind a cache insert.
	std::mutex per_value_optin_mutex_;
	std::unordered_set<std::string> per_value_optin_;

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
	std::atomic<uint64_t> partition_hits_ {0};
	std::atomic<uint64_t> partition_misses_ {0};
	std::atomic<uint64_t> partition_stores_ {0};

	// Signature of the last-applied Settings (see ConfigureIfChanged).
	std::atomic<uint64_t> config_signature_ {0};
	// Sum of bytes reserved by in-flight captures across this cache.
	std::atomic<int64_t> inflight_capture_bytes_ {0};

	std::thread cleanup_thread_;
	std::atomic<bool> shutdown_ {false};
	std::condition_variable cleanup_cv_;
	std::mutex cleanup_mutex_;
};

} // namespace vgi
} // namespace duckdb
