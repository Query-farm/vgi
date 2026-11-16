// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"

// VgiMemoArena — columnar per-value memoization store.
//
// One arena holds all of one function's per-value memos under a single static
// key: a contiguous columnar row store plus a slot map `sort-key blob -> {row
// range, expiry, validator}`. Serving K values is one gather; storing is one
// append. No DuckDB types: operators convert at the boundary.
// Arenas live in the database-owned VgiMemoArenaRegistry, not the VgiResultCache
// LRU, whose whole-entry staleness reaper cannot handle per-slot expiry.

#include "vgi_settings_defaults.hpp"

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>


namespace duckdb {
class ClientContext;
namespace vgi {
class VgiMemoArenaRegistry;
VgiMemoArenaRegistry &GetMemoArenaRegistry(ClientContext &context);

// PerValueDiskBackend — optional disk tier for arena slots. Each slot persists as
// a self-contained Arrow IPC blob, so a warm memo survives a restart and is shared
// across same-host processes. Null = memory-only (e.g. WASM). Touched only on a
// cold-arena hydrate and on a store, never on a warm serve.
struct PersistedSlot {
	std::string input_blob;      // the slot key (raw sort-key blob)
	std::string ipc;             // self-contained Arrow IPC bytes of this slot's rows
	int64_t rows = 0;            // row count (0 = negative memo)
	int64_t expires_unix = -1;   // wall-clock expiry seconds (-1 = never)
	std::string scope;           // validator fields (see ArenaValidator)
	std::string etag;
	std::string last_modified;
	bool revalidatable = false;
};

class PerValueDiskBackend {
public:
	virtual ~PerValueDiskBackend() = default;
	// Persist slots for a static key. `schema_ipc` may be empty (each slot's IPC carries
	// its own schema).
	virtual void Persist(const std::string &static_fp, const std::string &schema_ipc,
	                     const std::vector<PersistedSlot> &slots) = 0;
	// Load all unexpired slots for a static key as of `now_unix`; false if absent.
	virtual bool Hydrate(const std::string &static_fp, int64_t now_unix, std::string &schema_ipc_out,
	                     std::vector<PersistedSlot> &slots_out) = 0;
	// Clear everything (for vgi_result_cache_flush()).
	virtual void Flush() = 0;
	// Cap the on-disk size in bytes (0 = unlimited); the backend reaps expired rows
	// and evicts LRU entries to stay under it. Default no-op.
	virtual void SetMaxBytes(int64_t /*max_bytes*/) {}
};

// SQLite-backed PerValueDiskBackend rooted at `dir` (WAL mode, so same-host
// processes share it). Null if SQLite is not in this build (WASM) or the DB
// cannot be opened.
std::shared_ptr<PerValueDiskBackend> MakeSqliteDiskBackend(const std::string &dir);

// The per-value cache-control a slot was stored with; deduplicated per arena and
// referenced by index from a slot.
struct ArenaValidator {
	std::string scope;
	std::string etag;
	std::string last_modified;
	bool revalidatable = false;
	bool operator==(const ArenaValidator &o) const {
		return scope == o.scope && etag == o.etag && last_modified == o.last_modified &&
		       revalidatable == o.revalidatable;
	}
};

// One memoized value; `length == 0` is a negative memo (a 1->0 map). A slot's rows
// are contiguous and never span the base/tail boundary: wholly in the tail when
// stored, wholly in the base after compaction.
struct VgiMemoSlot {
	int64_t row_start = 0;  // logical offset into base(∪tail)
	int32_t length = 0;     // output rows for this value (0 = negative memo)
	int32_t validator_ref = -1;
	std::chrono::steady_clock::time_point expires_at;
	bool never_expires = false;
};

// One key's probe outcome, plus the materialized rows for all hits in one batch.
struct ArenaProbeResult {
	// Rows for every hit, concatenated; `parent[i]` is the index into `keys` of the
	// key that produced row i. Null when no rows were served.
	VgiBatchPtr rows;
	std::vector<int32_t> parent;
	// Per probed key: hit flag and validator ref. Equal refs mean identical
	// validators.
	std::vector<char> hit; // 0/1, parallel to keys
	std::vector<int32_t> validator_ref; // parallel to keys; -1 where miss or no validator
	int64_t served_bytes = 0;
	int64_t num_hits = 0;
	// Over the hit slots, for latching cache-control onto a fully cached result: the
	// minimum remaining lifetime and whether all hits share one validator.
	bool latch_all_never_expires = true;
	int64_t latch_min_remaining_s = 0; // seconds; meaningful only if !latch_all_never_expires
	bool latch_validators_agree = true;
	int32_t latch_validator_ref = -1; // the shared validator ref (valid iff agree)
};

// One value to publish. `key` must outlive the Store call. Consecutive specs' rows
// are laid out consecutively in `batch`.
struct ArenaStoreSpec {
	const std::string *key = nullptr;
	int32_t length = 0; // rows this value contributes in `batch` (0 = negative memo)
	int32_t validator_ref = -1;
};

class VgiMemoArena {
public:
	explicit VgiMemoArena(VgiSchemaPtr schema) : schema_(std::move(schema)) {}

	// Attach a backend + this arena's static-key fingerprint: Store then also persists
	// new slots. Shared ownership keeps it alive across a registry backend change.
	void SetPersistence(std::shared_ptr<PerValueDiskBackend> backend, std::string static_fp) {
		backend_ = backend;
		static_fp_ = std::move(static_fp);
	}
	// Populate a new arena from the backend; returns the footprint. Unlocked: call
	// only before the arena is published.
	int64_t LoadFromBackend(int64_t now_unix);

	// Deduplicate a validator into this arena, returning a stable ref (-1 for the
	// empty validator), for ArenaStoreSpec::validator_ref.
	int32_t ResolveValidator(const ArenaValidator &v);
	// ref must come from ResolveValidator.
	ArenaValidator GetValidator(int32_t ref);

	// Probe K keys under a shared lock; fresh hits are materialized into one batch.
	// Stale slots count as misses and are left for Store/compaction to reclaim.
	ArenaProbeResult Probe(const std::vector<std::string> &keys, std::chrono::steady_clock::time_point now);

	// Append `batch` (the missed rows, in `specs` order) and publish a slot per spec.
	// First-writer-wins: a key with a fresh slot is skipped (its rows become dead);
	// a stale slot is replaced. Values beyond 2^20 slots are not memoized. Returns the
	// net footprint delta, for VgiMemoArenaRegistry::NoteFootprintDelta.
	int64_t Store(const VgiBatchPtr &batch, const std::vector<ArenaStoreSpec> &specs,
	              std::chrono::steady_clock::time_point expires_at, bool never_expires);

	int64_t FootprintBytes();
	// Diagnostics for the vgi_result_cache() one-row-per-arena view.
	struct Stats {
		int64_t live_slots = 0;
		int64_t dead_rows = 0;
		int64_t total_rows = 0;
		int64_t footprint_bytes = 0;
	};
	Stats GetStats();
	const VgiSchemaPtr &schema() const { return schema_; }

private:
	// Probe takes it shared, so concurrent probes gather in parallel over the
	// immutable base/tail buffers. All mutation (Store, compaction, stale reclamation,
	// validator dedup) takes it exclusive; Probe must never mutate.
	std::shared_mutex mu_;
	VgiSchemaPtr schema_;
	VgiBatchPtr base_; // contiguous, immutable once published
	VgiBatchPtr tail_; // recent appends coalesced into one batch
	int64_t base_rows_ = 0;
	int64_t tail_rows_ = 0;
	int64_t dead_rows_ = 0;
	int64_t footprint_ = 0;
	std::unordered_map<std::string, VgiMemoSlot> slots_;
	std::vector<ArenaValidator> validators_;
	std::shared_ptr<PerValueDiskBackend> backend_; // retained while this arena is in use
	std::string static_fp_;

	// Persist the newly stored slots; here each slot's row_start is its offset within
	// `batch`. Called from Store under the exclusive lock.
	void PersistNewSlotsLocked(const VgiBatchPtr &batch,
	                           const std::vector<std::pair<std::string, VgiMemoSlot>> &new_slots);

	// Materialize `logical` rows (offsets into base∪tail) in order: one Take when all
	// are in base, else a 2-source gather. O(#rows), never O(arena).
	VgiBatchPtr MaterializeLocked(const std::vector<int64_t> &logical);
	// Rebuild base_ from the fresh slots only (dropping dead rows and stale slots as of
	// `now`), clear tail_, and rebase every row_start.
	void CompactLocked(std::chrono::steady_clock::time_point now);
	bool ShouldCompactLocked() const;
	int64_t RecomputeFootprintLocked();
};

// Database-owned arena registry (GetMemoArenaRegistry), keyed by static-key
// fingerprint. Arenas are shared_ptrs so a reader keeps its arena alive across the
// worker round trip even if the registry evicts it.
class VgiMemoArenaRegistry {
public:
	// Get or create (hydrating from the backend) the arena for a static key. A schema
	// mismatch with an existing arena returns null: the caller skips memoization.
	std::shared_ptr<VgiMemoArena> GetOrCreate(const std::string &static_fp,
	                                           const VgiSchemaPtr &schema);
	// Probe-only: an absent arena is hydrated from the backend only if it has slots.
	std::shared_ptr<VgiMemoArena> Get(const std::string &static_fp);

	// Fold a Store's footprint delta into the byte budget and evict to fit (whole
	// arenas, coldest first; an arena alone over budget is dropped too). Returns false
	// if that arena is no longer retained. `arena` stops an old snapshot being
	// accounted against a replacement arena with the same key.
	bool NoteFootprintDelta(const std::string &static_fp, int64_t delta, const VgiMemoArena *arena);

	void SetMaxBytes(int64_t max_bytes);
	// Null = memory-only. Affects arenas created afterwards.
	void SetBackend(std::shared_ptr<PerValueDiskBackend> backend);
	// Attach a SQLite backend at `dir` (empty = detach) and apply the disk cap
	// (0 = unlimited). Same dir only refreshes the cap; a new dir drops all arenas.
	void EnsureSqliteBackend(const std::string &dir, int64_t disk_max_bytes);
	void FlushAll();
	// Drop all arenas and the backend, closing the SQLite file once no active arena
	// holds it. Needed before removing the cache directory on Windows. The next
	// EnsureSqliteBackend re-opens it.
	void ReleaseBackend();

	// Snapshot for diagnostics (one row per arena).
	struct ArenaRow {
		std::string static_fp;
		VgiMemoArena::Stats stats;
	};
	std::vector<ArenaRow> Snapshot();

private:
	std::mutex mu_;
	int64_t total_bytes_ = 0;
	int64_t max_bytes_ = defaults::RESULT_CACHE_MAX_BYTES;
	uint64_t tick_ = 0;
	struct Entry {
		std::shared_ptr<VgiMemoArena> arena;
		int64_t bytes = 0;  // last-noted footprint (mirrors arena, under mu_)
		uint64_t last_used = 0;
	};
	std::unordered_map<std::string, Entry> arenas_;
	std::shared_ptr<PerValueDiskBackend> backend_;
	std::string backend_dir_;
	void EvictToFitLocked(int64_t incoming, const std::string &keep);
};

} // namespace vgi
} // namespace duckdb
