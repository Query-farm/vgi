// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// ============================================================================
// VgiMemoArena — columnar per-value memoization store
// ============================================================================
// Replaces the old "one VgiResultCacheEntry per memoized value" representation,
// where an 8-byte scalar result cost ~6.3 KB of RSS (a self-contained Arrow IPC
// stream + a doubly-stored 17-field key) and serving K values cost K IPC-stream
// decodes plus a K-way concat.
//
// An arena holds ALL of one function's per-value memos under a single static key:
// one contiguous columnar row store + a slot map `sort-key blob -> {row range,
// expiry, validator}`. Serving K values is one gather; storing is one append.
//
// The arena is PURE arrow + std — no DuckDB types — so the operators convert at
// the boundary (they already hold the worker output as an arrow::RecordBatch).
// Arenas live in the process-wide VgiMemoArenaRegistry, keyed by a static-key
// fingerprint, NOT in the VgiResultCache LRU (heterogeneous per-slot expiry would
// mis-fire the whole-entry staleness reaper).

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <arrow/record_batch.h>
#include <arrow/type.h>

namespace duckdb {
namespace vgi {

// ----------------------------------------------------------------------------
// PerValueDiskBackend — optional persistence for arena slots
// ----------------------------------------------------------------------------
// The arena is the MEMORY tier. A backend persists each slot's rows (as a
// self-contained Arrow IPC blob — the columnar layout is a memory optimization,
// disk stays per-slot IPC) so a warm memo survives a restart and is shared across
// same-host processes. Null by default (memory-only, e.g. WASM). Disk is off the
// hot path: a backend is only touched on a cold-arena hydrate (once per static key
// per process) and on a store (a miss, infrequent) — never on a warm serve.
struct PersistedSlot {
	std::string input_blob;      // the slot key (raw sort-key blob)
	std::string ipc;             // self-contained Arrow IPC bytes of this slot's rows
	int64_t rows = 0;            // row count (0 = negative memo)
	int64_t expires_unix = -1;   // wall-clock expiry seconds (-1 = never)
	std::string scope;           // validator...
	std::string etag;
	std::string last_modified;
	bool revalidatable = false;
};

class PerValueDiskBackend {
public:
	virtual ~PerValueDiskBackend() = default;
	// Persist a batch of slots for a static key. `schema_ipc` is the arena's Arrow schema
	// serialized once (so a cold process can rebuild the arena's schema before hydrating).
	virtual void Persist(const std::string &static_fp, const std::string &schema_ipc,
	                     const std::vector<PersistedSlot> &slots) = 0;
	// Load all UNEXPIRED slots for a static key (as of `now_unix`). Fills `schema_ipc_out`
	// and `slots_out`; returns false if the key is absent.
	virtual bool Hydrate(const std::string &static_fp, int64_t now_unix, std::string &schema_ipc_out,
	                     std::vector<PersistedSlot> &slots_out) = 0;
	// Clear everything (for vgi_result_cache_flush()).
	virtual void Flush() = 0;
	// Cap the on-disk size in bytes (0 = unlimited). When set, the backend reaps expired
	// rows and evicts least-recently-used entries to stay under the cap. Default no-op.
	virtual void SetMaxBytes(int64_t /*max_bytes*/) {}
};

// Create a SQLite-backed PerValueDiskBackend rooted at `dir` (WAL mode: concurrent
// readers + one writer across same-host processes, crash-safe, cross-restart). Returns
// null if SQLite is not compiled into this build (WASM) or the DB cannot be opened.
std::shared_ptr<PerValueDiskBackend> MakeSqliteDiskBackend(const std::string &dir);

// The per-value cache-control a slot was stored with. Deduplicated per arena (most
// slots of one function share one advertisement), referenced by index from a slot.
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

// One memoized value. `length == 0` is a valid negative memo (a 1->0 map). Rows are
// always contiguous in the arena's logical row space (a store appends one batch of
// the chunk's misses in slot order, so a slot never spans the base/tail boundary
// after publication — it is placed wholly in the tail, then wholly in the base on
// compaction).
struct VgiMemoSlot {
	int64_t row_start = 0;  // logical offset into base(∪tail)
	int32_t length = 0;     // output rows for this value (0 = negative memo)
	int32_t validator_ref = -1;
	std::chrono::steady_clock::time_point expires_at;
	bool never_expires = false;
	uint64_t last_used = 0; // LRU tick (v1: updated under the arena mutex on hit)
};

// One key's probe outcome, plus the materialized rows for all hits in one batch.
struct ArenaProbeResult {
	// Materialized cached rows for every hit, concatenated. `parent[i]` is the index
	// (into the probed `keys` vector) of the key that produced output row i. Null when
	// no rows were served (all-miss, or all hits were negative memos).
	std::shared_ptr<arrow::RecordBatch> rows;
	std::vector<int32_t> parent;
	// Per probed key: whether it hit, and (if it hit) the validator it carried. A
	// caller latching cache-control across a full hit compares validator_ref across
	// keys — equal refs mean identical validators (B2).
	std::vector<char> hit; // 0/1, parallel to keys
	std::vector<int32_t> validator_ref; // parallel to keys; -1 where miss or no validator
	int64_t served_bytes = 0;
	int64_t num_hits = 0;
	// Aggregates over the HIT slots, for the LATERAL M2 cache-control latch (B1/B2):
	// the minimum REMAINING lifetime and whether every hit slot shares one validator.
	bool latch_all_never_expires = true;
	int64_t latch_min_remaining_s = 0; // seconds; meaningful only if !latch_all_never_expires
	bool latch_validators_agree = true;
	int32_t latch_validator_ref = -1; // the shared validator ref (valid iff agree)
};

// One value to publish. `key` points into the caller's key vector (must outlive the
// Store call). The rows for consecutive specs are laid out consecutively in `batch`.
struct ArenaStoreSpec {
	const std::string *key = nullptr;
	int32_t length = 0; // rows this value contributes in `batch` (0 = negative memo)
	int32_t validator_ref = -1;
};

class VgiMemoArena {
public:
	explicit VgiMemoArena(std::shared_ptr<arrow::Schema> schema) : schema_(std::move(schema)) {}

	// Attach a persistence backend + this arena's static-key fingerprint. When set, Store
	// also persists new slots and a cold arena can be hydrated (LoadFromBackend). The
	// backend outlives all arenas (owned by the registry singleton), so a raw pointer.
	void SetPersistence(PerValueDiskBackend *backend, std::string static_fp) {
		backend_ = backend;
		static_fp_ = std::move(static_fp);
	}
	// Populate a cold arena from the backend (deserialize each persisted slot's IPC and
	// append). Returns the footprint after loading. Called once, right after creation.
	int64_t LoadFromBackend(int64_t now_unix);

	// Deduplicate a validator into this arena, returning a stable ref (or -1 for the
	// empty validator). Cheap; call before Store to fill ArenaStoreSpec::validator_ref.
	int32_t ResolveValidator(const ArenaValidator &v);
	// Read a validator back (for the cc-latch). ref must be one returned by Resolve.
	ArenaValidator GetValidator(int32_t ref);

	// Probe K keys. Fresh hits are materialized into one batch (see ArenaProbeResult).
	// Stale slots are dropped (their rows become dead until reclamation). `now` drives
	// per-slot TTL.
	ArenaProbeResult Probe(const std::vector<std::string> &keys, std::chrono::steady_clock::time_point now);

	// Append `batch` (the missed rows, laid out in `specs` order) and publish a slot
	// per spec. First-writer-wins: a spec whose key already has a live slot is skipped
	// (its rows in `batch` become dead). Returns the net footprint delta in bytes (for
	// the registry's global accounting).
	int64_t Store(const std::shared_ptr<arrow::RecordBatch> &batch, const std::vector<ArenaStoreSpec> &specs,
	              std::chrono::steady_clock::time_point expires_at, bool never_expires);

	int64_t FootprintBytes();
	// Diagnostics: live slots, dead rows, total rows, min remaining ttl seconds (-1 if
	// none expire), for the vgi_result_cache() one-row-per-arena view.
	struct Stats {
		int64_t live_slots = 0;
		int64_t dead_rows = 0;
		int64_t total_rows = 0;
		int64_t footprint_bytes = 0;
	};
	Stats GetStats();
	const std::shared_ptr<arrow::Schema> &schema() const { return schema_; }

private:
	// shared_mutex: Probe is a SHARED-lock reader (concurrent probes of a hot arena run
	// their gathers in parallel over the immutable base/tail buffers — the measured Step-5
	// contention was Probe holding an EXCLUSIVE lock during the Take). All mutation —
	// Store, compaction, stale reclamation, validator dedup — takes the exclusive lock.
	std::shared_mutex mu_;
	std::shared_ptr<arrow::Schema> schema_;
	std::shared_ptr<arrow::RecordBatch> base_; // contiguous, immutable once published
	std::shared_ptr<arrow::RecordBatch> tail_; // recent appends coalesced into one batch
	int64_t base_rows_ = 0;
	int64_t tail_rows_ = 0;
	int64_t dead_rows_ = 0;
	int64_t footprint_ = 0;
	uint64_t tick_ = 0;
	std::unordered_map<std::string, VgiMemoSlot> slots_;
	std::vector<ArenaValidator> validators_;
	PerValueDiskBackend *backend_ = nullptr; // optional; owned by the registry
	std::string static_fp_;

	// Serialize the newly-stored slots (their rows sliced from `batch`, offset held in the
	// slot's row_start) and hand them to the backend. Called under the exclusive lock from
	// Store when a backend is attached.
	void PersistNewSlotsLocked(const std::shared_ptr<arrow::RecordBatch> &batch,
	                           const std::vector<std::pair<std::string, VgiMemoSlot>> &new_slots);

	// Materialize `logical` rows (offsets into base∪tail) into one batch, preserving
	// the order of `logical`. One Take when all rows are in base (the common path after
	// a compaction); otherwise a 2-source gather (still O(#rows), never O(arena)).
	std::shared_ptr<arrow::RecordBatch> MaterializeLocked(const std::vector<int64_t> &logical);
	// Rebuild base_ from only the live+FRESH slots (dropping dead rows AND stale slots as
	// of `now`) and clear tail_. Also serves as compaction. Rebases every slot's row_start.
	void CompactLocked(std::chrono::steady_clock::time_point now);
	bool ShouldCompactLocked() const;
	int64_t RecomputeFootprintLocked();
};

// ----------------------------------------------------------------------------
// Process-wide arena registry
// ----------------------------------------------------------------------------
// Keyed by a static-key fingerprint (SHA-256 of the exchange static key, minus the
// per-value input_hash). Holds shared_ptr<VgiMemoArena> so a reader that fetched an
// arena keeps it alive across the worker round-trip even if the registry evicts it.
class VgiMemoArenaRegistry {
public:
	static VgiMemoArenaRegistry &Instance();

	// Get (or create) the arena for a static key + worker-output schema. `schema` is
	// used only on creation; a schema mismatch on an existing arena returns null (the
	// caller then skips memoization — never serves the wrong shape).
	std::shared_ptr<VgiMemoArena> GetOrCreate(const std::string &static_fp,
	                                           const std::shared_ptr<arrow::Schema> &schema);
	// Fetch without creating (probe-only path). Null if absent.
	std::shared_ptr<VgiMemoArena> Get(const std::string &static_fp);

	// Fold an arena's footprint change into the global byte budget and evict-to-fit
	// (whole arenas, coldest first). Called by an arena after append/compaction.
	// Returns false if the growth cannot be accommodated (caller drops the append).
	bool NoteFootprintDelta(const std::string &static_fp, int64_t delta);

	void SetMaxBytes(int64_t max_bytes);
	// Attach the persistence backend (null = memory-only). New arenas hydrate from it on
	// a cold GetOrCreate, and their stores persist through it.
	void SetBackend(std::shared_ptr<PerValueDiskBackend> backend);
	// Idempotently attach a SQLite backend rooted at `dir` (empty → detach) and apply the
	// on-disk byte cap (0 = unlimited). Tracks the current dir so a repeated call with the
	// same dir only refreshes the cap. Called from SyncResultCacheSettings so
	// `SET vgi_result_cache_dir` / `..._per_value_disk_max_bytes` take effect.
	void EnsureSqliteBackend(const std::string &dir, int64_t disk_max_bytes);
	void FlushAll();
	// Close and drop the SQLite backend, releasing its open file handle on
	// `vgi_per_value.sqlite`. Required before removing the cache directory on Windows,
	// where an open file cannot be deleted (POSIX unlinks it happily). The next
	// EnsureSqliteBackend re-opens it (backend_dir_ is cleared so the re-open fires).
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
	int64_t max_bytes_ = 256LL * 1024 * 1024;
	uint64_t tick_ = 0;
	struct Entry {
		std::shared_ptr<VgiMemoArena> arena;
		int64_t bytes = 0;  // last-noted footprint (mirrors arena, under mu_)
		uint64_t last_used = 0;
	};
	std::unordered_map<std::string, Entry> arenas_;
	std::shared_ptr<PerValueDiskBackend> backend_;
	std::string backend_dir_; // current SQLite dir (for idempotent EnsureSqliteBackend)
	void EvictToFitLocked(int64_t incoming, const std::string &keep);
};

} // namespace vgi
} // namespace duckdb
