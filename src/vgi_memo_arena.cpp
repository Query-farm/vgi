// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_memo_arena.hpp"

#include <stdexcept>
#include <utility>

#include <arrow/array/builder_primitive.h>
#include <arrow/compute/api_vector.h> // arrow::compute::Take
#include <arrow/datum.h>              // arrow::Datum (complete type for Result<Datum>)
#include <arrow/io/memory.h>          // BufferReader / BufferOutputStream (IPC persist)
#include <arrow/ipc/reader.h>         // RecordBatchStreamReader
#include <arrow/ipc/writer.h>         // MakeStreamWriter
#include <arrow/util/byte_size.h>     // arrow::util::TotalBufferSize

namespace duckdb {
namespace vgi {

namespace {

// Throw-on-error unwrap so a mid-serve arrow failure surfaces as an exception the
// operator boundary catches (the arena stays DuckDB-free, so std::runtime_error).
template <typename T>
T Unwrap(arrow::Result<T> r, const char *what) {
	if (!r.ok()) {
		throw std::runtime_error(std::string("vgi memo arena: ") + what + ": " + r.status().ToString());
	}
	return std::move(r).ValueUnsafe();
}

void Check(const arrow::Status &s, const char *what) {
	if (!s.ok()) {
		throw std::runtime_error(std::string("vgi memo arena: ") + what + ": " + s.ToString());
	}
}

// Take `idx` rows from `src` (in idx order), bounds-check off (indices are arena
// offsets, valid by construction).
std::shared_ptr<arrow::RecordBatch> TakeRows(const std::shared_ptr<arrow::RecordBatch> &src,
                                             const std::vector<int64_t> &idx) {
	arrow::Int64Builder b;
	Check(b.Reserve(static_cast<int64_t>(idx.size())), "index reserve");
	for (auto v : idx) {
		b.UnsafeAppend(v);
	}
	std::shared_ptr<arrow::Array> ia;
	Check(b.Finish(&ia), "index build");
	auto res = arrow::compute::Take(arrow::Datum(src), arrow::Datum(ia),
	                                arrow::compute::TakeOptions::NoBoundsCheck());
	return Unwrap(std::move(res), "take").record_batch();
}

std::shared_ptr<arrow::RecordBatch> ConcatBatches(const std::vector<std::shared_ptr<arrow::RecordBatch>> &parts) {
	return Unwrap(arrow::ConcatenateRecordBatches(parts), "concat");
}

// Serialize one batch as a self-contained Arrow IPC stream (schema + batch). The
// columnar arena is the memory optimization; the DISK representation is per-slot IPC
// (the same bytes the pre-arena per-value entries used), which the backend stores.
std::string SerializeBatchToIpc(const std::shared_ptr<arrow::RecordBatch> &batch) {
	auto sink = Unwrap(arrow::io::BufferOutputStream::Create(), "ipc sink");
	auto writer = Unwrap(arrow::ipc::MakeStreamWriter(sink, batch->schema(), arrow::ipc::IpcWriteOptions::Defaults()),
	                     "ipc writer");
	Check(writer->WriteRecordBatch(*batch), "ipc write");
	Check(writer->Close(), "ipc close");
	auto buf = Unwrap(sink->Finish(), "ipc finish");
	return std::string(reinterpret_cast<const char *>(buf->data()), static_cast<size_t>(buf->size()));
}

int64_t NowUnix() {
	return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch())
	    .count();
}

std::shared_ptr<arrow::RecordBatch> DeserializeBatchFromIpc(const std::string &ipc) {
	auto buf = arrow::Buffer::FromString(ipc);
	auto in = std::make_shared<arrow::io::BufferReader>(buf);
	auto reader = Unwrap(arrow::ipc::RecordBatchStreamReader::Open(in), "ipc open");
	std::shared_ptr<arrow::RecordBatch> batch;
	Check(reader->ReadNext(&batch), "ipc readnext");
	return batch; // schema included; may be a 0-row batch (a negative memo)
}

} // namespace

// ============================================================================
// VgiMemoArena
// ============================================================================

int32_t VgiMemoArena::ResolveValidator(const ArenaValidator &v) {
	if (v.scope.empty() && v.etag.empty() && v.last_modified.empty() && !v.revalidatable) {
		return -1;
	}
	std::unique_lock<std::shared_mutex> lg(mu_);
	for (size_t i = 0; i < validators_.size(); i++) {
		if (validators_[i] == v) {
			return static_cast<int32_t>(i);
		}
	}
	validators_.push_back(v);
	return static_cast<int32_t>(validators_.size() - 1);
}

ArenaValidator VgiMemoArena::GetValidator(int32_t ref) {
	std::shared_lock<std::shared_mutex> lg(mu_);
	if (ref < 0 || ref >= static_cast<int32_t>(validators_.size())) {
		return ArenaValidator {};
	}
	return validators_[static_cast<size_t>(ref)];
}

std::shared_ptr<arrow::RecordBatch> VgiMemoArena::MaterializeLocked(const std::vector<int64_t> &logical) {
	const int64_t n = static_cast<int64_t>(logical.size());
	if (n == 0) {
		return nullptr;
	}
	bool all_base = true;
	for (auto l : logical) {
		if (l >= base_rows_) {
			all_base = false;
			break;
		}
	}
	if (all_base) {
		return TakeRows(base_, logical); // already base offsets, in output order
	}
	// 2-source gather: split into base/tail, take each (only the touched rows, ≤ n),
	// concat, then one reorder Take back into output order. O(n), never O(arena).
	std::vector<int64_t> base_idx, tail_idx;
	std::vector<int64_t> base_out, tail_out;
	for (int64_t i = 0; i < n; i++) {
		const int64_t l = logical[i];
		if (l < base_rows_) {
			base_idx.push_back(l);
			base_out.push_back(i);
		} else {
			tail_idx.push_back(l - base_rows_);
			tail_out.push_back(i);
		}
	}
	std::vector<std::shared_ptr<arrow::RecordBatch>> parts;
	if (!base_idx.empty()) {
		parts.push_back(TakeRows(base_, base_idx));
	}
	if (!tail_idx.empty()) {
		parts.push_back(TakeRows(tail_, tail_idx));
	}
	auto combined = parts.size() == 1 ? parts[0] : ConcatBatches(parts);
	// combined rows are [base_out order..., tail_out order...]; reorder to output order.
	std::vector<int64_t> inv(static_cast<size_t>(n));
	const int64_t nb = static_cast<int64_t>(base_idx.size());
	for (int64_t r = 0; r < static_cast<int64_t>(base_out.size()); r++) {
		inv[static_cast<size_t>(base_out[r])] = r;
	}
	for (int64_t r = 0; r < static_cast<int64_t>(tail_out.size()); r++) {
		inv[static_cast<size_t>(tail_out[r])] = nb + r;
	}
	return TakeRows(combined, inv);
}

ArenaProbeResult VgiMemoArena::Probe(const std::vector<std::string> &keys,
                                     std::chrono::steady_clock::time_point now) {
	ArenaProbeResult r;
	r.hit.assign(keys.size(), 0);
	r.validator_ref.assign(keys.size(), -1);
	std::vector<int64_t> logical;
	std::vector<int32_t> parent;

	// SHARED lock: read-only. Concurrent probes of the same (hot) arena run their gathers
	// in parallel. NO mutation here — a stale slot is treated as a MISS and left in place;
	// the exclusive Store path reclaims stale slots + their rows (either by overwriting the
	// key on a re-store, or by dropping it on the next compaction). This is what removes the
	// Step-5 warm-serve contention (Probe used to hold an exclusive lock during the Take).
	std::shared_lock<std::shared_mutex> lg(mu_);
	bool first_hit = true;
	for (size_t k = 0; k < keys.size(); k++) {
		auto it = slots_.find(keys[k]);
		if (it == slots_.end()) {
			continue;
		}
		const VgiMemoSlot &s = it->second;
		if (!s.never_expires && now >= s.expires_at) {
			continue; // stale → MISS (reclaimed by Store, never mutated under the shared lock)
		}
		r.hit[k] = 1;
		r.validator_ref[k] = s.validator_ref;
		r.num_hits++;
		// Aggregate for the M2 cc-latch (B1/B2).
		if (!s.never_expires) {
			r.latch_all_never_expires = false;
			int64_t rem = std::chrono::duration_cast<std::chrono::seconds>(s.expires_at - now).count();
			if (rem < 0) {
				rem = 0;
			}
			if (first_hit || rem < r.latch_min_remaining_s) {
				r.latch_min_remaining_s = rem;
			}
		}
		if (first_hit) {
			r.latch_validator_ref = s.validator_ref;
		} else if (s.validator_ref != r.latch_validator_ref) {
			r.latch_validators_agree = false;
		}
		first_hit = false;
		for (int32_t o = 0; o < s.length; o++) {
			logical.push_back(s.row_start + static_cast<int64_t>(o));
			parent.push_back(static_cast<int32_t>(k));
		}
	}
	if (!logical.empty()) {
		r.rows = MaterializeLocked(logical);
		r.parent = std::move(parent);
		r.served_bytes = r.rows ? arrow::util::TotalBufferSize(*r.rows) : 0;
	}
	return r;
}

int64_t VgiMemoArena::Store(const std::shared_ptr<arrow::RecordBatch> &batch,
                            const std::vector<ArenaStoreSpec> &specs,
                            std::chrono::steady_clock::time_point expires_at, bool never_expires) {
	std::unique_lock<std::shared_mutex> lg(mu_);
	const int64_t before = footprint_;
	const auto now = std::chrono::steady_clock::now();

	// Append the whole batch to the tail (coalesced into one contiguous batch). Rows
	// land at [base_rows_ + old_tail_rows, ...). Specs whose key already has a FRESH
	// slot are skipped (first-writer-wins); their rows in `batch` become dead.
	const int64_t appended = batch ? batch->num_rows() : 0;
	if (appended > 0) {
		if (!tail_) {
			tail_ = batch;
			tail_rows_ = appended;
		} else {
			tail_ = ConcatBatches({tail_, batch});
			tail_rows_ += appended;
		}
	}
	const int64_t batch_base = base_rows_ + (tail_rows_ - appended);
	int64_t off = 0;
	int64_t new_dead = 0;
	// Structural bound on a single arena (the registry's byte-budget eviction cannot evict
	// the arena currently being appended to, so a single pathological high-cardinality key
	// must not grow without limit). Over the cap, new values are simply not memoized.
	const int64_t kMaxSlots = 1 << 20;
	// Track newly-published slots (with their offset WITHIN `batch`) so they can be
	// persisted to the disk backend after the loop (before compaction rebases offsets).
	std::vector<std::pair<std::string, VgiMemoSlot>> new_slots;
	for (const auto &sp : specs) {
		const int64_t start = batch_base + off;
		const int64_t batch_off = off; // this slot's offset within `batch`
		off += sp.length;
		auto it = slots_.find(*sp.key);
		if (it != slots_.end()) {
			if (!it->second.never_expires && now >= it->second.expires_at) {
				// Existing slot is STALE — reclaim it so this value can be re-memoized
				// (Probe treated it as a miss). Its old rows become dead; fall through to
				// insert the fresh slot below.
				dead_rows_ += it->second.length;
				slots_.erase(it);
			} else {
				new_dead += sp.length; // fresh slot present → first-writer-wins
				continue;
			}
		}
		if (static_cast<int64_t>(slots_.size()) >= kMaxSlots) {
			new_dead += sp.length; // arena full: this value's rows become dead, not memoized
			continue;
		}
		VgiMemoSlot s;
		s.row_start = start;
		s.length = sp.length;
		s.validator_ref = sp.validator_ref;
		s.expires_at = expires_at;
		s.never_expires = never_expires;
		slots_.emplace(*sp.key, s);
		if (backend_) {
			VgiMemoSlot ps = s;
			ps.row_start = batch_off; // repurposed: offset within `batch` for persistence
			new_slots.emplace_back(*sp.key, ps);
		}
	}
	dead_rows_ += new_dead;
	if (backend_ && !new_slots.empty()) {
		PersistNewSlotsLocked(batch, new_slots);
	}
	if (ShouldCompactLocked()) {
		CompactLocked(now);
	}
	const int64_t after = RecomputeFootprintLocked();
	return after - before;
}

void VgiMemoArena::PersistNewSlotsLocked(const std::shared_ptr<arrow::RecordBatch> &batch,
                                         const std::vector<std::pair<std::string, VgiMemoSlot>> &new_slots) {
	if (!backend_ || !batch) {
		return;
	}
	const int64_t now_unix =
	    std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
	const auto steady_now = std::chrono::steady_clock::now();
	std::vector<PersistedSlot> ps;
	ps.reserve(new_slots.size());
	for (const auto &kv : new_slots) {
		const VgiMemoSlot &s = kv.second; // s.row_start holds the offset within `batch`
		PersistedSlot p;
		p.input_blob = kv.first;
		p.rows = s.length;
		auto rows_batch = batch->Slice(s.row_start, s.length); // 0-length → 0-row batch (negative memo)
		p.ipc = SerializeBatchToIpc(rows_batch);
		if (s.never_expires) {
			p.expires_unix = -1;
		} else {
			const int64_t rem =
			    std::chrono::duration_cast<std::chrono::seconds>(s.expires_at - steady_now).count();
			p.expires_unix = now_unix + rem;
		}
		if (s.validator_ref >= 0 && s.validator_ref < static_cast<int32_t>(validators_.size())) {
			const ArenaValidator &v = validators_[static_cast<size_t>(s.validator_ref)];
			p.scope = v.scope;
			p.etag = v.etag;
			p.last_modified = v.last_modified;
			p.revalidatable = v.revalidatable;
		}
		ps.push_back(std::move(p));
	}
	backend_->Persist(static_fp_, std::string(), ps);
}

int64_t VgiMemoArena::LoadFromBackend(int64_t now_unix) {
	if (!backend_) {
		return 0;
	}
	std::string schema_ipc;
	std::vector<PersistedSlot> slots;
	if (!backend_->Hydrate(static_fp_, now_unix, schema_ipc, slots) || slots.empty()) {
		return 0;
	}
	// No lock: called pre-publication (the registry holds this arena's only reference).
	const auto steady_now = std::chrono::steady_clock::now();
	std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
	int64_t cursor = 0;
	for (auto &p : slots) {
		auto b = DeserializeBatchFromIpc(p.ipc);
		if (!schema_) {
			schema_ = b->schema();
		}
		VgiMemoSlot s;
		s.row_start = cursor;
		s.length = static_cast<int32_t>(p.rows);
		s.never_expires = (p.expires_unix < 0);
		s.expires_at = s.never_expires ? steady_now
		                               : steady_now + std::chrono::seconds(p.expires_unix - now_unix);
		// Deduplicate the validator (no lock; single-threaded during hydrate).
		ArenaValidator v {p.scope, p.etag, p.last_modified, p.revalidatable};
		s.validator_ref = -1;
		if (!(v.scope.empty() && v.etag.empty() && v.last_modified.empty() && !v.revalidatable)) {
			for (size_t i = 0; i < validators_.size(); i++) {
				if (validators_[i] == v) {
					s.validator_ref = static_cast<int32_t>(i);
					break;
				}
			}
			if (s.validator_ref < 0) {
				validators_.push_back(v);
				s.validator_ref = static_cast<int32_t>(validators_.size() - 1);
			}
		}
		slots_.emplace(p.input_blob, s);
		if (b->num_rows() > 0) {
			batches.push_back(b);
		}
		cursor += p.rows;
	}
	base_ = batches.empty() ? Unwrap(arrow::RecordBatch::MakeEmpty(schema_), "empty base") : ConcatBatches(batches);
	base_rows_ = cursor;
	return RecomputeFootprintLocked();
}

bool VgiMemoArena::ShouldCompactLocked() const {
	if (tail_rows_ == 0) {
		return dead_rows_ * 2 >= (base_rows_ + tail_rows_) && (base_rows_ + tail_rows_) > 0;
	}
	// Doubling policy bounds total compaction work to O(N log N).
	return base_rows_ == 0 || tail_rows_ * 2 >= base_rows_ || dead_rows_ * 2 >= (base_rows_ + tail_rows_);
}

void VgiMemoArena::CompactLocked(std::chrono::steady_clock::time_point now) {
	// Rebuild base_ from ONLY the live+FRESH slots (dropping dead rows AND stale slots)
	// in slot-iteration order, rebasing each slot's row_start. Reads the OLD
	// base_/tail_/base_rows_ via MaterializeLocked, so publish the new state only after
	// materializing. This is where stale slots left behind by Probe are reclaimed.
	std::vector<int64_t> logical;
	logical.reserve(static_cast<size_t>(base_rows_ + tail_rows_ - dead_rows_));
	std::vector<std::string> stale_keys;
	int64_t cursor = 0;
	for (auto &kv : slots_) {
		VgiMemoSlot &s = kv.second;
		if (!s.never_expires && now >= s.expires_at) {
			stale_keys.push_back(kv.first); // drop (and its rows) — do not carry into the new base
			continue;
		}
		for (int32_t o = 0; o < s.length; o++) {
			logical.push_back(s.row_start + static_cast<int64_t>(o));
		}
		s.row_start = cursor; // new offset in the rebuilt base
		cursor += s.length;
	}
	for (auto &k : stale_keys) {
		slots_.erase(k);
	}
	std::shared_ptr<arrow::RecordBatch> nb;
	if (logical.empty()) {
		nb = Unwrap(arrow::RecordBatch::MakeEmpty(schema_), "empty base");
	} else {
		nb = MaterializeLocked(logical);
	}
	base_ = nb;
	base_rows_ = cursor;
	tail_.reset();
	tail_rows_ = 0;
	dead_rows_ = 0;
}

int64_t VgiMemoArena::RecomputeFootprintLocked() {
	int64_t f = 0;
	if (base_) {
		f += arrow::util::TotalBufferSize(*base_);
	}
	if (tail_) {
		f += arrow::util::TotalBufferSize(*tail_);
	}
	// Slot-map + key-blob overhead. Per slot: the unordered_map node (~48), the
	// VgiMemoSlot (~48), the std::string key control block (~32) + slack, plus the key
	// bytes. Calibrated against a measured ~240 B/value at 1M BIGINT values (of which
	// ~8 B is the payload, counted above): 160 + key bytes tracks real RSS within ~1.35x
	// so the byte budget genuinely bounds memory rather than under-counting it.
	for (auto &kv : slots_) {
		f += static_cast<int64_t>(kv.first.size()) + 160;
	}
	f += static_cast<int64_t>(validators_.size()) * 96;
	footprint_ = f;
	return f;
}

int64_t VgiMemoArena::FootprintBytes() {
	std::shared_lock<std::shared_mutex> lg(mu_);
	return footprint_;
}

VgiMemoArena::Stats VgiMemoArena::GetStats() {
	std::shared_lock<std::shared_mutex> lg(mu_);
	Stats st;
	st.live_slots = static_cast<int64_t>(slots_.size());
	st.dead_rows = dead_rows_;
	st.total_rows = base_rows_ + tail_rows_;
	st.footprint_bytes = footprint_;
	return st;
}

// ============================================================================
// VgiMemoArenaRegistry
// ============================================================================

VgiMemoArenaRegistry &VgiMemoArenaRegistry::Instance() {
	static VgiMemoArenaRegistry *inst = new VgiMemoArenaRegistry(); // leaked, like the cache singleton
	return *inst;
}

std::shared_ptr<VgiMemoArena> VgiMemoArenaRegistry::GetOrCreate(const std::string &static_fp,
                                                                const std::shared_ptr<arrow::Schema> &schema) {
	std::lock_guard<std::mutex> lg(mu_);
	++tick_;
	auto it = arenas_.find(static_fp);
	if (it != arenas_.end()) {
		// Schema mismatch → refuse (never serve the wrong shape). Caller skips memo.
		if (schema && it->second.arena->schema() && !it->second.arena->schema()->Equals(*schema)) {
			return nullptr;
		}
		it->second.last_used = tick_;
		return it->second.arena;
	}
	auto arena = std::make_shared<VgiMemoArena>(schema);
	int64_t fp_bytes = 0;
	if (backend_) {
		arena->SetPersistence(backend_.get(), static_fp);
		fp_bytes = arena->LoadFromBackend(NowUnix()); // hydrate prior-process / peer slots
	}
	Entry e;
	e.arena = arena;
	e.bytes = fp_bytes;
	e.last_used = tick_;
	total_bytes_ += fp_bytes;
	arenas_.emplace(static_fp, std::move(e));
	if (total_bytes_ > max_bytes_) {
		EvictToFitLocked(0, static_fp);
	}
	return arena;
}

std::shared_ptr<VgiMemoArena> VgiMemoArenaRegistry::Get(const std::string &static_fp) {
	std::lock_guard<std::mutex> lg(mu_);
	auto it = arenas_.find(static_fp);
	if (it != arenas_.end()) {
		it->second.last_used = ++tick_;
		return it->second.arena;
	}
	// Cold: hydrate from the backend so a warm memo survives a restart and is shared
	// across same-host processes. Only materialise the arena if the backend actually has
	// slots for this key (otherwise a probe of an unmemoized function would create empties).
	if (backend_) {
		auto arena = std::make_shared<VgiMemoArena>(nullptr);
		arena->SetPersistence(backend_.get(), static_fp);
		const int64_t fp_bytes = arena->LoadFromBackend(NowUnix());
		if (arena->GetStats().live_slots > 0) {
			Entry e;
			e.arena = arena;
			e.bytes = fp_bytes;
			e.last_used = ++tick_;
			total_bytes_ += fp_bytes;
			arenas_.emplace(static_fp, std::move(e));
			if (total_bytes_ > max_bytes_) {
				EvictToFitLocked(0, static_fp);
			}
			return arena;
		}
	}
	return nullptr;
}

void VgiMemoArenaRegistry::EvictToFitLocked(int64_t incoming, const std::string &keep) {
	(void)incoming;
	while (total_bytes_ > max_bytes_) {
		// Find the coldest evictable arena that is not `keep`.
		auto victim = arenas_.end();
		uint64_t oldest = UINT64_MAX;
		for (auto it = arenas_.begin(); it != arenas_.end(); ++it) {
			if (it->first == keep) {
				continue;
			}
			if (it->second.last_used < oldest) {
				oldest = it->second.last_used;
				victim = it;
			}
		}
		if (victim == arenas_.end()) {
			break; // only `keep` remains; a single over-cap arena is bounded by arena_max_slots
		}
		total_bytes_ -= victim->second.bytes;
		arenas_.erase(victim);
	}
}

bool VgiMemoArenaRegistry::NoteFootprintDelta(const std::string &static_fp, int64_t delta) {
	std::lock_guard<std::mutex> lg(mu_);
	auto it = arenas_.find(static_fp);
	if (it == arenas_.end()) {
		return false;
	}
	it->second.bytes += delta;
	it->second.last_used = ++tick_;
	total_bytes_ += delta;
	if (total_bytes_ > max_bytes_) {
		EvictToFitLocked(0, static_fp);
	}
	return true;
}

void VgiMemoArenaRegistry::SetMaxBytes(int64_t max_bytes) {
	std::lock_guard<std::mutex> lg(mu_);
	max_bytes_ = max_bytes;
	if (total_bytes_ > max_bytes_) {
		EvictToFitLocked(0, std::string());
	}
}

void VgiMemoArenaRegistry::SetBackend(std::shared_ptr<PerValueDiskBackend> backend) {
	std::lock_guard<std::mutex> lg(mu_);
	backend_ = std::move(backend);
}

void VgiMemoArenaRegistry::EnsureSqliteBackend(const std::string &dir, int64_t disk_max_bytes) {
	std::lock_guard<std::mutex> lg(mu_);
	if (dir != backend_dir_) {
		backend_dir_ = dir;
		backend_ = dir.empty() ? nullptr : MakeSqliteDiskBackend(dir);
		// Existing in-memory arenas keep serving; new/cold ones will use the new backend.
	}
	if (backend_) {
		backend_->SetMaxBytes(disk_max_bytes); // refresh the cap even when the dir is unchanged
	}
}

void VgiMemoArenaRegistry::FlushAll() {
	std::shared_ptr<PerValueDiskBackend> b;
	{
		std::lock_guard<std::mutex> lg(mu_);
		arenas_.clear();
		total_bytes_ = 0;
		b = backend_;
	}
	if (b) {
		b->Flush(); // clear the disk tier too (outside the registry lock)
	}
}

void VgiMemoArenaRegistry::ReleaseBackend() {
	std::shared_ptr<PerValueDiskBackend> b;
	{
		std::lock_guard<std::mutex> lg(mu_);
		arenas_.clear();         // arenas may hold the backend shared_ptr; drop them first
		total_bytes_ = 0;
		b = std::move(backend_); // registry drops its ref
		backend_dir_.clear();    // so the next EnsureSqliteBackend re-opens
	}
	// `b`'s destructor (sqlite3_close) runs here, outside the lock — releasing the OS file
	// handle so the cache directory can be removed on Windows.
}

std::vector<VgiMemoArenaRegistry::ArenaRow> VgiMemoArenaRegistry::Snapshot() {
	std::vector<std::shared_ptr<VgiMemoArena>> live;
	std::vector<std::string> fps;
	{
		std::lock_guard<std::mutex> lg(mu_);
		for (auto &kv : arenas_) {
			fps.push_back(kv.first);
			live.push_back(kv.second.arena);
		}
	}
	std::vector<ArenaRow> rows;
	rows.reserve(live.size());
	for (size_t i = 0; i < live.size(); i++) {
		rows.push_back(ArenaRow {fps[i], live[i]->GetStats()});
	}
	return rows;
}

} // namespace vgi
} // namespace duckdb
