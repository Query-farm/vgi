// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_memo_arena.hpp"

#include <stdexcept>
#include <utility>

#include <arrow/array/builder_primitive.h>
#include <arrow/compute/api_vector.h> // arrow::compute::Take
#include <arrow/datum.h>              // arrow::Datum (complete type for Result<Datum>)
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

} // namespace

// ============================================================================
// VgiMemoArena
// ============================================================================

int32_t VgiMemoArena::ResolveValidator(const ArenaValidator &v) {
	if (v.scope.empty() && v.etag.empty() && v.last_modified.empty() && !v.revalidatable) {
		return -1;
	}
	std::lock_guard<std::mutex> lg(mu_);
	for (size_t i = 0; i < validators_.size(); i++) {
		if (validators_[i] == v) {
			return static_cast<int32_t>(i);
		}
	}
	validators_.push_back(v);
	return static_cast<int32_t>(validators_.size() - 1);
}

ArenaValidator VgiMemoArena::GetValidator(int32_t ref) {
	std::lock_guard<std::mutex> lg(mu_);
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

	std::lock_guard<std::mutex> lg(mu_);
	++tick_;
	bool first_hit = true;
	for (size_t k = 0; k < keys.size(); k++) {
		auto it = slots_.find(keys[k]);
		if (it == slots_.end()) {
			continue;
		}
		VgiMemoSlot &s = it->second;
		if (!s.never_expires && now >= s.expires_at) {
			dead_rows_ += s.length; // stale: unpublish; its rows are now dead
			slots_.erase(it);
			continue;
		}
		r.hit[k] = 1;
		r.validator_ref[k] = s.validator_ref;
		s.last_used = tick_;
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
	// A pure-read arena whose slots expire accumulates dead rows with no append to
	// trigger compaction; reclaim here when the dead fraction is high.
	const int64_t total = base_rows_ + tail_rows_;
	if (total > 0 && dead_rows_ * 2 >= total) {
		CompactLocked();
		RecomputeFootprintLocked();
	}
	return r;
}

int64_t VgiMemoArena::Store(const std::shared_ptr<arrow::RecordBatch> &batch,
                            const std::vector<ArenaStoreSpec> &specs,
                            std::chrono::steady_clock::time_point expires_at, bool never_expires) {
	std::lock_guard<std::mutex> lg(mu_);
	++tick_;
	const int64_t before = footprint_;

	// Append the whole batch to the tail (coalesced into one contiguous batch). Rows
	// land at [base_rows_ + old_tail_rows, ...). Specs whose key already has a live
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
	for (const auto &sp : specs) {
		const int64_t start = batch_base + off;
		off += sp.length;
		if (slots_.find(*sp.key) != slots_.end()) {
			new_dead += sp.length; // first-writer-wins: keep the existing slot
			continue;
		}
		VgiMemoSlot s;
		s.row_start = start;
		s.length = sp.length;
		s.validator_ref = sp.validator_ref;
		s.expires_at = expires_at;
		s.never_expires = never_expires;
		s.last_used = tick_;
		slots_.emplace(*sp.key, s);
	}
	dead_rows_ += new_dead;
	if (ShouldCompactLocked()) {
		CompactLocked();
	}
	const int64_t after = RecomputeFootprintLocked();
	return after - before;
}

bool VgiMemoArena::ShouldCompactLocked() const {
	if (tail_rows_ == 0) {
		return dead_rows_ * 2 >= (base_rows_ + tail_rows_) && (base_rows_ + tail_rows_) > 0;
	}
	// Doubling policy bounds total compaction work to O(N log N).
	return base_rows_ == 0 || tail_rows_ * 2 >= base_rows_ || dead_rows_ * 2 >= (base_rows_ + tail_rows_);
}

void VgiMemoArena::CompactLocked() {
	// Rebuild base_ from ONLY the live slots (dropping dead rows) in slot-iteration
	// order, rebasing each slot's row_start. Reads the OLD base_/tail_/base_rows_ via
	// MaterializeLocked, so publish the new state only after materializing.
	std::vector<int64_t> logical;
	logical.reserve(static_cast<size_t>(base_rows_ + tail_rows_ - dead_rows_));
	int64_t cursor = 0;
	for (auto &kv : slots_) {
		VgiMemoSlot &s = kv.second;
		for (int32_t o = 0; o < s.length; o++) {
			logical.push_back(s.row_start + static_cast<int64_t>(o));
		}
		s.row_start = cursor; // new offset in the rebuilt base
		cursor += s.length;
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
	// Slot-map + key-blob overhead (approximate but honest — the whole point of the
	// arena is that this is a bounded, measurable per-value cost, ~tens of bytes).
	for (auto &kv : slots_) {
		f += static_cast<int64_t>(kv.first.size()) + 64;
	}
	f += static_cast<int64_t>(validators_.size()) * 96;
	footprint_ = f;
	return f;
}

int64_t VgiMemoArena::FootprintBytes() {
	std::lock_guard<std::mutex> lg(mu_);
	return footprint_;
}

VgiMemoArena::Stats VgiMemoArena::GetStats() {
	std::lock_guard<std::mutex> lg(mu_);
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
	Entry e;
	e.arena = arena;
	e.bytes = 0;
	e.last_used = tick_;
	arenas_.emplace(static_fp, std::move(e));
	return arena;
}

std::shared_ptr<VgiMemoArena> VgiMemoArenaRegistry::Get(const std::string &static_fp) {
	std::lock_guard<std::mutex> lg(mu_);
	auto it = arenas_.find(static_fp);
	if (it == arenas_.end()) {
		return nullptr;
	}
	it->second.last_used = ++tick_;
	return it->second.arena;
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

void VgiMemoArenaRegistry::FlushAll() {
	std::lock_guard<std::mutex> lg(mu_);
	arenas_.clear();
	total_bytes_ = 0;
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
