// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_result_cache.hpp"

#include <arrow/api.h>

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <functional>
#include <set>

#include "duckdb/common/file_system.hpp"
#include "mbedtls_wrapper.hpp"
#include "vgi_arrow_ipc.hpp" // TranscodeIpcWithCodec / ResolveDiskCompressionCodec (disk compression)
#include "vgi_platform.hpp" // VGI_SUBPROCESS_TRANSPORT (thread gate — Emscripten is single-threaded)

namespace duckdb {
namespace vgi {

namespace {

// 64-bit hash_combine (splitmix-style mixer over std::hash results).
inline void HashCombine(uint64_t &seed, uint64_t value) {
	value += 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
	seed ^= value;
}
inline void HashStr(uint64_t &seed, const std::string &s) {
	HashCombine(seed, std::hash<std::string> {}(s));
}

int64_t ParseInt(const std::string &s, int64_t fallback) {
	if (s.empty()) {
		return fallback;
	}
	try {
		return static_cast<int64_t>(std::stoll(s));
	} catch (...) {
		return fallback;
	}
}

// ---- Disk-tier helpers (content-addressed store) ------------------------

FileSystem &LocalFs() {
	static unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	return *fs;
}

std::string HexEncode(const std::string &raw) {
	static const char *digits = "0123456789abcdef";
	std::string hex;
	hex.resize(raw.size() * 2);
	for (size_t i = 0; i < raw.size(); i++) {
		hex[i * 2] = digits[(static_cast<unsigned char>(raw[i]) >> 4) & 0xF];
		hex[i * 2 + 1] = digits[static_cast<unsigned char>(raw[i]) & 0xF];
	}
	return hex;
}

std::string Sha256Hex(const std::string &bytes) {
	return HexEncode(duckdb_mbedtls::MbedTlsWrapper::ComputeSha256Hash(bytes));
}

// Write all bytes to a FileHandle (handles short writes). Throws on a stuck write.
void WriteAll(FileHandle &h, const char *data, idx_t len) {
	idx_t off = 0;
	while (off < len) {
		int64_t w = h.Write(const_cast<char *>(data + off), len - off);
		if (w <= 0) {
			throw std::runtime_error("VGI result cache: short write to disk blob");
		}
		off += static_cast<idx_t>(w);
	}
}

// Per-identity subdirectory of the cache dir. Sharding by the (auth-scoped)
// identity_scope prevents cross-identity object dedup (an existence/timing
// side-channel on a shared cache dir) and makes per-identity flush an O(1)
// subtree removal instead of a body-grep that could nuke another tenant's refs.
std::string IdentityShard(const std::string &identity_scope) {
	return Sha256Hex(identity_scope);
}

// True iff `s` is exactly 64 lowercase hex chars (a SHA-256 digest). Guards the
// ref's `content` field before it is joined into an object path — a poisoned
// ref on a writable/shared dir could otherwise carry `../` traversal.
bool IsHex64(const std::string &s) {
	if (s.size() != 64) {
		return false;
	}
	for (char c : s) {
		if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
			return false;
		}
	}
	return true;
}

void MkdirP(const std::string &path) {
	if (!LocalFs().DirectoryExists(path)) {
		LocalFs().CreateDirectory(path);
	}
}

// Short unique suffix for the same-directory atomic-write temp. Kept COMPACT so
// the transient `<final>.tmp-<suffix>` path stays under Windows' 260-char MAX_PATH:
// the final path already carries a 64-char identity shard + a 64-char content name,
// so appending another 64-char SHA (the old suffix) tipped the tmp path over the
// limit and `OpenFile(FILE_CREATE)` failed — silently disabling the whole disk tier
// on Windows. base36(clock)+base36(seq) is ~16 chars and only needs per-directory
// uniqueness during the brief write window (a cross-process collision self-heals:
// the loser's CREATE throws, the memory tier still holds the entry, and the winner
// wrote identical content-addressed bytes). NOT prefixed "stream-", so a crash-
// orphaned atomic temp gets the reaper's SHORT grace, unlike an in-flight spill.
std::string ShortTempSuffix() {
	static const uint64_t base = static_cast<uint64_t>(
	    std::chrono::steady_clock::now().time_since_epoch().count());
	static std::atomic<uint64_t> counter{0};
	auto b36 = [](uint64_t v) {
		static const char *d = "0123456789abcdefghijklmnopqrstuvwxyz";
		std::string s;
		do {
			s.push_back(d[v % 36]);
			v /= 36;
		} while (v);
		return s;
	};
	return b36(base) + "-" + b36(counter.fetch_add(1, std::memory_order_relaxed));
}

// Atomic write: tmp file in the SAME directory + MoveFile (same-mount atomic).
void WriteFileAtomic(const std::string &dir, const std::string &final_path, const std::string &bytes,
                     const std::string &tmp_suffix) {
	auto &fs = LocalFs();
	std::string tmp = final_path + ".tmp-" + tmp_suffix;
	{
		auto h = fs.OpenFile(tmp, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
		idx_t off = 0;
		while (off < bytes.size()) {
			int64_t w = h->Write(const_cast<char *>(bytes.data() + off),
			                     static_cast<idx_t>(bytes.size() - off));
			if (w <= 0) {
				break;
			}
			off += static_cast<idx_t>(w);
		}
		h->Sync();
	}
	fs.MoveFile(tmp, final_path);
}

bool ReadFileAll(const std::string &path, std::string &out) {
	auto &fs = LocalFs();
	if (!fs.FileExists(path)) {
		return false;
	}
	try {
		auto h = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
		idx_t sz = static_cast<idx_t>(h->GetFileSize());
		out.resize(sz);
		idx_t off = 0;
		while (off < sz) {
			int64_t r = h->Read(&out[off], sz - off);
			if (r <= 0) {
				break;
			}
			off += static_cast<idx_t>(r);
		}
		return true;
	} catch (...) {
		return false;
	}
}

// Little-endian POD append/read for the blob framing.
void PutU64(std::string &b, uint64_t v) {
	for (int i = 0; i < 8; i++) {
		b.push_back(static_cast<char>((v >> (i * 8)) & 0xFF));
	}
}
uint64_t GetU64(const std::string &b, size_t &pos) {
	uint64_t v = 0;
	for (int i = 0; i < 8; i++) {
		v |= static_cast<uint64_t>(static_cast<unsigned char>(b[pos + i])) << (i * 8);
	}
	pos += 8;
	return v;
}

// Flat blob: [magic VRC1][u64 num_batches]{ per batch: [u64 has_index][u64 batch_index]
//   [u64 rows][u64 pv_len][pv bytes][u64 ipc_len][ipc bytes] }
// Blob layout: "VRC1" magic + a flat run of batch records to EOF (NO batch-count
// header). Dropping the count lets the streaming capture writer hash the blob
// INCREMENTALLY as batches arrive — it never knows the total upfront — while a
// re-hash of the finished file (integrity check D) still matches. Readers loop
// until they exhaust the bytes. Per batch:
//   [u64 has_index][u64 batch_index][u64 rows][u64 pv_len][pv][u64 ipc_len][ipc]
// `codec`/`level` (already resolved via ResolveDiskCompressionCodec) apply Arrow's
// built-in IPC compression to each batch's payload — the per-batch `ipc_len` framing
// becomes the COMPRESSED length, and the content SHA (taken over this blob) covers the
// compressed bytes. codec=="none" → TranscodeIpcWithCodec is a passthrough (unchanged
// format). The ref's logical `bytes=` is entry.total_bytes, set by the caller — not here.
std::string SerializeEntryBlob(const VgiResultCacheEntry &entry, const std::string &codec, uint64_t level) {
	std::string b;
	b.append("VRC1", 4);
	for (const auto &s : entry.streams) {
		for (const auto &cb : s.batches) {
			PutU64(b, cb.has_batch_index ? 1 : 0);
			PutU64(b, cb.batch_index);
			PutU64(b, static_cast<uint64_t>(cb.rows));
			PutU64(b, cb.partition_values_bytes.size());
			b.append(cb.partition_values_bytes);
			auto payload = TranscodeIpcWithCodec(cb.ipc, codec, level); // no-op when codec=="none"
			uint64_t ipc_len = payload ? static_cast<uint64_t>(payload->size()) : 0;
			PutU64(b, ipc_len);
			if (payload) {
				b.append(reinterpret_cast<const char *>(payload->data()), payload->size());
			}
		}
	}
	return b;
}

// Wall-clock expiry for a ref (steady_clock is per-process; disk needs wall time).
// ttl was clamped to MAX_TTL at parse, so the addition cannot overflow.
int64_t ComputeExpiresUnix(const VgiResultCacheEntry &entry) {
	if (entry.never_expires) {
		return INT64_MAX;
	}
	auto ttl =
	    std::chrono::duration_cast<std::chrono::seconds>(entry.expires_at - entry.stored_at).count();
	return static_cast<int64_t>(std::time(nullptr)) + ttl;
}

// Build + atomically write the key→content ref. Shared by the RAM-persist path
// (PersistToDisk) and the streaming-capture commit so the ref format stays in one place.
void WriteRef(const std::string &refs_dir, const std::string &fp, const std::string &content_sha,
              int64_t expires_unix, const VgiResultCacheEntry &meta, const std::string &codec) {
	std::string ref;
	ref += "keyfp=" + fp + "\n";
	ref += "content=" + content_sha + "\n";
	ref += "expires_unix=" + std::to_string(expires_unix) + "\n";
	ref += "scope=" + meta.scope + "\n";
	ref += "etag=" + meta.etag + "\n";
	ref += "last_modified=" + meta.last_modified + "\n";
	ref += "rows=" + std::to_string(meta.rows) + "\n";
	ref += "bytes=" + std::to_string(meta.total_bytes) + "\n"; // LOGICAL/uncompressed size
	ref += "catalog=" + meta.catalog_name + "\n";
	ref += "function=" + meta.key.function_name + "\n"; // for the vgi_result_cache_disk() diagnostic
	ref += "codec=" + codec + "\n";                     // diagnostic only; batches self-describe
	WriteFileAtomic(refs_dir, refs_dir + "/" + fp + ".ref", ref, ShortTempSuffix());
}

// Process-unique temp-blob suffix for a streaming capture (the content sha is
// unknown until finalize, so the temp can't be content-named yet). A per-process
// clock-seeded base distinguishes processes sharing a cache dir; the atomic counter
// distinguishes concurrent captures within one process. Portable (no getpid).
std::string UniqueTempSuffix() {
	static const uint64_t base = static_cast<uint64_t>(
	    std::chrono::steady_clock::now().time_since_epoch().count());
	static std::atomic<uint64_t> counter{0};
	uint64_t seq = counter.fetch_add(1, std::memory_order_relaxed);
	return "stream-" + std::to_string(static_cast<unsigned long long>(base)) + "-" + std::to_string(seq);
}

} // namespace

// ============================================================================
// VgiResultCacheKey
// ============================================================================

bool VgiResultCacheKey::operator==(const VgiResultCacheKey &o) const {
	return identity_scope == o.identity_scope && worker_path == o.worker_path &&
	       function_name == o.function_name && canonical_arguments == o.canonical_arguments &&
	       canonical_settings == o.canonical_settings && attach_options == o.attach_options &&
	       projection == o.projection && attached_data_version == o.attached_data_version &&
	       implementation_version == o.implementation_version && catalog_version == o.catalog_version &&
	       at_unit == o.at_unit && at_value == o.at_value && filter_bytes == o.filter_bytes &&
	       order_by_hint == o.order_by_hint && sample_hint == o.sample_hint &&
	       transaction_id == o.transaction_id && input_hash == o.input_hash;
}

uint64_t VgiResultCacheKey::Hash() const {
	uint64_t seed = 0xcbf29ce484222325ULL;
	HashStr(seed, identity_scope);
	HashStr(seed, worker_path);
	HashStr(seed, function_name);
	HashStr(seed, canonical_arguments);
	HashStr(seed, canonical_settings);
	HashStr(seed, attach_options);
	HashStr(seed, projection);
	HashStr(seed, attached_data_version);
	HashStr(seed, implementation_version);
	HashCombine(seed, static_cast<uint64_t>(catalog_version));
	HashStr(seed, at_unit);
	HashStr(seed, at_value);
	HashStr(seed, filter_bytes);
	HashStr(seed, order_by_hint);
	HashStr(seed, sample_hint);
	HashStr(seed, transaction_id);
	HashStr(seed, input_hash);
	return seed;
}

std::string VgiResultCacheKey::HexDigest() const {
	char buf[17];
	std::snprintf(buf, sizeof(buf), "%016llx", static_cast<unsigned long long>(Hash()));
	return std::string(buf);
}

std::string VgiResultCacheKey::Fingerprint() const {
	// Injective canonical serialization of EVERY key field: each field is
	// length-prefixed (decimal + ':') so no concatenation of two distinct field
	// sets can collide, then SHA-256'd. This is the on-disk identity — a 64-bit
	// HexDigest bucket collision cannot cross-serve because the loader re-checks
	// this full digest.
	std::string m;
	auto add = [&m](const std::string &s) {
		m += std::to_string(s.size());
		m += ':';
		m += s;
	};
	add(identity_scope);
	add(worker_path);
	add(function_name);
	add(canonical_arguments);
	add(canonical_settings);
	add(attach_options);
	add(projection);
	add(attached_data_version);
	add(implementation_version);
	add(std::to_string(catalog_version));
	add(at_unit);
	add(at_value);
	add(filter_bytes);
	add(order_by_hint);
	add(sample_hint);
	add(transaction_id);
	add(input_hash);
	return Sha256Hex(m); // "" if SHA-256 unavailable → caller skips disk tier
}

// ============================================================================
// ParseVgiCacheControl
// ============================================================================

VgiCacheControl ParseVgiCacheControl(const std::shared_ptr<const arrow::KeyValueMetadata> &metadata) {
	VgiCacheControl cc;
	if (!metadata) {
		return cc;
	}
	auto get = [&](const char *key, std::string &out) -> bool {
		int idx = metadata->FindKey(key);
		if (idx < 0) {
			return false;
		}
		out = metadata->value(idx);
		cc.present = true;
		return true;
	};

	std::string tmp;
	if (get(VGI_CACHE_TTL_KEY, tmp)) {
		// Clamp worker-advertised TTL to a sane maximum. A hostile/buggy worker
		// advertising ttl≈INT64_MAX would otherwise overflow `stored_at +
		// seconds(ttl)` (steady_clock) or `time(nullptr)+ttl` (wall clock) — UB.
		// MAX_TTL (10 years) is "effectively forever" for a cache; negatives clamp
		// to 0 (already-stale).
		int64_t ttl = ParseInt(tmp, 0);
		if (ttl < 0) {
			ttl = 0;
		} else if (ttl > VGI_CACHE_MAX_TTL_SECONDS) {
			ttl = VGI_CACHE_MAX_TTL_SECONDS;
		}
		cc.ttl_seconds = ttl;
	}
	get(VGI_CACHE_EXPIRES_KEY, cc.expires_rfc3339);
	if (get(VGI_CACHE_NO_STORE_KEY, tmp)) {
		cc.no_store = (tmp == "1");
	}
	std::string scope;
	if (get(VGI_CACHE_SCOPE_KEY, scope) && !scope.empty()) {
		cc.scope = scope;
	}
	get(VGI_CACHE_ETAG_KEY, cc.etag);
	get(VGI_CACHE_LAST_MODIFIED_KEY, cc.last_modified);
	if (get(VGI_CACHE_REVALIDATABLE_KEY, tmp)) {
		cc.revalidatable = (tmp == "1");
	}
	if (get(VGI_CACHE_STALE_WHILE_REVALIDATE_KEY, tmp)) {
		cc.stale_while_revalidate = ParseInt(tmp, 0);
	}
	if (get(VGI_CACHE_STALE_IF_ERROR_KEY, tmp)) {
		cc.stale_if_error = ParseInt(tmp, 0);
	}
	if (get(VGI_CACHE_NOT_MODIFIED_KEY, tmp)) {
		cc.not_modified = (tmp == "1");
	}
	return cc;
}

// ============================================================================
// VgiResultCache singleton
// ============================================================================

VgiResultCache &VgiResultCache::Instance() {
	// Intentionally leaked (same rationale as VgiWorkerPool::Instance): cached
	// entries hold Arrow Buffers whose destructors touch the Arrow memory pool;
	// leaking sidesteps the static-destruction-order question at process exit.
	static VgiResultCache *instance = new VgiResultCache();
	return *instance;
}

VgiResultCache::VgiResultCache() {
#if VGI_SUBPROCESS_TRANSPORT
	// Background TTL/disk reaper. Gated exactly like VgiWorkerPool's cleanup
	// thread: Emscripten is single-threaded (no pthreads) and HTTP-only, so no
	// thread is started there. Under WASM, staleness is handled lazily instead
	// — Lookup() drops stale non-revalidatable entries on access and Insert()
	// bounds bytes via LRU, so the cache stays correct and bounded without a
	// reaper (only proactive eviction of never-re-accessed entries is lost).
	cleanup_thread_ = std::thread(&VgiResultCache::CleanupThread, this);
#endif
}

VgiResultCache::~VgiResultCache() {
	// NOTE: Instance() intentionally leaks this singleton (Arrow static-
	// destruction-order hazard), so this destructor never actually runs in a
	// normal process — the reaper thread is a daemon reaped at process exit.
	// The shutdown handshake below only matters if the singleton is ever
	// explicitly deleted (e.g. a future test harness).
	shutdown_.store(true);
	cleanup_cv_.notify_all();
#if VGI_SUBPROCESS_TRANSPORT
	if (cleanup_thread_.joinable()) {
		cleanup_thread_.join();
	}
#endif
	std::lock_guard<std::mutex> lock(mutex_);
	index_.clear();
	lru_.clear();
	total_bytes_ = 0;
}

namespace {
// 64-bit signature of a Settings for the ConfigureIfChanged fast path. A hash
// collision only costs a missed reconfigure until the next differing SET —
// benign (never a correctness issue), and astronomically unlikely.
uint64_t SettingsSignature(const VgiResultCache::Settings &s) {
	uint64_t seed = 0xcbf29ce484222325ULL;
	HashCombine(seed, s.max_bytes);
	HashCombine(seed, s.max_entry_bytes);
	HashCombine(seed, s.max_entries);
	HashCombine(seed, s.max_inflight_bytes);
	HashCombine(seed, s.disk_max_bytes);
	HashCombine(seed, s.disk_reap_interval_seconds);
	HashCombine(seed, s.disk_compression_level);
	HashCombine(seed, s.exchange_disk_min_bytes);
	HashStr(seed, s.disk_dir);
	HashStr(seed, s.disk_compression);
	return seed;
}
} // namespace

void VgiResultCache::Configure(const Settings &settings) {
	std::lock_guard<std::mutex> lock(mutex_);
	settings_ = settings;
	disk_dir_ = settings.disk_dir;
	disk_max_bytes_ = settings.disk_max_bytes;
	disk_compression_ = settings.disk_compression;
	disk_compression_level_ = settings.disk_compression_level;
	exchange_disk_min_bytes_.store(static_cast<int64_t>(settings.exchange_disk_min_bytes),
	                               std::memory_order_relaxed);
	// Shrinking the cap mid-flight is honored on the next Insert / reap tick;
	// evict now so a lowered cap takes effect immediately.
	EvictToFitLocked(0);
}

void VgiResultCache::ConfigureIfChanged(const Settings &settings) {
	// [S1] Common case: settings unchanged → no lock, no evict. This is the hot
	// path (every scan's InitGlobal); making it lock-free is what lets cache
	// access scale with query concurrency.
	uint64_t sig = SettingsSignature(settings);
	if (config_signature_.load(std::memory_order_relaxed) == sig) {
		return;
	}
	Configure(settings);
	config_signature_.store(sig, std::memory_order_relaxed);
}

bool VgiResultCache::TryReserveInflightCapture(int64_t bytes) {
	// [S6] Bound total transient capture RAM across all concurrent captures.
	uint64_t budget;
	{
		std::lock_guard<std::mutex> lock(mutex_);
		budget = settings_.max_inflight_bytes;
	}
	if (budget == 0) {
		return true; // 0 = unbounded (opt-out)
	}
	int64_t prev = inflight_capture_bytes_.fetch_add(bytes, std::memory_order_relaxed);
	if (prev + bytes > static_cast<int64_t>(budget)) {
		inflight_capture_bytes_.fetch_sub(bytes, std::memory_order_relaxed);
		return false;
	}
	return true;
}

void VgiResultCache::ReleaseInflightCapture(int64_t bytes) {
	if (bytes > 0) {
		inflight_capture_bytes_.fetch_sub(bytes, std::memory_order_relaxed);
	}
}

int64_t VgiResultCache::ExchangeDiskMinBytes() const {
	return exchange_disk_min_bytes_.load(std::memory_order_relaxed);
}

std::shared_ptr<const VgiResultCacheEntry>
VgiResultCache::Lookup(const VgiResultCacheKey &key, std::chrono::steady_clock::time_point now) {
	std::string disk_dir; // snapshot of disk config, taken under the lock
	uint64_t max_entry_bytes = 0;
	{
		std::lock_guard<std::mutex> lock(mutex_);
		auto it = index_.find(key);
		if (it != index_.end()) {
			auto entry = *(it->second);
			if (entry->IsStale(now)) {
				// Drop the stale entry; fall through to disk / miss.
				total_bytes_ -= entry->total_bytes;
				lru_.erase(it->second);
				index_.erase(it);
				evictions_ttl_.fetch_add(1, std::memory_order_relaxed);
			} else {
				lru_.splice(lru_.begin(), lru_, it->second);
				it->second = lru_.begin();
				entry->hits++;
				hits_.fetch_add(1, std::memory_order_relaxed);
				return entry;
			}
		}
		if (DiskEnabledLocked()) {
			disk_dir = disk_dir_;
			max_entry_bytes = settings_.max_entry_bytes;
		}
	}
	// In-memory miss — try the disk tier (cross-process / cross-restart). Disk
	// I/O runs WITHOUT the cache lock, over the config snapshot taken above.
	if (!disk_dir.empty()) {
		// [S8] Peek the entry size (cheap ref read) and route: SMALL entries
		// materialize + adopt into the memory LRU (fast repeat hits); LARGE
		// entries (> max_entry_bytes) are served by a disk-backed STREAMING entry
		// that never materializes the payload — RAM stays flat for results far
		// larger than the memory cap. Streamed entries are NOT adopted (they'd
		// defeat the point) — they are re-loaded (TOC only) per hit.
		int64_t ref_bytes = PeekDiskRefBytes(key, disk_dir);
		if (ref_bytes >= 0 && ref_bytes > static_cast<int64_t>(max_entry_bytes)) {
			auto streaming = LoadFromDiskStreaming(key, disk_dir);
			if (streaming) {
				hits_.fetch_add(1, std::memory_order_relaxed);
				return streaming;
			}
		} else if (ref_bytes >= 0) {
			auto disk_entry = LoadFromDisk(key, disk_dir);
			if (disk_entry) {
				std::lock_guard<std::mutex> lock(mutex_);
				// Adopt into memory (no re-persist). Evict-to-fit first.
				auto existing = index_.find(key);
				if (existing == index_.end() &&
				    disk_entry->total_bytes <= static_cast<int64_t>(max_entry_bytes)) {
					EvictToFitLocked(disk_entry->total_bytes);
					total_bytes_ += disk_entry->total_bytes;
					disk_entry->hits++;
					lru_.push_front(disk_entry);
					index_[key] = lru_.begin();
				}
				hits_.fetch_add(1, std::memory_order_relaxed);
				return disk_entry;
			}
		}
	}
	misses_.fetch_add(1, std::memory_order_relaxed);
	return nullptr;
}

std::shared_ptr<const VgiResultCacheEntry>
VgiResultCache::LookupForRevalidation(const VgiResultCacheKey &key,
                                      std::chrono::steady_clock::time_point now) {
	// Only in-memory STALE + revalidatable entries qualify (the disk tier reaps
	// stale refs and freshly-loaded entries carry a non-stale expiry, so a stale
	// revalidatable candidate only ever lives in the in-memory index).
	std::lock_guard<std::mutex> lock(mutex_);
	auto it = index_.find(key);
	if (it != index_.end()) {
		auto entry = *(it->second);
		// Fresh entries are served by Lookup; we do NOT drop the stale one here.
		if (entry->IsStale(now) && entry->revalidatable) {
			return entry;
		}
	}
	return nullptr;
}

bool VgiResultCache::Insert(std::shared_ptr<VgiResultCacheEntry> entry, bool allow_disk) {
	if (!entry) {
		return false;
	}
	std::string disk_dir; // snapshot of disk config, taken under the lock
	uint64_t disk_max = 0;
	std::string disk_codec;
	uint64_t disk_level = 1;
	{
		std::lock_guard<std::mutex> lock(mutex_);
		if (entry->total_bytes > static_cast<int64_t>(settings_.max_entry_bytes)) {
			return false; // too large for the memory tier
		}
		// Replace any existing entry for this key.
		auto existing = index_.find(entry->key);
		if (existing != index_.end()) {
			total_bytes_ -= (*existing->second)->total_bytes;
			lru_.erase(existing->second);
			index_.erase(existing);
		}
		EvictToFitLocked(entry->total_bytes);
		total_bytes_ += entry->total_bytes;
		lru_.push_front(entry);
		index_[entry->key] = lru_.begin();
		inserts_.fetch_add(1, std::memory_order_relaxed);
		if (allow_disk && DiskEnabledLocked()) {
			disk_dir = disk_dir_;
			disk_max = disk_max_bytes_;
			disk_codec = disk_compression_;
			disk_level = disk_compression_level_;
		}
	}
	// Persist to the disk tier OUTSIDE the cache lock (file I/O), over the
	// config snapshot taken above.
	if (!disk_dir.empty()) {
		PersistToDisk(*entry, disk_dir, disk_max, disk_codec, disk_level);
	}
	return true;
}

void VgiResultCache::EvictToFitLocked(int64_t incoming_bytes) {
	// [S5] Evict oldest while over the BYTE cap OR the ENTRY-COUNT cap. The count
	// cap bounds unbounded small-entry accumulation (a workload with high key
	// cardinality could otherwise pin ~700k tiny entries under the byte cap),
	// which in turn keeps the reaper/Snapshot O(N) walks small.
	const bool count_capped = settings_.max_entries > 0;
	while (!lru_.empty() &&
	       (total_bytes_ + incoming_bytes > static_cast<int64_t>(settings_.max_bytes) ||
	        (count_capped && lru_.size() >= settings_.max_entries))) {
		auto &victim = lru_.back();
		total_bytes_ -= victim->total_bytes;
		index_.erase(victim->key);
		lru_.pop_back();
		evictions_lru_.fetch_add(1, std::memory_order_relaxed);
	}
}

size_t VgiResultCache::FlushAll() {
	size_t n;
	bool disk_enabled;
	std::string disk_dir;
	{
		std::lock_guard<std::mutex> lock(mutex_);
		n = lru_.size();
		index_.clear();
		lru_.clear();
		total_bytes_ = 0;
		disk_enabled = DiskEnabledLocked();
		disk_dir = disk_dir_;
	}
	// Clear the disk tier too (best-effort; outside the lock).
	if (disk_enabled) {
		auto &fs = LocalFs();
		if (fs.DirectoryExists(disk_dir)) {
			fs.RemoveDirectory(disk_dir);
		}
	}
	return n;
}

size_t VgiResultCache::FlushCatalog(const std::string &catalog_name, const std::string &identity_scope) {
	size_t n = 0;
	std::string disk_dir; // snapshot of disk config, taken under the lock
	{
		std::lock_guard<std::mutex> lock(mutex_);
		// Memory tier: match by the catalog label (alias is unique per process).
		for (auto it = lru_.begin(); it != lru_.end();) {
			if ((*it)->catalog_name == catalog_name) {
				total_bytes_ -= (*it)->total_bytes;
				index_.erase((*it)->key);
				it = lru_.erase(it);
				++n;
			} else {
				++it;
			}
		}
		if (DiskEnabledLocked()) {
			disk_dir = disk_dir_;
		}
	}
	// Disk tier: remove this identity's shard (outside the lock — file I/O). Only
	// when the identity is resolvable — an "" scope means nothing was cached.
	if (!disk_dir.empty() && !identity_scope.empty()) {
		FlushCatalogDisk(identity_scope, disk_dir);
	}
	return n;
}

VgiResultCache::ReapStats VgiResultCache::ReapNow(int64_t advance_seconds) {
	ReapStats stats;
	std::string disk_dir; // snapshot of disk config, taken under the lock
	uint64_t disk_max = 0;
	{
		std::lock_guard<std::mutex> lock(mutex_);
		// Simulate `advance_seconds` of elapsed time for the memory tier
		// (steady_clock): any non-revalidatable entry stale within the window is
		// dropped. This is exactly the background thread's per-tick reap, but
		// synchronous and clock-injectable so tests are deterministic.
		stats.memory_reaped =
		    ReapStaleLocked(std::chrono::steady_clock::now() + std::chrono::seconds(advance_seconds));
		if (DiskEnabledLocked()) {
			disk_dir = disk_dir_;
			disk_max = disk_max_bytes_;
		}
	}
	// Disk tier (wall-clock): inject `now + advance` as now_unix so expired refs
	// and past-grace orphans are reaped without waiting real time.
	if (!disk_dir.empty()) {
		stats.disk_refs_removed =
		    ReapDisk(disk_dir, disk_max, static_cast<int64_t>(std::time(nullptr)) + advance_seconds);
	}
	return stats;
}

std::vector<VgiResultCache::EntryInfo> VgiResultCache::Snapshot() const {
	auto now = std::chrono::steady_clock::now();
	// [S3] Copy the entry shared_ptrs under a SHORT lock (O(N) pointer bumps),
	// then build the EntryInfo rows (string/timestamp work) OUTSIDE the lock so a
	// large `vgi_result_cache()` never freezes the cache for concurrent queries.
	std::vector<std::shared_ptr<const VgiResultCacheEntry>> entries;
	{
		std::lock_guard<std::mutex> lock(mutex_);
		entries.reserve(lru_.size());
		for (const auto &e : lru_) {
			entries.push_back(e);
		}
	}
	std::vector<EntryInfo> out;
	out.reserve(entries.size());
	for (const auto &e : entries) {
		EntryInfo info;
		info.catalog_name = e->catalog_name;
		info.function_name = e->key.function_name;
		info.key_hex = e->key.HexDigest();
		info.scope = e->scope;
		info.attached_data_version = e->key.attached_data_version;
		info.implementation_version = e->key.implementation_version;
		info.catalog_version = e->key.catalog_version;
		info.at_unit = e->key.at_unit;
		info.at_value = e->key.at_value;
		int64_t nb = 0;
		for (const auto &s : e->streams) {
			nb += static_cast<int64_t>(s.batches.size());
		}
		info.num_batches = nb;
		info.num_substreams = static_cast<int64_t>(e->streams.size());
		info.num_rows = e->rows;
		info.total_bytes = e->total_bytes;
		info.age_seconds =
		    std::chrono::duration_cast<std::chrono::seconds>(now - e->stored_at).count();
		if (e->never_expires) {
			info.ttl_seconds = -1;
		} else {
			info.ttl_seconds =
			    std::chrono::duration_cast<std::chrono::seconds>(e->expires_at - e->stored_at).count();
		}
		info.stale = e->IsStale(now);
		info.tier = "memory";
		info.etag = e->etag;
		info.last_modified = e->last_modified;
		info.revalidatable = e->revalidatable;
		info.hits = e->hits;
		out.push_back(std::move(info));
	}
	return out;
}

std::vector<VgiResultCache::EntryInfo> VgiResultCache::SnapshotDisk() const {
	std::string dir;
	{
		std::lock_guard<std::mutex> lock(mutex_);
		dir = disk_dir_;
	}
	std::vector<EntryInfo> out;
	if (dir.empty()) {
		return out;
	}
	const int64_t now_unix = static_cast<int64_t>(std::time(nullptr));
	try {
		auto &fs = LocalFs();
		if (!fs.DirectoryExists(dir)) {
			return out;
		}
		std::vector<std::string> shards;
		fs.ListFiles(dir, [&](const std::string &name, bool is_dir) {
			if (is_dir) {
				shards.push_back(dir + "/" + name);
			}
		});
		for (const auto &shard : shards) {
			const std::string refs = shard + "/refs";
			if (!fs.DirectoryExists(refs)) {
				continue;
			}
			fs.ListFiles(refs, [&](const std::string &name, bool) {
				std::string body;
				if (!ReadFileAll(refs + "/" + name, body)) {
					return;
				}
				EntryInfo info;
				info.tier = "disk";
				// ref filename is "<fingerprint>.ref"; expose the fingerprint prefix.
				info.key_hex = name.size() > 4 ? name.substr(0, name.size() - 4) : name;
				int64_t expires_unix = INT64_MAX;
				size_t start = 0;
				while (start < body.size()) {
					size_t nl = body.find('\n', start);
					std::string line =
					    body.substr(start, nl == std::string::npos ? std::string::npos : nl - start);
					start = (nl == std::string::npos) ? body.size() : nl + 1;
					size_t eq = line.find('=');
					if (eq == std::string::npos) {
						continue;
					}
					std::string k = line.substr(0, eq), v = line.substr(eq + 1);
					if (k == "function") info.function_name = v;
					else if (k == "catalog") info.catalog_name = v;
					else if (k == "scope") info.scope = v;
					else if (k == "etag") info.etag = v;
					else if (k == "last_modified") info.last_modified = v;
					else if (k == "rows") info.num_rows = ParseInt(v, 0);
					else if (k == "bytes") info.total_bytes = ParseInt(v, 0);
					else if (k == "codec") info.codec = v;
					else if (k == "expires_unix") expires_unix = ParseInt(v, INT64_MAX);
				}
				if (expires_unix == INT64_MAX) {
					info.ttl_seconds = -1; // never expires
				} else {
					info.ttl_seconds = expires_unix - now_unix;
					info.stale = now_unix >= expires_unix;
				}
				out.push_back(std::move(info));
			});
		}
	} catch (...) {
	}
	return out;
}

VgiResultCache::Counters VgiResultCache::GetCounters() const {
	Counters c;
	c.hits = hits_.load(std::memory_order_relaxed);
	c.misses = misses_.load(std::memory_order_relaxed);
	c.inserts = inserts_.load(std::memory_order_relaxed);
	c.evictions_lru = evictions_lru_.load(std::memory_order_relaxed);
	c.evictions_ttl = evictions_ttl_.load(std::memory_order_relaxed);
	c.capture_aborts = capture_aborts_.load(std::memory_order_relaxed);
	c.exchange_hits = exchange_hits_.load(std::memory_order_relaxed);
	c.exchange_misses = exchange_misses_.load(std::memory_order_relaxed);
	c.exchange_stores = exchange_stores_.load(std::memory_order_relaxed);
	c.exchange_revalidations = exchange_revalidations_.load(std::memory_order_relaxed);
	c.exchange_bytes_served = exchange_bytes_served_.load(std::memory_order_relaxed);
	return c;
}

void VgiResultCache::RecordExchangeHit(int64_t bytes_served) {
	exchange_hits_.fetch_add(1, std::memory_order_relaxed);
	if (bytes_served > 0) {
		exchange_bytes_served_.fetch_add(static_cast<uint64_t>(bytes_served), std::memory_order_relaxed);
	}
}

void VgiResultCache::RecordExchangeMiss() {
	exchange_misses_.fetch_add(1, std::memory_order_relaxed);
}

void VgiResultCache::RecordExchangeStore() {
	exchange_stores_.fetch_add(1, std::memory_order_relaxed);
}

void VgiResultCache::RecordExchangeRevalidation(int64_t bytes_served) {
	exchange_revalidations_.fetch_add(1, std::memory_order_relaxed);
	if (bytes_served > 0) {
		exchange_bytes_served_.fetch_add(static_cast<uint64_t>(bytes_served), std::memory_order_relaxed);
	}
}

void VgiResultCache::CleanupThread() {
	int64_t seconds_since_disk_reap = 0;
	while (!shutdown_.load()) {
		{
			std::unique_lock<std::mutex> lock(cleanup_mutex_);
			cleanup_cv_.wait_for(lock, std::chrono::seconds(1), [this] { return shutdown_.load(); });
		}
		if (shutdown_.load()) {
			break;
		}
		auto now = std::chrono::steady_clock::now();
		// Memory reap runs every tick (~1 s) — it's in-RAM (IsStale checks), cheap,
		// and bounded by the entry-count cap [S5].
		std::string disk_dir; // snapshot of disk config, taken under the lock
		uint64_t disk_max = 0;
		int64_t disk_interval = 60;
		{
			std::lock_guard<std::mutex> lock(mutex_);
			ReapStaleLocked(now);
			disk_interval = static_cast<int64_t>(settings_.disk_reap_interval_seconds);
			if (DiskEnabledLocked()) {
				disk_dir = disk_dir_;
				disk_max = disk_max_bytes_;
			}
		}
		// [S7] Disk reap is O(total refs) I/O per pass (it reads every ref +
		// stats every object), so run it on a MUCH coarser cadence than the memory
		// reap — every `disk_reap_interval_seconds` (default 60) instead of every
		// 1 s. Expiry is wall-clock, so a slightly later reap is harmless.
		seconds_since_disk_reap += 1;
		if (!disk_dir.empty() && seconds_since_disk_reap >= std::max<int64_t>(1, disk_interval)) {
			seconds_since_disk_reap = 0;
			ReapDisk(disk_dir, disk_max, static_cast<int64_t>(std::time(nullptr)));
		}
	}
}

// ============================================================================
// Disk tier (M2): content-addressed store, cross-process / cross-restart
// ============================================================================
// Layout (per-identity sharded — see IdentityShard):
//   <dir>/<shard>/objects/<content_sha256>.vrc  immutable entry blob
//   <dir>/<shard>/refs/<key_fingerprint>.ref    key -> content + expiry + keyfp
// The ref filename is the SHA-256 Fingerprint() of the FULL key, and the ref
// carries `keyfp=` which the loader re-verifies — so a 64-bit HexDigest bucket
// collision can never cross-serve. Correctness rests on atomic rename + content
// immutability (no locks).

// ---- VgiCaptureDiskWriter (streaming capture — RAM-flat) ----------------

struct VgiCaptureDiskWriter::Impl {
	std::mutex mu;
	std::unique_ptr<FileHandle> handle;
	std::string tmp_path;
	std::string objects_dir;
	std::string refs_dir;
	std::string fp; // key fingerprint = ref filename
	uint64_t disk_max = 0;
	std::string codec = "none"; // resolved disk-compression codec ("none"/"zstd"/"lz4")
	uint64_t level = 1;         // zstd level (ignored for lz4/none)
	duckdb_mbedtls::MbedTlsWrapper::SHA256State sha; // incremental hash of the blob
	int64_t bytes = 0;          // running ON-DISK (compressed) blob size (incl. "VRC1"); vs disk_max
	int64_t logical_bytes = 0;  // running UNCOMPRESSED payload size → the ref's logical bytes=
	int64_t rows = 0;
	int64_t batch_count = 0;
	std::atomic<int64_t> producers{0};
	bool failed = false;
	bool finalized = false;
};

VgiCaptureDiskWriter::VgiCaptureDiskWriter() : impl_(make_uniq<Impl>()) {
}
VgiCaptureDiskWriter::~VgiCaptureDiskWriter() = default;

void VgiCaptureDiskWriter::RegisterProducer() {
	impl_->producers.fetch_add(1, std::memory_order_relaxed);
}
int64_t VgiCaptureDiskWriter::Rows() const {
	std::lock_guard<std::mutex> lk(impl_->mu);
	return impl_->rows;
}
int64_t VgiCaptureDiskWriter::Bytes() const {
	std::lock_guard<std::mutex> lk(impl_->mu);
	return impl_->bytes;
}
int64_t VgiCaptureDiskWriter::Producers() const {
	return impl_->producers.load(std::memory_order_relaxed);
}
const std::string &VgiCaptureDiskWriter::CodecName() const {
	return impl_->codec;
}
uint64_t VgiCaptureDiskWriter::Level() const {
	return impl_->level;
}

bool VgiCaptureDiskWriter::AppendBatch(bool has_batch_index, uint64_t batch_index, int64_t rows,
                                       const std::string &pv_bytes, const uint8_t *ipc_data,
                                       int64_t ipc_len, int64_t logical_len) {
	std::lock_guard<std::mutex> lk(impl_->mu);
	if (impl_->failed || impl_->finalized || !impl_->handle) {
		return false;
	}
	// `ipc_data`/`ipc_len` are the ON-DISK (already codec-compressed by the caller)
	// bytes; `logical_len` is the uncompressed size, accounted separately so the ref
	// bytes= stays logical. Per-batch record framing (matches SerializeEntryBlob).
	std::string rec;
	PutU64(rec, has_batch_index ? 1 : 0);
	PutU64(rec, batch_index);
	PutU64(rec, static_cast<uint64_t>(rows));
	PutU64(rec, pv_bytes.size());
	rec.append(pv_bytes);
	PutU64(rec, static_cast<uint64_t>(ipc_len));
	const int64_t add = static_cast<int64_t>(rec.size()) + ipc_len;
	if (impl_->disk_max != 0 && impl_->bytes + add > static_cast<int64_t>(impl_->disk_max)) {
		impl_->failed = true;
		return false; // over the per-entry disk budget → caller aborts, keeps streaming uncached
	}
	try {
		WriteAll(*impl_->handle, rec.data(), rec.size());
		impl_->sha.AddString(rec);
		if (ipc_len > 0) {
			WriteAll(*impl_->handle, reinterpret_cast<const char *>(ipc_data),
			         static_cast<idx_t>(ipc_len));
			// AddSalt is just mbedtls_sha256_update — hash the IPC bytes with no copy.
			impl_->sha.AddSalt(const_cast<unsigned char *>(ipc_data), static_cast<size_t>(ipc_len));
		}
	} catch (...) {
		impl_->failed = true;
		return false;
	}
	impl_->bytes += add;                 // on-disk (compressed) — budget counter
	impl_->logical_bytes += logical_len; // uncompressed — the ref's logical bytes=
	impl_->rows += rows;
	impl_->batch_count++;
	return true;
}

std::shared_ptr<VgiCaptureDiskWriter> VgiResultCache::BeginStreamingCapture(const VgiResultCacheKey &key,
                                                                            const std::string &dir,
                                                                            uint64_t disk_max,
                                                                            const std::string &codec,
                                                                            uint64_t level) {
	if (dir.empty() || disk_max == 0) {
		return nullptr;
	}
	try {
		std::string fp = key.Fingerprint();
		if (fp.empty()) {
			return nullptr; // SHA-256 unavailable → no disk tier
		}
		const std::string base = dir + "/" + IdentityShard(key.identity_scope);
		const std::string objects = base + "/objects";
		const std::string refs = base + "/refs";
		MkdirP(dir);
		MkdirP(base);
		MkdirP(objects);
		MkdirP(refs);
		std::string tmp = objects + "/.tmp-" + UniqueTempSuffix();
		auto writer = std::shared_ptr<VgiCaptureDiskWriter>(new VgiCaptureDiskWriter());
		auto &impl = *writer->impl_;
		impl.handle =
		    LocalFs().OpenFile(tmp, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
		impl.tmp_path = tmp;
		impl.objects_dir = objects;
		impl.refs_dir = refs;
		impl.fp = fp;
		impl.disk_max = disk_max;
		// Resolve the codec once (availability-checked; "none" fallback recorded) so
		// every AppendBatch compresses with it and the ref's codec= matches.
		impl.codec = ResolveDiskCompressionCodec(codec);
		impl.level = level;
		// The magic is the first hashed bytes; the content sha covers "VRC1" + batches.
		const std::string magic("VRC1", 4);
		WriteAll(*impl.handle, magic.data(), magic.size());
		impl.sha.AddString(magic);
		impl.bytes = 4;         // on-disk includes the 4-byte magic
		impl.logical_bytes = 0; // logical = uncompressed batch payloads only (no magic)
		return writer;
	} catch (...) {
		return nullptr;
	}
}

bool VgiResultCache::CommitStreamingCapture(VgiCaptureDiskWriter &writer,
                                            const VgiResultCacheEntry &meta) {
	auto &impl = *writer.impl_;
	std::lock_guard<std::mutex> lk(impl.mu);
	if (impl.failed || impl.finalized || !impl.handle) {
		return false;
	}
	try {
		impl.handle->Sync();
		impl.handle.reset(); // close before rename
		std::string content_sha = HexEncode(impl.sha.Finalize());
		std::string obj_path = impl.objects_dir + "/" + content_sha + ".vrc";
		auto &fs = LocalFs();
		if (fs.FileExists(obj_path)) {
			fs.RemoveFile(impl.tmp_path); // identical content already stored (dedup)
		} else {
			fs.MoveFile(impl.tmp_path, obj_path); // atomic publish
		}
		// The ref carries the FINISHED blob's rows + LOGICAL bytes (uncompressed, for
		// the materialize-vs-stream threshold); meta supplies scope/etag/ttl.
		VgiResultCacheEntry m = meta;
		m.rows = impl.rows;
		m.total_bytes = impl.logical_bytes;
		WriteRef(impl.refs_dir, impl.fp, content_sha, ComputeExpiresUnix(meta), m, impl.codec);
		impl.finalized = true;
		inserts_.fetch_add(1, std::memory_order_relaxed);
		return true;
	} catch (...) {
		return false; // temp orphaned (no ref written → invisible); reaper sweeps it
	}
}

void VgiResultCache::AbortStreamingCapture(VgiCaptureDiskWriter &writer) {
	auto &impl = *writer.impl_;
	std::lock_guard<std::mutex> lk(impl.mu);
	if (impl.finalized) {
		return;
	}
	impl.failed = true;
	try {
		impl.handle.reset();
		if (!impl.tmp_path.empty() && LocalFs().FileExists(impl.tmp_path)) {
			LocalFs().RemoveFile(impl.tmp_path);
		}
	} catch (...) {
	}
}

void VgiResultCache::PersistToDisk(const VgiResultCacheEntry &entry, const std::string &dir,
                                   uint64_t max_bytes, const std::string &codec, uint64_t level) {
	if (dir.empty() || max_bytes == 0) {
		return;
	}
	try {
		std::string fp = entry.key.Fingerprint();
		if (fp.empty()) {
			return; // SHA-256 unavailable → skip the disk tier
		}
		const std::string base = dir + "/" + IdentityShard(entry.key.identity_scope);
		const std::string objects = base + "/objects";
		const std::string refs = base + "/refs";
		MkdirP(dir);
		MkdirP(base);
		MkdirP(objects);
		MkdirP(refs);

		// Resolve the codec once (availability-checked; falls back to "none") so the
		// blob and the ref's codec= agree, and a fallback is recorded.
		std::string eff_codec = ResolveDiskCompressionCodec(codec);
		std::string blob = SerializeEntryBlob(entry, eff_codec, level);
		if (blob.size() > max_bytes) {
			return; // too large for the disk tier (compressed on-disk size)
		}
		std::string content_sha = Sha256Hex(blob);
		std::string obj_path = objects + "/" + content_sha + ".vrc";
		if (!LocalFs().FileExists(obj_path)) {
			WriteFileAtomic(objects, obj_path, blob, ShortTempSuffix());
		}
		// Ref bytes= stays LOGICAL (entry.total_bytes = uncompressed sum from capture).
		WriteRef(refs, fp, content_sha, ComputeExpiresUnix(entry), entry, eff_codec);
	} catch (...) {
		// Best-effort: the memory tier still holds the entry.
	}
}

std::shared_ptr<VgiResultCacheEntry> VgiResultCache::LoadFromDisk(const VgiResultCacheKey &key,
                                                                 const std::string &dir) {
	if (dir.empty()) {
		return nullptr;
	}
	try {
		std::string fp = key.Fingerprint();
		if (fp.empty()) {
			return nullptr; // SHA-256 unavailable
		}
		const std::string base = dir + "/" + IdentityShard(key.identity_scope);
		std::string ref_path = base + "/refs/" + fp + ".ref";
		std::string ref;
		if (!ReadFileAll(ref_path, ref)) {
			return nullptr;
		}
		// Parse key=value lines.
		std::string keyfp, content, scope = VGI_CACHE_SCOPE_CATALOG, etag, last_modified, catalog;
		int64_t expires_unix = 0, rows = 0;
		size_t start = 0;
		while (start < ref.size()) {
			size_t nl = ref.find('\n', start);
			std::string line = ref.substr(start, nl == std::string::npos ? std::string::npos : nl - start);
			start = (nl == std::string::npos) ? ref.size() : nl + 1;
			size_t eq = line.find('=');
			if (eq == std::string::npos) {
				continue;
			}
			std::string k = line.substr(0, eq), v = line.substr(eq + 1);
			if (k == "keyfp") keyfp = v;
			else if (k == "content") content = v;
			else if (k == "expires_unix") expires_unix = ParseInt(v, 0);
			else if (k == "scope") scope = v;
			else if (k == "etag") etag = v;
			else if (k == "last_modified") last_modified = v;
			else if (k == "rows") rows = ParseInt(v, 0);
			else if (k == "catalog") catalog = v;
		}
		// (B) Re-verify the full-key fingerprint: the ref filename is a 256-bit
		// digest, but re-checking guards against any tampering / partial writes
		// and makes a filename collision a clean miss, never a cross-serve.
		if (keyfp != fp) {
			return nullptr;
		}
		// (C) The `content` field becomes an object-path component — it MUST be a
		// 64-hex SHA-256, never attacker-controlled traversal.
		if (!IsHex64(content)) {
			return nullptr;
		}
		int64_t now_unix = static_cast<int64_t>(std::time(nullptr));
		if (expires_unix != INT64_MAX && now_unix >= expires_unix) {
			return nullptr; // stale on disk
		}
		std::string blob;
		if (!ReadFileAll(base + "/objects/" + content + ".vrc", blob)) {
			return nullptr; // orphaned ref (object reaped) → clean miss
		}
		// (D) Integrity: the object is content-addressed — re-hash and confirm it
		// matches the ref's `content` digest. A tampered/corrupt blob on a shared
		// dir becomes a clean miss instead of a poisoned serve or a decode crash.
		if (Sha256Hex(blob) != content) {
			return nullptr;
		}
		// Deserialize the flat blob into a single-substream entry. The blob is
		// "VRC1" + batch records to EOF (no count header) — loop until the bytes run out.
		if (blob.size() < 4 || blob.compare(0, 4, "VRC1") != 0) {
			return nullptr;
		}
		size_t pos = 4;
		auto entry = std::make_shared<VgiResultCacheEntry>();
		entry->key = key;
		entry->scope = scope;
		entry->etag = etag;
		entry->last_modified = last_modified;
		entry->catalog_name = catalog;
		entry->rows = rows;
		CachedStream stream;
		int64_t total_bytes = 0;
		while (pos < blob.size()) {
			VgiCachedBatch cb;
			cb.has_batch_index = GetU64(blob, pos) != 0;
			cb.batch_index = GetU64(blob, pos);
			cb.rows = static_cast<int64_t>(GetU64(blob, pos));
			uint64_t pv_len = GetU64(blob, pos);
			cb.partition_values_bytes = blob.substr(pos, pv_len);
			pos += pv_len;
			uint64_t ipc_len = GetU64(blob, pos);
			cb.ipc = arrow::Buffer::FromString(blob.substr(pos, ipc_len));
			pos += ipc_len;
			total_bytes += static_cast<int64_t>(ipc_len);
			stream.rows += cb.rows;
			stream.bytes += static_cast<int64_t>(ipc_len);
			stream.batches.push_back(std::move(cb));
		}
		entry->streams.push_back(std::move(stream));
		entry->total_bytes = total_bytes;
		entry->stored_at = std::chrono::steady_clock::now();
		if (expires_unix == INT64_MAX) {
			entry->never_expires = true;
		} else {
			entry->expires_at =
			    entry->stored_at + std::chrono::seconds(expires_unix - now_unix);
		}
		return entry;
	} catch (...) {
		return nullptr;
	}
}

int64_t VgiResultCache::PeekDiskRefBytes(const VgiResultCacheKey &key, const std::string &dir) {
	if (dir.empty()) {
		return -1;
	}
	try {
		std::string fp = key.Fingerprint();
		if (fp.empty()) {
			return -1;
		}
		std::string ref;
		if (!ReadFileAll(dir + "/" + IdentityShard(key.identity_scope) + "/refs/" + fp + ".ref", ref)) {
			return -1;
		}
		size_t start = 0;
		while (start < ref.size()) {
			size_t nl = ref.find('\n', start);
			std::string line = ref.substr(start, nl == std::string::npos ? std::string::npos : nl - start);
			start = (nl == std::string::npos) ? ref.size() : nl + 1;
			if (line.rfind("bytes=", 0) == 0) {
				return ParseInt(line.substr(6), -1);
			}
		}
		return -1;
	} catch (...) {
		return -1;
	}
}

std::shared_ptr<VgiResultCacheEntry> VgiResultCache::LoadFromDiskStreaming(const VgiResultCacheKey &key,
                                                                          const std::string &dir) {
	if (dir.empty()) {
		return nullptr;
	}
	try {
		std::string fp = key.Fingerprint();
		if (fp.empty()) {
			return nullptr;
		}
		const std::string base = dir + "/" + IdentityShard(key.identity_scope);
		std::string ref;
		if (!ReadFileAll(base + "/refs/" + fp + ".ref", ref)) {
			return nullptr;
		}
		std::string keyfp, content, scope = VGI_CACHE_SCOPE_CATALOG, etag, last_modified, catalog;
		int64_t expires_unix = 0, rows = 0;
		size_t start = 0;
		while (start < ref.size()) {
			size_t nl = ref.find('\n', start);
			std::string line = ref.substr(start, nl == std::string::npos ? std::string::npos : nl - start);
			start = (nl == std::string::npos) ? ref.size() : nl + 1;
			size_t eq = line.find('=');
			if (eq == std::string::npos) continue;
			std::string k = line.substr(0, eq), v = line.substr(eq + 1);
			if (k == "keyfp") keyfp = v;
			else if (k == "content") content = v;
			else if (k == "expires_unix") expires_unix = ParseInt(v, 0);
			else if (k == "scope") scope = v;
			else if (k == "etag") etag = v;
			else if (k == "last_modified") last_modified = v;
			else if (k == "rows") rows = ParseInt(v, 0);
			else if (k == "catalog") catalog = v;
		}
		if (keyfp != fp || !IsHex64(content)) {
			return nullptr;
		}
		int64_t now_unix = static_cast<int64_t>(std::time(nullptr));
		if (expires_unix != INT64_MAX && now_unix >= expires_unix) {
			return nullptr;
		}
		// [S8] Build the per-batch TOC by reading the header + each batch's small
		// fixed fields (+ partition_values) via positioned reads and SEEKING past
		// each IPC payload — the multi-GB payload is never read here. NB: we skip
		// the whole-blob integrity re-hash (D) that materializing LoadFromDisk does,
		// because re-hashing would require reading the whole blob — defeating the
		// point. The content-addressed name + keyfp still bind the object to the
		// key; a corrupt batch throws cleanly at IPC-decode time on replay.
		const std::string blob_path = base + "/objects/" + content + ".vrc";
		auto &fs = LocalFs();
		if (!fs.FileExists(blob_path)) {
			return nullptr; // orphaned ref (object reaped) → clean miss
		}
		auto handle = fs.OpenFile(blob_path, FileFlags::FILE_FLAGS_READ);
		const int64_t file_size = static_cast<int64_t>(fs.GetFileSize(*handle));
		auto read_at = [&](void *buf, int64_t nbytes, int64_t loc) {
			fs.Read(*handle, buf, nbytes, static_cast<idx_t>(loc));
		};
		std::string hdr(4, '\0');
		read_at(&hdr[0], 4, 0);
		if (hdr.compare(0, 4, "VRC1") != 0) {
			return nullptr;
		}

		auto entry = std::make_shared<VgiResultCacheEntry>();
		entry->key = key;
		entry->scope = scope;
		entry->etag = etag;
		entry->last_modified = last_modified;
		entry->catalog_name = catalog;
		entry->rows = rows;
		entry->disk_backed = true;
		entry->disk_path = blob_path;
		CachedStream stream;
		int64_t total_bytes = 0;
		int64_t pos = 4; // past the "VRC1" magic; no batch-count header — read to EOF
		while (pos < file_size) {
			std::string fx(32, '\0'); // has_index, batch_index, rows, pv_len (4×u64)
			read_at(&fx[0], 32, pos);
			pos += 32;
			size_t fp2 = 0;
			VgiCachedBatch cb;
			cb.has_batch_index = GetU64(fx, fp2) != 0;
			cb.batch_index = GetU64(fx, fp2);
			cb.rows = static_cast<int64_t>(GetU64(fx, fp2));
			uint64_t pv_len = GetU64(fx, fp2);
			if (pv_len > 0) {
				cb.partition_values_bytes.resize(pv_len);
				read_at(&cb.partition_values_bytes[0], static_cast<int64_t>(pv_len), pos);
				pos += static_cast<int64_t>(pv_len);
			}
			std::string il(8, '\0');
			read_at(&il[0], 8, pos);
			pos += 8;
			size_t ip = 0;
			uint64_t ipc_len = GetU64(il, ip);
			cb.disk_ipc_offset = pos; // IPC payload starts here — NOT read now
			cb.disk_ipc_length = static_cast<int64_t>(ipc_len);
			pos += static_cast<int64_t>(ipc_len); // SKIP the payload
			total_bytes += static_cast<int64_t>(ipc_len);
			stream.rows += cb.rows;
			stream.bytes += static_cast<int64_t>(ipc_len);
			stream.batches.push_back(std::move(cb));
		}
		entry->streams.push_back(std::move(stream));
		entry->total_bytes = total_bytes; // informational only (not in the LRU)
		entry->stored_at = std::chrono::steady_clock::now();
		if (expires_unix == INT64_MAX) {
			entry->never_expires = true;
		} else {
			entry->expires_at = entry->stored_at + std::chrono::seconds(expires_unix - now_unix);
		}
		return entry;
	} catch (...) {
		return nullptr;
	}
}

void VgiResultCache::FlushCatalogDisk(const std::string &identity_scope, const std::string &dir) {
	if (dir.empty() || identity_scope.empty()) {
		return;
	}
	try {
		// Per-identity sharding makes this a single O(1) subtree removal that
		// touches ONLY this identity's entries — never another tenant's refs that
		// happen to share the same catalog alias on a shared cache dir.
		const std::string shard = dir + "/" + IdentityShard(identity_scope);
		if (LocalFs().DirectoryExists(shard)) {
			LocalFs().RemoveDirectory(shard);
		}
	} catch (...) {
	}
}

size_t VgiResultCache::ReapDisk(const std::string &dir, uint64_t max_bytes, int64_t now_unix) {
	size_t removed = 0;
	if (dir.empty() || max_bytes == 0) {
		return removed;
	}
	static constexpr int64_t kGraceSeconds = 60;
	// A streaming-capture temp (`.tmp-stream-*`) is written incrementally over the
	// whole scan — minutes for a multi-GB spill — so the 60 s orphan grace used for
	// the synchronous WriteFileAtomic temps would race the reaper into unlinking a
	// LIVE capture's file mid-write (POSIX keeps writing to the unlinked inode, then
	// the finalize rename fails → the result silently isn't cached). Grace is measured
	// from the file's mtime (last write), so an actively-progressing capture keeps
	// resetting it; this only matters for a capture STALLED between batches. An hour is
	// far beyond any real capture, while a truly-abandoned temp from a crashed process
	// still eventually gets swept.
	static constexpr int64_t kStreamTmpGraceSeconds = 3600;
	try {
		auto &fs = LocalFs();
		if (!fs.DirectoryExists(dir)) {
			return removed;
		}
		// The store is sharded per identity (<dir>/<shard>/{refs,objects}). Passes
		// 1 (expiry) and 3 (orphan sweep) are shard-local; pass 2 (the byte cap)
		// is GLOBAL across all shards so `disk_max_bytes` bounds total disk use,
		// not per-identity use.
		std::vector<std::string> shards;
		fs.ListFiles(dir, [&](const std::string &name, bool is_dir) {
			if (is_dir) {
				shards.push_back(dir + "/" + name);
			}
		});

		struct RefInfo {
			std::string path;
			std::string shard;
			std::string content;
			int64_t mtime;
			int64_t bytes;
		};
		std::vector<RefInfo> live_refs;
		std::map<std::string, std::set<std::string>> live_objects; // shard -> live content shas

		// Pass 1 (per shard): unlink expired refs; collect the live set.
		for (auto &shard : shards) {
			const std::string refs = shard + "/refs";
			if (!fs.DirectoryExists(refs)) {
				continue;
			}
			fs.ListFiles(refs, [&](const std::string &name, bool) {
				std::string path = refs + "/" + name;
				std::string body;
				if (!ReadFileAll(path, body)) {
					return;
				}
				int64_t expires = INT64_MAX, bytes = 0;
				std::string content;
				size_t start = 0;
				while (start < body.size()) {
					size_t nl = body.find('\n', start);
					std::string line =
					    body.substr(start, nl == std::string::npos ? std::string::npos : nl - start);
					start = (nl == std::string::npos) ? body.size() : nl + 1;
					size_t eq = line.find('=');
					if (eq == std::string::npos) continue;
					std::string k = line.substr(0, eq), v = line.substr(eq + 1);
					if (k == "content") content = v;
					else if (k == "expires_unix") expires = ParseInt(v, INT64_MAX);
					else if (k == "bytes") bytes = ParseInt(v, 0);
				}
				if (expires != INT64_MAX && now_unix >= expires) {
					if (fs.TryRemoveFile(path)) {
						++removed;
					}
					return;
				}
				if (!content.empty()) {
					live_objects[shard].insert(content);
					int64_t mt = 0;
					try {
						auto h = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
						mt = fs.GetLastModifiedTime(*h).value / 1000000; // micros → seconds
					} catch (...) {}
					live_refs.push_back({path, shard, content, mt, bytes});
				}
			});
		}

		// Pass 2 (GLOBAL): evict oldest-mtime whole entries while over the cap.
		int64_t total = 0;
		for (auto &r : live_refs) {
			total += r.bytes;
		}
		if (total > static_cast<int64_t>(max_bytes)) {
			std::sort(live_refs.begin(), live_refs.end(),
			          [](const RefInfo &a, const RefInfo &b) { return a.mtime < b.mtime; });
			for (auto &r : live_refs) {
				if (total <= static_cast<int64_t>(max_bytes)) {
					break;
				}
				if (fs.TryRemoveFile(r.path)) {
					++removed;
				}
				live_objects[r.shard].erase(r.content);
				total -= r.bytes;
			}
		}

		// Pass 3 (per shard): sweep orphan objects + stale .tmp-* past the grace
		// window (protects an object written but not yet ref-published).
		for (auto &shard : shards) {
			const std::string objects = shard + "/objects";
			if (!fs.DirectoryExists(objects)) {
				continue;
			}
			const auto &live = live_objects[shard];
			std::vector<std::string> orphans;
			fs.ListFiles(objects, [&](const std::string &name, bool) {
				std::string full = objects + "/" + name;
				bool is_tmp = name.find(".tmp-") != std::string::npos;
				bool is_obj = name.size() > 4 && name.compare(name.size() - 4, 4, ".vrc") == 0;
				if (!is_tmp && !is_obj) {
					return;
				}
				if (is_obj) {
					std::string sha = name.substr(0, name.size() - 4);
					if (live.find(sha) != live.end()) {
						return; // reachable — keep
					}
				}
				int64_t mt = 0;
				try {
					auto h = fs.OpenFile(full, FileFlags::FILE_FLAGS_READ);
					mt = fs.GetLastModifiedTime(*h).value / 1000000;
				} catch (...) {
					return;
				}
				// Long-lived streaming-capture temps get a much longer idle grace so
				// the reaper never unlinks an in-flight spill (see kStreamTmpGraceSeconds).
				const bool is_stream_tmp = name.find(".tmp-stream-") != std::string::npos;
				const int64_t grace = is_stream_tmp ? kStreamTmpGraceSeconds : kGraceSeconds;
				if (now_unix - mt >= grace) {
					orphans.push_back(full);
				}
			});
			for (auto &p : orphans) {
				fs.TryRemoveFile(p);
			}
		}
	} catch (...) {
	}
	return removed;
}

size_t VgiResultCache::ReapStaleLocked(std::chrono::steady_clock::time_point now) {
	size_t removed = 0;
	for (auto it = lru_.begin(); it != lru_.end();) {
		// Revalidatable entries are refreshed on access (a conditional request),
		// not dropped on staleness — keep them until LRU pressure evicts them.
		// Only non-revalidatable stale entries are TTL-reaped.
		if ((*it)->IsStale(now) && !(*it)->revalidatable) {
			total_bytes_ -= (*it)->total_bytes;
			index_.erase((*it)->key);
			it = lru_.erase(it);
			evictions_ttl_.fetch_add(1, std::memory_order_relaxed);
			++removed;
		} else {
			++it;
		}
	}
	return removed;
}

} // namespace vgi
} // namespace duckdb
