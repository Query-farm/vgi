// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Shared-memory side-channel. POSIX uses shm_open/mmap; Windows uses
// CreateFileMapping/MapViewOfFile (page-file-backed named mapping). Under
// Emscripten this translation unit is empty. The allocator/IPC logic below is
// platform-independent; only Create/dtor/GenerateName differ. Opt-in via
// VGI_RPC_SHM_SIZE_BYTES. See vgi_platform.hpp.

#include "vgi_platform.hpp"

#if VGI_SHM_TRANSPORT

#include "vgi_shm_segment.hpp"

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <random>
#include <sstream>
#if VGI_POSIX_TRANSPORT
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/type.h>

#include "duckdb/common/exception.hpp"

#include "vgi_rpc_client.hpp" // for SHM_OFFSET_KEY / SHM_LENGTH_KEY

#if defined(_WIN32)
// windows.h must come AFTER the arrow / vgi_rpc_client headers: it #defines
// ERROR (and other all-caps tokens) that collide with identifiers in them.
#include <process.h> // _getpid
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#endif

namespace duckdb {
namespace vgi {

namespace {

// Virtually concatenates a list of byte regions and exposes them as a single
// arrow::io::InputStream — no allocation, no copy of the regions themselves.
// Used to splice together [schema_msg | mmap_slice | EOS] when reading a
// dictionary-encoded batch out of the shm segment, instead of memcpy'ing the
// whole 16 MiB slice into a fresh buffer.
class ChainedBufferInputStream final : public arrow::io::InputStream {
public:
	struct Region {
		const uint8_t *data;
		int64_t length;
		// Optional buffer to pin (keeps `data` alive for the stream's lifetime).
		std::shared_ptr<arrow::Buffer> owner;
	};

	explicit ChainedBufferInputStream(std::vector<Region> regions)
	    : regions_(std::move(regions)), region_idx_(0), region_pos_(0), abs_pos_(0), closed_(false) {
	}

	arrow::Status Close() override {
		closed_ = true;
		return arrow::Status::OK();
	}

	bool closed() const override {
		return closed_;
	}

	arrow::Result<int64_t> Tell() const override {
		return abs_pos_;
	}

	arrow::Result<int64_t> Read(int64_t nbytes, void *out) override {
		auto *dst = static_cast<uint8_t *>(out);
		int64_t total = 0;
		while (nbytes > 0 && region_idx_ < regions_.size()) {
			const auto &r = regions_[region_idx_];
			int64_t avail = r.length - region_pos_;
			if (avail <= 0) {
				region_idx_++;
				region_pos_ = 0;
				continue;
			}
			int64_t n = std::min(nbytes, avail);
			std::memcpy(dst + total, r.data + region_pos_, static_cast<size_t>(n));
			region_pos_ += n;
			abs_pos_ += n;
			total += n;
			nbytes -= n;
			if (region_pos_ == r.length) {
				region_idx_++;
				region_pos_ = 0;
			}
		}
		return total;
	}

	arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
		// Fast path: if the requested range is entirely within the current
		// region, return a zero-copy slice of that region's owner buffer.
		if (region_idx_ < regions_.size()) {
			const auto &r = regions_[region_idx_];
			int64_t avail = r.length - region_pos_;
			if (nbytes <= avail && r.owner) {
				auto sliced = arrow::SliceBuffer(r.owner, region_pos_, nbytes);
				region_pos_ += nbytes;
				abs_pos_ += nbytes;
				if (region_pos_ == r.length) {
					region_idx_++;
					region_pos_ = 0;
				}
				return sliced;
			}
		}
		// Slow path: allocate and copy across regions.
		ARROW_ASSIGN_OR_RAISE(auto out, arrow::AllocateResizableBuffer(nbytes));
		ARROW_ASSIGN_OR_RAISE(auto n, Read(nbytes, out->mutable_data()));
		ARROW_RETURN_NOT_OK(out->Resize(n, /*shrink_to_fit=*/false));
		return std::shared_ptr<arrow::Buffer>(std::move(out));
	}

private:
	std::vector<Region> regions_;
	size_t region_idx_;
	int64_t region_pos_;
	int64_t abs_pos_;
	bool closed_;
};

} // namespace

namespace {

inline uint32_t LoadU32LE(const uint8_t *p) {
	uint32_t v;
	std::memcpy(&v, p, 4);
	return v;
}

inline uint64_t LoadU64LE(const uint8_t *p) {
	uint64_t v;
	std::memcpy(&v, p, 8);
	return v;
}

inline void StoreU32LE(uint8_t *p, uint32_t v) {
	std::memcpy(p, &v, 4);
}

inline void StoreU64LE(uint8_t *p, uint64_t v) {
	std::memcpy(p, &v, 8);
}

// Generate a unique posix shm name. Python's SharedMemory uses
// "/wnsm_<8-hex>"; we match the leading-slash convention.
std::string GenerateName() {
	static std::atomic<uint64_t> counter {0};
	uint64_t n = counter.fetch_add(1, std::memory_order_relaxed);
	std::random_device rd;
	uint32_t r = rd();
	std::ostringstream oss;
#if defined(_WIN32)
	// Win32 file-mapping object name (session-local namespace). No leading slash:
	// Python's multiprocessing.shared_memory uses the name verbatim as the mmap
	// tagname on Windows.
	oss << "vgi_shm_" << std::hex << ::_getpid() << "_" << r << "_" << n;
#else
	oss << "/vgi_shm_" << std::hex << ::getpid() << "_" << r << "_" << n;
#endif
	return oss.str();
}

} // namespace

std::unique_ptr<VgiShmSegment> VgiShmSegment::Create(size_t size_bytes) {
	if (size_bytes <= VGI_SHM_HEADER_SIZE) {
		throw IOException("VgiShmSegment: size must be > " + std::to_string(VGI_SHM_HEADER_SIZE));
	}

	std::string name = GenerateName();

#if defined(_WIN32)
	// Page-file-backed named mapping; the worker attaches by this same name
	// (Python's multiprocessing.shared_memory, which maps it read-write).
	HANDLE h = ::CreateFileMappingA(INVALID_HANDLE_VALUE, nullptr, PAGE_READWRITE,
	                                static_cast<DWORD>(static_cast<uint64_t>(size_bytes) >> 32),
	                                static_cast<DWORD>(size_bytes & 0xFFFFFFFFu), name.c_str());
	if (h == nullptr) {
		throw IOException("VgiShmSegment: CreateFileMapping failed: " + std::to_string(::GetLastError()));
	}
	if (::GetLastError() == ERROR_ALREADY_EXISTS) {
		::CloseHandle(h);
		throw IOException("VgiShmSegment: name collision: " + name);
	}
	void *map = ::MapViewOfFile(h, FILE_MAP_ALL_ACCESS, 0, 0, size_bytes);
	if (map == nullptr) {
		DWORD e = ::GetLastError();
		::CloseHandle(h);
		throw IOException("VgiShmSegment: MapViewOfFile failed: " + std::to_string(e));
	}
	// Windows maps exactly size_bytes; the worker attaches with the same size.
	size_t actual_size = size_bytes;
	auto *base = static_cast<uint8_t *>(map);
#else
	// O_CREAT | O_EXCL: fail if a stale segment exists with this name.
	int fd = ::shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
	if (fd < 0) {
		throw IOException("VgiShmSegment: shm_open(" + name + ") failed: " + std::string(std::strerror(errno)));
	}

	if (::ftruncate(fd, static_cast<off_t>(size_bytes)) != 0) {
		int err = errno;
		::close(fd);
		::shm_unlink(name.c_str());
		throw IOException("VgiShmSegment: ftruncate failed: " + std::string(std::strerror(err)));
	}

	// stat to learn the actual mapped size — POSIX may round up to page
	// boundary, and we want the header's data_size field to match what a
	// Python peer attaching with shm.size will see.
	struct stat st;
	if (::fstat(fd, &st) != 0) {
		int err = errno;
		::close(fd);
		::shm_unlink(name.c_str());
		throw IOException("VgiShmSegment: fstat failed: " + std::string(std::strerror(err)));
	}
	size_t actual_size = static_cast<size_t>(st.st_size);

	void *map = ::mmap(nullptr, actual_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	if (map == MAP_FAILED) {
		int err = errno;
		::close(fd);
		::shm_unlink(name.c_str());
		throw IOException("VgiShmSegment: mmap failed: " + std::string(std::strerror(err)));
	}
	auto *base = static_cast<uint8_t *>(map);
#endif

	// Initialize header: magic, version, data_size, num_allocs=0, padding.
	std::memcpy(base, VGI_SHM_MAGIC, 4);
	StoreU32LE(base + 4, VGI_SHM_VERSION);
	StoreU64LE(base + 8, static_cast<uint64_t>(actual_size - VGI_SHM_HEADER_SIZE));
	StoreU32LE(base + 16, 0);
	StoreU32LE(base + 20, 0);

#if defined(_WIN32)
	return std::unique_ptr<VgiShmSegment>(new VgiShmSegment(h, std::move(name), base, actual_size));
#else
	return std::unique_ptr<VgiShmSegment>(new VgiShmSegment(fd, std::move(name), base, actual_size));
#endif
}

#if defined(_WIN32)
VgiShmSegment::VgiShmSegment(void *map_handle, std::string name, uint8_t *base, size_t size)
    : map_handle_(map_handle), name_(std::move(name)), base_(base), size_(size) {
}
#else
VgiShmSegment::VgiShmSegment(int fd, std::string name, uint8_t *base, size_t size)
    : fd_(fd), name_(std::move(name)), base_(base), size_(size) {
}
#endif

VgiShmSegment::~VgiShmSegment() {
#if defined(_WIN32)
	if (base_) {
		::UnmapViewOfFile(base_);
		base_ = nullptr;
	}
	if (map_handle_) {
		::CloseHandle(map_handle_);
		map_handle_ = nullptr;
	}
	// Windows reclaims the mapping when the last handle closes — no unlink.
#else
	if (base_) {
		::munmap(base_, size_);
		base_ = nullptr;
	}
	if (fd_ >= 0) {
		::close(fd_);
		fd_ = -1;
	}
	if (!name_.empty()) {
		::shm_unlink(name_.c_str());
	}
#endif
}

void VgiShmSegment::ResetAllocator() {
	// Lockstep guarantee: no peer is mid-write when this is called.
	if (base_) {
		StoreU32LE(base_ + 16, 0);
	}
}

void VgiShmSegment::FreeAllocation(uint64_t offset) {
	// Mirrors ShmAllocator.free in vgi_rpc/shm.py: scan the (offset,length)
	// entries in the header, drop the one whose offset matches, and shift
	// the remainder down. Lockstep guarantees the worker isn't mid-allocate
	// while we do this.
	if (!base_) {
		return;
	}
	uint32_t num = LoadU32LE(base_ + 16);
	if (num > VGI_SHM_MAX_ALLOCS) {
		// Corrupt header: num_allocs can't exceed what the header region holds.
		// Trusting it would walk reads far past the mapping (OOB).
		throw IOException("VgiShmSegment: corrupt header num_allocs=" + std::to_string(num) +
		                  " exceeds max " + std::to_string(VGI_SHM_MAX_ALLOCS));
	}
	uint8_t *entries = base_ + 24;
	for (uint32_t i = 0; i < num; i++) {
		uint64_t entry_off = LoadU64LE(entries + i * 16);
		if (entry_off == offset) {
			// Shift remaining entries left by one slot.
			if (i + 1 < num) {
				std::memmove(entries + i * 16, entries + (i + 1) * 16, (num - i - 1) * 16);
			}
			StoreU32LE(base_ + 16, num - 1);
			return;
		}
	}
	// Unknown offset — nothing to free. Silent no-op.
}

std::optional<uint64_t> VgiShmSegment::Allocate(size_t len) {
	// First-fit allocate, the inverse of FreeAllocation. Mirrors
	// ShmAllocator.allocate in vgi_rpc/shm.py: scan the sorted (offset,length)
	// entries for the first gap >= len starting at HEADER_SIZE, insert the new
	// entry keeping the table sorted. Reserves the slot only; the caller copies
	// or serializes its bytes into MutableData(offset). Lockstep guarantees the
	// worker isn't mutating the header concurrently.
	if (!base_ || len == 0) {
		return std::nullopt;
	}
	uint32_t num = LoadU32LE(base_ + 16);
	if (num > VGI_SHM_MAX_ALLOCS) {
		throw IOException("VgiShmSegment: corrupt header num_allocs=" + std::to_string(num) +
		                  " exceeds max " + std::to_string(VGI_SHM_MAX_ALLOCS));
	}
	if (num >= VGI_SHM_MAX_ALLOCS) {
		return std::nullopt; // allocation table full → caller falls back to inline
	}
	uint8_t *entries = base_ + 24;
	uint64_t cursor = VGI_SHM_HEADER_SIZE;
	uint32_t ins = num; // default: append after the last entry
	for (uint32_t i = 0; i < num; i++) {
		uint64_t entry_off = LoadU64LE(entries + i * 16);
		uint64_t entry_len = LoadU64LE(entries + i * 16 + 8);
		if (entry_off >= cursor && entry_off - cursor >= len) {
			ins = i; // fits in the gap before entry i
			break;
		}
		uint64_t end = entry_off + entry_len;
		if (end > cursor) {
			cursor = end;
		}
	}
	if (cursor + len > size_) {
		return std::nullopt; // no contiguous gap large enough → inline fallback
	}
	// Make room at `ins` and write the new (offset,length) entry, keeping the
	// table sorted by offset so the next first-fit scan stays correct.
	if (ins < num) {
		std::memmove(entries + (ins + 1) * 16, entries + ins * 16, (num - ins) * 16);
	}
	StoreU64LE(entries + ins * 16, cursor);
	StoreU64LE(entries + ins * 16 + 8, static_cast<uint64_t>(len));
	StoreU32LE(base_ + 16, num + 1);
	return cursor;
}

std::optional<uint64_t> VgiShmSegment::AllocateAndWrite(const uint8_t *data, size_t len) {
	auto offset = Allocate(len);
	if (offset) {
		std::memcpy(base_ + *offset, data, len);
	}
	return offset;
}

std::shared_ptr<arrow::RecordBatch>
VgiShmSegment::MaybeResolveBatch(const std::shared_ptr<arrow::RecordBatch> &batch,
                                 const std::shared_ptr<arrow::KeyValueMetadata> &custom_metadata,
                                 int64_t *out_offset) const {
	if (out_offset) {
		*out_offset = -1;
	}
	if (!custom_metadata) {
		return nullptr;
	}
	// Pointer batches are 0-row; non-zero-row batches are always real data.
	if (batch && batch->num_rows() != 0) {
		return nullptr;
	}
	auto offset_str = custom_metadata->Get(SHM_OFFSET_KEY);
	auto length_str = custom_metadata->Get(SHM_LENGTH_KEY);
	if (!offset_str.ok() || !length_str.ok()) {
		return nullptr;
	}

	uint64_t offset;
	uint64_t length;
	try {
		offset = std::stoull(offset_str.ValueUnsafe());
		length = std::stoull(length_str.ValueUnsafe());
	} catch (const std::exception &e) {
		throw IOException("VgiShmSegment: malformed pointer batch offset/length metadata: " +
		                  std::string(e.what()));
	}
	// Bounds check written without addition so a huge worker-supplied length
	// can't overflow uint64 and wrap past the guard. offset <= size_ is checked
	// first so size_ - offset below can't underflow.
	if (offset < VGI_SHM_HEADER_SIZE || offset > size_ || length > size_ - offset) {
		throw IOException("VgiShmSegment: pointer batch out of range (offset=" + std::to_string(offset) +
		                  ", length=" + std::to_string(length) + ", size=" + std::to_string(size_) + ")");
	}

	// Wrap the segment slice as a non-owning Arrow buffer.
	// Per vgi_rpc/shm.py:
	//   * Non-dictionary schemas: shm slice is a complete IPC stream
	//     (schema + record batch + EOS). open_stream reads directly.
	//   * Dictionary schemas: shm slice contains only the dict+batch
	//     messages (no schema, no EOS); the schema message is reconstructed
	//     from the *pointer batch's* schema and prepended, EOS appended.
	auto schema = batch ? batch->schema() : nullptr;
	bool has_dict = false;
	if (schema) {
		for (int i = 0; i < schema->num_fields(); ++i) {
			if (schema->field(i)->type()->id() == arrow::Type::DICTIONARY) {
				has_dict = true;
				break;
			}
		}
	}

	// Static EOS marker: continuation token 0xFFFFFFFF + 4-byte 0-length.
	static const uint8_t kEosMarker[8] = {0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00};

	std::shared_ptr<arrow::io::InputStream> input;
	if (!has_dict) {
		// Non-dict: shm slice is already a complete IPC stream
		// (schema + record batch + EOS). Wrap as a non-owning Buffer view —
		// zero copy.
		auto buffer = std::make_shared<arrow::Buffer>(base_ + offset, static_cast<int64_t>(length));
		input = std::make_shared<arrow::io::BufferReader>(buffer);
	} else {
		// Dict path: build/cache the schema message bytes once per scan
		// (the same schema repeats across every batch), then virtually
		// concatenate [schema_msg | mmap_slice | EOS] via a chained
		// InputStream — no allocation, no copy of the slice.
		std::shared_ptr<arrow::Buffer> schema_msg;
		if (cached_schema_ptr_ == schema.get() && cached_schema_msg_) {
			schema_msg = cached_schema_msg_;
		} else {
			auto out_buffer_result = arrow::io::BufferOutputStream::Create();
			if (!out_buffer_result.ok()) {
				throw IOException("VgiShmSegment: BufferOutputStream::Create failed: " +
				                  out_buffer_result.status().ToString());
			}
			auto out_stream = out_buffer_result.ValueUnsafe();
			auto schema_writer_result = arrow::ipc::MakeStreamWriter(out_stream, schema);
			if (!schema_writer_result.ok()) {
				throw IOException("VgiShmSegment: MakeStreamWriter failed: " +
				                  schema_writer_result.status().ToString());
			}
			auto status = schema_writer_result.ValueUnsafe()->Close();
			if (!status.ok()) {
				throw IOException("VgiShmSegment: schema writer close failed: " + status.ToString());
			}
			auto finish_result = out_stream->Finish();
			if (!finish_result.ok()) {
				throw IOException("VgiShmSegment: BufferOutputStream::Finish failed: " +
				                  finish_result.status().ToString());
			}
			auto schema_with_eos = finish_result.ValueUnsafe();
			const int64_t schema_msg_len = schema_with_eos->size() - sizeof(kEosMarker);
			if (schema_msg_len < 0) {
				throw IOException("VgiShmSegment: schema message shorter than EOS marker");
			}
			schema_msg = arrow::SliceBuffer(schema_with_eos, 0, schema_msg_len);
			cached_schema_ptr_ = schema.get();
			cached_schema_msg_ = schema_msg;
		}

		// EOS marker as a tiny non-owning buffer (the byte array is static).
		auto eos_buffer =
		    std::make_shared<arrow::Buffer>(kEosMarker, static_cast<int64_t>(sizeof(kEosMarker)));
		auto slice_buffer =
		    std::make_shared<arrow::Buffer>(base_ + offset, static_cast<int64_t>(length));

		std::vector<ChainedBufferInputStream::Region> regions;
		regions.reserve(3);
		regions.push_back({schema_msg->data(), schema_msg->size(), schema_msg});
		regions.push_back({slice_buffer->data(), slice_buffer->size(), slice_buffer});
		regions.push_back({eos_buffer->data(), eos_buffer->size(), eos_buffer});
		input = std::make_shared<ChainedBufferInputStream>(std::move(regions));
	}

	auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(input);
	if (!reader_result.ok()) {
		throw IOException("VgiShmSegment: failed to open IPC stream from segment slice: " +
		                  reader_result.status().ToString());
	}
	auto reader = reader_result.ValueUnsafe();
	std::shared_ptr<arrow::RecordBatch> resolved;
	auto status = reader->ReadNext(&resolved);
	if (!status.ok()) {
		throw IOException("VgiShmSegment: failed to read batch from segment slice: " + status.ToString());
	}
	if (!resolved) {
		throw IOException("VgiShmSegment: empty IPC stream in segment slice");
	}
	if (out_offset) {
		*out_offset = static_cast<int64_t>(offset);
	}
	return resolved;
}

} // namespace vgi
} // namespace duckdb

#endif // VGI_POSIX_TRANSPORT
