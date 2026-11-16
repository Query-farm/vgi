// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Shared-memory side channel for vgi_rpc (POSIX shm_open/mmap, Windows file
// mappings). Mirrors vgi-rpc's vgi_rpc/shm.py:
//
//   * The client creates the segment with a 64 KiB header (layout below) and
//     advertises it on init requests (SHM_SEGMENT_NAME_KEY / _SIZE_KEY).
//   * The worker writes IPC streams into it with the embedded allocator and
//     sends 0-row pointer batches (SHM_OFFSET_KEY / SHM_LENGTH_KEY) instead;
//     MaybeResolveBatch turns those back into batches.
//
// Lifecycle: between requests the connection resets the allocator (RetainOnly).
// The NAME is unlinked when the connection goes away; the MAPPING lives while
// any resolved batch views it (VgiShmSlotLease). Resolved batches are zero-copy
// and may be retained indefinitely; their slot returns to the worker only when
// unreferenced. A full segment only makes the worker fall back to inline
// transport. Size it with VGI_RPC_SHM_SIZE_BYTES.
#pragma once

#include "vgi_ipc.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>


namespace duckdb {
namespace vgi {

// Header byte layout (must match vgi_rpc/shm.py exactly).
//
//   Offset  Size  Field
//   0       4     magic: "VGIS"
//   4       4     version: uint32 LE = 1
//   8       8     data_size: uint64 LE  (= total_size - HEADER_SIZE)
//   16      4     num_allocs: uint32 LE
//   20      4     padding
//   24      ...   alloc[i] = (offset: uint64, length: uint64), sorted by offset
constexpr size_t VGI_SHM_HEADER_SIZE = 65536;
constexpr const char *VGI_SHM_MAGIC = "VGIS";
constexpr uint32_t VGI_SHM_VERSION = 1;
// Max entries after the 24-byte preamble: (65536 - 24) / 16 = 4094. The
// worker-written num_allocs must be validated against it before iterating.
constexpr uint32_t VGI_SHM_MAX_ALLOCS = (VGI_SHM_HEADER_SIZE - 24) / 16;

class VgiShmSegment;

// A lease on one slot, held by every buffer that views it (so by the resolved
// batch and its slices). It keeps the MAPPING alive but deliberately does NOT
// free the slot on destruction: the allocator table is unlocked shared memory
// protected only by RPC lockstep, so a free from an arbitrary thread could race
// the worker's next allocate. The connection holds its own reference and, at
// lockstep points, frees slots whose only reference is its own.
struct VgiShmSlotLease {
	std::shared_ptr<const VgiShmSegment> segment;
	uint64_t offset;
};

class VgiShmSegment : public std::enable_shared_from_this<VgiShmSegment> {
public:
	// `size_bytes` must be > VGI_SHM_HEADER_SIZE. The header records the actual
	// mapped size (POSIX may round up to a page).
	static std::shared_ptr<VgiShmSegment> Create(size_t size_bytes);

	~VgiShmSegment();

	VgiShmSegment(const VgiShmSegment &) = delete;
	VgiShmSegment &operator=(const VgiShmSegment &) = delete;

	// POSIX name with leading '/' (none on Windows). The connection strips the
	// slash when advertising it: Python's multiprocessing.shared_memory adds one.
	const std::string &name() const {
		return name_;
	}
	size_t size() const {
		return size_;
	}
	const uint8_t *base() const {
		return base_;
	}

	// Clear num_allocs. Lockstep only: the worker must not be writing.
	void ResetAllocator();

	// ResetAllocator(), except entries at `offsets` (still-referenced batches)
	// survive, so the worker cannot allocate over live data. The between-request
	// reset. Unknown offsets are ignored. Lockstep only.
	void RetainOnly(const std::vector<uint64_t> &offsets);

	// shm_unlink the NAME, keeping the mapping, so a segment kept alive by a
	// retained batch does not pin a name. Idempotent; no-op on Windows.
	void Unlink();

	// Free the entry at `offset` once its batch is unreferenced, so the worker
	// can reuse the slot (otherwise the segment fills and the worker falls back
	// to inline). Unknown offsets are ignored. Lockstep only.
	void FreeAllocation(uint64_t offset);

	// Client→worker direction: first-fit allocate and copy `data` in, then send
	// a pointer batch; the worker frees the slot. Returns the absolute offset,
	// or nullopt when the segment or alloc table is full (send inline).
	// Lockstep only.
	std::optional<uint64_t> AllocateAndWrite(const uint8_t *data, size_t len);

	// AllocateAndWrite without the copy: the caller serializes straight into
	// MutableData(offset).
	std::optional<uint64_t> Allocate(size_t len);

	// Valid only for [offset, offset+len) of an Allocate() reservation, until
	// its pointer batch is sent.
	uint8_t *MutableData(uint64_t offset) {
		return base_ + offset;
	}

	// Resolve a pointer batch (SHM_OFFSET_KEY) from the segment; nullptr when it
	// is not one. *out_lease is shared by the batch's buffers, so the caller
	// may FreeAllocation() only once out_lease->use_count() == 1.
	VgiBatchPtr
	MaybeResolveBatch(const VgiBatchPtr &batch,
	                  const VgiMetadataPtr &custom_metadata,
	                  std::shared_ptr<VgiShmSlotLease> *out_lease) const;

private:
#if defined(_WIN32)
	VgiShmSegment(void *map_handle, std::string name, uint8_t *base, size_t size);
	void *map_handle_; // HANDLE from CreateFileMapping (closed on destruction)
#else
	VgiShmSegment(int fd, std::string name, uint8_t *base, size_t size);
	int fd_;
#endif
	std::string name_;
	uint8_t *base_;
	size_t size_;

	// Schema-message bytes for the dictionary path, serialized once per schema
	// (constant across a scan's batches). Holding the schema keeps the pointer
	// key from being freed and reused by a different schema.
	mutable VgiSchemaPtr cached_schema_;
	mutable VgiBufferPtr cached_schema_msg_;
};

} // namespace vgi
} // namespace duckdb
