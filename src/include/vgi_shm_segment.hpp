// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Shared-memory transport for vgi_rpc on POSIX hosts. Mirrors the
// Python reference implementation at vgi-rpc/vgi_rpc/shm.py:
//
//   * Client (this side) creates a posix shm segment via shm_open + mmap,
//     initializes a fixed 64 KiB header (magic "VGIS", version 1, data_size,
//     num_allocs, then up to 4094 (offset,length) entries — see header
//     layout in shm.py).
//   * Client advertises (segment_name, segment_size_bytes) on init requests
//     via SHM_SEGMENT_NAME_KEY / SHM_SEGMENT_SIZE_KEY metadata.
//   * Server (Python worker) attaches read-write to the same segment and
//     uses the embedded allocator to write Arrow IPC streams into it. For
//     each batch it emits a 0-row "pointer batch" with SHM_OFFSET_KEY and
//     SHM_LENGTH_KEY in custom_metadata.
//   * Client recognizes pointer batches in ReadDataBatch, parses the IPC
//     bytes from the segment, returns the resolved RecordBatch.
//
// Lifecycle: client owns the segment. Reset between requests so the
// server's allocator starts fresh. shm_unlink + munmap on destruction.
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include <arrow/record_batch.h>
#include <arrow/util/key_value_metadata.h>

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
// Max (offset,length) entries that fit in the header region after the 24-byte
// preamble: (65536 - 24) / 16 = 4094. Mirrors the bound vgi_rpc/shm.py enforces
// by construction; the C++ side must validate the worker-written num_allocs
// against it before iterating the entry table.
constexpr uint32_t VGI_SHM_MAX_ALLOCS = (VGI_SHM_HEADER_SIZE - 24) / 16;

class VgiShmSegment {
public:
	// Create a fresh segment of size `size_bytes` (must be > VGI_SHM_HEADER_SIZE).
	// The OS may round up to a page boundary; the actual mapped size is
	// written into the header.
	static std::unique_ptr<VgiShmSegment> Create(size_t size_bytes);

	~VgiShmSegment();

	VgiShmSegment(const VgiShmSegment &) = delete;
	VgiShmSegment &operator=(const VgiShmSegment &) = delete;

	// Posix name (with leading '/'). Pass to Python's
	// SharedMemory(name=..., create=False) directly — Python's
	// _posixshmem.shm_open accepts the leading-slash form.
	const std::string &name() const {
		return name_;
	}
	size_t size() const {
		return size_;
	}
	const uint8_t *base() const {
		return base_;
	}

	// Reset the allocator (clear num_allocs to 0). Called between requests
	// so the worker's allocator starts fresh; the lockstep RPC protocol
	// guarantees the worker isn't writing while we reset.
	void ResetAllocator();

	// Free the allocation entry whose offset matches `offset`. Called after
	// the client has fully consumed a pointer-batch's bytes so the slot can
	// be reused by the worker for a subsequent allocation. Without this,
	// the allocator fills monotonically and the worker silently falls back
	// to inline transport once the segment is full.
	void FreeAllocation(uint64_t offset);

	// First-fit allocate `len` bytes and copy `data` into the segment, the
	// inverse of FreeAllocation. Used for the client→worker direction: the
	// client writes a request/input batch's IPC bytes into the segment and
	// sends a 0-row pointer batch in their place; the worker resolves it and
	// FreeAllocation()s the slot. Returns the absolute offset, or nullopt when
	// the segment (or the 4094-entry alloc table) is full — caller then falls
	// back to inline transport. Lockstep guarantees the worker isn't
	// mid-allocate while we mutate the header here.
	std::optional<uint64_t> AllocateAndWrite(const uint8_t *data, size_t len);

	// If `custom_metadata` carries SHM_OFFSET_KEY (i.e. the batch is a
	// shm pointer batch), parse the IPC bytes from the segment and return
	// the resolved RecordBatch. Returns nullptr when not a pointer batch.
	// On a successful resolve, *out_offset is set to the segment offset
	// the caller is responsible for FreeAllocation()-ing once DuckDB has
	// finished consuming the resolved batch.
	std::shared_ptr<arrow::RecordBatch>
	MaybeResolveBatch(const std::shared_ptr<arrow::RecordBatch> &batch,
	                  const std::shared_ptr<arrow::KeyValueMetadata> &custom_metadata,
	                  int64_t *out_offset) const;

private:
	VgiShmSegment(int fd, std::string name, uint8_t *base, size_t size);

	int fd_;
	std::string name_;
	uint8_t *base_;
	size_t size_;

	// Cached schema-message bytes for the dictionary path. The schema is
	// constant across all batches in a scan, so serialize it once per
	// connection. Keyed by the raw arrow::Schema pointer — schema objects
	// in a single scan are the same shared_ptr instance.
	mutable const arrow::Schema *cached_schema_ptr_ = nullptr;
	mutable std::shared_ptr<arrow::Buffer> cached_schema_msg_;
};

} // namespace vgi
} // namespace duckdb
