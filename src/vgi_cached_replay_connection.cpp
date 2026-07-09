// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_cached_replay_connection.hpp"

#include <algorithm>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp" // FileHandle / FileSystem positioned reads (S8)
#include "vgi_worker_pool.hpp"           // complete PooledWorker for ReleaseForPooling's deleter

namespace duckdb {
namespace vgi {

namespace {
// Process-static local FileSystem for disk-backed replay. A FileHandle keeps a
// reference to the FileSystem that opened it, so the FS must outlive the handle
// — a function-local static (leaked, like the cache singleton) guarantees that.
FileSystem &ReplayLocalFs() {
	static std::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	return *fs;
}
} // namespace

CachedReplayConnection::~CachedReplayConnection() = default;

std::unique_ptr<PooledWorker> CachedReplayConnection::ReleaseForPooling() {
	return nullptr; // cache serve owns no worker
}

CachedReplayConnection::CachedReplayConnection(std::shared_ptr<const VgiResultCacheEntry> entry)
    : entry_(std::move(entry)) {
	// Flatten substreams into a single replay order.
	bool any_batch_index = false;
	for (const auto &stream : entry_->streams) {
		for (const auto &b : stream.batches) {
			order_.push_back(&b);
			any_batch_index = any_batch_index || b.has_batch_index;
		}
	}
	// If the worker tagged batch_index, replay in non-decreasing index order so
	// InstallBatch's per-stream monotonicity check passes on the single replay
	// stream (capture may have interleaved substreams from parallel workers).
	if (any_batch_index) {
		std::stable_sort(order_.begin(), order_.end(),
		                 [](const VgiCachedBatch *a, const VgiCachedBatch *b) {
			                 return a->batch_index < b->batch_index;
		                 });
	}
}

std::shared_ptr<arrow::RecordBatch> CachedReplayConnection::ReadDataBatch() {
	if (cursor_ >= order_.size()) {
		finished_ = true;
		last_batch_index_ = DConstants::INVALID_INDEX;
		last_partition_values_bytes_.clear();
		return nullptr; // EOS
	}
	const VgiCachedBatch *cached = order_[cursor_++];

	// [S8] Disk-backed batch: positioned-read just this batch's IPC bytes from the
	// blob file (open the handle lazily, hold it for the connection's life). Only
	// one batch is resident at a time, so a result far larger than RAM streams
	// with flat memory. In-RAM batches (small adopted entries) use `cached->ipc`.
	std::shared_ptr<arrow::Buffer> ipc = cached->ipc;
	if (!ipc && cached->disk_ipc_offset >= 0) {
		if (!disk_handle_) {
			// The blob is opened once and held for the connection's life; the handle
			// references ReplayLocalFs(), a process-static FS that outlives it.
			disk_handle_ = ReplayLocalFs().OpenFile(entry_->disk_path, FileFlags::FILE_FLAGS_READ);
		}
		auto buf_result = arrow::AllocateBuffer(cached->disk_ipc_length);
		if (!buf_result.ok()) {
			throw IOException("VGI result cache: failed to allocate %lld bytes for disk batch",
			                  static_cast<long long>(cached->disk_ipc_length));
		}
		std::shared_ptr<arrow::Buffer> b = std::move(buf_result.ValueUnsafe());
		ReplayLocalFs().Read(*disk_handle_, b->mutable_data(), cached->disk_ipc_length,
		                     static_cast<idx_t>(cached->disk_ipc_offset));
		ipc = b;
	}

	// Deserialize the self-contained per-batch IPC stream blob.
	auto input = std::make_shared<arrow::io::BufferReader>(ipc);
	auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(input);
	if (!reader_result.ok()) {
		throw IOException("VGI result cache: failed to open cached batch IPC stream: %s",
		                  reader_result.status().ToString());
	}
	auto reader = reader_result.ValueUnsafe();
	auto next_result = reader->ReadNext();
	if (!next_result.ok() || !next_result.ValueUnsafe().batch) {
		throw IOException("VGI result cache: cached batch IPC stream was empty");
	}

	// Replay the stored per-batch vgi_meta so InstallBatch sees identical values.
	last_batch_index_ = cached->has_batch_index ? static_cast<idx_t>(cached->batch_index)
	                                            : DConstants::INVALID_INDEX;
	last_partition_values_bytes_ = cached->partition_values_bytes;
	return next_result.ValueUnsafe().batch;
}

} // namespace vgi
} // namespace duckdb
