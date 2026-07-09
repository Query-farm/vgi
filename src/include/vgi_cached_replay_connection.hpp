// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// CachedReplayConnection — an IFunctionConnection that replays a cached result
// instead of talking to a worker. On a cache hit the scan's global state hands
// each (single, in v1) local state one of these in place of a real connection,
// so InitLocal / GetNextBatch / InstallBatch run genuinely unchanged: they call
// ReadDataBatch() and get the cached batches back, with the stored per-batch
// vgi_meta replayed via GetLastBatchIndex()/GetLastPartitionValuesBytes().

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "vgi_ifunction_connection.hpp"
#include "vgi_result_cache.hpp"

namespace duckdb {

class FileHandle; // forward-declared; the real file_system.hpp include lives in the .cpp

namespace vgi {

class CachedReplayConnection : public IFunctionConnection {
public:
	// Replays every batch of `entry` in a single ordered stream. When any
	// cached batch carries a batch_index, batches are replayed in
	// non-decreasing batch_index order (InstallBatch enforces per-stream
	// monotonicity); otherwise substream/arrival order is used.
	explicit CachedReplayConnection(std::shared_ptr<const VgiResultCacheEntry> entry);
	// Out-of-line (the .cpp holds a unique_ptr<FileHandle> of an incomplete type).
	~CachedReplayConnection() override;

	// The only methods the scan actually drives on a hit:
	std::shared_ptr<arrow::RecordBatch> ReadDataBatch() override;
	idx_t GetLastBatchIndex() const override {
		return last_batch_index_;
	}
	const std::string &GetLastPartitionValuesBytes() const override {
		return last_partition_values_bytes_;
	}
	bool IsFinished() const override {
		return finished_;
	}
	void MarkDataFinished() override {
		finished_ = true;
	}
	std::string GetConnIdHex() const override {
		return conn_id_hex_;
	}
	int Wait() override {
		return 0;
	}
	// Out-of-line: returns nullptr, but `unique_ptr<PooledWorker>` needs the complete
	// type to instantiate its deleter (MSVC enforces this even for a null return;
	// clang defers it). Defined in the .cpp where vgi_worker_pool.hpp is included.
	std::unique_ptr<PooledWorker> ReleaseForPooling() override;
	std::vector<uint8_t> GetLastStateToken() const override {
		return {};
	}
	std::string GetExecutionIdHex() const override {
		return "";
	}
	std::string GetAttachOpaqueDataHex() const override {
		return "";
	}
	std::string GetTransactionOpaqueDataHex() const override {
		return "";
	}
	bool IsTableInOut() const override {
		return false;
	}

	// Everything below is a no-op / unreachable on the cache-serve path (the
	// worker RPC surface). A cache hit skips bind/init entirely.
	void SetTickFilterState(shared_ptr<TickFilterState>) override {}
	BindResult PerformBindRpc() override {
		return {};
	}
	void EnsureWorkerSpawned() override {}
	void SetInputSchema(const std::shared_ptr<arrow::Schema> &) override {}
	void UpdateInputSchemaForExecution(const std::shared_ptr<arrow::Schema> &) override {}
	InitResult PerformInit(const BindResult &, const std::vector<int32_t> &,
	                       std::shared_ptr<arrow::Buffer>, std::vector<std::shared_ptr<arrow::Buffer>>,
	                       const std::string &, const std::optional<OrderByHint> &,
	                       const std::optional<TableSampleHint> &, const std::vector<uint8_t> &,
	                       const std::optional<std::vector<uint8_t>> &) override {
		return {};
	}
	void PerformFinalizeInit(const BindResult &) override {}
	void OpenInputWriter() override {}
	void WriteInputBatch(const std::shared_ptr<arrow::RecordBatch> &) override {}
	void CloseInputWriter() override {}
	std::vector<uint8_t> RpcTableBufferingProcess(const std::string &, const std::vector<uint8_t> &,
	                                              const std::shared_ptr<arrow::RecordBatch> &,
	                                              std::optional<int64_t>) override {
		return {};
	}
	std::vector<std::vector<uint8_t>> RpcTableBufferingCombine(const std::string &,
	                                                           const std::vector<uint8_t> &,
	                                                           const std::vector<std::vector<uint8_t>> &) override {
		return {};
	}
	void RpcTableBufferingDestructor(const std::string &, const std::vector<uint8_t> &) override {}
	void CancelStream(const std::vector<uint8_t> &, ClientContext &) override {}

private:
	std::shared_ptr<const VgiResultCacheEntry> entry_;
	// Flattened replay order: pointers into entry_->streams[*].batches.
	std::vector<const VgiCachedBatch *> order_;
	size_t cursor_ = 0;
	bool finished_ = false;
	idx_t last_batch_index_ = DConstants::INVALID_INDEX;
	std::string last_partition_values_bytes_;
	std::string conn_id_hex_ = "cachehit";
	// [S8] For a disk-backed (streaming) entry: the blob file, opened lazily on
	// the first disk-backed batch and held for the connection's life so each
	// batch is a positioned read (one batch resident, RAM flat for >RAM results).
	std::unique_ptr<FileHandle> disk_handle_;
};

} // namespace vgi
} // namespace duckdb
