// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"

// CachedReplayConnection — an IFunctionConnection that replays a cached result
// instead of talking to a worker. On a cache hit the local state gets one in
// place of a real connection, so the scan path runs unchanged: ReadDataBatch()
// returns the cached batches and the stored per-batch vgi_meta is replayed via
// GetLastBatchIndex()/GetLastPartitionValuesBytes().

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
	bool IsCachedReplay() const override {
		return true;
	}

public:
	// Replays every batch of `entry` as one stream. When any cached batch has a
	// batch_index, replay is in non-decreasing batch_index order (InstallBatch
	// enforces per-stream monotonicity); otherwise substream/arrival order.
	explicit CachedReplayConnection(std::shared_ptr<const VgiResultCacheEntry> entry);
	// Out-of-line (the .cpp holds a unique_ptr<FileHandle> of an incomplete type).
	~CachedReplayConnection() override;

	// The only methods the scan actually drives on a hit:
	VgiBatchPtr ReadDataBatch() override;
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
	// Out-of-line: `unique_ptr<PooledWorker>` needs the complete type for its
	// deleter even for a null return (MSVC enforces this).
	std::unique_ptr<PooledWorker> ReleaseForPooling() override;
	std::vector<uint8_t> GetLastStateToken() const override {
		return {};
	}
	std::string GetExecutionIdHex() const override {
		return "";
	}
	std::string GetAttachOpaqueDataDigest() const override {
		return "";
	}
	std::string GetTransactionOpaqueDataDigest() const override {
		return "";
	}
	bool IsTableInOut() const override {
		return false;
	}

	// Everything below is a no-op / unreachable on the cache-serve path (the
	// worker RPC surface). A cache hit skips bind/init entirely.
	void SetTickFilterState(shared_ptr<TickFilterState>) override {}

	// Unreachable: a cache hit skips the plan RPC and the split path refuses
	// revalidation, so a replay never enters the split drain loop. Throws rather
	// than no-ops: otherwise the WHOLE entry would replay once per split.
	void ResetForNextSplit() override {
		throw InternalException("CachedReplayConnection::ResetForNextSplit: a cached replay must never "
		                        "enter the split drain loop");
	}
	BindResult PerformBindRpc() override {
		return {};
	}
	void EnsureWorkerSpawned() override {}
	void SetInputSchema(const VgiSchemaPtr &) override {}
	void UpdateInputSchemaForExecution(const VgiSchemaPtr &) override {}
	InitResult PerformInit(const BindResult &, const std::vector<int32_t> &,
	                       VgiBufferPtr, std::vector<VgiBufferPtr>,
	                       const std::string &, const std::optional<OrderByHint> &,
	                       const std::optional<TableSampleHint> &, const std::vector<uint8_t> &,
	                       const std::optional<std::vector<uint8_t>> &,
	                       const std::vector<std::string> &) override {
		return {};
	}
	void PerformFinalizeInit(const BindResult &) override {}
	void OpenInputWriter() override {}
	void WriteInputBatch(const VgiBatchPtr &) override {}
	void CloseInputWriter() override {}
	std::vector<uint8_t> RpcTableBufferingProcess(const std::string &, const std::vector<uint8_t> &,
	                                              const VgiBatchPtr &,
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
	// Disk-backed entry: the blob file, opened lazily and held for the
	// connection's life so each batch is a positioned read (one batch resident).
	std::unique_ptr<FileHandle> disk_handle_;
};

} // namespace vgi
} // namespace duckdb
