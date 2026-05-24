// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "duckdb/main/client_context.hpp"

#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_http_client.hpp"
#include "vgi_ifunction_connection.hpp"
#include "vgi_protocol.hpp"

namespace duckdb {
namespace vgi {

struct VgiSecretRequirement;

// ============================================================================
// HttpFunctionConnection - HTTP transport for VGI function connections
// ============================================================================
// Implements the IFunctionConnection interface using stateless HTTP POST requests.
// Each operation (bind, init, exchange) is a separate HTTP request.
// Server-side state is carried via opaque stream_state tokens in batch metadata.

class HttpFunctionConnection : public IFunctionConnection {
public:
	HttpFunctionConnection(const std::string &worker_path, const std::string &function_name,
	                       const ArrowArguments &arguments, const std::vector<uint8_t> &attach_opaque_data,
	                       const std::vector<uint8_t> &transaction_opaque_data,
	                       ClientContext &context, const std::string &function_type = "TABLE",
	                       const std::vector<uint8_t> &global_execution_id = {},
	                       bool worker_debug = false,
	                       const std::map<std::string, Value> &settings = {},
	                       const std::vector<VgiSecretRequirement> &required_secrets = {},
	                       const std::shared_ptr<VgiAttachParameters> &attach_params = nullptr);

	~HttpFunctionConnection() override = default;

	// Set shared state for dynamic filter pushdown via tick batches
	void SetTickFilterState(shared_ptr<TickFilterState> state) override {
		tick_filter_state_ = std::move(state);
	}

	// Bind RPC. See IFunctionConnection::PerformBindRpc.
	BindResult PerformBindRpc() override;
	// HTTP has no spawn step; this is a no-op satisfying the interface.
	void EnsureWorkerSpawned() override {}
	void SetInputSchema(const std::shared_ptr<arrow::Schema> &input_schema) override;
	void UpdateInputSchemaForExecution(const std::shared_ptr<arrow::Schema> &input_schema) override;

	// Init RPC. bind_result must come from a prior PerformBindRpc.
	InitResult PerformInit(const BindResult &bind_result,
	                       const std::vector<int32_t> &projection_ids = {},
	                       std::shared_ptr<arrow::Buffer> pushdown_filters = nullptr,
	                       std::vector<std::shared_ptr<arrow::Buffer>> join_keys = {},
	                       const std::string &phase = "",
	                       const std::optional<OrderByHint> &order_by = std::nullopt,
	                       const std::optional<TableSampleHint> &table_sample = std::nullopt,
	                       const std::vector<uint8_t> &init_opaque_data = {},
	                       const std::optional<std::vector<uint8_t>> &finalize_state_id = std::nullopt) override;
	void PerformFinalizeInit(const BindResult &bind_result) override;

	// Phase 3: Data exchange
	void OpenInputWriter() override;
	void WriteInputBatch(const std::shared_ptr<arrow::RecordBatch> &batch) override;
	void CloseInputWriter() override;
	std::shared_ptr<arrow::RecordBatch> ReadDataBatch() override;
	void CancelStream(const std::vector<uint8_t> &state_token, ClientContext &live_context) override;


	// ========== Table sink+source RPC family ==========
	std::vector<uint8_t>
	RpcTableBufferingProcess(const std::string &function_name,
	                           const std::vector<uint8_t> &execution_id,
	                           const std::shared_ptr<arrow::RecordBatch> &input_batch,
	                           std::optional<int64_t> batch_index = std::nullopt) override;
	std::vector<std::vector<uint8_t>>
	RpcTableBufferingCombine(const std::string &function_name,
	                           const std::vector<uint8_t> &execution_id,
	                           const std::vector<std::vector<uint8_t>> &state_ids) override;
	void
	RpcTableBufferingDestructor(const std::string &function_name,
	                              const std::vector<uint8_t> &execution_id) override;
	std::vector<uint8_t> GetLastStateToken() const override {
		return std::vector<uint8_t>(stream_state_token_.begin(), stream_state_token_.end());
	}
	idx_t GetLastBatchIndex() const override {
		return last_batch_index_;
	}
	const std::string &GetLastPartitionValuesBytes() const override {
		return last_partition_values_bytes_;
	}

	// State queries
	bool IsTableInOut() const override { return input_schema_ != nullptr; }
	bool IsFinished() const override { return data_finished_; }
	void MarkDataFinished() override { data_finished_ = true; }

	// Identity/diagnostics
	std::string GetExecutionIdHex() const override;
	std::string GetAttachOpaqueDataHex() const override;
	std::string GetTransactionOpaqueDataHex() const override;
	std::string GetConnIdHex() const override { return conn_id_hex_; }

	// Lifecycle (no-ops for HTTP — no subprocess to pool)
	int Wait() override { return 0; }
	std::unique_ptr<PooledWorker> ReleaseForPooling() override { return nullptr; }

	// Non-copyable
	HttpFunctionConnection(const HttpFunctionConnection &) = delete;
	HttpFunctionConnection &operator=(const HttpFunctionConnection &) = delete;

private:
	// Per-checkout correlation id (8 hex). Generated at construction.
	std::string conn_id_hex_;
	std::string base_url_;  // e.g. "http://localhost:8000/vgi"
	std::string function_name_;
	std::string function_type_;
	std::shared_ptr<arrow::DataType> arguments_type_;
	std::shared_ptr<arrow::Array> arguments_array_;
	std::vector<uint8_t> attach_opaque_data_;
	std::vector<uint8_t> transaction_opaque_data_;
	std::vector<uint8_t> global_execution_id_;
	ClientContext &context_;
	bool worker_debug_;
	std::map<std::string, Value> settings_;
	std::vector<VgiSecretRequirement> required_secrets_;
	std::shared_ptr<VgiAttachParameters> attach_params_;  // for auth()

	// State tracking. Bind is just an RPC; nothing tracked on the connection.
	bool init_done_ = false;
	bool data_finished_ = false;
	std::vector<uint8_t> execution_id_;

	// Output schema captured at PerformInit time (from the BindResult passed
	// in). Used purely as a fallback during exchange-mode reads when the
	// server returns no batches: we synthesize a 0-row batch with this
	// schema rather than nullptr, so downstream callers see an empty result
	// instead of EOS. Not bind state — just streaming-shape data.
	std::shared_ptr<arrow::Schema> cached_output_schema_;

	// Input schema for exchange mode
	std::shared_ptr<arrow::Schema> input_schema_;

	// Server capabilities (lazy-discovered)
	ServerCapabilities capabilities_;

	// HTTP streaming state
	std::string stream_state_token_;
	// Raw vgi_batch_index value parsed off the most recent data batch's
	// custom_metadata, or INVALID if absent. Validated in
	// VgiTableFunctionImpl's InstallBatch on the consumer thread.
	idx_t last_batch_index_ = DConstants::INVALID_INDEX;
	// Base64-decoded raw IPC bytes from the most recent data batch's
	// ``vgi_partition_values#b64`` metadata. Empty when the key is
	// absent. IPC decode happens in InstallBatch on the consumer thread.
	std::string last_partition_values_bytes_;
	std::vector<std::shared_ptr<arrow::RecordBatch>> buffered_batches_;
	// Parallel to ``buffered_batches_``: ``vgi_batch_index`` parsed off
	// each batch's custom_metadata at buffer time (or INVALID if absent).
	// We must capture this here because ``ReadDataBatch`` later returns
	// batches from the buffer WITHOUT the original wire metadata.
	std::vector<idx_t> buffered_batch_indexes_;
	// Parallel to ``buffered_batches_``: ``vgi_partition_values#b64``
	// base64-decoded bytes per batch (empty when absent). Same reason
	// as ``buffered_batch_indexes_``: the buffered path returns batches
	// without their wire metadata, so we stash the bytes here.
	std::vector<std::string> buffered_partition_values_bytes_;
	size_t buffered_batch_index_ = 0;
	bool is_producer_mode_ = false;

	// Exchange mode pending input
	std::shared_ptr<arrow::RecordBatch> pending_input_;
	bool input_writer_opened_ = false;
	bool input_writer_closed_ = false;

	// Dynamic filter state for tick-based pushdown
	shared_ptr<TickFilterState> tick_filter_state_;

	// Helper: serialize a batch with optional stream_state token as metadata
	std::vector<uint8_t> SerializeBatchWithState(const std::shared_ptr<arrow::RecordBatch> &batch,
	                                              const std::shared_ptr<arrow::Schema> &schema);

	// Helper: extract stream_state from batch metadata, strip it, return the cleaned batch
	std::shared_ptr<arrow::RecordBatch> ExtractStreamState(
	    const std::shared_ptr<arrow::RecordBatch> &batch,
	    const std::shared_ptr<arrow::KeyValueMetadata> &metadata);

	// Helper: read all data batches from an IPC stream buffer (starting at offset)
	void BufferDataBatches(const std::string &response_body, size_t offset = 0);
};

} // namespace vgi
} // namespace duckdb
