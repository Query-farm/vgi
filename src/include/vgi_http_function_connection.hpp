#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "duckdb/main/client_context.hpp"

#include "vgi_arrow_utils.hpp"
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
	                       const ArrowArguments &arguments, const std::vector<uint8_t> &attach_id,
	                       const std::vector<uint8_t> &transaction_id,
	                       ClientContext &context, const std::string &function_type = "TABLE",
	                       const std::vector<uint8_t> &global_execution_id = {},
	                       bool worker_debug = false,
	                       const std::map<std::string, Value> &settings = {},
	                       const std::vector<VgiSecretRequirement> &required_secrets = {});

	~HttpFunctionConnection() override = default;

	// Set shared state for dynamic filter pushdown via tick batches
	void SetTickFilterState(shared_ptr<TickFilterState> state) override {
		tick_filter_state_ = std::move(state);
	}

	// Phase 1: Bind
	BindResult PerformBindFull() override;
	void SetInputSchema(const std::shared_ptr<arrow::Schema> &input_schema) override;
	void UpdateInputSchemaForExecution(const std::shared_ptr<arrow::Schema> &input_schema) override;

	// Phase 2: Init
	InitResult PerformInit(const std::vector<int32_t> &projection_ids = {},
	                       std::shared_ptr<arrow::Buffer> pushdown_filters = nullptr,
	                       std::shared_ptr<arrow::Buffer> join_keys = nullptr,
	                       const std::string &phase = "") override;
	void PerformFinalizeInit() override;

	// Phase 3: Data exchange
	void OpenInputWriter() override;
	void WriteInputBatch(const std::shared_ptr<arrow::RecordBatch> &batch) override;
	void CloseInputWriter() override;
	std::shared_ptr<arrow::RecordBatch> ReadDataBatch() override;

	// State queries
	bool IsTableInOut() const override { return input_schema_ != nullptr; }
	bool IsFinished() const override { return data_finished_; }
	void MarkDataFinished() override { data_finished_ = true; }

	// Identity/diagnostics
	pid_t GetPid() const override { return -1; }
	std::string GetExecutionIdHex() const override;
	std::string GetAttachIdHex() const override;

	// Lifecycle (no-ops for HTTP)
	int Wait() override { return 0; }
	bool CanBePooled() const override { return false; }
	std::unique_ptr<PooledWorker> ReleaseForPooling() override { return nullptr; }

	// Non-copyable
	HttpFunctionConnection(const HttpFunctionConnection &) = delete;
	HttpFunctionConnection &operator=(const HttpFunctionConnection &) = delete;

private:
	std::string base_url_;  // e.g. "http://localhost:8000/vgi"
	std::string function_name_;
	std::string function_type_;
	std::shared_ptr<arrow::DataType> arguments_type_;
	std::shared_ptr<arrow::Array> arguments_array_;
	std::vector<uint8_t> attach_id_;
	std::vector<uint8_t> transaction_id_;
	std::vector<uint8_t> global_execution_id_;
	ClientContext &context_;
	bool worker_debug_;
	std::map<std::string, Value> settings_;
	std::vector<VgiSecretRequirement> required_secrets_;

	// State tracking
	bool bind_done_ = false;
	bool init_done_ = false;
	bool data_finished_ = false;
	BindResult bind_result_;
	std::vector<uint8_t> execution_id_;

	// Input schema for exchange mode
	std::shared_ptr<arrow::Schema> input_schema_;

	// Server capabilities (lazy-discovered)
	ServerCapabilities capabilities_;

	// HTTP streaming state
	std::string stream_state_token_;
	std::vector<std::shared_ptr<arrow::RecordBatch>> buffered_batches_;
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
