// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>


#include "duckdb/main/client_context.hpp"

#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_metadata.hpp"
#include "vgi_http_client.hpp"
#include "vgi_ifunction_connection.hpp"
#include "vgi_rpc_types.hpp" // CopyFromBindContext (complete type for optional<> member)
#include "vgi_protocol.hpp"
#include "vgi_worker_pool.hpp" // complete PooledWorker for inline ReleaseForPooling()

namespace duckdb {
namespace vgi {

struct VgiSecretRequirement;

// IFunctionConnection over stateless HTTP POSTs: bind, init and each exchange
// are separate requests; server-side state rides opaque stream_state /
// call_state tokens in batch metadata.

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

	// Resets per-init stream state but keeps http_client_ (the keep-alive client
	// is the connection; dropping it costs a TCP+TLS handshake per split). Also
	// clears call_state_token_, which survives turns but must not address the
	// previous split's server-side call state.
	void ResetForNextSplit() override;

	void SetTickFilterState(shared_ptr<TickFilterState> state) override {
		tick_filter_state_ = std::move(state);
		tick_filter_cursor_.Reset();
	}

	BindResult PerformBindRpc() override;
	// HTTP has no spawn step.
	void EnsureWorkerSpawned() override {}
	void SetInputSchema(const VgiSchemaPtr &input_schema) override;
	void SetSchemaName(const std::string &schema_name) override {
		schema_name_ = schema_name;
	}
	void SetArgumentNames(
	    const std::optional<std::vector<std::optional<std::string>>> &argument_names) override {
		argument_names_ = argument_names;
	}

	void SetAtClause(const std::string &at_unit, const std::string &at_value) override {
		at_unit_ = at_unit;
		at_value_ = at_value;
	}
	void SetSubstreamId(const std::vector<uint8_t> &substream_id) override {
		substream_id_ = substream_id;
	}
	void SetCopyFromContext(const CopyFromBindContext &copy_from) override {
		copy_from_ = copy_from;
	}
	void SetCopyToContext(const CopyToBindContext &copy_to) override {
		copy_to_ = copy_to;
	}
	void UpdateInputSchemaForExecution(const VgiSchemaPtr &input_schema) override;

	InitResult PerformInit(const BindResult &bind_result,
	                       const std::vector<int32_t> &projection_ids = {},
	                       VgiBufferPtr pushdown_filters = nullptr,
	                       std::vector<VgiBufferPtr> join_keys = {},
	                       const std::string &phase = "",
	                       const std::optional<OrderByHint> &order_by = std::nullopt,
	                       const std::optional<TableSampleHint> &table_sample = std::nullopt,
	                       const std::vector<uint8_t> &init_opaque_data = {},
	                       const std::optional<std::vector<uint8_t>> &finalize_state_id = std::nullopt,
	                       const std::vector<std::string> &split_tokens = {}) override;
	void PerformFinalizeInit(const BindResult &bind_result) override;

	void OpenInputWriter() override;
	void WriteInputBatch(const VgiBatchPtr &batch) override;
	void CloseInputWriter() override;
	VgiBatchPtr ReadDataBatch() override;
	void CancelStream(const std::vector<uint8_t> &state_token, ClientContext &live_context) override;


	// Buffered table functions (table_buffering_*).
	std::vector<uint8_t>
	RpcTableBufferingProcess(const std::string &function_name,
	                           const std::vector<uint8_t> &execution_id,
	                           const VgiBatchPtr &input_batch,
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
	const std::string &GetLastParentRowBytes() const override {
		return last_parent_row_bytes_;
	}
	bool GetLastMoreOutput() const override {
		return last_more_output_;
	}
	void WriteContinuationTick() override;
	VgiCacheControl GetLastCacheControl() const override {
		return last_cache_control_;
	}
	void SetConditionalRequest(const std::string &if_none_match,
	                           const std::string &if_modified_since) override {
		cond_if_none_match_ = if_none_match;
		cond_if_modified_since_ = if_modified_since;
		cond_sent_ = false;
	}

	bool IsTableInOut() const override { return input_schema_ != nullptr; }
	bool IsFinished() const override { return data_finished_; }
	void MarkDataFinished() override { data_finished_ = true; }

	std::string GetExecutionIdHex() const override;
	std::string GetAttachOpaqueDataDigest() const override;
	std::string GetTransactionOpaqueDataDigest() const override;
	std::string GetConnIdHex() const override { return conn_id_hex_; }

	// No subprocess to wait for or pool.
	int Wait() override { return 0; }
	std::unique_ptr<PooledWorker> ReleaseForPooling() override { return nullptr; }

	HttpFunctionConnection(const HttpFunctionConnection &) = delete;
	HttpFunctionConnection &operator=(const HttpFunctionConnection &) = delete;

private:
	// Per-connection correlation id, generated at construction.
	std::string conn_id_hex_;
	std::string base_url_;  // e.g. "http://localhost:8000/vgi"
	std::string function_name_;
	std::string function_type_;
	//! One-row {args: struct} batch (ArrowArguments::batch); null when there are none.
	VgiBatchPtr arguments_batch_;
	std::vector<uint8_t> attach_opaque_data_;
	std::vector<uint8_t> transaction_opaque_data_;
	std::vector<uint8_t> global_execution_id_;
	// Client-minted per-substream id (parallel streaming table-in-out), sent on
	// every InitRequest. Empty = none.
	std::vector<uint8_t> substream_id_;
	ClientContext &context_;
	// Keep-alive client for this connection's requests (bind, /init, every
	// exchange), created lazily. HTTPClient is not thread-safe, so the
	// off-thread CancelStream must not use it (it uses a fresh client).
	duckdb::unique_ptr<HTTPClient> http_client_;
	bool worker_debug_;
	std::map<std::string, Value> settings_;
	std::vector<VgiSecretRequirement> required_secrets_;
	std::shared_ptr<VgiAttachParameters> attach_params_;

	bool init_done_ = false;
	bool data_finished_ = false;
	std::vector<uint8_t> execution_id_;

	// From the BindResult at PerformInit. An exchange reply with no batches
	// returns a 0-row batch of this schema, so callers see an empty result
	// rather than EOS. vgi_query also checks its output stream against it.
	VgiSchemaPtr cached_output_schema_;

	// Set = exchange mode.
	VgiSchemaPtr input_schema_;

	// Bind-request context (empty = none): time-travel AT clause, owning schema,
	// argument names, COPY FROM / TO.
	std::string at_unit_;
	std::string at_value_;
	std::string schema_name_;
	std::optional<std::vector<std::optional<std::string>>> argument_names_;
	std::optional<CopyFromBindContext> copy_from_;
	std::optional<CopyToBindContext> copy_to_;

	// Harvested from every response's capability headers and shared per catalog
	// via attach_params_; ReadDataBatch probes (HttpDiscoverCapabilities) only
	// when no snapshot exists or a probed one expired.
	ServerCapabilities capabilities_;

	// Best known snapshot, seeding a request's in/out capability slot so the
	// request Content-Encoding is right on the first try. Falls back to the
	// per-catalog cache.
	ServerCapabilities CurrentCapabilities() {
		if (!capabilities_.discovered && attach_params_) {
			capabilities_ = attach_params_->LoadServerCapabilities();
		}
		return capabilities_;
	}

	// Store harvested capabilities here and in the per-catalog cache.
	void PublishHarvestedCapabilities(const ServerCapabilities &caps) {
		if (!caps.discovered) {
			return;
		}
		capabilities_ = caps;
		if (attach_params_) {
			attach_params_->StoreServerCapabilities(caps);
		}
	}

	std::string stream_state_token_;
	// Set from the /init response and echoed on every later request of the
	// stream. Not cleared per turn: the worker sends it only once, and later
	// requests need it to resolve their call state on a cold worker.
	std::string call_state_token_;
	// Per-batch metadata of the most recently returned data batch, raw (the
	// consumer, VgiTableFunctionImpl's InstallBatch, validates/decodes):
	// vgi_batch_index (INVALID if absent), base64-decoded
	// vgi_partition_values#b64 IPC bytes and vgi_rpc.parent_row#b64 int32[]
	// (empty if absent), and the vgi.cache.* advertisement.
	idx_t last_batch_index_ = DConstants::INVALID_INDEX;
	std::string last_partition_values_bytes_;
	std::string last_parent_row_bytes_;
	std::vector<VgiBatchPtr> buffered_batches_;
	// Parallel to buffered_batches_: the same metadata captured at buffer time,
	// since buffered batches are returned without their wire metadata.
	std::vector<idx_t> buffered_batch_indexes_;
	std::vector<std::string> buffered_partition_values_bytes_;
	std::vector<std::string> buffered_parent_row_bytes_;
	VgiCacheControl last_cache_control_;
	std::vector<VgiCacheControl> buffered_cache_controls_;
	size_t buffered_batch_index_ = 0;
	// Conditional-revalidation validators, sent once (on the init request, or
	// else the first exchange).
	std::string cond_if_none_match_;
	std::string cond_if_modified_since_;
	bool cond_sent_ = false;
	bool is_producer_mode_ = false;

	VgiBatchPtr pending_input_;
	// pending_input_ is a continuation tick: its request carries vgi.status: CONTINUE.
	bool pending_tick_ = false;
	// Whether the most recent exchange reply said vgi.status: HAVE_MORE_OUTPUT.
	bool last_more_output_ = false;
	bool input_writer_opened_ = false;
	bool input_writer_closed_ = false;

	// Dynamic filter pushdown via tick metadata.
	shared_ptr<TickFilterState> tick_filter_state_;
	// Which delta of tick_filter_state_ the current stream has been sent.
	TickFilterCursor tick_filter_cursor_;

	// Serialize a request batch with the stream/call tokens, filter delta,
	// validators and continuation status as metadata.
	std::vector<uint8_t> SerializeBatchWithState(const VgiBatchPtr &batch,
	                                              const VgiSchemaPtr &schema);

	// Capture stream_state / call_state tokens from metadata; returns `batch`.
	VgiBatchPtr ExtractStreamState(
	    const VgiBatchPtr &batch,
	    const VgiMetadataPtr &metadata);

	// Buffer all data batches of the IPC stream in `owned` from `offset`. The
	// batches zero-copy reference `owned`, so the payload is never copied.
	void BufferDataBatches(VgiBufferPtr owned, size_t offset = 0);
};

} // namespace vgi
} // namespace duckdb
