// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>


#include "duckdb/common/types/value.hpp"

#include "vgi_arrow_utils.hpp"
#include "vgi_ifunction_connection.hpp"
#include "vgi_protocol.hpp"
#include "vgi_rpc_client.hpp"  // UnaryResponseResult (return of WebWorkerInvokeUnary)
#include "vgi_rpc_types.hpp" // CopyFromBindContext/CopyToBindContext (complete type for optional<> members)

namespace duckdb {

class ClientContext;

namespace vgi {

struct VgiSecretRequirement;

// One unary RPC (catalog RPCs: ATTACH, discovery) over the `worker:` SAB
// transport on a transient slot: request + EOS, one response, release. Used by
// InvokePooledUnaryRpc for worker: and browser iroh: locations.
UnaryResponseResult WebWorkerInvokeUnary(ClientContext &context, const std::string &worker_path,
                                         const std::string &method_name,
                                         const VgiBatchPtr &params,
                                         const VgiProtocolId &protocol);

// The subprocess FunctionConnection's protocol (bind/init, producer ticks,
// exchange input, one-ahead tick pipeline) driving an in-browser Web Worker over
// a SharedArrayBuffer duplex ring slot (vgi_sab_abi.hpp) instead of pipes:
// requests and the tick/input VgiIpcWriter go to a SabOutputStream, responses
// are read from a SabInputStream. No worker pool, stderr drainer or shm side
// channel: the JS bridge owns the worker's lifecycle. The connection owns one
// slot from EnsureWorkerSpawned to destruction.
//
// Definitions are compiled only for wasm (__EMSCRIPTEN__) and the native SAB
// unit test (VGI_SAB_NATIVE_TEST); the guard is on the .cpp.
class WebWorkerFunctionConnection : public IFunctionConnection {
public:
	// Does not open the slot yet. `location` is the `worker:` LOCATION
	// (canonicalized for the JS bridge). global_execution_id: for secondary
	// workers, the primary worker's InitResult id.
	WebWorkerFunctionConnection(const std::string &location, const std::string &function_name,
	                            const ArrowArguments &arguments, const std::vector<uint8_t> &attach_opaque_data,
	                            const std::vector<uint8_t> &transaction_opaque_data, ClientContext &context,
	                            const std::string &function_type = "TABLE",
	                            const std::vector<uint8_t> &global_execution_id = {},
	                            const std::map<std::string, Value> &settings = {},
	                            const std::vector<VgiSecretRequirement> &required_secrets = {});

	~WebWorkerFunctionConnection() override;

	// Reset per-init stream state, keeping the slot. Like PerformFinalizeInit,
	// it must leave the rings between frames, not mid-frame.
	void ResetForNextSplit() override;


	void SetTickFilterState(shared_ptr<TickFilterState> state) override {
		tick_filter_state_ = std::move(state);
		tick_filter_cursor_.Reset();
	}

	BindResult PerformBindRpc() override;
	void EnsureWorkerSpawned() override;

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

	// Writes a zero-row VGI_RPC_CANCEL_KEY tick on the producer tick stream
	// (no-op in exchange mode). state_token and live_context are unused.
	void CancelStream(const std::vector<uint8_t> &state_token, ClientContext &live_context) override;

	// SAB: no state token (the lockstep ring is self-addressing).
	std::vector<uint8_t> GetLastStateToken() const override {
		return {};
	}

	void CloseInputWriter() override;

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

	bool IsTableInOut() const override {
		return input_schema_ != nullptr;
	}

	VgiBatchPtr ReadDataBatch() override;

	bool IsFinished() const override {
		return data_finished_;
	}

	void MarkDataFinished() override {
		data_finished_ = true;
	}

	std::string GetExecutionIdHex() const override;
	std::string GetAttachOpaqueDataDigest() const override;
	std::string GetTransactionOpaqueDataDigest() const override;

	std::string GetConnIdHex() const override {
		return conn_id_hex_;
	}

	// No OS process backs a Web Worker connection.
	std::optional<pid_t> GetSubprocessPid() const override {
		return std::nullopt;
	}

	// No worker process to wait on.
	int Wait() override {
		return 0;
	}

	// Never pooled; returns nullptr. Out-of-line because unique_ptr<PooledWorker>
	// needs the complete type for its deleter (libstdc++ requires it even for a
	// null return), and only the .cpp includes vgi_worker_pool.hpp.
	std::unique_ptr<PooledWorker> ReleaseForPooling() override;

	WebWorkerFunctionConnection(const WebWorkerFunctionConnection &) = delete;
	WebWorkerFunctionConnection &operator=(const WebWorkerFunctionConnection &) = delete;
	WebWorkerFunctionConnection(WebWorkerFunctionConnection &&) = delete;
	WebWorkerFunctionConnection &operator=(WebWorkerFunctionConnection &&) = delete;

private:
	// Per-connection correlation id, generated at construction.
	std::string conn_id_hex_;
	// Canonical browser-worker target; also the worker label in errors/logs.
	std::string location_;
	std::string function_name_;
	std::string function_type_; // "TABLE", "SCALAR", "AGGREGATE"
	//! One-row {args: struct} batch (ArrowArguments::batch); null when there are none.
	VgiBatchPtr arguments_batch_;
	std::vector<uint8_t> attach_opaque_data_;
	std::vector<uint8_t> transaction_opaque_data_;
	std::vector<uint8_t> global_execution_id_;
	// Client-minted per-substream id sent on every InitRequest (INPUT and
	// FINALIZE). Empty = none.
	std::vector<uint8_t> substream_id_;
	ClientContext &context_;
	std::map<std::string, Value> settings_;
	std::vector<VgiSecretRequirement> required_secrets_;

	// -1 until EnsureWorkerSpawned opens it; released on destruction (or by
	// ReadDataBatch's abandon path). Non-negative means the worker exists.
	int slot_ = -1;
	// Byte offset of this canonical target's independent ABI-v1 region. Zero is
	// the native-test region; wasm regions are nonzero malloc offsets.
	int region_offset_ = 0;

	bool init_done_ = false;
	bool data_finished_ = false;

	std::vector<uint8_t> execution_id_;

	bool is_producer_mode_ = false;
	VgiSchemaPtr tick_schema_; // set only in producer mode

	// Opened in PerformInit (producer) or lazily by ReadDataBatch (exchange).
	std::shared_ptr<VgiInputStream> data_stream_;
	std::shared_ptr<VgiIpcReader> data_reader_;

	// Set = exchange mode (scalar / table-in-out).
	VgiSchemaPtr input_schema_;

	// Bind-request context (empty = none): time-travel AT clause, owning schema,
	// argument names, COPY FROM / TO.
	std::string at_unit_;
	std::string at_value_;
	std::string schema_name_;
	std::optional<std::vector<std::optional<std::string>>> argument_names_;
	std::optional<CopyFromBindContext> copy_from_;
	std::optional<CopyToBindContext> copy_to_;

	// Tick writer (producer) or input writer (exchange); owns its sink.
	std::shared_ptr<VgiIpcWriter> input_writer_;
	bool input_writer_opened_ = false;
	bool input_writer_closed_ = false;

	// Dynamic filter pushdown via tick metadata (shared with the scan operator).
	shared_ptr<TickFilterState> tick_filter_state_;
	// Which delta of tick_filter_state_ the current stream has been sent.
	TickFilterCursor tick_filter_cursor_;

	// Raw metadata parsed off the most recent data batch's custom_metadata.
	idx_t last_batch_index_ = DConstants::INVALID_INDEX;
	std::string last_partition_values_bytes_;
	VgiCacheControl last_cache_control_;
	// Whether the most recent data batch said vgi.status: HAVE_MORE_OUTPUT.
	bool last_more_output_ = false;

public:
	idx_t GetLastBatchIndex() const override {
		return last_batch_index_;
	}
	const std::string &GetLastPartitionValuesBytes() const override {
		return last_partition_values_bytes_;
	}
	VgiCacheControl GetLastCacheControl() const override {
		return last_cache_control_;
	}
	bool GetLastMoreOutput() const override {
		return last_more_output_;
	}
	void WriteContinuationTick() override;
	void SetConditionalRequest(const std::string &if_none_match,
	                           const std::string &if_modified_since) override {
		cond_if_none_match_ = if_none_match;
		cond_if_modified_since_ = if_modified_since;
	}

private:
	// Conditional-revalidation validators, sent on a producer stream's first
	// tick. Empty = no key emitted.
	std::string cond_if_none_match_;
	std::string cond_if_modified_since_;
};

} // namespace vgi
} // namespace duckdb
