// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include "duckdb/common/types/value.hpp"

#include "vgi_arrow_utils.hpp"
#include "vgi_ifunction_connection.hpp"
#include "vgi_protocol.hpp"
#include "vgi_rpc_client.hpp"  // UnaryResponseResult (return of WebWorkerInvokeUnary)
#include "vgi_rpc_types.hpp" // CopyFromBindContext/CopyToBindContext (complete type for optional<> members)

namespace duckdb {

class ClientContext;

namespace vgi {

// Forward declarations
struct VgiSecretRequirement;

// Standalone unary RPC over the `worker:` SAB transport, for catalog RPCs (ATTACH
// + discovery) which are request -> single response. Claims a transient slot,
// writes the request + EOS, reads the one response, releases the slot. Called from
// InvokePooledUnaryRpc's worker: branch. Definition in the .cpp (guarded on
// __EMSCRIPTEN__ || VGI_SAB_NATIVE_TEST).
UnaryResponseResult WebWorkerInvokeUnary(ClientContext &context, const std::string &worker_path,
                                         const std::string &method_name,
                                         const std::shared_ptr<arrow::RecordBatch> &params,
                                         const std::string &protocol_version_override);

// ============================================================================
// WebWorkerFunctionConnection - vgi_rpc Protocol over the `worker:` SAB transport
// ============================================================================
//
// A focused port of the subprocess FunctionConnection for the in-browser
// `worker:` transport. Instead of a spawned child process wired over stdin/
// stdout pipe fds, this connection drives an in-browser Web Worker across a
// SharedArrayBuffer duplex ring (the SAB slot — see vgi_sab_abi.hpp). The
// streaming structure is identical to the subprocess path — same bind→init
// dance, same producer (tick) / exchange (input) modes, same one-ahead tick
// pipeline in ReadDataBatch — only the byte transport changes:
//
//   * Requests: SerializeRpcRequest(method, params) bytes written to a
//     SabOutputStream (client->worker ring), replacing WriteRpcRequest(fd, …).
//   * Responses: a RecordBatchStreamReader over a SabInputStream (worker->client
//     ring), replacing ReadUnaryResponse(fd)/ReadStreamHeader(fd).
//   * Tick / input streams: MakeStreamWriter over a SabOutputStream, exactly as
//     the subprocess path opens them over an FdOutputStream.
//
// There is no subprocess, no worker pool, no stderr drainer, and no shared-
// memory side-channel here — the JS bridge owns the worker's lifecycle and the
// SAB ring is the transfer medium. The connection owns exactly one SAB slot
// (vgi_wasm_slot_open at first use → vgi_wasm_slot_release at destruction);
// CloseInputWriter signals ring EOF via vgi_wasm_slot_write_eos.
//
// Compiled only for the wasm extension (__EMSCRIPTEN__) and the native SAB unit
// test (VGI_SAB_NATIVE_TEST) — the guard lives on the .cpp, not this header.
class WebWorkerFunctionConnection : public IFunctionConnection {
public:
	// Create connection parameters (does not open the SAB slot yet).
	// `location` is the full `worker:<url>` LOCATION; the stripped URL is what
	// the JS bridge resolves to a Web Worker. arguments: Arrow struct array
	// with fields named positional_0, positional_1, etc. global_execution_id:
	// for secondary workers, pass the ID from the primary worker's InitResult.
	// function_type: "TABLE", "SCALAR", or "AGGREGATE".
	WebWorkerFunctionConnection(const std::string &location, const std::string &function_name,
	                            const ArrowArguments &arguments, const std::vector<uint8_t> &attach_opaque_data,
	                            const std::vector<uint8_t> &transaction_opaque_data, ClientContext &context,
	                            const std::string &function_type = "TABLE",
	                            const std::vector<uint8_t> &global_execution_id = {},
	                            const std::map<std::string, Value> &settings = {},
	                            const std::vector<VgiSecretRequirement> &required_secrets = {});

	~WebWorkerFunctionConnection() override;

	// Set shared state for dynamic filter pushdown via tick batches
	void SetTickFilterState(shared_ptr<TickFilterState> state) override {
		tick_filter_state_ = std::move(state);
	}

	BindResult PerformBindRpc() override;
	void EnsureWorkerSpawned() override;

	void SetInputSchema(const std::shared_ptr<arrow::Schema> &input_schema) override;

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

	void UpdateInputSchemaForExecution(const std::shared_ptr<arrow::Schema> &input_schema) override;

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

	// ========================================================================
	// Input Data (Scalar and Table-In-Out Functions)
	// ========================================================================

	void OpenInputWriter() override;
	void WriteInputBatch(const std::shared_ptr<arrow::RecordBatch> &batch) override;

	// Cancel the current stream by writing a zero-row batch tagged with
	// VGI_RPC_CANCEL_KEY custom metadata on the input/tick writer. state_token +
	// live_context unused (the SAB slot is self-addressing; no context-bound
	// logging/HTTP on this path).
	void CancelStream(const std::vector<uint8_t> &state_token, ClientContext &live_context) override;

	// SAB: no state token (the lockstep ring is self-addressing).
	std::vector<uint8_t> GetLastStateToken() const override {
		return {};
	}

	void CloseInputWriter() override;

	// ========== Table sink+source (buffered) RPC family ==========
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

	bool IsTableInOut() const override {
		return input_schema_ != nullptr;
	}

	std::shared_ptr<arrow::RecordBatch> ReadDataBatch() override;

	bool IsFinished() const override {
		return data_finished_;
	}

	void MarkDataFinished() override {
		data_finished_ = true;
	}

	std::string GetExecutionIdHex() const override;
	std::string GetAttachOpaqueDataHex() const override;
	std::string GetTransactionOpaqueDataHex() const override;

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

	// The SAB transport is never pooled (the JS bridge owns worker lifecycle).
	std::unique_ptr<PooledWorker> ReleaseForPooling() override {
		return nullptr;
	}

	// Non-copyable and non-movable (contains reference)
	WebWorkerFunctionConnection(const WebWorkerFunctionConnection &) = delete;
	WebWorkerFunctionConnection &operator=(const WebWorkerFunctionConnection &) = delete;
	WebWorkerFunctionConnection(WebWorkerFunctionConnection &&) = delete;
	WebWorkerFunctionConnection &operator=(WebWorkerFunctionConnection &&) = delete;

private:
	// Per-checkout correlation id (8 hex). Generated at construction.
	std::string conn_id_hex_;
	// The stripped `worker:` URL the JS bridge resolves. Used as the worker
	// label in error messages / logging (there is no filesystem path here).
	std::string location_;
	std::string function_name_;
	std::string function_type_; // "TABLE", "SCALAR", "AGGREGATE"
	std::shared_ptr<arrow::DataType> arguments_type_;
	std::shared_ptr<arrow::Array> arguments_array_;
	std::vector<uint8_t> attach_opaque_data_;
	std::vector<uint8_t> transaction_opaque_data_;
	std::vector<uint8_t> global_execution_id_;
	// Stable client-minted per-substream id folded onto every InitRequest this
	// connection builds (INPUT + FINALIZE). Empty = none. Set via SetSubstreamId.
	std::vector<uint8_t> substream_id_;
	ClientContext &context_;
	std::map<std::string, Value> settings_;
	std::vector<VgiSecretRequirement> required_secrets_;

	// The owned SAB slot. -1 until EnsureWorkerSpawned opens it; released on
	// destruction. A non-negative slot_ is the "worker exists" flag (analog of
	// the subprocess proc_).
	int slot_ = -1;

	// Streaming-state flags. Bind is just an RPC; no state is tracked for it.
	bool init_done_ = false;
	bool data_finished_ = false;

	// Execution ID from GlobalInitResponse
	std::vector<uint8_t> execution_id_;

	// Producer mode (true for table functions - tick-based data exchange)
	bool is_producer_mode_ = false;
	std::shared_ptr<arrow::Schema> tick_schema_; // Empty schema for tick batches

	// Data stream reader (opened during PerformInit / lazily, used for ReadDataBatch)
	std::shared_ptr<arrow::io::InputStream> data_stream_;
	std::shared_ptr<arrow::ipc::RecordBatchStreamReader> data_reader_;

	// Input schema for table-in-out/scalar functions (nullptr for regular table functions)
	std::shared_ptr<arrow::Schema> input_schema_;

	// Time-travel AT clause (empty = none) for the bind request. See SetAtClause.
	std::string at_unit_;
	std::string at_value_;

	// COPY ... FROM / TO context for the bind request (empty = none).
	std::optional<CopyFromBindContext> copy_from_;
	std::optional<CopyToBindContext> copy_to_;

	// Input data writer for exchange-mode functions (its SabOutputStream sink is
	// held alive by the writer for the streaming phase's lifetime).
	std::shared_ptr<arrow::ipc::RecordBatchWriter> input_writer_;
	bool input_writer_opened_ = false;
	bool input_writer_closed_ = false;

	// Dynamic filter state for tick-based pushdown (shared with the scan operator)
	shared_ptr<TickFilterState> tick_filter_state_;

	// Raw metadata parsed off the most recent data batch's custom_metadata.
	idx_t last_batch_index_ = DConstants::INVALID_INDEX;
	std::string last_partition_values_bytes_;
	VgiCacheControl last_cache_control_;

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
	void SetConditionalRequest(const std::string &if_none_match,
	                           const std::string &if_modified_since) override {
		cond_if_none_match_ = if_none_match;
		cond_if_modified_since_ = if_modified_since;
	}

private:
	// Conditional-revalidation validators (M6) to attach to the next init
	// request's custom_metadata. Empty = no key emitted.
	std::string cond_if_none_match_;
	std::string cond_if_modified_since_;
};

} // namespace vgi
} // namespace duckdb
