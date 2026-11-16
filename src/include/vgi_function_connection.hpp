// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>


#include "duckdb/common/types/value.hpp"

#include "vgi_arrow_utils.hpp"
#include "vgi_ifunction_connection.hpp"
#include "vgi_protocol.hpp"
#include "vgi_rpc_types.hpp" // CopyFromBindContext (complete type for optional<> member)
#include "vgi_stderr_drainer.hpp"
#include "vgi_subprocess.hpp"
#include "vgi_worker_pool.hpp"

namespace duckdb {

class ClientContext;

namespace vgi {

// vgi_attach_parameters.hpp, vgi_iroh_config.hpp, vgi_rpc_client.hpp
struct VgiAttachParameters;
struct VgiSecretRequirement;
struct IrohClientConfig;
struct UnaryResponseResult;
struct StreamHeaderResult;

class FunctionConnection;

// Inputs to AcquireAndBindConnection / AcquireConnectionForInit.
struct FunctionConnectionParams {
	std::shared_ptr<VgiAttachParameters> attach_params;
	std::vector<uint8_t> attach_opaque_data;
	std::string function_name;
	// Catalog schema that owns `function_name`. The same name may be registered
	// in several schemas, so the worker resolves the pair; empty leaves it to a
	// cross-schema lookup by name (non-catalog call sites).
	std::string schema_name;
	ArrowArguments arguments;
	std::vector<uint8_t> transaction_opaque_data;
	std::vector<uint8_t> global_execution_id;  // Empty for primary workers
	std::map<std::string, Value> settings;
	std::vector<vgi::VgiSecretRequirement> required_secrets;
	std::string phase;  // For logging (e.g., "bind", "init_local_secondary")
	std::string function_type = "TABLE";  // "TABLE", "SCALAR", "AGGREGATE"

	// Time travel (AT clause); empty when none. Carried in the bind request (and so
	// in init_call.bind_call). Only the RPC bind path reads these; the inline-bind
	// path passes bind_data.at_* to BuildBindRequestBytes directly.
	std::string at_unit;
	std::string at_value;

	// COPY FROM / COPY TO context, set only for those binds. Carried in the bind
	// request (and init_call.bind_call) for the worker's Copy{From,To}Function.
	std::optional<CopyFromBindContext> copy_from;
	std::optional<CopyToBindContext> copy_to;

	// Input schema for functions with a typed input stream (table-in-out, scalar);
	// applied via SetInputSchema before the first RPC.
	VgiSchemaPtr input_schema;

	// Forwarders to attach_params.
	const std::string &worker_path() const;
	bool worker_debug() const;
	bool use_pool() const;
	const std::string &data_version_spec() const;
	const std::string &implementation_version() const;

	FunctionConnectionParams() = default;
};

struct AcquireAndBindResult {
	std::unique_ptr<IFunctionConnection> connection;
	BindResult bind_result;
};

// Acquire a connection (pooled if possible) and run PerformBindRpc. If a pooled
// worker's bind throws IOException (it died while idle), retry once on a fresh
// connection; a fresh connection's failure propagates.
AcquireAndBindResult AcquireAndBindConnection(ClientContext &context, const FunctionConnectionParams &params);

struct AcquireForInitResult {
	std::unique_ptr<IFunctionConnection> connection;
	// True when the connection came from the worker pool (never for HTTP).
	bool from_pool = false;
};

// Acquire a connection without a bind RPC, for a caller that holds a BindResult
// from the planner-phase bind: the init request carries bind_request_bytes and
// opaque_data, so the worker needs no second bind. The caller owns stale-pool
// retry: if the first RPC throws IOException and from_pool, call again with
// `force_fresh = true`.
AcquireForInitResult AcquireConnectionForInit(ClientContext &context, const FunctionConnectionParams &params,
                                                bool force_fresh = false);

// The VGI function protocol over a byte transport: a spawned or pooled
// subprocess, an adopted connected fd (launch:, unix://, tcp://, shared
// container), or a native Iroh stream. It tracks only streaming state
// (init_done_, data_finished_) and caches no RPC results: callers pass the
// BindResult from PerformBindRpc into PerformInit.
//
// Producer mode (table functions): the client sends 0-row ticks, the worker
// answers with output batches. Exchange mode (scalar / table-in-out): input
// batches in, output batches back.
class FunctionConnection : public IFunctionConnection {
public:
	// Does not spawn the worker (EnsureWorkerSpawned does). global_execution_id:
	// for secondary workers, the primary's InitResult execution id. A non-null
	// iroh_config selects the native Iroh stream instead of a subprocess.
	FunctionConnection(const std::string &worker_path, const std::string &function_name,
	                   const ArrowArguments &arguments, const std::vector<uint8_t> &attach_opaque_data,
	                   const std::vector<uint8_t> &transaction_opaque_data, ClientContext &context,
	                   const std::string &function_type = "TABLE",
	                   const std::vector<uint8_t> &global_execution_id = {}, bool worker_debug = false,
	                   const std::map<std::string, Value> &settings = {},
	                   const std::vector<VgiSecretRequirement> &required_secrets = {},
	                   const std::string &data_version_spec = "",
	                   const std::string &implementation_version = "",
	                   std::shared_ptr<IrohClientConfig> iroh_config = nullptr);

	// Adopt a pooled worker and its stderr drainer.
	FunctionConnection(std::unique_ptr<PooledWorker> pooled_worker, const std::string &function_name,
	                   const ArrowArguments &arguments, const std::vector<uint8_t> &attach_opaque_data,
	                   const std::vector<uint8_t> &transaction_opaque_data, ClientContext &context,
	                   const std::string &function_type = "TABLE",
	                   const std::vector<uint8_t> &global_execution_id = {}, bool worker_debug = false,
	                   const std::map<std::string, Value> &settings = {},
	                   const std::vector<VgiSecretRequirement> &required_secrets = {});

	// Adopt an already-connected SubProcess (UnixSocketWorker / NamedPipeWorker for
	// launch:, unix://, tcp://, shared containers). Such workers report
	// !IsPoolable(), so they never enter the per-path pool.
	FunctionConnection(std::unique_ptr<SubProcess> proc, const std::string &worker_path,
	                   const std::string &function_name, const ArrowArguments &arguments,
	                   const std::vector<uint8_t> &attach_opaque_data,
	                   const std::vector<uint8_t> &transaction_opaque_data, ClientContext &context,
	                   const std::string &function_type = "TABLE",
	                   const std::vector<uint8_t> &global_execution_id = {}, bool worker_debug = false,
	                   const std::map<std::string, Value> &settings = {},
	                   const std::vector<VgiSecretRequirement> &required_secrets = {});

	~FunctionConnection() override;

	void ResetForNextSplit() override;
	void MarkSplitInitFailed() override;

	// Shared dynamic-filter state; deltas ride producer-mode ticks.
	void SetTickFilterState(shared_ptr<TickFilterState> state) override {
		tick_filter_state_ = std::move(state);
		tick_filter_cursor_.Reset();
	}

	// Send the "bind" RPC (spawning the worker if needed), including the
	// secret-scope retry. The result is not cached; callers pass it to
	// PerformInit / PerformFinalizeInit.
	BindResult PerformBindRpc() override;
	void EnsureWorkerSpawned() override;

	// Must be called before PerformBindRpc for table-in-out / scalar functions.
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

	// Replace the input schema when execution DataChunk types differ from bind
	// types; only before the input writer opens.
	void UpdateInputSchemaForExecution(const VgiSchemaPtr &input_schema) override;

	// Send the "init" RPC and open the data streams. bind_result's
	// bind_request_bytes, output_schema_bytes and opaque_data are folded into the
	// InitRequest. Enters producer mode when there is no input schema or phase is
	// FINALIZE / TABLE_BUFFERING_FINALIZE, else exchange mode.
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

	// Table-in-out FINALIZE: close and drain the INPUT-phase streams, then init
	// again with phase="FINALIZE" (producer mode) under this execution id.
	void PerformFinalizeInit(const BindResult &bind_result) override;

	// Exchange-mode input. PerformInit already opens the writer, so
	// OpenInputWriter is normally a no-op.
	void OpenInputWriter() override;
	void WriteInputBatch(const VgiBatchPtr &batch) override;

	// Cancel the current stream: the Iroh cancel handle if present, else (producer
	// mode only) a zero-row tick tagged VGI_RPC_CANCEL_KEY. Both arguments unused.
	void CancelStream(const std::vector<uint8_t> &state_token, ClientContext &live_context) override;

	// Subprocess: no state token (the lockstep stream is self-addressing).
	std::vector<uint8_t> GetLastStateToken() const override { return {}; }

	// Write input EOS; the worker then finishes its output stream.
	void CloseInputWriter() override;

	// Buffered (sink+source) table function RPCs.
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

	// Next output batch, skipping log batches; nullptr at EOS. In exchange mode
	// the caller must have written input first.
	VgiBatchPtr ReadDataBatch() override;

	bool IsFinished() const override {
		return data_finished_;
	}

	// Lets ReleaseForPooling treat the stream as done without reading to EOS.
	void MarkDataFinished() override {
		data_finished_ = true;
	}

	// Empty before init.
	std::string GetExecutionIdHex() const override;

	// nullopt before the process exists (and for Iroh).
	std::optional<pid_t> GetSubprocessPid() const override {
		return proc_ ? std::make_optional(proc_->GetPid()) : std::nullopt;
	}

	// GetSubprocessPid(), but -1 when there is no process.
	pid_t GetPid() const {
		return proc_ ? proc_->GetPid() : -1;
	}

	// OpaqueDigest of the opaque data; empty if none.
	std::string GetAttachOpaqueDataDigest() const override;
	std::string GetTransactionOpaqueDataDigest() const override;

	std::string GetConnIdHex() const override {
		return conn_id_hex_;
	}

	int Wait() override;

	// Hand the subprocess and its running stderr drainer to the pool. Returns
	// nullptr (keeping ownership) if the process exited, is not poolable, has an
	// advertised SHM session, has a stream in flight, or a split reset failed.
	std::unique_ptr<PooledWorker> ReleaseForPooling() override;

	// Holds a ClientContext reference.
	FunctionConnection(const FunctionConnection &) = delete;
	FunctionConnection &operator=(const FunctionConnection &) = delete;
	FunctionConnection(FunctionConnection &&) = delete;
	FunctionConnection &operator=(FunctionConnection &&) = delete;

private:
	std::string conn_id_hex_;
	std::string worker_path_;
	// Pool-key dimensions, so ReleaseForPooling returns the worker to the bucket
	// it came from.
	std::string data_version_spec_;
	std::string implementation_version_;
	std::string function_name_;
	std::string function_type_;  // "TABLE", "SCALAR", "AGGREGATE"
	//! One-row {args: struct} batch (ArrowArguments::batch); null when there are none.
	VgiBatchPtr arguments_batch_;
	std::vector<uint8_t> attach_opaque_data_;
	std::vector<uint8_t> transaction_opaque_data_;
	std::vector<uint8_t> global_execution_id_;
	// Client-minted per-substream id (parallel streaming table-in-out), sent on
	// every InitRequest (INPUT and FINALIZE). Empty = none.
	std::vector<uint8_t> substream_id_;
	ClientContext &context_;
	bool worker_debug_;
	std::map<std::string, Value> settings_;
	std::vector<VgiSecretRequirement> required_secrets_;
	std::shared_ptr<IrohClientConfig> iroh_config_;
	std::shared_ptr<VgiInputStream> transport_input_;
	std::shared_ptr<VgiOutputStream> transport_output_;
	std::function<void()> transport_cancel_;

	// Worker process: spawned by EnsureWorkerSpawned or adopted. Null for Iroh,
	// which uses transport_input_/transport_output_ instead.
	std::unique_ptr<SubProcess> proc_;

	bool TransportReady() const;
	pid_t TransportPid() const;
	std::shared_ptr<VgiInputStream> TransportInput();
	std::shared_ptr<VgiOutputStream> TransportOutput();
	void WriteTransportRpc(const std::string &method,
	                       const VgiBatchPtr &params,
	                       const VgiMetadataPtr &metadata = nullptr);
	UnaryResponseResult ReadTransportUnary();
	StreamHeaderResult ReadTransportHeader();

	// Streaming state; bind tracks none.
	bool init_done_ = false;
	bool data_finished_ = false;

	std::vector<uint8_t> execution_id_;

	bool is_producer_mode_ = false;
	VgiSchemaPtr tick_schema_;  // Set only in producer mode

	// Producer mode opens these in PerformInit; exchange mode in the first ReadDataBatch.
	std::shared_ptr<VgiInputStream> data_stream_;
	std::shared_ptr<VgiIpcReader> data_reader_;

	// Null for plain table functions.
	VgiSchemaPtr input_schema_;

	// Bind-request context (empty = none); see the matching setters.
	std::string at_unit_;
	std::string at_value_;
	std::string schema_name_;
	std::optional<std::vector<std::optional<std::string>>> argument_names_;
	std::optional<CopyFromBindContext> copy_from_;
	std::optional<CopyToBindContext> copy_to_;

	// Tick writer (producer mode) or input writer (exchange mode).
	std::shared_ptr<VgiIpcWriter> input_writer_;
	bool input_writer_opened_ = false;
	bool input_writer_closed_ = false;

	// Shared with the scan operator.
	shared_ptr<TickFilterState> tick_filter_state_;
	// Which delta of tick_filter_state_ the current stream has been sent.
	TickFilterCursor tick_filter_cursor_;

	// Per-batch metadata of the most recent data batch, set by ReadDataBatch and
	// read by the consumer (InstallBatch in vgi_table_function_impl.cpp, the
	// result-cache capture, the batched-lateral operator) before the next
	// ReadDataBatch — lockstep, so no race.
	// vgi_batch_index, or INVALID_INDEX when absent.
	idx_t last_batch_index_ = DConstants::INVALID_INDEX;
	// Base64-decoded vgi_partition_values#b64 (IPC bytes, decoded by InstallBatch).
	std::string last_partition_values_bytes_;
	// Base64-decoded vgi_rpc.parent_row#b64: little-endian int32 parent input row
	// per output row.
	std::string last_parent_row_bytes_;
	// vgi.cache.* advertisement (present=false when absent).
	VgiCacheControl last_cache_control_;
	// vgi.status: HAVE_MORE_OUTPUT.
	bool last_more_output_ = false;
public:
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
	}
private:
	// Conditional-revalidation validators, sent on the first producer tick or on
	// exchange input batches. Empty = no key emitted.
	std::string cond_if_none_match_;
	std::string cond_if_modified_since_;

	// A failed split init (or the reset before it) leaves the protocol state
	// unknown. ResetForNextSplit clears init_done_/data_finished_, so without this
	// flag ReleaseForPooling would pool a connection with an unanswered init in
	// flight, and the next checkout would read that response as its own header.
	bool split_reset_failed_ = false;

#if VGI_SHM_TRANSPORT
	// A worker keeps its SHM attachment for the transport session, so once a
	// segment is advertised the process must not go to pooled readers that cannot
	// resolve its pointers. Sticky, even if segment setup later falls back.
	bool shm_session_advertised_ = false;
	// Advertised on each init request when shm is negotiated; data and unary
	// responses resolve pointer batches against it.
	std::shared_ptr<class VgiShmSegment> shm_segment_;
	// One lease per slot the worker filled and we resolved, shared with the
	// resolved batch's buffers. A slot goes back to the worker only when ours is
	// the LAST reference — so whoever is handed a batch may keep it (a result-
	// cache capture, a memo arena) without copying and without it being
	// overwritten under them. Swept by ReleaseUnreferencedShmSlots().
	std::vector<std::shared_ptr<struct VgiShmSlotLease>> shm_leases_;
	// Free every slot nobody references any more. Call ONLY at a lockstep point
	// (the allocator table is unlocked shared memory): the top of ReadDataBatch,
	// after a unary response, and the between-requests reset.
	void ReleaseUnreferencedShmSlots();
	// Resolve a unary response's shm pointer batch in place, leasing its slot.
	void ResolveUnaryShm(struct UnaryResponseResult &response);

#endif

	// Background stderr drainer; ReleaseForPooling hands it, still running, to
	// the pool so an idle worker never blocks on a full stderr pipe.
	std::unique_ptr<StderrDrainer> stderr_drainer_;

	// No-op without a stderr pipe.
	void StartStderrReader();
	// Forward buffered stderr lines to VGI_LOG on the calling thread.
	void DrainStderrLog();
};

} // namespace vgi
} // namespace duckdb
