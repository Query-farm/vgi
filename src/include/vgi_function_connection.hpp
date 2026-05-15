#pragma once

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include "duckdb/common/types/value.hpp"

#include "vgi_arrow_utils.hpp"
#include "vgi_ifunction_connection.hpp"
#include "vgi_protocol.hpp"
#include "vgi_stderr_drainer.hpp"
#include "vgi_subprocess.hpp"
#include "vgi_worker_pool.hpp"

namespace duckdb {

class ClientContext;

namespace vgi {

// Forward declarations — full definitions in vgi_catalog_api.hpp
struct VgiAttachParameters;
struct VgiSecretRequirement;

// ============================================================================
// Connection Acquisition with Retry
// ============================================================================

// Forward declaration for FunctionConnection (defined below)
class FunctionConnection;

// Parameters for creating a FunctionConnection
// Used with AcquireAndBindConnection to handle pool acquire + stale connection retry
struct FunctionConnectionParams {
	std::shared_ptr<VgiAttachParameters> attach_params;  // replaces worker_path, worker_debug, use_pool
	std::vector<uint8_t> attach_opaque_data;
	std::string function_name;
	ArrowArguments arguments;
	std::vector<uint8_t> transaction_opaque_data;
	std::vector<uint8_t> global_execution_id;  // Empty for primary workers
	std::map<std::string, Value> settings;
	std::vector<vgi::VgiSecretRequirement> required_secrets;
	std::string phase;  // For logging (e.g., "bind", "init_local_secondary")
	std::string function_type = "TABLE";  // "TABLE", "SCALAR", "AGGREGATE"

	// Optional input schema, required for function types that bind with a
	// typed input stream (table-in-out, scalar). If set, AcquireAndBindConnection
	// calls SetInputSchema before PerformBindRpc.
	std::shared_ptr<arrow::Schema> input_schema;

	// Convenience accessors (defined out-of-line in vgi_function_connection.cpp)
	const std::string &worker_path() const;
	bool worker_debug() const;
	bool use_pool() const;
	const std::string &data_version_spec() const;
	const std::string &implementation_version() const;

	FunctionConnectionParams() = default;
};

// Result of AcquireAndBindConnection
struct AcquireAndBindResult {
	std::unique_ptr<IFunctionConnection> connection;
	BindResult bind_result;
};

// Acquire a FunctionConnection and perform bind with automatic retry for stale pool connections.
//
// This helper encapsulates the common pattern of:
// 1. Try to acquire a pooled worker
// 2. Create FunctionConnection (from pool or fresh)
// 3. Attempt PerformBindRpc()
// 4. If bind fails and connection was from pool, retry with fresh connection
//
// The retry handles the case where a pooled worker died while idle. If the
// fresh connection also fails, the exception propagates to the caller.
//
// Returns the successfully-bound connection and the BindResult.
// Throws IOException if bind fails (after retry if applicable).
AcquireAndBindResult AcquireAndBindConnection(ClientContext &context, const FunctionConnectionParams &params);

// Result of AcquireConnectionForInit — the caller already has a usable BindResult
// (from a prior planner-phase bind) and is going straight to PerformInit.
struct AcquireForInitResult {
	std::unique_ptr<IFunctionConnection> connection;
	// True when the connection came from the worker pool (subprocess only).
	// Caller uses this to decide whether to retry with a fresh connection if
	// the first RPC after acquire — typically PerformInit — throws IOException
	// (a stale-pool indicator). Always false for HTTP transport.
	bool from_pool = false;
};

// Acquire a FunctionConnection without firing a bind RPC. Use when the caller
// has a cached BindResult from an earlier planner-phase bind and is about to
// call PerformInit directly. The init RPC's payload carries bind_request_bytes
// + opaque_data inline, so the worker reconstructs bind context from the init
// request alone — a wire-level second bind is redundant.
//
// The caller is responsible for stale-pool retry around the first RPC on the
// returned connection: catch IOException, and if from_pool, call this helper
// again with `force_fresh = true` to bypass the pool and retry.
AcquireForInitResult AcquireConnectionForInit(ClientContext &context, const FunctionConnectionParams &params,
                                                bool force_fresh = false);

// ============================================================================
// Function Connection - vgi_rpc Protocol
// ============================================================================

// FunctionConnection implements the VGI function protocol using vgi_rpc.
// It is a thin transport channel: the worker exposes several unary/streaming
// RPCs (bind, init, cardinality, ...), and the methods on this class invoke
// them. The class itself only tracks streaming state — init_done_ once init
// has opened the data streams, data_finished_ once they're drained — and
// does not cache any RPC results. Callers thread bind→init data through
// themselves by passing the BindResult from PerformBindRpc into PerformInit.
//
// PerformBindRpc lazily spawns the worker subprocess on first call (or
// adopts a pooled one); subsequent RPCs share that process. Calling
// PerformBindRpc twice on the same connection sends two bind RPCs (the
// worker will reject the second).
//
// For table functions (producer mode):
//   Client sends 0-row "tick" batches, server responds with output batches
// For scalar/table-in-out functions (exchange mode):
//   Client sends input batches, server responds with output batches
class FunctionConnection : public IFunctionConnection {
public:
	// Create connection parameters (does not spawn worker yet)
	// arguments: Arrow struct array with fields named positional_0, positional_1, etc.
	// global_execution_id: For secondary workers, pass the ID from the primary worker's InitResult
	// function_type: "TABLE", "SCALAR", or "AGGREGATE" (for BindRequest)
	// settings: Optional map of settings to pass to the worker (e.g., DuckDB pragmas)
	FunctionConnection(const std::string &worker_path, const std::string &function_name,
	                   const ArrowArguments &arguments, const std::vector<uint8_t> &attach_opaque_data,
	                   const std::vector<uint8_t> &transaction_opaque_data, ClientContext &context,
	                   const std::string &function_type = "TABLE",
	                   const std::vector<uint8_t> &global_execution_id = {}, bool worker_debug = false,
	                   const std::map<std::string, Value> &settings = {},
	                   const std::vector<VgiSecretRequirement> &required_secrets = {},
	                   const std::string &data_version_spec = "",
	                   const std::string &implementation_version = "");

	// Create connection using a pooled worker (skips spawning new subprocess)
	FunctionConnection(std::unique_ptr<PooledWorker> pooled_worker, const std::string &function_name,
	                   const ArrowArguments &arguments, const std::vector<uint8_t> &attach_opaque_data,
	                   const std::vector<uint8_t> &transaction_opaque_data, ClientContext &context,
	                   const std::string &function_type = "TABLE",
	                   const std::vector<uint8_t> &global_execution_id = {}, bool worker_debug = false,
	                   const std::map<std::string, Value> &settings = {},
	                   const std::vector<VgiSecretRequirement> &required_secrets = {});

	// Adopt a pre-built SubProcess (typically a ``UnixSocketWorker`` from
	// the AF_UNIX launcher path).  Skips both spawning and the per-path
	// pool — the SubProcess is treated as already-connected.  ``worker_path``
	// is for diagnostics only; AF_UNIX workers don't go through the pool.
	FunctionConnection(std::unique_ptr<SubProcess> proc, const std::string &worker_path,
	                   const std::string &function_name, const ArrowArguments &arguments,
	                   const std::vector<uint8_t> &attach_opaque_data,
	                   const std::vector<uint8_t> &transaction_opaque_data, ClientContext &context,
	                   const std::string &function_type = "TABLE",
	                   const std::vector<uint8_t> &global_execution_id = {}, bool worker_debug = false,
	                   const std::map<std::string, Value> &settings = {},
	                   const std::vector<VgiSecretRequirement> &required_secrets = {});

	~FunctionConnection() override;

	// Set shared state for dynamic filter pushdown via tick batches
	void SetTickFilterState(shared_ptr<TickFilterState> state) override {
		tick_filter_state_ = std::move(state);
	}

	// Send the VGI "bind" RPC. Spawns the worker subprocess on first call
	// (unless adopted from the pool), runs the bind protocol — including
	// the optional secret-scope retry — and returns the resulting BindResult
	// by value. The connection does NOT cache the result; callers thread it
	// into the matching PerformInit / PerformFinalizeInit.
	BindResult PerformBindRpc() override;
	void EnsureWorkerSpawned() override;

	// Set the input schema for table-in-out/scalar functions
	// Must be called before PerformBindRpc() for table-in-out functions
	// input_schema: Arrow schema describing the input data
	void SetInputSchema(const std::shared_ptr<arrow::Schema> &input_schema) override;

	// Update input schema for execution phase (when DataChunk types differ from bind types)
	void UpdateInputSchemaForExecution(const std::shared_ptr<arrow::Schema> &input_schema) override;

	// Send the VGI "init" RPC. Opens the data streams and returns the
	// init response. The bind_result must come from a prior PerformBindRpc
	// on this connection — its bind_request_bytes, output_schema_bytes, and
	// opaque_data are folded into the InitRequest.
	// For table functions: enters producer mode (tick-based)
	// For scalar/table-in-out: enters exchange mode
	// Optional phase parameter for FINALIZE init calls
	InitResult PerformInit(const BindResult &bind_result,
	                       const std::vector<int32_t> &projection_ids = {},
	                       std::shared_ptr<arrow::Buffer> pushdown_filters = nullptr,
	                       std::vector<std::shared_ptr<arrow::Buffer>> join_keys = {},
	                       const std::string &phase = "",
	                       const std::optional<OrderByHint> &order_by = std::nullopt,
	                       const std::optional<TableSampleHint> &table_sample = std::nullopt,
	                       const std::vector<uint8_t> &init_opaque_data = {}) override;

	// Re-init for table-in-out FINALIZE: closes the current data streams
	// and sends a new init RPC with phase="FINALIZE" that references the
	// original bind via bind_result. Opens new data streams in producer
	// mode (tick-based).
	void PerformFinalizeInit(const BindResult &bind_result) override;

	// ========================================================================
	// Input Data (Scalar and Table-In-Out Functions)
	// ========================================================================

	// Open input writer for sending input data batches
	// Must be called after PerformInit() and before WriteInputBatch()
	// For scalar and table-in-out functions
	void OpenInputWriter() override;

	// Write an input batch (scalar and table-in-out functions)
	// Must be called after OpenInputWriter()
	void WriteInputBatch(const std::shared_ptr<arrow::RecordBatch> &batch) override;

	// Cancel the current stream by writing a zero-row batch tagged with
	// VGI_RPC_CANCEL_KEY custom metadata. state_token unused (ignored).
	void CancelStream(const std::vector<uint8_t> &state_token) override;

	// Subprocess: no state token (the lockstep stream is self-addressing).
	std::vector<uint8_t> GetLastStateToken() const override { return {}; }

	// Close input writer (signals end of input to the worker)
	// After this, the worker will send EOS on the output stream
	void CloseInputWriter() override;

	// ========== Buffered Table Function RPCs ==========
	void RpcBufferedTableProcess(const std::string &function_name,
	                             const std::vector<uint8_t> &execution_id,
	                             int64_t state_id,
	                             const std::shared_ptr<arrow::RecordBatch> &input_batch,
	                             std::optional<int64_t> batch_index = std::nullopt) override;
	std::vector<int64_t> RpcBufferedTableCombine(const std::string &function_name,
	                                             const std::vector<uint8_t> &execution_id,
	                                             const std::vector<int64_t> &state_ids) override;
	BufferedTableFinalizeResult RpcBufferedTableFinalize(const std::string &function_name,
	                                                      const std::vector<uint8_t> &execution_id,
	                                                      int64_t finalize_state_id) override;
	void RpcBufferedTableDestructor(const std::string &function_name,
	                                 const std::vector<uint8_t> &execution_id) override;

	// Check if this is a table-in-out function (has input schema)
	bool IsTableInOut() const override {
		return input_schema_ != nullptr;
	}

	// Phase 3: Read a data batch from the output stream
	// For producer mode: sends tick, reads response
	// For exchange mode: reads response (caller must write input first)
	// Returns nullptr when stream is exhausted (EOS)
	std::shared_ptr<arrow::RecordBatch> ReadDataBatch() override;

	// Check if data stream is exhausted
	bool IsFinished() const override {
		return data_finished_;
	}

	// Mark the data phase as finished (for scalar functions after sending finalize)
	// This allows the connection to be pooled without reading a FINISHED status
	void MarkDataFinished() override {
		data_finished_ = true;
	}

	// Get the execution ID as hex string (empty if init not done)
	std::string GetExecutionIdHex() const override;

	// Subprocess transport: return the spawned worker's OS pid (or nullopt
	// before the process exists). Used for diagnostics — log emitters call
	// this to add a worker_pid= field alongside conn= for subprocess
	// connections; HTTP connections inherit the default nullopt.
	std::optional<pid_t> GetSubprocessPid() const override {
		return proc_ ? std::make_optional(proc_->GetPid()) : std::nullopt;
	}

	// Internal subprocess accessor — same value as GetSubprocessPid() but
	// returns -1 when no process exists. Use this only inside subprocess
	// implementation files where the concrete type is known and the
	// optional unwrapping noise is unwelcome.
	pid_t GetPid() const {
		return proc_ ? proc_->GetPid() : -1;
	}

	// Get the attach ID as hex string (empty if no attach_opaque_data)
	std::string GetAttachOpaqueDataHex() const override;

	// Get the transaction ID as hex string (empty if no transaction_opaque_data)
	std::string GetTransactionOpaqueDataHex() const override;

	// Stable 8-hex correlation id for this connection checkout
	std::string GetConnIdHex() const override {
		return conn_id_hex_;
	}

	// Wait for worker process to exit
	int Wait() override;

	// Release the subprocess for pooling (transfers ownership).
	// Returns nullptr if the worker isn't parked at its RPC accept-loop
	// (streaming data phase in-flight) or if the process has exited.
	// Stops stderr thread and releases subprocess ownership on success.
	std::unique_ptr<PooledWorker> ReleaseForPooling() override;

	// Non-copyable and non-movable (contains reference)
	FunctionConnection(const FunctionConnection &) = delete;
	FunctionConnection &operator=(const FunctionConnection &) = delete;
	FunctionConnection(FunctionConnection &&) = delete;
	FunctionConnection &operator=(FunctionConnection &&) = delete;

private:
	// Per-checkout correlation id (8 hex). Generated at construction.
	std::string conn_id_hex_;
	std::string worker_path_;
	// Pool-key dimensions. Populated from VgiAttachParameters when the
	// connection is created so ReleaseForPooling can route the worker back
	// to the same version-specific bucket it came from.
	std::string data_version_spec_;
	std::string implementation_version_;
	std::string function_name_;
	std::string function_type_;  // "TABLE", "SCALAR", "AGGREGATE"
	std::shared_ptr<arrow::DataType> arguments_type_;
	std::shared_ptr<arrow::Array> arguments_array_;
	std::vector<uint8_t> attach_opaque_data_;
	std::vector<uint8_t> transaction_opaque_data_;
	std::vector<uint8_t> global_execution_id_;
	ClientContext &context_;
	bool worker_debug_;
	std::map<std::string, Value> settings_;
	std::vector<VgiSecretRequirement> required_secrets_;

	// Worker process (spawned lazily by PerformBindRpc, or adopted from the
	// pool). Used as the spawn-once flag too: a non-null proc_ means the
	// worker exists and at least bind has been attempted.
	std::unique_ptr<SubProcess> proc_;

	// Streaming-state flags. Bind is just an RPC; no state is tracked for
	// it on the connection.
	bool init_done_ = false;
	bool data_finished_ = false;

	// Execution ID from GlobalInitResponse
	std::vector<uint8_t> execution_id_;

	// Producer mode (true for table functions - tick-based data exchange)
	bool is_producer_mode_ = false;
	std::shared_ptr<arrow::Schema> tick_schema_;  // Empty schema for tick batches

	// Data stream reader (opened during PerformInit, used for ReadDataBatch)
	std::shared_ptr<arrow::io::InputStream> data_stream_;
	std::shared_ptr<arrow::ipc::RecordBatchStreamReader> data_reader_;

	// Input schema for table-in-out/scalar functions (nullptr for regular table functions)
	std::shared_ptr<arrow::Schema> input_schema_;

	// Input data writer for exchange-mode functions
	std::shared_ptr<arrow::ipc::RecordBatchWriter> input_writer_;
	bool input_writer_opened_ = false;
	bool input_writer_closed_ = false;

	// Dynamic filter state for tick-based pushdown (shared with the scan operator)
	shared_ptr<TickFilterState> tick_filter_state_;

	// Raw ``vgi_batch_index`` value parsed off the most recent data
	// batch's custom_metadata, or INVALID if the worker did not emit the
	// key. Set inside ``ReadDataBatch`` on the prefetch / connection
	// thread; read by ``VgiTableFunctionInstallBatch`` on the consumer
	// thread BEFORE the next ReadDataBatch can fire (lockstep RPC
	// protocol), so there is no aliasing race with the writer.
	idx_t last_batch_index_ = DConstants::INVALID_INDEX;

	// Base64-decoded raw IPC bytes from the most recent data batch's
	// ``vgi_partition_values#b64`` metadata. Empty when the key is
	// absent. IPC decode + validation happen in ``InstallBatch`` on
	// the consumer thread (uniform across transports).
	std::string last_partition_values_bytes_;
public:
	idx_t GetLastBatchIndex() const override {
		return last_batch_index_;
	}
	const std::string &GetLastPartitionValuesBytes() const override {
		return last_partition_values_bytes_;
	}
private:

	// Optional shared-memory segment for zero-copy batch transfer.
	// When non-null, the segment name + size are advertised to the worker
	// in PerformInit's request metadata, and ReadDataBatch resolves
	// pointer batches against it. Reset between requests.
	std::unique_ptr<class VgiShmSegment> shm_segment_;
	// Offset of the most-recently-resolved shm batch. Freed when the next
	// ReadDataBatch is called (lockstep: DuckDB has finished with the
	// previous chunk before requesting the next), and on connection close.
	int64_t shm_last_offset_ = -1;

	// Stderr reader: owned drainer that spawns a background thread for the
	// lifetime of this connection. ReleaseFd() transfers fd ownership to the
	// pool on ReleaseForPooling() so the cleanup thread can close it later.
	std::unique_ptr<StderrDrainer> stderr_drainer_;

	// Start stderr reader (called after spawning worker or taking over from a
	// pooled worker). No-op if the subprocess has no stderr pipe.
	void StartStderrReader();
	// Stop the reader and return the fd for pool reuse (keeps fd open).
	// Returns -1 if no fd was held.
	int ReleaseStderrReaderFd();
	// Stop the reader and close the fd (destructor path).
	void StopStderrReader();
	// Forward buffered stderr lines to VGI_LOG (main thread).
	void DrainStderrLog();
};

} // namespace vgi
} // namespace duckdb
