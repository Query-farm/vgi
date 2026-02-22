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
#include "vgi_protocol.hpp"
#include "vgi_subprocess.hpp"
#include "vgi_worker_pool.hpp"

namespace duckdb {

class ClientContext;

namespace vgi {

// ============================================================================
// Connection Acquisition with Retry
// ============================================================================

// Forward declaration for FunctionConnection (defined below)
class FunctionConnection;

// Parameters for creating a FunctionConnection
// Used with AcquireAndBindConnection to handle pool acquire + stale connection retry
struct FunctionConnectionParams {
	std::string worker_path;
	std::string function_name;
	ArrowArguments arguments;
	std::vector<uint8_t> attach_id;
	std::vector<uint8_t> global_execution_id;  // Empty for primary workers
	bool worker_debug = false;
	std::map<std::string, std::string> settings;
	bool use_pool = true;
	std::string phase;  // For logging (e.g., "bind", "init_local_secondary")
	std::string function_type = "TABLE";  // "TABLE", "SCALAR", "AGGREGATE"

	// Default constructor
	FunctionConnectionParams() = default;

	// Convenience constructor for common case
	FunctionConnectionParams(const std::string &worker_path, const std::string &function_name,
	                         const ArrowArguments &arguments, const std::vector<uint8_t> &attach_id,
	                         const std::vector<uint8_t> &global_execution_id, bool worker_debug,
	                         const std::map<std::string, std::string> &settings, bool use_pool,
	                         const std::string &phase, const std::string &function_type = "TABLE")
	    : worker_path(worker_path), function_name(function_name), arguments(arguments), attach_id(attach_id),
	      global_execution_id(global_execution_id), worker_debug(worker_debug), settings(settings), use_pool(use_pool),
	      phase(phase), function_type(function_type) {
	}
};

// Result of AcquireAndBindConnection
struct AcquireAndBindResult {
	std::unique_ptr<FunctionConnection> connection;
	BindResult bind_result;
};

// Acquire a FunctionConnection and perform bind with automatic retry for stale pool connections.
//
// This helper encapsulates the common pattern of:
// 1. Try to acquire a pooled worker
// 2. Create FunctionConnection (from pool or fresh)
// 3. Attempt PerformBindFull()
// 4. If bind fails and connection was from pool, retry with fresh connection
//
// The retry handles the case where a pooled worker died while idle. If the
// fresh connection also fails, the exception propagates to the caller.
//
// Returns the successfully-bound connection and the BindResult.
// Throws IOException if bind fails (after retry if applicable).
AcquireAndBindResult AcquireAndBindConnection(ClientContext &context, const FunctionConnectionParams &params);

// ============================================================================
// Function Connection - vgi_rpc Protocol
// ============================================================================

// FunctionConnection implements the VGI function protocol using vgi_rpc:
// Phase 1: bind RPC (unary) - send BindRequest, receive BindResponse
// Phase 2: init RPC (streaming) - send InitRequest, receive GlobalInitResponse + data streams
// Phase 3: data exchange - lockstep IPC streams (client sends → server responds)
//
// State machine with validation:
// - Created → bind_done_ (via PerformBindFull)
// - bind_done_ → init_done_ (via PerformInit)
// - init_done_ → data_finished_ (via ReadDataBatch returning nullptr)
//
// For table functions (producer mode):
//   Client sends 0-row "tick" batches, server responds with output batches
// For scalar/table-in-out functions (exchange mode):
//   Client sends input batches, server responds with output batches
class FunctionConnection {
public:
	// Create connection parameters (does not spawn worker yet)
	// arguments: Arrow struct array with fields named positional_0, positional_1, etc.
	// global_execution_id: For secondary workers, pass the ID from the primary worker's InitResult
	// function_type: "TABLE", "SCALAR", or "AGGREGATE" (for BindRequest)
	// settings: Optional map of settings to pass to the worker (e.g., DuckDB pragmas)
	FunctionConnection(const std::string &worker_path, const std::string &function_name,
	                   const ArrowArguments &arguments, const std::vector<uint8_t> &attach_id, ClientContext &context,
	                   const std::string &function_type = "TABLE",
	                   const std::vector<uint8_t> &global_execution_id = {}, bool worker_debug = false,
	                   const std::map<std::string, std::string> &settings = {});

	// Create connection using a pooled worker (skips spawning new subprocess)
	FunctionConnection(std::unique_ptr<PooledWorker> pooled_worker, const std::string &function_name,
	                   const ArrowArguments &arguments, const std::vector<uint8_t> &attach_id, ClientContext &context,
	                   const std::string &function_type = "TABLE",
	                   const std::vector<uint8_t> &global_execution_id = {}, bool worker_debug = false,
	                   const std::map<std::string, std::string> &settings = {});

	~FunctionConnection();

	// Phase 1: Perform bind via vgi_rpc "bind" RPC call
	// Spawns worker if needed, sends BindRequest, reads BindResponse
	// Returns BindResult with output schema and opaque data
	BindResult PerformBindFull();

	// Set the input schema for table-in-out/scalar functions
	// Must be called before PerformBindFull() for table-in-out functions
	// input_schema: Arrow schema describing the input data
	void SetInputSchema(const std::shared_ptr<arrow::Schema> &input_schema);

	// Phase 2: Perform init via vgi_rpc "init" RPC call
	// Sends InitRequest, reads GlobalInitResponse, opens data streams
	// For table functions: enters producer mode (tick-based)
	// For scalar/table-in-out: enters exchange mode
	// Optional phase parameter for FINALIZE init calls
	InitResult PerformInit(const std::vector<int32_t> &projection_ids = {},
	                       std::shared_ptr<arrow::Buffer> pushdown_filters = nullptr,
	                       const std::string &phase = "");

	// Perform finalize init for table-in-out functions
	// Closes current data streams, sends new init RPC with phase=FINALIZE
	// Opens new data streams in producer mode (tick-based)
	void PerformFinalizeInit();

	// ========================================================================
	// Input Data (Scalar and Table-In-Out Functions)
	// ========================================================================

	// Open input writer for sending input data batches
	// Must be called after PerformInit() and before WriteInputBatch()
	// For scalar and table-in-out functions
	void OpenInputWriter();

	// Write an input batch (scalar and table-in-out functions)
	// Must be called after OpenInputWriter()
	void WriteInputBatch(const std::shared_ptr<arrow::RecordBatch> &batch);

	// Close input writer (signals end of input to the worker)
	// After this, the worker will send EOS on the output stream
	void CloseInputWriter();

	// Check if this is a table-in-out function (has input schema)
	bool IsTableInOut() const {
		return input_schema_ != nullptr;
	}

	// Phase 3: Read a data batch from the output stream
	// For producer mode: sends tick, reads response
	// For exchange mode: reads response (caller must write input first)
	// Returns nullptr when stream is exhausted (EOS)
	std::shared_ptr<arrow::RecordBatch> ReadDataBatch();

	// Check if data stream is exhausted
	bool IsFinished() const {
		return data_finished_;
	}

	// Mark the data phase as finished (for scalar functions after sending finalize)
	// This allows the connection to be pooled without reading a FINISHED status
	void MarkDataFinished() {
		data_finished_ = true;
	}

	// Get the worker process PID (returns -1 if not yet spawned)
	pid_t GetPid() const {
		return proc_ ? proc_->GetPid() : -1;
	}

	// Get the execution ID as hex string (empty if init not done)
	std::string GetExecutionIdHex() const;

	// Get the attach ID as hex string (empty if no attach_id)
	std::string GetAttachIdHex() const;

	// Wait for worker process to exit
	int Wait();

	// Check if this connection can be returned to the pool
	// Returns true if data finished and subprocess is still alive
	bool CanBePooled() const;

	// Release the subprocess for pooling (transfers ownership)
	// Returns nullptr if cannot be pooled
	// Stops stderr thread and releases subprocess ownership
	std::unique_ptr<PooledWorker> ReleaseForPooling();

	// Non-copyable and non-movable (contains reference)
	FunctionConnection(const FunctionConnection &) = delete;
	FunctionConnection &operator=(const FunctionConnection &) = delete;
	FunctionConnection(FunctionConnection &&) = delete;
	FunctionConnection &operator=(FunctionConnection &&) = delete;

private:
	std::string worker_path_;
	std::string function_name_;
	std::string function_type_;  // "TABLE", "SCALAR", "AGGREGATE"
	std::shared_ptr<arrow::DataType> arguments_type_;
	std::shared_ptr<arrow::Array> arguments_array_;
	std::vector<uint8_t> attach_id_;
	std::vector<uint8_t> global_execution_id_;
	ClientContext &context_;
	bool worker_debug_;
	std::map<std::string, std::string> settings_;

	// Worker process (created during bind)
	std::unique_ptr<SubProcess> proc_;

	// State tracking
	bool bind_done_ = false;
	bool init_done_ = false;
	bool data_finished_ = false;

	// Cached bind results (vgi_rpc BindResult)
	BindResult bind_result_;

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

	// Stderr reader thread - reads stderr and buffers lines for main thread to log
	std::thread stderr_thread_;
	std::atomic<bool> stderr_stop_{false};
	std::mutex stderr_mutex_;
	std::vector<std::string> stderr_lines_;
	int stderr_fd_ = -1; // Stderr fd, owned by this class after release from SubProcess

	// Start stderr reader thread (called after spawning worker)
	void StartStderrReader();
	// Stop stderr reader thread (called in destructor or before pooling)
	// If close_fd is true, also closes the stderr fd (use in destructor)
	// If false, keeps fd open for reuse (use when pooling)
	void StopStderrReader(bool close_fd = true);
	// Drain buffered stderr lines and log them via VGI_LOG (call from main thread)
	void DrainStderrLog();
};

} // namespace vgi
} // namespace duckdb
