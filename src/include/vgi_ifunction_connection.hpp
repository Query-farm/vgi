#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <arrow/api.h>

#include <sys/types.h>

#include "duckdb/common/types/value.hpp"

#include "vgi_arrow_utils.hpp"
#include "vgi_protocol.hpp"

namespace duckdb {

class ClientContext;

namespace vgi {

class PooledWorker;
struct VgiAttachParameters;
struct VgiSecretRequirement;

// ============================================================================
// IFunctionConnection - Abstract interface for VGI function connections
// ============================================================================
// Both subprocess (FunctionConnection) and HTTP (HttpFunctionConnection) implement
// this interface. All call sites store unique_ptr<IFunctionConnection>.

// Shared state for dynamic filter values that tighten during execution.
// Written by the table scan (after Top-N sink updates), read by the connection (before tick).
struct TickFilterState {
	mutex lock;
	//! Base64-encoded Arrow IPC bytes of the complete filter set (static + dynamic merged)
	string encoded_filters;
	//! True if encoded_filters has been set at least once
	bool has_filters = false;
};

class IFunctionConnection {
public:
	virtual ~IFunctionConnection() = default;

	// Tick metadata: set shared state for dynamic filter pushdown via tick batches.
	// The connection reads this before each tick and includes it as custom metadata.
	virtual void SetTickFilterState(shared_ptr<TickFilterState> state) = 0;

	// Send the VGI bind RPC. Spawns the worker subprocess on first call
	// (subprocess transport only) and runs the bind protocol — including
	// any secret-scope retry — returning the resulting BindResult by value.
	// The connection does NOT cache the result; callers thread it into the
	// matching PerformInit / PerformFinalizeInit call.
	virtual BindResult PerformBindRpc() = 0;
	virtual void SetInputSchema(const std::shared_ptr<arrow::Schema> &input_schema) = 0;

	// Update input schema for execute phase (after bind, before OpenInputWriter)
	// Used when reusing bind connection and actual DataChunk types differ from bind types
	virtual void UpdateInputSchemaForExecution(const std::shared_ptr<arrow::Schema> &input_schema) = 0;

	// Send the VGI init RPC. The bind_result must come from a prior
	// PerformBindRpc on this connection — its bind_request_bytes,
	// output_schema_bytes, and opaque_data are folded into the InitRequest.
	virtual InitResult PerformInit(const BindResult &bind_result,
	                               const std::vector<int32_t> &projection_ids = {},
	                               std::shared_ptr<arrow::Buffer> pushdown_filters = nullptr,
	                               std::vector<std::shared_ptr<arrow::Buffer>> join_keys = {},
	                               const std::string &phase = "",
	                               const std::optional<OrderByHint> &order_by = std::nullopt,
	                               const std::optional<TableSampleHint> &table_sample = std::nullopt) = 0;
	// Re-init the connection in FINALIZE mode (table-in-out). Closes the
	// current data streams and sends a new init RPC that references the
	// original bind via bind_result.
	virtual void PerformFinalizeInit(const BindResult &bind_result) = 0;

	// Phase 3: Data exchange
	virtual void OpenInputWriter() = 0;
	virtual void WriteInputBatch(const std::shared_ptr<arrow::RecordBatch> &batch) = 0;
	virtual void CloseInputWriter() = 0;
	virtual std::shared_ptr<arrow::RecordBatch> ReadDataBatch() = 0;

	// Most recent stream-state token observed on this connection
	// (HTTP only). Returns empty on subprocess. Used by the cancel
	// dispatcher to address the right worker under HTTP pooling.
	virtual std::vector<uint8_t> GetLastStateToken() const = 0;

	// Cancel the current stream. Called off-thread by VgiCancelDispatcher
	// from a destructor-triggered teardown; may throw (dispatcher catches).
	// - Subprocess: writes a zero-row batch with VGI_RPC_CANCEL_KEY custom
	//   metadata on the input writer. state_token is ignored.
	// - HTTP: POSTs {state, cancel} to /{method}/exchange addressed by
	//   state_token (the most recent token seen on the stream).
	virtual void CancelStream(const std::vector<uint8_t> &state_token) = 0;

	// State queries
	virtual bool IsTableInOut() const = 0;
	virtual bool IsFinished() const = 0;
	virtual void MarkDataFinished() = 0;

	// Identity/diagnostics
	virtual std::string GetExecutionIdHex() const = 0;
	virtual std::string GetAttachIdHex() const = 0;
	virtual std::string GetTransactionIdHex() const = 0;
	//! Stable short hex id for this connection checkout. Generated at construction,
	//! unique per IFunctionConnection instance regardless of transport. Use as the
	//! primary correlation key in log lines: one `conn=<hex>` covers the full
	//! bind → init → data → release lifecycle for a single checkout.
	virtual std::string GetConnIdHex() const = 0;
	//! OS process id of the worker, only meaningful for subprocess transport.
	//! Returns std::nullopt for HTTP (the C++ client does not own the worker
	//! process and cannot know its pid). Subprocess implementations return
	//! the spawned worker's pid. Use this only for human-readable diagnostics
	//! (debugger attach, stderr correlation); use GetConnIdHex() for the
	//! transport-neutral connection identifier in log fields.
	virtual std::optional<pid_t> GetSubprocessPid() const {
		return std::nullopt;
	}

	// Lifecycle
	virtual int Wait() = 0;
	// ReleaseForPooling hands the worker process back to the pool if the
	// connection is in a poolable state (worker alive and parked at its
	// RPC accept-loop). Returns nullptr otherwise.
	virtual std::unique_ptr<PooledWorker> ReleaseForPooling() = 0;
};

// ============================================================================
// Factory functions
// ============================================================================

// Create a connection of the appropriate type (subprocess or HTTP) based on worker_path.
std::unique_ptr<IFunctionConnection> CreateFunctionConnection(
    const std::string &worker_path, const std::string &function_name,
    const ArrowArguments &arguments, const std::vector<uint8_t> &attach_id,
    const std::vector<uint8_t> &transaction_id,
    ClientContext &context, const std::string &function_type = "TABLE",
    const std::vector<uint8_t> &global_execution_id = {},
    bool worker_debug = false,
    const std::map<std::string, Value> &settings = {},
    const std::vector<VgiSecretRequirement> &required_secrets = {},
    const std::shared_ptr<VgiAttachParameters> &attach_params = nullptr);

// Create from a pooled worker (subprocess only — HTTP connections are never pooled).
std::unique_ptr<IFunctionConnection> CreateFunctionConnectionFromPool(
    std::unique_ptr<PooledWorker> pooled_worker, const std::string &function_name,
    const ArrowArguments &arguments, const std::vector<uint8_t> &attach_id,
    const std::vector<uint8_t> &transaction_id,
    ClientContext &context, const std::string &function_type = "TABLE",
    const std::vector<uint8_t> &global_execution_id = {},
    bool worker_debug = false,
    const std::map<std::string, Value> &settings = {},
    const std::vector<VgiSecretRequirement> &required_secrets = {});

} // namespace vgi
} // namespace duckdb
