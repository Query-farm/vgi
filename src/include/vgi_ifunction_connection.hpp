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

	// Ensure the underlying worker is ready to receive RPCs. PerformBindRpc
	// has historically been the lazy-spawn hook for subprocess transport
	// (proc_ was created on first bind); call sites that skip the on-wire
	// bind — they have a cached BindResult and go straight to PerformInit —
	// must invoke this to materialize the subprocess before issuing init.
	// HTTP transport has no spawn step; this is a no-op there.
	virtual void EnsureWorkerSpawned() = 0;

	virtual void SetInputSchema(const std::shared_ptr<arrow::Schema> &input_schema) = 0;

	// Update input schema for execute phase (after bind, before OpenInputWriter)
	// Used when reusing bind connection and actual DataChunk types differ from bind types
	virtual void UpdateInputSchemaForExecution(const std::shared_ptr<arrow::Schema> &input_schema) = 0;

	// Send the VGI init RPC. The bind_result must come from a prior
	// PerformBindRpc on this connection — its bind_request_bytes,
	// output_schema_bytes, and opaque_data are folded into the InitRequest.
	//
	// `init_opaque_data` is the opaque blob returned by the worker's
	// primary init (`InitResult::opaque_data`). For secondary inits — those
	// against a connection constructed with a non-empty `global_execution_id`
	// — pass the primary's opaque bytes here so the worker echoes them in
	// `init_response.opaque_data`. The worker's secondary-init branch reads
	// this field directly and skips `on_init`; without it, secondaries
	// always see `None` and break any function that uses init opaque data.
	// Empty `{}` for primary inits.
	virtual InitResult PerformInit(const BindResult &bind_result,
	                               const std::vector<int32_t> &projection_ids = {},
	                               std::shared_ptr<arrow::Buffer> pushdown_filters = nullptr,
	                               std::vector<std::shared_ptr<arrow::Buffer>> join_keys = {},
	                               const std::string &phase = "",
	                               const std::optional<OrderByHint> &order_by = std::nullopt,
	                               const std::optional<TableSampleHint> &table_sample = std::nullopt,
	                               const std::vector<uint8_t> &init_opaque_data = {}) = 0;
	// Re-init the connection in FINALIZE mode (table-in-out). Closes the
	// current data streams and sends a new init RPC that references the
	// original bind via bind_result.
	virtual void PerformFinalizeInit(const BindResult &bind_result) = 0;

	// Phase 3: Data exchange
	virtual void OpenInputWriter() = 0;
	virtual void WriteInputBatch(const std::shared_ptr<arrow::RecordBatch> &batch) = 0;
	virtual void CloseInputWriter() = 0;
	virtual std::shared_ptr<arrow::RecordBatch> ReadDataBatch() = 0;

	// ========== Buffered Table Function RPCs ==========
	// Used only when the function declared Meta.buffered_table = True. The
	// connection must have been initialized via PerformInit(..., phase="BUFFERED_TABLE")
	// before any of these are called.

	// Sink one input batch into the worker's per-(execution_id, state_id) state.
	// The C++ side assigns state_ids from an atomic counter — one per Sink
	// thread. Multiple chunks for the same state_id accumulate into the same
	// worker-side state instance.
	//
	// `batch_index` is forwarded only when the operator declared
	// RequiredPartitionInfo()=BatchIndex() (because the worker's
	// Meta.requires_input_batch_index=True). Pass nullopt otherwise. Workers
	// that need source ordering accumulate (batch_index, payload) tuples and
	// sort in combine().
	virtual void RpcBufferedTableProcess(const std::string &function_name,
	                                     const std::vector<uint8_t> &execution_id,
	                                     int64_t state_id,
	                                     const std::shared_ptr<arrow::RecordBatch> &input_batch,
	                                     std::optional<int64_t> batch_index = std::nullopt) = 0;

	// Once-per-query end-of-input signal. The worker may coordinate with
	// peer workers (e.g. via shared BoundStorage) and returns partition
	// keys (finalize_state_ids) that the C++ source phase will iterate.
	virtual std::vector<int64_t> RpcBufferedTableCombine(const std::string &function_name,
	                                                     const std::vector<uint8_t> &execution_id,
	                                                     const std::vector<int64_t> &state_ids) = 0;

	// Pull one output batch from the worker's generator for finalize_state_id.
	// has_more indicates whether further calls on the same id will yield more.
	struct BufferedTableFinalizeResult {
		std::shared_ptr<arrow::RecordBatch> batch;
		bool has_more = false;
	};
	virtual BufferedTableFinalizeResult RpcBufferedTableFinalize(const std::string &function_name,
	                                                              const std::vector<uint8_t> &execution_id,
	                                                              int64_t finalize_state_id) = 0;

	// Best-effort end-of-query cleanup for buffered_table. The worker pops
	// any per-execution caches and wipes its FunctionStorage rows. The
	// PhysicalVgiBufferedTableFunction operator fires this from its
	// global-state destructor on every code path (success, cancel, throw).
	// Implementations must not let exceptions escape — destructor context.
	virtual void RpcBufferedTableDestructor(const std::string &function_name,
	                                         const std::vector<uint8_t> &execution_id) = 0;

	// Most recent stream-state token observed on this connection
	// (HTTP only). Returns empty on subprocess. Used by the cancel
	// dispatcher to address the right worker under HTTP pooling.
	virtual std::vector<uint8_t> GetLastStateToken() const = 0;

	// Most recent ``vgi_batch_index`` value observed in the data batch's
	// wire custom_metadata. Set inside ``ReadDataBatch`` immediately after
	// the metadata is parsed; read by ``InstallBatch`` on the consumer
	// thread to populate ``VgiTableFunctionLocalState::current_batch_index``.
	// Returns ``DConstants::INVALID_INDEX`` for connections whose worker
	// did not opt in to batch_index (no metadata key present). Default
	// implementation returns INVALID so transports that don't yet
	// implement parse (e.g. legacy connection types) safely report
	// "no batch_index seen."
	virtual idx_t GetLastBatchIndex() const {
		return duckdb::DConstants::INVALID_INDEX;
	}

	// Most recent ``vgi_partition_values#b64`` payload observed in the
	// data batch's wire custom_metadata, base64-decoded into raw Arrow
	// IPC stream bytes. Empty string when the metadata key is absent.
	// Decoding the bytes into a 2-row RecordBatch + validation happens
	// in ``InstallBatch`` on the consumer (pipeline-executor) thread,
	// NOT here — uniform error reporting across transports, and the
	// HTTP buffer path stashes raw bytes alongside batches without
	// paying for the IPC decode until the consumer asks.
	// Default returns empty so connections that don't yet implement
	// the parse safely report "no partition_values seen."
	virtual const std::string &GetLastPartitionValuesBytes() const {
		static const std::string kEmpty;
		return kEmpty;
	}

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
	virtual std::string GetAttachOpaqueDataHex() const = 0;
	virtual std::string GetTransactionOpaqueDataHex() const = 0;
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
    const ArrowArguments &arguments, const std::vector<uint8_t> &attach_opaque_data,
    const std::vector<uint8_t> &transaction_opaque_data,
    ClientContext &context, const std::string &function_type = "TABLE",
    const std::vector<uint8_t> &global_execution_id = {},
    bool worker_debug = false,
    const std::map<std::string, Value> &settings = {},
    const std::vector<VgiSecretRequirement> &required_secrets = {},
    const std::shared_ptr<VgiAttachParameters> &attach_params = nullptr);

// Create from a pooled worker (subprocess only — HTTP connections are never pooled).
std::unique_ptr<IFunctionConnection> CreateFunctionConnectionFromPool(
    std::unique_ptr<PooledWorker> pooled_worker, const std::string &function_name,
    const ArrowArguments &arguments, const std::vector<uint8_t> &attach_opaque_data,
    const std::vector<uint8_t> &transaction_opaque_data,
    ClientContext &context, const std::string &function_type = "TABLE",
    const std::vector<uint8_t> &global_execution_id = {},
    bool worker_debug = false,
    const std::map<std::string, Value> &settings = {},
    const std::vector<VgiSecretRequirement> &required_secrets = {});

} // namespace vgi
} // namespace duckdb
