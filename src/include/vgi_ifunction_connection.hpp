// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "vgi_platform.hpp" // pid_t (real on POSIX, shim on Windows)

#include "duckdb/common/types/value.hpp"

#include "vgi_arrow_utils.hpp"
#include "vgi_cache_control.hpp" // VgiCacheControl (returned by GetLastCacheControl)
#include "vgi_protocol.hpp"

namespace duckdb {

class ClientContext;

namespace vgi {

class PooledWorker;
struct VgiAttachParameters;
struct VgiSecretRequirement;
struct CopyFromBindContext;
struct CopyToBindContext;

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

	// Set the time-travel AT clause (empty = none) carried into the bind request
	// so function-backed tables can read it at init via init_call.bind_call.at_value.
	// Default no-op: only the real subprocess/HTTP connections override it.
	virtual void SetAtClause(const std::string &at_unit, const std::string &at_value) {
		(void)at_unit;
		(void)at_value;
	}

	// Set the catalog schema that owns this function, carried into the bind
	// request. A worker may register the same function name in several schemas,
	// so the bare name does not identify the implementation — the worker
	// resolves (schema_name, function_name). Empty = none, which leaves the
	// worker to look the name up across every schema.
	// Default no-op: only the real subprocess/HTTP/webworker connections override it.
	virtual void SetSchemaName(const std::string &schema_name) {
		(void)schema_name;
	}

	// Set the stable, client-minted per-substream id carried on the InitRequest
	// for the parallel streaming table-in-out path (see InitRequest.substream_id).
	// It is the same for this substream's init / process / finalize, so a
	// finalize that lands on a different HTTP backend can still key the
	// substream's shared state. Empty = none (serial path / not table-in-out).
	// Default no-op: only the real subprocess/HTTP connections override it.
	virtual void SetSubstreamId(const std::vector<uint8_t> &substream_id) {
		(void)substream_id;
	}

	// Set the COPY ... FROM context (source path + target schema) carried into
	// the bind request so a worker CopyFromFunction reads it at bind/init via
	// init_call.bind_call.copy_from. Default no-op: only the real
	// subprocess/HTTP connections override it.
	virtual void SetCopyFromContext(const CopyFromBindContext &copy_from) {
		(void)copy_from;
	}

	// Set the COPY ... TO context (destination path + format) carried into the
	// bind request so a worker CopyToFunction reads it at bind/init via
	// init_call.bind_call.copy_to. Default no-op: only the real
	// subprocess/HTTP connections override it.
	virtual void SetCopyToContext(const CopyToBindContext &copy_to) {
		(void)copy_to;
	}

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
	// finalize_state_id is opaque worker-chosen bytes returned from
	// table_buffering_combine. Empty (b"") is a legitimate value;
	// nullopt means "no finalize_state_id supplied" (non-finalize phases).
	virtual InitResult PerformInit(const BindResult &bind_result,
	                               const std::vector<int32_t> &projection_ids = {},
	                               std::shared_ptr<arrow::Buffer> pushdown_filters = nullptr,
	                               std::vector<std::shared_ptr<arrow::Buffer>> join_keys = {},
	                               const std::string &phase = "",
	                               const std::optional<OrderByHint> &order_by = std::nullopt,
	                               const std::optional<TableSampleHint> &table_sample = std::nullopt,
	                               const std::vector<uint8_t> &init_opaque_data = {},
	                               const std::optional<std::vector<uint8_t>> &finalize_state_id = std::nullopt) = 0;
	// Arm conditional-revalidation validators (M6) sent on the NEXT PerformInit
	// via the init request's custom_metadata (vgi.cache.if_none_match /
	// vgi.cache.if_modified_since). Empty strings clear the corresponding key.
	// Default no-op: connection types that don't do RPCs (cached replay) ignore
	// it. Must be called before PerformInit.
	virtual void SetConditionalRequest(const std::string & /*if_none_match*/,
	                                   const std::string & /*if_modified_since*/) {}

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
	// Used only when the function declared Meta.table_buffering = True. The
	// connection must have been initialized via PerformInit(..., phase="TABLE_BUFFERING")
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
	// --- Table sink+source (buffered) RPC family ---
	//
	// Source-phase finalize is a producer-mode stream opened via
	// PerformInit(phase="TABLE_BUFFERING_FINALIZE", finalize_state_id=N)
	// and drained via ReadDataBatch until null (EOS).

	// Returns the worker-chosen state_id (opaque bytes) for this batch.
	virtual std::vector<uint8_t>
	RpcTableBufferingProcess(const std::string &function_name,
	                           const std::vector<uint8_t> &execution_id,
	                           const std::shared_ptr<arrow::RecordBatch> &input_batch,
	                           std::optional<int64_t> batch_index = std::nullopt) = 0;

	virtual std::vector<std::vector<uint8_t>>
	RpcTableBufferingCombine(const std::string &function_name,
	                           const std::vector<uint8_t> &execution_id,
	                           const std::vector<std::vector<uint8_t>> &state_ids) = 0;

	virtual void
	RpcTableBufferingDestructor(const std::string &function_name,
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

	// Raw little-endian int32[] from the most recent data batch's
	// ``vgi_rpc.parent_row#b64`` metadata: for each output row, the 0-based index
	// of the input row that produced it (batched-lateral provenance). Empty = no
	// provenance on this batch (the operator assumes identity for a 1->1 map).
	// Default empty so connections that don't implement the parse report "none."
	virtual const std::string &GetLastParentRowBytes() const {
		static const std::string kEmpty;
		return kEmpty;
	}

	// Parsed `vgi.cache.*` cache-control advertisement observed on the most
	// recent data batch's wire custom_metadata. Set inside ``ReadDataBatch``
	// (per batch); the result-cache capture layer reads it on the FIRST batch
	// (the only one a worker advertises on) via the same lockstep timing as
	// GetLastBatchIndex. Default returns present=false so transports/connection
	// types that don't parse it report "not cacheable."
	virtual VgiCacheControl GetLastCacheControl() const {
		return {};
	}

	// Cancel the current stream. Called off-thread by VgiCancelDispatcher
	// from a destructor-triggered teardown; may throw (dispatcher catches).
	// - Subprocess: writes a zero-row batch with VGI_RPC_CANCEL_KEY custom
	//   metadata on the input writer. state_token is ignored.
	// - HTTP: POSTs {state, cancel} to /{method}/exchange addressed by
	//   state_token (the most recent token seen on the stream).
	//
	// ``live_context`` MUST be a context guaranteed alive for the duration of
	// this call — the dispatcher passes its long-lived bot connection's
	// context. The connection's own ``context_`` is a reference to the
	// originating query's ClientContext, which is typically already destroyed
	// by the time the dispatcher runs this off-thread (use-after-free of its
	// Logger otherwise). Implementations must route all logging / HTTP through
	// ``live_context``, never ``context_``.
	virtual void CancelStream(const std::vector<uint8_t> &state_token, ClientContext &live_context) = 0;

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
