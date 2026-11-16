// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>


#include "vgi_platform.hpp" // pid_t (real on POSIX, shim on Windows)

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"

#include "vgi_arrow_utils.hpp"
#include "vgi_cache_control.hpp" // VgiCacheControl (returned by GetLastCacheControl)
#include "vgi_protocol.hpp"

namespace duckdb {

class ClientContext;

namespace vgi {

// Continued table-in-out output: a reply's ``vgi.status`` says more output follows
// for the same input, and a zero-row input carrying ``vgi.status: CONTINUE`` asks
// for it. See IFunctionConnection::GetLastMoreOutput / WriteContinuationTick.
inline constexpr const char *VGI_STATUS_KEY = "vgi.status";
inline constexpr const char *VGI_STATUS_HAVE_MORE_OUTPUT = "HAVE_MORE_OUTPUT";
inline constexpr const char *VGI_STATUS_CONTINUE = "CONTINUE";

// Whether a data batch's metadata says ``vgi.status: HAVE_MORE_OUTPUT``.
inline bool HasMoreOutputStatus(const VgiMetadataPtr &metadata) {
	if (!metadata) {
		return false;
	}
	const int idx = metadata->FindKey(VGI_STATUS_KEY);
	return idx >= 0 && metadata->value(idx) == VGI_STATUS_HAVE_MORE_OUTPUT;
}

// The metadata a continuation tick carries.
inline VgiMetadataPtr ContinuationTickMetadata() {
	return VgiMetadata::Make({{VGI_STATUS_KEY, VGI_STATUS_CONTINUE}});
}

class PooledWorker;
struct VgiAttachParameters;
struct VgiSecretRequirement;
struct CopyFromBindContext;
struct CopyToBindContext;

// Shared state for dynamic filter values that tighten during execution.
// Written by the table scan (after Top-N sink updates), read by the connection (before tick).
struct TickFilterState {
	mutex lock;
	//! Base64-encoded Arrow IPC bytes of the latest v2 delta document.
	string encoded_filters;
	//! True when encoded_filters contains the latest delta.
	bool has_filters = false;
	//! Bumped each time a new delta replaces encoded_filters; each stream is sent
	//! a given delta once (TickFilterCursor).
	uint64_t generation = 0;
};

//! Which TickFilterState delta the current stream has been sent (shared by all
//! connection kinds). Reset whenever a new stream starts (fresh init, next split,
//! new TickFilterState) so its first tick carries the latest delta.
struct TickFilterCursor {
	uint64_t sent_generation = 0;

	void Reset() {
		sent_generation = 0;
	}

	//! The delta this tick should carry, or empty when the stream already has it.
	//! Marks it sent: a tick that fails to reach the worker fails the scan.
	string Take(TickFilterState &state) {
		lock_guard<mutex> l(state.lock);
		if (!state.has_filters || state.generation == sent_generation) {
			return string();
		}
		sent_generation = state.generation;
		return state.encoded_filters;
	}
};

class IFunctionConnection {
public:
	virtual ~IFunctionConnection() = default;

	// Dynamic filter state, read before each tick and sent as tick metadata.
	virtual void SetTickFilterState(shared_ptr<TickFilterState> state) = 0;

	// Reset per-init stream state so PerformInit can run again for the next split
	// on the same transport (like PerformFinalizeInit). Must first close the input
	// writer (IPC EOS) so the worker's tick reader ends; otherwise the next init is
	// written inside an unterminated tick stream and the subprocess hangs.
	//
	// A failure must leave the connection poisoned, not idle: ReleaseForPooling
	// gates on `init_done_ && !data_finished_`, so clearing both before a failed
	// init would let the next checkout read this split's init response.
	virtual void ResetForNextSplit() = 0;

	//! True only for the cached-replay connection, which streams our own captured
	//! bytes rather than a live worker's. Callers skip live-worker-only checks
	//! (e.g. wire-type conformance) for it: the bytes were validated at capture,
	//! and the result cache may replay a projection subset from a wider entry.
	virtual bool IsCachedReplay() const {
		return false;
	}

	//! Mark this connection unusable because a split init failed partway (e.g.
	//! interrupted): ResetForNextSplit already cleared the in-flight flags, so it
	//! would otherwise look idle with an unanswered init on the wire.
	virtual void MarkSplitInitFailed() {
	}

	// Send the bind RPC (spawning a subprocess worker on first call), including
	// any secret-scope retry. Not cached: callers pass the result to
	// PerformInit / PerformFinalizeInit.
	virtual BindResult PerformBindRpc() = 0;

	// Spawn the subprocess worker if needed. Callers that skip PerformBindRpc
	// (cached BindResult, straight to PerformInit) must call this first.
	// No-op where there is no spawn step (HTTP).
	virtual void EnsureWorkerSpawned() = 0;

	virtual void SetInputSchema(const VgiSchemaPtr &input_schema) = 0;

	// Time-travel AT clause (empty = none) carried in the bind request, so
	// function-backed tables see it at init via init_call.bind_call.at_value.
	virtual void SetAtClause(const std::string &at_unit, const std::string &at_value) {
		(void)at_unit;
		(void)at_value;
	}

	// Catalog schema owning this function, carried in the bind request: one
	// name may exist in several schemas. Empty = worker searches every schema.
	virtual void SetSchemaName(const std::string &schema_name) {
		(void)schema_name;
	}

	// Argument names for the bind request, aligned with the logical argument
	// order; null elements are unnamed varargs, a null list means unknown.
	virtual void SetArgumentNames(
	    const std::optional<std::vector<std::optional<std::string>>> &argument_names) {
		(void)argument_names;
	}

	// Client-minted InitRequest.substream_id for parallel streaming table-in-out:
	// stable across the substream's init / process / finalize, so a finalize on a
	// different HTTP backend still finds its state. Empty = none.
	virtual void SetSubstreamId(const std::vector<uint8_t> &substream_id) {
		(void)substream_id;
	}

	// COPY ... FROM context (source path + target schema) for the bind request
	// (init_call.bind_call.copy_from).
	virtual void SetCopyFromContext(const CopyFromBindContext &copy_from) {
		(void)copy_from;
	}

	// COPY ... TO context (destination path + format) for the bind request
	// (init_call.bind_call.copy_to).
	virtual void SetCopyToContext(const CopyToBindContext &copy_to) {
		(void)copy_to;
	}

	// Replace the input schema after bind, before OpenInputWriter, when the
	// actual DataChunk types differ from the bind types.
	virtual void UpdateInputSchemaForExecution(const VgiSchemaPtr &input_schema) = 0;

	// Send the init RPC; `bind_result` (from PerformBindRpc) is folded into the
	// InitRequest.
	//
	// `init_opaque_data`: empty for a primary init. A secondary init (connection
	// built with a non-empty `global_execution_id`) must pass the primary's
	// InitResult::opaque_data: the worker skips on_init and uses it directly.
	// `finalize_state_id`: worker bytes from table_buffering_combine (empty is
	// valid); nullopt outside finalize phases.
	virtual InitResult PerformInit(const BindResult &bind_result,
	                               const std::vector<int32_t> &projection_ids = {},
	                               VgiBufferPtr pushdown_filters = nullptr,
	                               std::vector<VgiBufferPtr> join_keys = {},
	                               const std::string &phase = "",
	                               const std::optional<OrderByHint> &order_by = std::nullopt,
	                               const std::optional<TableSampleHint> &table_sample = std::nullopt,
	                               const std::vector<uint8_t> &init_opaque_data = {},
	                               const std::optional<std::vector<uint8_t>> &finalize_state_id = std::nullopt,
	                               const std::vector<std::string> &split_tokens = {}) = 0;
	// Conditional-revalidation validators (vgi.cache.if_none_match /
	// if_modified_since) for the next PerformInit's metadata; empty clears a key.
	virtual void SetConditionalRequest(const std::string & /*if_none_match*/,
	                                   const std::string & /*if_modified_since*/) {}

	// Re-init the connection in FINALIZE mode (table-in-out). Closes the
	// current data streams and sends a new init RPC that references the
	// original bind via bind_result.
	virtual void PerformFinalizeInit(const BindResult &bind_result) = 0;

	virtual void OpenInputWriter() = 0;
	virtual void WriteInputBatch(const VgiBatchPtr &batch) = 0;
	virtual void CloseInputWriter() = 0;
	virtual VgiBatchPtr ReadDataBatch() = 0;

	// Buffered table function RPCs (Meta.table_buffering), after
	// PerformInit(phase="TABLE_BUFFERING"). The Source-phase finalize is a
	// producer stream: PerformInit(phase="TABLE_BUFFERING_FINALIZE",
	// finalize_state_id) then ReadDataBatch until EOS.

	// Sink one input batch; returns the worker-chosen state_id (opaque bytes).
	// `batch_index` is set only when the worker requires input batch indexes.
	virtual std::vector<uint8_t>
	RpcTableBufferingProcess(const std::string &function_name,
	                           const std::vector<uint8_t> &execution_id,
	                           const VgiBatchPtr &input_batch,
	                           std::optional<int64_t> batch_index = std::nullopt) = 0;

	virtual std::vector<std::vector<uint8_t>>
	RpcTableBufferingCombine(const std::string &function_name,
	                           const std::vector<uint8_t> &execution_id,
	                           const std::vector<std::vector<uint8_t>> &state_ids) = 0;

	virtual void
	RpcTableBufferingDestructor(const std::string &function_name,
	                              const std::vector<uint8_t> &execution_id) = 0;

	// Most recent stream-state token (HTTP only; empty otherwise), used by the
	// cancel dispatcher to address the stream.
	virtual std::vector<uint8_t> GetLastStateToken() const = 0;

	// ``vgi_batch_index`` from the last data batch's metadata (set in
	// ReadDataBatch, read by InstallBatch); INVALID_INDEX when absent.
	virtual idx_t GetLastBatchIndex() const {
		return duckdb::DConstants::INVALID_INDEX;
	}

	// The last data batch's ``vgi_partition_values#b64`` as raw Arrow IPC bytes
	// (empty when absent). Decoding and validation happen in InstallBatch on the
	// consumer thread, uniformly across transports.
	virtual const std::string &GetLastPartitionValuesBytes() const {
		static const std::string kEmpty;
		return kEmpty;
	}

	// Raw little-endian int32[] from the most recent data batch's
	// ``vgi_rpc.parent_row#b64`` metadata: for each output row, the 0-based index
	// of the input row that produced it (batched-lateral provenance). Empty = no
	// provenance on this batch (the operator assumes identity for a 1->1 map).
	virtual const std::string &GetLastParentRowBytes() const {
		static const std::string kEmpty;
		return kEmpty;
	}

	// Continued output (table-in-out exchange). True when the most recent data
	// batch carried ``vgi.status: HAVE_MORE_OUTPUT``: the worker has more output for
	// the same input batch, and the only valid next input is a continuation tick
	// (WriteContinuationTick). Every such reply's parent_row indexes that same input.
	// Set in ReadDataBatch.
	virtual bool GetLastMoreOutput() const {
		return false;
	}

	// Ask for the next batch of the last input's output: write a zero-row input
	// batch carrying ``vgi.status: CONTINUE``, then ReadDataBatch reads the reply.
	// Valid only right after a reply for which GetLastMoreOutput() was true.
	virtual void WriteContinuationTick() {
		throw NotImplementedException("This VGI connection type cannot continue an exchange's output");
	}

	// `vgi.cache.*` advertisement parsed from the last data batch (set in
	// ReadDataBatch); the result cache reads it after the first batch, the only
	// one a worker advertises on.
	virtual VgiCacheControl GetLastCacheControl() const {
		return {};
	}

	// Cancel the current stream; called off-thread by VgiCancelDispatcher and
	// may throw. Subprocess: a zero-row batch with VGI_RPC_CANCEL_KEY metadata
	// (state_token ignored). HTTP: a cancel exchange addressed by state_token.
	//
	// Route all logging / HTTP through ``live_context`` (the dispatcher's
	// long-lived connection), never ``context_``: the originating query's
	// ClientContext is usually destroyed by then.
	virtual void CancelStream(const std::vector<uint8_t> &state_token, ClientContext &live_context) = 0;

	// State queries
	virtual bool IsTableInOut() const = 0;
	virtual bool IsFinished() const = 0;
	virtual void MarkDataFinished() = 0;

	// Identity/diagnostics
	virtual std::string GetExecutionIdHex() const = 0;
	// Short SHA-256 digests (OpaqueDigest), never the raw values.
	virtual std::string GetAttachOpaqueDataDigest() const = 0;
	virtual std::string GetTransactionOpaqueDataDigest() const = 0;
	//! Short hex id unique per connection instance: the log correlation key for
	//! one checkout's bind → init → data → release.
	virtual std::string GetConnIdHex() const = 0;
	//! The worker's pid for subprocess transport, nullopt otherwise. For human
	//! diagnostics only; log fields use GetConnIdHex().
	virtual std::optional<pid_t> GetSubprocessPid() const {
		return std::nullopt;
	}

	// Lifecycle
	virtual int Wait() = 0;
	// The worker for the pool if alive and parked at its RPC loop, else nullptr.
	virtual std::unique_ptr<PooledWorker> ReleaseForPooling() = 0;
};

// A connection of the type worker_path selects.
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
