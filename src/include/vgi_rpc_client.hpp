// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"


#include <exception>
#include <memory>
#include <functional>
#include <string>
#include <unordered_map>
#include <string_view>
#include <utility>
#include <vector>

#include "duckdb/main/client_context.hpp"
// Relative: only ``src/include`` is on the include path, and these generated
// headers live beside it in ``src/generated``.
#include "../generated/vgi_protocol_names.hpp"
#include "../generated/vgi_protocol_version.hpp"
#include "../generated/vgi_secret_protocol_version.hpp"
#include "vgi_platform.hpp" // pid_t (real on POSIX, shim on Windows)

namespace duckdb {
namespace vgi {

// ---- vgi_rpc wire protocol constants ----

// Request metadata keys (written on request batches)
constexpr const char *RPC_METHOD_KEY = "vgi_rpc.method";
constexpr const char *RPC_REQUEST_VERSION_KEY = "vgi_rpc.request_version";
constexpr const char *RPC_REQUEST_VERSION_VALUE = "1";

// The addressed protocol's surface version (VgiProtocolId::version). The
// server enforces an exact major+minor match at dispatch; a mismatch surfaces
// as IOException with "upgrade the client" / "upgrade the worker" guidance.
constexpr const char *RPC_PROTOCOL_VERSION_KEY = "vgi_rpc.protocol_version";

// Protocol routing key. Servers dispatch on (protocol, method): one server may
// co-host protocols whose method names collide. On raw transports (subprocess,
// AF_UNIX, TCP, SAB, Iroh) this key is the only carrier of the protocol; over
// HTTP it is also projected into the URL path and the server rejects a
// mismatch. Required on every protocol-owned request: without it the server
// raises ProtocolNotSpecifiedError rather than guess.
constexpr const char *RPC_PROTOCOL_KEY = "vgi_rpc.protocol";

// One application protocol: its routing name and its independently versioned
// surface. Carried as a pair because a name stamped with another protocol's
// version is routed to a binding that rejects it. Both halves are GENERATED
// from vgi-python (vgi_protocol_names.hpp, vgi_*protocol_version.hpp; see
// scripts/regen_generated.py) — never hand-edit them: a stale name is a silent
// misroute. The major version is part of the name, so an incompatible client
// gets a 404 any proxy understands rather than a handler-level rejection.
struct VgiProtocolId {
	// Value of RPC_PROTOCOL_KEY, and the HTTP path's protocol segment.
	std::string_view name;
	// Value of RPC_PROTOCOL_VERSION_KEY. Each protocol is versioned on its own:
	// the secret protocol's 1.x has nothing to do with the worker protocol's 2.x.
	std::string_view version;
};

// The worker/catalog protocol: bind/init, the catalog_* family, aggregates,
// table_buffering_*. Everything this extension sends except secret_lookup.
inline constexpr VgiProtocolId VGI_MAIN_PROTOCOL {::duckdb::vgi::generated::VGI_PROTOCOL_NAME,
                                                 ::duckdb::vgi::generated::VGI_PROTOCOL_VERSION};

// Orchard's standalone secret service: the single unary ``secret_lookup``,
// versioned independently of the worker protocol.
inline constexpr VgiProtocolId VGI_SECRET_PROTOCOL {
    ::duckdb::vgi::generated::VGI_SECRET_PROTOCOL_NAME,
    ::duckdb::vgi::generated::VGI_SECRET_PROTOCOL_VERSION};

// Reserved server-level methods (``__transport_options__``, ``__upload_url__``)
// belong to no protocol: resolved before routing and mounted flat over HTTP at
// ``{prefix}/{method}``. They must NOT carry RPC_PROTOCOL_KEY: over HTTP the
// server compares it to the method's (empty) protocol and rejects the mismatch.
inline bool IsReservedRpcMethod(const std::string &method_name) {
	return method_name.rfind("__", 0) == 0;
}

// Response/log/error metadata keys (read from response batches)
constexpr const char *RPC_LOG_LEVEL_KEY = "vgi_rpc.log_level";
constexpr const char *RPC_LOG_MESSAGE_KEY = "vgi_rpc.log_message";
constexpr const char *RPC_LOG_EXTRA_KEY = "vgi_rpc.log_extra";
constexpr const char *RPC_SERVER_ID_KEY = "vgi_rpc.server_id";
constexpr const char *RPC_REQUEST_ID_KEY = "vgi_rpc.request_id";

// HTTP streaming state: the per-turn cursor, returned on every turn.
constexpr const char *RPC_STREAM_STATE_KEY = "vgi_rpc.stream_state#b64";
// The fixed half of a stream's state (init request, resolved schemas). Minted
// once on /init; the client echoes it on every later request so any worker can
// serve the turn.
constexpr const char *RPC_CALL_STATE_KEY = "vgi_rpc.call_state#b64";

// External-location pointer batch keys.
constexpr const char *RPC_LOCATION_KEY = "vgi_rpc.location";
constexpr const char *RPC_LOCATION_SHA256_KEY = "vgi_rpc.location.sha256";

// Transport capability negotiation (mirrors vgi-rpc TransportOptions):
// capabilities ride as vgi_rpc.transport.* metadata; shm is used only when
// both peers report it.
constexpr const char *TRANSPORT_OPTIONS_METHOD = "__transport_options__";
// Mounted over HTTP at {prefix}/__upload_url__/init; the request names the
// method itself, without the /init route suffix.
constexpr const char *UPLOAD_URL_METHOD = "__upload_url__";
constexpr const char *TRANSPORT_CAP_SHM_KEY = "vgi_rpc.transport.shm";

// Shared-memory keys (mirror vgi_rpc/metadata.py). On the request batch they
// advertise a client-allocated segment the worker may write responses into.
constexpr const char *SHM_SEGMENT_NAME_KEY = "vgi_rpc.shm_segment_name";
constexpr const char *SHM_SEGMENT_SIZE_KEY = "vgi_rpc.shm_segment_size";
// On response pointer batches (zero-row, no log_level): the real batch is at
// [shm_offset, shm_offset+shm_length) in the segment.
constexpr const char *SHM_OFFSET_KEY = "vgi_rpc.shm_offset";
constexpr const char *SHM_LENGTH_KEY = "vgi_rpc.shm_length";
constexpr const char *SHM_SOURCE_KEY = "vgi_rpc.shm_source";

// ---- Batch classification (vgi_rpc wire spec) ----
// - num_rows > 0 or no metadata → DATA
// - num_rows == 0 + log_level (or legacy vgi.log_level) "EXCEPTION" → ERROR
// - num_rows == 0 + any other log_level → LOG
// - num_rows == 0 + vgi_rpc.location → EXTERNAL_LOCATION
// - otherwise → DATA (void return / stream finish)

enum class RpcBatchType {
	DATA,
	LOG,
	ERROR,
	EXTERNAL_LOCATION
};

// Classifies only; does not throw or dispatch.
RpcBatchType ClassifyBatch(const VgiBatchPtr &batch,
                           const VgiMetadataPtr &custom_metadata);

// ---- Request writing ----

// Write schema + 1-row params batch (with method/protocol metadata) + EOS.
// params_batch has one field per method parameter.
void WriteRpcRequest(int fd, const std::string &method_name,
                     const VgiBatchPtr &params_batch,
                     const VgiMetadataPtr &extra_metadata = nullptr,
                     const VgiProtocolId &protocol = VGI_MAIN_PROTOCOL);

// Stream form for transports without an fd (native Iroh QUIC); identical bytes.
void WriteRpcRequest(const std::shared_ptr<VgiOutputStream> &sink,
                     const std::string &method_name,
                     const VgiBatchPtr &params_batch,
                     const VgiMetadataPtr &extra_metadata = nullptr,
                     const VgiProtocolId &protocol = VGI_MAIN_PROTOCOL);

// Parameterless request: zero-field schema, 1-row batch.
void WriteEmptyRpcRequest(int fd, const std::string &method_name,
                          const VgiProtocolId &protocol = VGI_MAIN_PROTOCOL);

// ---- Response reading ----

// How a unary / stream-header read treats its worker stream. Every transport
// reads the same shape (log/error batches, one data batch, drain to EOS) and
// differs only in these knobs.
struct WorkerStreamOptions {
	ClientContext *context = nullptr;
	// Worker path, or the URL for HTTP-style messages; appears in errors.
	std::string worker;
	// Worker named in log records when it differs from `worker` (a fetched
	// external location still logs against the worker that pointed at it).
	std::string log_worker;
	pid_t pid = -1;
	// Error wording: subprocess style ("... [worker: X]") or HTTP style ("... [url: X]").
	bool http_messages = false;
	// Lenient transports (HTTP bodies, browser SAB): a stream that ends without
	// the Arrow EOS marker ends the read instead of failing it, and errors while
	// draining after the data batch are ignored. Strict (fd, Iroh): every read
	// error is raised.
	bool lenient = false;
	// Unary only: what the stream is called in errors. Empty = "RPC response"
	// (or "HTTP RPC response" with http_messages).
	std::string noun;
	// Called before every read: fd readiness + cancellation. Empty = none.
	std::function<void()> before_read;
	std::string execution_id_hex;
	std::string attach_opaque_data_digest;
	std::string transaction_opaque_data_digest;
	std::string conn_id_hex;
};

// If `batch` is a log or error batch, handle it (log it, or throw the worker's
// error) and return true; return false for a data batch.
bool DispatchWorkerBatch(const VgiBatchPtr &batch,
                         const VgiMetadataPtr &custom_metadata,
                         const WorkerStreamOptions &opts);

struct UnaryResponseResult {
	VgiBatchPtr batch;           // The data batch (1-row with "result" column, or empty for void)
	VgiMetadataPtr metadata;   // Custom metadata from the data batch
};

// Read a unary response from an fd: dispatch log batches, return the first data
// batch, throw IOException on an EXCEPTION batch. worker_path/worker_pid are for
// error context. No wall-clock deadline: blocks until the worker speaks,
// polling the context's `interrupted` flag every 250ms, so slow responses are
// never killed by a timeout. Cleaning up a connection interrupted mid-RPC is
// the caller's job (route through VgiCancelDispatcher so the worker unblocks).
UnaryResponseResult ReadUnaryResponse(int fd, ClientContext *context,
                                      const std::string &worker_path = "",
                                      pid_t worker_pid = -1,
                                      const std::string &execution_id_hex = "",
                                      const std::string &attach_opaque_data_digest = "",
                                      const std::string &transaction_opaque_data_digest = "",
                                      const std::string &conn_id_hex = "");

// Stream form: cancellation polling is the VgiInputStream's job (native Iroh
// uses short timed reads and cancels its QUIC stream on interrupt).
UnaryResponseResult ReadUnaryResponse(const std::shared_ptr<VgiInputStream> &input,
                                      ClientContext *context,
                                      const std::string &worker_path = "",
                                      pid_t worker_pid = -1,
                                      const std::string &execution_id_hex = "",
                                      const std::string &attach_opaque_data_digest = "",
                                      const std::string &transaction_opaque_data_digest = "",
                                      const std::string &conn_id_hex = "");

struct StreamHeaderResult {
	VgiBatchPtr header_batch;    // The header data batch (1-row)
	VgiMetadataPtr metadata;   // Custom metadata from the header batch
};

// True when an IPC read/open failure means the stream is truncated, closed or
// not valid IPC; false for transport errors (e.g. a read error). Used by callers
// that tolerate a stream ending without its EOS marker.
bool IsIpcStreamInvalid(const std::exception &e);

// Read a unary response / stream header from any worker stream (the fd, buffer
// and SAB entry points wrap these). The header reader leaves `input` just past
// the header stream, where the data stream begins.
UnaryResponseResult ReadWorkerUnaryStream(const std::shared_ptr<VgiInputStream> &input,
                                          const WorkerStreamOptions &opts);
StreamHeaderResult ReadWorkerStreamHeader(const std::shared_ptr<VgiInputStream> &input,
                                          const WorkerStreamOptions &opts);

// Header stream of a streaming method that declares a header type (e.g. init).
StreamHeaderResult ReadStreamHeader(int fd, ClientContext *context,
                                    const std::string &worker_path = "",
                                    pid_t worker_pid = -1);

StreamHeaderResult ReadStreamHeader(const std::shared_ptr<VgiInputStream> &input,
                                    ClientContext *context,
                                    const std::string &worker_path = "",
                                    pid_t worker_pid = -1);

// ---- Buffer-based (HTTP) serialization ----

// Same bytes as WriteRpcRequest, to a buffer. extra_metadata is folded into the
// request metadata; HTTP uses it for the result-cache revalidation validators
// (vgi.cache.if_none_match / if_modified_since) on `/init`, because the first
// producer turn runs inside /init and must see them.
std::vector<uint8_t> SerializeRpcRequest(
    const std::string &method_name, const VgiBatchPtr &params_batch,
    const VgiProtocolId &protocol = VGI_MAIN_PROTOCOL,
    const std::vector<std::pair<std::string, std::string>> &extra_metadata = {});

std::vector<uint8_t> SerializeEmptyRpcRequest(const std::string &method_name,
                                              const VgiProtocolId &protocol = VGI_MAIN_PROTOCOL);

// ReadUnaryResponse over a buffer; `url` is the error context. The id/digest
// parameters give embedded log batches the same diagnostic fields as the
// streaming path.
UnaryResponseResult ReadUnaryResponseFromBuffer(const uint8_t *data, size_t len,
                                                 ClientContext *context,
                                                 const std::string &url = "",
                                                 const std::string &execution_id_hex = "",
                                                 const std::string &attach_opaque_data_digest = "",
                                                 const std::string &transaction_opaque_data_digest = "",
                                                 const std::string &conn_id_hex = "");

// Call context the error-body decoder lacks (worker location rather than the
// request URL, execution, extra fields such as function name/kind), so an HTTP
// error body is reported like an error read off a subprocess stream.
struct RpcErrorContext {
	std::string worker_path; // the worker's base location; empty = the request URL
	std::string execution_id_hex;
	std::string attach_opaque_data_digest;
	std::string conn_id_hex;
	std::unordered_map<std::string, std::string> fields;
};

// Dispatch every IPC stream concatenated in an error response body, throwing
// on the first batch with error metadata. A streaming RPC answers with a header
// stream then a data stream; an error raised after the header (e.g. on the
// first next_batch) rides in the second stream, which
// ReadUnaryResponseFromBuffer never reads. Also safe for a one-stream body.
void DispatchErrorStreamsFromBuffer(const uint8_t *data, size_t len, ClientContext *context,
                                    const std::string &url = "", const RpcErrorContext *call = nullptr);

// Move-in overload: adopts `body` as the owning buffer instead of copying it.
// Prefer it whenever the caller is done with the body.
UnaryResponseResult ReadUnaryResponseFromBuffer(std::string &&body,
                                                 ClientContext *context,
                                                 const std::string &url = "",
                                                 const std::string &execution_id_hex = "",
                                                 const std::string &attach_opaque_data_digest = "",
                                                 const std::string &transaction_opaque_data_digest = "",
                                                 const std::string &conn_id_hex = "");

// HTTP init response: header IPC stream followed by the data IPC stream.
struct BufferStreamHeaderResult {
	StreamHeaderResult header;
	size_t data_offset;  // Byte offset where the data IPC stream begins
};

BufferStreamHeaderResult ReadStreamHeaderFromBuffer(const uint8_t *data, size_t len,
                                                     ClientContext *context,
                                                     const std::string &url = "");

// Owning-buffer overload: no copy; the header batch references ``buffer``
// through refcounted slices. Lets the HTTP /init path materialize the body
// once and slice the data section at data_offset.
BufferStreamHeaderResult ReadStreamHeaderFromBuffer(VgiBufferPtr buffer,
                                                     ClientContext *context,
                                                     const std::string &url = "");

} // namespace vgi
} // namespace duckdb
