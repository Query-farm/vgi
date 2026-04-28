#pragma once

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <memory>
#include <string>
#include <vector>

#include "duckdb/main/client_context.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// vgi_rpc Wire Protocol Constants
// ============================================================================

// Request metadata keys (written on request batches)
constexpr const char *RPC_METHOD_KEY = "vgi_rpc.method";
constexpr const char *RPC_REQUEST_VERSION_KEY = "vgi_rpc.request_version";
constexpr const char *RPC_REQUEST_VERSION_VALUE = "1";

// Response/log/error metadata keys (read from response batches)
constexpr const char *RPC_LOG_LEVEL_KEY = "vgi_rpc.log_level";
constexpr const char *RPC_LOG_MESSAGE_KEY = "vgi_rpc.log_message";
constexpr const char *RPC_LOG_EXTRA_KEY = "vgi_rpc.log_extra";
constexpr const char *RPC_SERVER_ID_KEY = "vgi_rpc.server_id";
constexpr const char *RPC_REQUEST_ID_KEY = "vgi_rpc.request_id";

// HTTP streaming state token metadata key
constexpr const char *RPC_STREAM_STATE_KEY = "vgi_rpc.stream_state#b64";

// External location metadata key (pointer batch)
constexpr const char *RPC_LOCATION_KEY = "vgi_rpc.location";

// External location SHA-256 checksum metadata key (pointer batch)
constexpr const char *RPC_LOCATION_SHA256_KEY = "vgi_rpc.location.sha256";

// Shared-memory transport keys (mirror vgi_rpc/metadata.py).
// Set on the *request batch* to advertise a client-allocated segment to the
// worker; the worker may then write its response batches into the segment
// and emit "pointer batches" carrying offset/length per batch.
constexpr const char *SHM_SEGMENT_NAME_KEY = "vgi_rpc.shm_segment_name";
constexpr const char *SHM_SEGMENT_SIZE_KEY = "vgi_rpc.shm_segment_size";
// Set on response *pointer batches* (zero-row data batches with these
// metadata keys instead of vgi_rpc.log_level): the real batch lives at
// [shm_offset, shm_offset+shm_length) inside the segment.
constexpr const char *SHM_OFFSET_KEY = "vgi_rpc.shm_offset";
constexpr const char *SHM_LENGTH_KEY = "vgi_rpc.shm_length";
constexpr const char *SHM_SOURCE_KEY = "vgi_rpc.shm_source";

// ============================================================================
// Batch Classification
// ============================================================================
// Per wire protocol spec:
// - num_rows > 0 or no metadata → DATA
// - num_rows == 0 + vgi_rpc.log_level == "EXCEPTION" → ERROR (throw)
// - num_rows == 0 + vgi_rpc.log_level present → LOG (forward to logger)
// - Otherwise → DATA (void return / stream-finish)

enum class RpcBatchType {
	DATA,
	LOG,
	ERROR,
	EXTERNAL_LOCATION
};

// Classify a batch according to the vgi_rpc wire protocol.
// Does NOT throw or dispatch - just classifies.
RpcBatchType ClassifyBatch(const std::shared_ptr<arrow::RecordBatch> &batch,
                           const std::shared_ptr<arrow::KeyValueMetadata> &custom_metadata);

// ============================================================================
// Request Writing
// ============================================================================

// Write an RPC request with parameters to a file descriptor.
// Creates an IPC stream: schema (from params_batch) + 1-row batch with method metadata + EOS.
// The params_batch must have exactly 1 row with one field per method parameter.
void WriteRpcRequest(int fd, const std::string &method_name,
                     const std::shared_ptr<arrow::RecordBatch> &params_batch,
                     const std::shared_ptr<arrow::KeyValueMetadata> &extra_metadata = nullptr);

// Write an RPC request with no parameters (zero-field schema, 1-row batch).
// Used for parameterless methods like catalog_catalogs.
void WriteEmptyRpcRequest(int fd, const std::string &method_name);

// ============================================================================
// Response Reading
// ============================================================================

// Result from reading a unary response
struct UnaryResponseResult {
	std::shared_ptr<arrow::RecordBatch> batch;           // The data batch (1-row with "result" column, or empty for void)
	std::shared_ptr<arrow::KeyValueMetadata> metadata;   // Custom metadata from the data batch
};

// Read a unary RPC response from a file descriptor.
// Opens an IPC stream reader, dispatches log batches, returns the first data batch.
// Throws IOException on EXCEPTION-level log batches.
// The response stream has schema with "result" column (or empty for void methods).
// worker_path/worker_pid are for error context.
UnaryResponseResult ReadUnaryResponse(int fd, ClientContext *context,
                                      const std::string &worker_path = "",
                                      pid_t worker_pid = -1);

// Result from reading a stream header
struct StreamHeaderResult {
	std::shared_ptr<arrow::RecordBatch> header_batch;    // The header data batch (1-row)
	std::shared_ptr<arrow::KeyValueMetadata> metadata;   // Custom metadata from the header batch
};

// Read a stream header (phase 1.5) from a file descriptor.
// Opens an IPC stream reader on the header schema, dispatches log batches,
// returns the header data batch, and drains to EOS.
// Used for streaming RPC methods that declare a header type (e.g., init()).
StreamHeaderResult ReadStreamHeader(int fd, ClientContext *context,
                                    const std::string &worker_path = "",
                                    pid_t worker_pid = -1);

// ============================================================================
// Buffer-based Serialization/Deserialization (for HTTP transport)
// ============================================================================

// Serialize an RPC request to bytes (same format as WriteRpcRequest but to buffer).
// Returns a complete Arrow IPC stream: schema + 1-row batch with method metadata + EOS.
std::vector<uint8_t> SerializeRpcRequest(const std::string &method_name,
                                          const std::shared_ptr<arrow::RecordBatch> &params_batch);

// Serialize an RPC request with no parameters (zero-field schema, 1-row batch).
std::vector<uint8_t> SerializeEmptyRpcRequest(const std::string &method_name);

// Parse a unary response from bytes (same logic as ReadUnaryResponse but from buffer).
// url is used for error context (replaces worker_path in fd-based version).
UnaryResponseResult ReadUnaryResponseFromBuffer(const uint8_t *data, size_t len,
                                                 ClientContext *context,
                                                 const std::string &url = "");

// Result from reading a stream header from a buffer.
// For HTTP init responses: the response body contains header IPC stream + data IPC stream.
struct BufferStreamHeaderResult {
	StreamHeaderResult header;
	size_t data_offset;  // Byte offset where the data IPC stream begins
};

// Parse a stream header from a buffer.
// Reads the header IPC stream, dispatches log batches, returns header batch + data offset.
BufferStreamHeaderResult ReadStreamHeaderFromBuffer(const uint8_t *data, size_t len,
                                                     ClientContext *context,
                                                     const std::string &url = "");

} // namespace vgi
} // namespace duckdb
