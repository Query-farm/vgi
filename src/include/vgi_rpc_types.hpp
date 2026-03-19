#pragma once

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace duckdb {
namespace vgi {

// ============================================================================
// IPC Bytes Serialization (dataclass-as-binary pattern)
// ============================================================================

// Serialize a RecordBatch to a complete IPC stream in memory (schema + batch + EOS).
// Returns raw bytes suitable for storing in a binary column.
std::vector<uint8_t> SerializeToIpcBytes(const std::shared_ptr<arrow::RecordBatch> &batch);

// Deserialize a RecordBatch from IPC bytes.
std::shared_ptr<arrow::RecordBatch> DeserializeFromIpcBytes(const std::vector<uint8_t> &bytes);
std::shared_ptr<arrow::RecordBatch> DeserializeFromIpcBytes(const uint8_t *data, size_t len);

// Serialize an Arrow Schema to IPC bytes.
std::vector<uint8_t> SerializeSchemaToIpcBytes(const std::shared_ptr<arrow::Schema> &schema);

// ============================================================================
// Enum Serialization (dictionary(int16, utf8))
// ============================================================================

// Build a dictionary-encoded array with a single value for enum serialization.
// The dictionary_values should be the list of all enum member names.
// The value should be the name of the selected member.
std::shared_ptr<arrow::Array> BuildEnumArray(const std::string &value,
                                              const std::vector<std::string> &dictionary_values);

// ============================================================================
// BindRequest / BindResponse
// ============================================================================

// Build a BindRequest as a RecordBatch (for serialization to IPC bytes).
// Fields match Python BindRequest dataclass:
//   function_name: utf8
//   arguments: binary (Arguments serialized as IPC bytes)
//   function_type: dictionary(int16, utf8) ("SCALAR", "TABLE", "AGGREGATE")
//   input_schema: binary|null (serialized Arrow schema)
//   settings: binary|null (settings RecordBatch as IPC bytes)
//   secrets: binary|null (secrets RecordBatch as IPC bytes, one struct column per secret)
//   attach_id: binary|null
//   transaction_id: binary|null
//   resolved_secrets_provided: bool (true when scoped secrets have been resolved)
std::shared_ptr<arrow::RecordBatch> BuildBindRequest(
    const std::string &function_name,
    const std::vector<uint8_t> &arguments_ipc_bytes,
    const std::string &function_type,   // "SCALAR", "TABLE", "AGGREGATE"
    const std::vector<uint8_t> &input_schema_bytes = {},   // Empty = null
    const std::vector<uint8_t> &settings_bytes = {},       // Empty = null
    const std::vector<uint8_t> &secrets_bytes = {},        // Empty = null
    const std::vector<uint8_t> &attach_id = {},
    const std::vector<uint8_t> &transaction_id = {},
    bool resolved_secrets_provided = false);

// Parsed BindResponse
struct BindResponseResult {
	std::shared_ptr<arrow::Schema> output_schema;
	std::vector<uint8_t> opaque_data;   // Empty if null
};

// Parse BindResponse from a deserialized RecordBatch (1 row).
// Fields: output_schema: binary, opaque_data: binary|null
BindResponseResult ParseBindResponse(const std::shared_ptr<arrow::RecordBatch> &batch,
                                     const std::string &worker_path = "");

// Result of trying to parse scoped secret lookup requests from a BindResponse
struct BindSecretScopeResponseResult {
	struct Lookup {
		std::string secret_type;  // Required — C++ enforces type matching
		std::string scope;        // Empty = not specified
		std::string name;         // Empty = not specified
	};
	std::vector<Lookup> lookups;
};

// Try to parse scoped secret lookup requests from a BindResponse batch.
// If the BindResponse contains non-empty lookup_secret_types field, this is a
// secret scope request and the function returns the parsed lookups.
// Returns nullopt if this is a normal BindResponse (no lookup fields or all empty).
std::optional<BindSecretScopeResponseResult> TryParseBindSecretScopeResponse(
    const std::shared_ptr<arrow::RecordBatch> &batch);

// ============================================================================
// InitRequest
// ============================================================================

// Build an InitRequest as a RecordBatch (for serialization to IPC bytes).
// Fields match Python InitRequest dataclass:
//   bind_call: binary (BindRequest as IPC bytes)
//   output_schema: binary (serialized Arrow schema)
//   bind_opaque_data: binary|null
//   projection_ids: list<int64>|null
//   pushdown_filters: binary|null
//   phase: dictionary(int16, utf8)|null ("INPUT", "FINALIZE")
//   execution_id: binary|null
//   init_opaque_data: binary|null
std::shared_ptr<arrow::RecordBatch> BuildInitRequest(
    const std::vector<uint8_t> &bind_call_bytes,
    const std::vector<uint8_t> &output_schema_bytes,
    const std::vector<uint8_t> &bind_opaque_data = {},
    const std::vector<int64_t> &projection_ids = {},
    const std::vector<uint8_t> &pushdown_filters_bytes = {},
    const std::string &phase = "",          // Empty = null, "INPUT", "FINALIZE"
    const std::vector<uint8_t> &execution_id = {},
    const std::vector<uint8_t> &init_opaque_data = {});

// ============================================================================
// GlobalInitResponse
// ============================================================================

struct GlobalInitResponseResult {
	std::vector<uint8_t> execution_id;
	int64_t max_workers = 1;
	std::vector<uint8_t> opaque_data;   // Empty if null
};

// Parse GlobalInitResponse from a header batch (1 row).
// Fields: execution_id: binary, max_workers: int64, opaque_data: binary|null
GlobalInitResponseResult ParseGlobalInitResponse(const std::shared_ptr<arrow::RecordBatch> &batch,
                                                  const std::string &worker_path = "");

// ============================================================================
// Catalog Response Unwrapping
// ============================================================================

// Unwrap response items from a catalog response.
// Catalog responses wrap items in {items: list<binary>} or {items: list<utf8>}.
// This extracts the binary items from the list column.
std::vector<std::vector<uint8_t>> UnwrapBinaryResponseItems(const std::shared_ptr<arrow::RecordBatch> &batch);

// Unwrap string items (for CatalogsResponse which has items: list<str>)
std::vector<std::string> UnwrapStringResponseItems(const std::shared_ptr<arrow::RecordBatch> &batch);

// ============================================================================
// RPC Params Builders for Catalog Methods
// ============================================================================

// ============================================================================
// TableFunctionCardinalityRequest / TableCardinality
// ============================================================================

// Build a TableFunctionCardinalityRequest as a RecordBatch (for serialization to IPC bytes).
// Fields match Python TableFunctionCardinalityRequest dataclass:
//   bind_call: binary (BindRequest as IPC bytes)
//   bind_opaque_data: binary|null
std::shared_ptr<arrow::RecordBatch> BuildTableFunctionCardinalityRequest(
    const std::vector<uint8_t> &bind_call_bytes,
    const std::vector<uint8_t> &bind_opaque_data = {});

// Parsed TableCardinality result
struct TableFunctionCardinalityResult {
	int64_t estimate = -1;  // -1 = unknown (null from worker)
	int64_t max = -1;       // -1 = unknown (null from worker)
};

// Parse TableCardinality from a deserialized RecordBatch (1 row).
// Fields: estimate: int64|null, max: int64|null
TableFunctionCardinalityResult ParseTableFunctionCardinalityResult(
    const std::shared_ptr<arrow::RecordBatch> &batch,
    const std::string &worker_path = "");

// ============================================================================
// RPC Params Builders for Catalog Methods
// ============================================================================

// Build params batch for the "bind" RPC method.
// The bind RPC has a single "request" field containing the serialized BindRequest.
std::shared_ptr<arrow::RecordBatch> BuildBindRpcParams(const std::vector<uint8_t> &bind_request_bytes);

// Build params batch for the "init" RPC method.
// The init RPC has a single "request" field containing the serialized InitRequest.
std::shared_ptr<arrow::RecordBatch> BuildInitRpcParams(const std::vector<uint8_t> &init_request_bytes);

// Build params batch for catalog_attach: name: utf8, options: binary|null
std::shared_ptr<arrow::RecordBatch> BuildCatalogAttachParams(const std::string &name,
                                                              const std::vector<uint8_t> &options_bytes = {});

// Build params batch for methods with just attach_id and optional transaction_id
std::shared_ptr<arrow::RecordBatch> BuildAttachIdParams(const std::vector<uint8_t> &attach_id,
                                                         const std::vector<uint8_t> &transaction_id = {});

// Build params batch for catalog_schema_get: attach_id, name, transaction_id
std::shared_ptr<arrow::RecordBatch> BuildSchemaGetParams(const std::vector<uint8_t> &attach_id,
                                                          const std::string &name,
                                                          const std::vector<uint8_t> &transaction_id = {});

// Build params batch for catalog_schema_contents_*: attach_id, name, transaction_id
std::shared_ptr<arrow::RecordBatch> BuildSchemaContentsParams(const std::vector<uint8_t> &attach_id,
                                                               const std::string &name,
                                                               const std::vector<uint8_t> &transaction_id = {});

// Build params batch for catalog_schema_contents_functions: attach_id, name, type, transaction_id
std::shared_ptr<arrow::RecordBatch> BuildSchemaContentsFunctionsParams(
    const std::vector<uint8_t> &attach_id, const std::string &name,
    const std::string &function_type,   // "SCALAR_FUNCTION" or "TABLE_FUNCTION"
    const std::vector<uint8_t> &transaction_id = {});

// Build params batch for catalog_table_get / catalog_view_get:
//   attach_id, schema_name, name, transaction_id
std::shared_ptr<arrow::RecordBatch> BuildTableOrViewGetParams(const std::vector<uint8_t> &attach_id,
                                                               const std::string &schema_name,
                                                               const std::string &name,
                                                               const std::vector<uint8_t> &transaction_id = {});

// Build params for table_get with AT clause (time travel)
std::shared_ptr<arrow::RecordBatch> BuildTableGetWithAtParams(const std::vector<uint8_t> &attach_id,
                                                               const std::string &schema_name,
                                                               const std::string &name,
                                                               const std::string &at_unit,
                                                               const std::string &at_value,
                                                               const std::vector<uint8_t> &transaction_id = {});

// Build params batch for catalog_table_scan_function_get:
//   attach_id, schema_name, name, at_unit, at_value, transaction_id
std::shared_ptr<arrow::RecordBatch> BuildTableScanFunctionGetParams(
    const std::vector<uint8_t> &attach_id, const std::string &schema_name,
    const std::string &name, const std::string &at_unit = "",
    const std::string &at_value = "", const std::vector<uint8_t> &transaction_id = {});

} // namespace vgi
} // namespace duckdb
