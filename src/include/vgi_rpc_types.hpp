// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "duckdb/common/insertion_order_preserving_map.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// IPC Bytes Serialization (dataclass-as-binary pattern)
// ============================================================================

// Serialize a RecordBatch to a complete IPC stream in memory (schema + batch + EOS).
// Returns raw bytes suitable for storing in a binary column.
// custom_metadata (optional) is attached to the record-batch message, exactly
// as MakeStreamWriter+WriteRecordBatch(batch, metadata) would — but via the
// single-allocation payload path (payload sizes are computed up front, so the
// destination vector is allocated once; no BufferOutputStream realloc chain
// and no final buffer→vector copy).
std::vector<uint8_t> SerializeToIpcBytes(const std::shared_ptr<arrow::RecordBatch> &batch,
                                          const std::shared_ptr<arrow::KeyValueMetadata> &custom_metadata = nullptr);

// Deserialize a RecordBatch from IPC bytes.
std::shared_ptr<arrow::RecordBatch> DeserializeFromIpcBytes(const std::vector<uint8_t> &bytes);
std::shared_ptr<arrow::RecordBatch> DeserializeFromIpcBytes(const uint8_t *data, size_t len);

// Zero-copy variant: decode the inner IPC stream stored in cell ``index`` of a
// BinaryArray (the "dataclass-as-binary" envelope pattern) WITHOUT copying the
// bytes. The decode reads from an arrow::SliceBuffer of the array's values
// buffer, so the returned batch's arrays refcount-share that buffer — the
// outer response batch can be dropped freely. Use on response-decode hot
// paths (bind results, table-buffering envelopes, catalog results) where the
// pointer-based overload would alloc+memcpy the whole inner payload.
std::shared_ptr<arrow::RecordBatch> DeserializeFromIpcBytesZeroCopy(const arrow::BinaryArray &bin,
                                                                     int64_t index);

// Deserialized batch with optional custom metadata from the IPC message.
struct DeserializedBatch {
	std::shared_ptr<arrow::RecordBatch> batch;
	std::shared_ptr<arrow::KeyValueMetadata> custom_metadata;
};

// Deserialize a RecordBatch from IPC bytes, preserving custom metadata.
DeserializedBatch DeserializeFromIpcBytesWithMetadata(const uint8_t *data, size_t len);

// Serialize an Arrow Schema to IPC bytes.
std::vector<uint8_t> SerializeSchemaToIpcBytes(const std::shared_ptr<arrow::Schema> &schema);

// Serialize a foreign key constraint to IPC bytes.
// The format matches the Python ForeignKeyDef serialization used by TableInfo.
std::vector<uint8_t> SerializeForeignKeyToIpcBytes(const std::vector<std::string> &fk_columns,
                                                    const std::vector<std::string> &pk_columns,
                                                    const std::string &referenced_table,
                                                    const std::string &referenced_schema);

// ============================================================================
// Enum Serialization (dictionary(int16, utf8))
// ============================================================================

// Build a dictionary-encoded array with a single value for enum serialization.
// The dictionary_values should be the list of all enum member names.
// The value should be the name of the selected member.
std::shared_ptr<arrow::Array> BuildEnumArray(const std::string &value,
                                              const std::vector<std::string> &dictionary_values);

// Optional variant: when ``value`` is std::nullopt, emits a single-row null
// dictionary array; otherwise identical to ``BuildEnumArray``.
std::shared_ptr<arrow::Array> BuildOptionalEnumArray(const std::optional<std::string> &value,
                                                      const std::vector<std::string> &dictionary_values);

// ============================================================================
// Single-row scalar/list/map builders for the codegen'd request builders.
// ============================================================================
//
// These are public-API helpers consumed by ``vgi/src/generated/vgi_request_builders.hpp``
// (whose generator lives in vgi-python ``vgi/codegen/cpp_request_builders.py``).
// Every helper appends exactly one element to a new builder and returns a
// single-row Array. The non-Optional variants always emit a non-null entry
// (use them for fields the schema declares ``nullable=false``); the Optional
// variants emit a null entry when the input is std::nullopt.
//
// The generator picks helper names from a fixed mapping; if you change a
// helper signature here, regenerate the header and re-build.

std::shared_ptr<arrow::Array> BuildBinaryScalarRequired(const std::vector<uint8_t> &value);
std::shared_ptr<arrow::Array> BuildOptionalBinaryScalar(const std::optional<std::vector<uint8_t>> &value);

std::shared_ptr<arrow::Array> BuildStringScalar(const std::string &value);
std::shared_ptr<arrow::Array> BuildOptionalStringScalar(const std::optional<std::string> &value);

std::shared_ptr<arrow::Array> BuildBoolScalar(bool value);
std::shared_ptr<arrow::Array> BuildOptionalBoolScalar(std::optional<bool> value);

std::shared_ptr<arrow::Array> BuildInt32Scalar(int32_t value);
std::shared_ptr<arrow::Array> BuildOptionalInt32Scalar(std::optional<int32_t> value);

std::shared_ptr<arrow::Array> BuildInt64Scalar(int64_t value);
std::shared_ptr<arrow::Array> BuildOptionalInt64Scalar(std::optional<int64_t> value);

// list<utf8>, list<binary>, list<int32>, list<int64> — single-row lists, always non-null.
std::shared_ptr<arrow::Array> BuildStringListScalar(const std::vector<std::string> &values);
std::shared_ptr<arrow::Array> BuildBinaryListScalar(const std::vector<std::vector<uint8_t>> &values);
std::shared_ptr<arrow::Array> BuildInt32ListScalar(const std::vector<int32_t> &values);
std::shared_ptr<arrow::Array> BuildInt64ListScalar(const std::vector<int64_t> &values);

// map<utf8, utf8> single-row scalars.
std::shared_ptr<arrow::Array> BuildStringMapScalar(const std::vector<std::pair<std::string, std::string>> &entries);
std::shared_ptr<arrow::Array>
BuildOptionalStringMapScalar(const std::optional<std::vector<std::pair<std::string, std::string>>> &entries);

// ============================================================================
// BindRequest / BindResponse
// ============================================================================

// COPY ... FROM context attached to a BindRequest (and, via the reused
// bind_request_bytes, to the InitRequest's bind_call). Set only for a COPY-FROM
// scan; nullptr/empty otherwise. Mirrors Python's CopyFromContext dataclass:
//   format: utf8, file_path: utf8, expected_schema: binary (IPC schema bytes).
struct CopyFromBindContext {
	std::string format;
	std::string file_path;
	std::vector<uint8_t> expected_schema_bytes;
};

// COPY ... TO context attached to a BindRequest (and, via the reused
// bind_request_bytes, to the InitRequest's bind_call). Set only for a COPY-TO
// sink; nullptr otherwise. Mirrors Python's CopyToContext dataclass:
//   format: utf8, file_path: utf8. The source columns ride input_schema.
struct CopyToBindContext {
	std::string format;
	std::string file_path;
};

// Build a BindRequest as a RecordBatch (for serialization to IPC bytes).
// Fields match Python BindRequest dataclass:
//   function_name: utf8
//   arguments: binary (Arguments serialized as IPC bytes)
//   function_type: dictionary(int16, utf8) ("SCALAR", "TABLE", "AGGREGATE")
//   input_schema: binary|null (serialized Arrow schema)
//   settings: binary|null (settings RecordBatch as IPC bytes)
//   secrets: binary|null (secrets RecordBatch as IPC bytes, one struct column per secret)
//   attach_opaque_data: binary|null
//   transaction_opaque_data: binary|null
//   resolved_secrets_provided: bool (true when scoped secrets have been resolved)
std::shared_ptr<arrow::RecordBatch> BuildBindRequest(
    const std::string &function_name,
    const std::vector<uint8_t> &arguments_ipc_bytes,
    const std::string &function_type,   // "SCALAR", "TABLE", "AGGREGATE"
    const std::vector<uint8_t> &input_schema_bytes = {},   // Empty = null
    const std::vector<uint8_t> &settings_bytes = {},       // Empty = null
    const std::vector<uint8_t> &secrets_bytes = {},        // Empty = null
    const std::vector<uint8_t> &attach_opaque_data = {},
    const std::vector<uint8_t> &transaction_opaque_data = {},
    bool resolved_secrets_provided = false,
    const std::string &at_unit = {},    // time travel; empty = null
    const std::string &at_value = {},   // time travel; empty = null
    const CopyFromBindContext *copy_from = nullptr,   // COPY FROM; null = omit field
    const CopyToBindContext *copy_to = nullptr,       // COPY TO; null = omit field
    // Catalog schema that owns this function. A worker may register the same
    // function name in more than one schema, so the bare name is not a unique
    // key — the worker resolves (schema_name, function_name). Empty = null,
    // which makes the worker fall back to a cross-schema name lookup.
    const std::string &schema_name = {});

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
//   join_keys: list<large_binary>|null
//   phase: dictionary(int16, utf8)|null ("INPUT", "FINALIZE")
//   execution_id: binary|null
//   init_opaque_data: binary|null
std::shared_ptr<arrow::RecordBatch> BuildInitRequest(
    const std::vector<uint8_t> &bind_call_bytes,
    const std::vector<uint8_t> &output_schema_bytes,
    const std::vector<uint8_t> &bind_opaque_data = {},
    const std::vector<int64_t> &projection_ids = {},
    std::shared_ptr<arrow::Buffer> pushdown_filters = nullptr,
    std::vector<std::shared_ptr<arrow::Buffer>> join_keys = {},
    const std::string &phase = "",          // Empty = null, "INPUT", "FINALIZE"
    const std::vector<uint8_t> &execution_id = {},
    const std::vector<uint8_t> &init_opaque_data = {},
    const std::string &order_by_column_name = {},   // Empty = null
    const std::string &order_by_direction = {},      // "ASC" or "DESC", empty = null
    const std::string &order_by_null_order = {},     // "NULLS_FIRST" or "NULLS_LAST", empty = null
    int64_t order_by_limit = -1,                     // -1 = null
    double tablesample_percentage = -1.0,            // -1.0 = null (no sample)
    int64_t tablesample_seed = -1,                   // -1 = null (no seed)
    const std::optional<std::vector<uint8_t>> &finalize_state_id = std::nullopt,  // TABLE_BUFFERING_FINALIZE only
    const std::vector<uint8_t> &substream_id = {});  // parallel streaming table-in-out per-substream id

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

// Unwrap items, deserialize each to a RecordBatch, and validate each item's
// schema against the registered item_schema for `method_name`. Zero-row items
// are skipped. Throws IOException on item schema mismatch (see
// ValidateItemSchema in vgi_schema_registry.hpp for the error format).
std::vector<std::shared_ptr<arrow::RecordBatch>>
UnwrapAndValidateItems(const std::shared_ptr<arrow::RecordBatch> &batch,
                       const std::string &method_name,
                       const std::string &worker_path);

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
// TableFunctionStatisticsRequest
// ============================================================================

// Build a TableFunctionStatisticsRequest as a RecordBatch (for serialization to IPC bytes).
// Wire shape matches TableFunctionCardinalityRequest — the Python worker reconstructs
// BindParams[TArgs] from bind_call and forwards it to the function's statistics() method,
// so the worker sees the user's original arguments (e.g. count=10000).
//   bind_call: binary (BindRequest as IPC bytes)
//   bind_opaque_data: binary|null
std::shared_ptr<arrow::RecordBatch> BuildTableFunctionStatisticsRequest(
    const std::vector<uint8_t> &bind_call_bytes,
    const std::vector<uint8_t> &bind_opaque_data = {});

// ============================================================================
// TableFunctionDynamicToStringRequest / Response
// ============================================================================

// Build a TableFunctionDynamicToStringRequest inner batch. Caller serialises and
// wraps with ``generated::BuildTableFunctionDynamicToStringParams`` to produce the
// wire params. Fields match Python TableFunctionDynamicToStringRequest:
//   bind_call: binary (BindRequest as IPC bytes)
//   bind_opaque_data: binary|null
//   global_execution_id: binary
std::shared_ptr<arrow::RecordBatch> BuildTableFunctionDynamicToStringRequest(
    const std::vector<uint8_t> &bind_call_bytes,
    const std::vector<uint8_t> &bind_opaque_data,
    const std::vector<uint8_t> &global_execution_id);

// Parse the response into an InsertionOrderPreservingMap<string>. The wire schema
// is parallel ``keys: list<utf8>`` / ``values: list<utf8>`` — order on the wire is
// the order the user emitted from their dynamic_to_string hook.
InsertionOrderPreservingMap<std::string> ParseTableFunctionDynamicToStringResult(
    const std::shared_ptr<arrow::RecordBatch> &batch,
    const std::string &worker_path = "");

// ============================================================================
// Hand-coded inner request builders
// ============================================================================
//
// Every catalog method's outer ``*Params`` shape is now generated into
// ``generated/vgi_request_builders.hpp`` from the Python ``VgiProtocol``
// (see ``vgi.codegen.cpp_request_builders`` in the sibling vgi-python repo).
// The hand-coded builders below produce the *inner* request batches that some
// of those generated outer wrappers expect — the caller serialises an inner
// batch with ``SerializeToIpcBytes`` and passes the bytes through
// ``generated::BuildXxxParams(bytes)`` to get the wire-shape outer params.
// Inner shapes carry list<list<int32>>, list<binary>, embedded Arrow schemas,
// or other fields the generator does not yet emit.

// Build the inner CatalogAttachRequest batch. Caller serialises and wraps
// with ``generated::BuildCatalogAttachParams`` to produce the wire params.
std::shared_ptr<arrow::RecordBatch> BuildCatalogAttachRequest(
    const std::string &name,
    const std::vector<uint8_t> &options_ipc_bytes = {},
    const std::string &data_version_spec = "",
    const std::string &implementation_version = "");


// Build the inner TableCreateRequest batch (catalog_table_create's payload).
// Fields match Python TableCreateRequest dataclass; serialise + wrap with
// ``generated::BuildCatalogTableCreateParams`` for the wire params.
std::shared_ptr<arrow::RecordBatch> BuildTableCreateRequest(
    const std::vector<uint8_t> &attach_opaque_data, const std::string &schema_name, const std::string &name,
    const std::shared_ptr<arrow::Schema> &columns_schema, const std::string &on_conflict,
    const std::vector<int> &not_null_constraints, const std::vector<std::vector<int>> &unique_constraints,
    const std::vector<std::string> &check_constraints, const std::vector<std::vector<int>> &primary_key_constraints,
    const std::vector<std::vector<uint8_t>> &foreign_key_constraints,
    const std::vector<uint8_t> &transaction_opaque_data = {});


} // namespace vgi
} // namespace duckdb
