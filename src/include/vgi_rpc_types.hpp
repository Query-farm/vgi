// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"


#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "vgi_batch_validation.hpp"
#include "vgi_protocol.hpp"
#include "../generated/vgi_protocol_records.hpp"

namespace duckdb {
namespace vgi {

// IPC bytes serialization (the "dataclass-as-binary" pattern).

// Serialize a batch to a complete in-memory IPC stream (schema + batch + EOS).
// custom_metadata, if given, is attached to the record-batch message.
std::vector<uint8_t> SerializeToIpcBytes(const VgiBatchPtr &batch,
                                          const VgiMetadataPtr &custom_metadata = nullptr);

// Deserialize a batch from IPC bytes. Nested IPC payloads come from workers, so
// the decoded batch is validated (vgi_batch_validation.hpp); callers without a
// query context keep FULL.
VgiBatchPtr DeserializeFromIpcBytes(const std::vector<uint8_t> &bytes,
                                                             WorkerBatchValidation validation = WorkerBatchValidation::FULL);
VgiBatchPtr DeserializeFromIpcBytes(const uint8_t *data, size_t len,
                                                             WorkerBatchValidation validation = WorkerBatchValidation::FULL);

// Zero-copy variant: decodes from `bytes` (typically a slice of an outer
// batch's values buffer) without copying. The returned batch's arrays share
// that buffer's refcount, so the outer batch may be dropped. Prefer this on
// response-decode hot paths; the pointer overload copies the whole payload.
VgiBatchPtr DeserializeFromIpcBytesZeroCopy(const VgiBufferPtr &bytes,
                                            WorkerBatchValidation validation = WorkerBatchValidation::FULL);

// The inner batch of a unary response envelope: a one-row ``{result: binary}``
// batch whose cell is the IPC-serialized method result. Decoded zero-copy and
// validated at `validation`. A missing envelope, a missing/non-binary `result`
// column or an undecodable payload is a protocol error carrying `info` (plus
// the RPC name); a NULL `result` is too, unless `null_result_ok` (methods whose
// result is empty), in which case nullptr is returned. `custom_metadata`, when
// given, receives the inner batch message's custom metadata.
VgiBatchPtr UnwrapResultEnvelope(const VgiBatchPtr &envelope,
                                                         const std::string &method,
                                                         const std::unordered_map<std::string, std::string> &info,
                                                         bool null_result_ok = false,
                                                         WorkerBatchValidation validation = WorkerBatchValidation::FULL,
                                                         VgiMetadataPtr *custom_metadata = nullptr);

// The callers' "empty means absent" convention, for nullable record members.
inline std::optional<std::vector<uint8_t>> BytesOrNull(const std::vector<uint8_t> &bytes) {
	return bytes.empty() ? std::nullopt : std::make_optional(bytes);
}
inline std::optional<std::string> StringOrNull(const std::string &value) {
	return value.empty() ? std::nullopt : std::make_optional(value);
}

// A generated protocol record carried as an IPC blob (the "dataclass-as-binary"
// pattern: list<binary> items, nested specs). Protocol errors name `worker_path`.
template <class T>
T DecodeIpcRecord(const std::vector<uint8_t> &bytes, const std::string &worker_path,
                  WorkerBatchValidation validation = WorkerBatchValidation::FULL) {
	return generated::Decode<T>(DeserializeFromIpcBytes(bytes, validation), 0, worker_path);
}

// A generated protocol record as an IPC blob (the inverse of DecodeIpcRecord).
template <class T>
std::vector<uint8_t> SerializeRecord(const T &value) {
	return SerializeToIpcBytes(generated::ToBatch(value));
}

// Deserialized batch with optional custom metadata from the IPC message.
struct DeserializedBatch {
	VgiBatchPtr batch;
	VgiMetadataPtr custom_metadata;
};

// Deserialize a RecordBatch from IPC bytes, preserving custom metadata.
DeserializedBatch DeserializeFromIpcBytesWithMetadata(const uint8_t *data, size_t len,
                                                      WorkerBatchValidation validation = WorkerBatchValidation::FULL);

// Serialize an Arrow Schema to IPC bytes.
std::vector<uint8_t> SerializeSchemaToIpcBytes(const VgiSchemaPtr &schema);

// Serialize a foreign key constraint to IPC bytes.
// The format matches the Python ForeignKeyDef serialization used by TableInfo.
std::vector<uint8_t> SerializeForeignKeyToIpcBytes(const std::vector<std::string> &fk_columns,
                                                    const std::vector<std::string> &pk_columns,
                                                    const std::string &referenced_table,
                                                    const std::string &referenced_schema);

// Enum serialization: dictionary(int16, utf8).

// A one-row dictionary array whose dictionary is all of `dictionary_values`
// (the enum member names) and whose value is `value`; throws if absent.
ArrowArray BuildEnumArray(const std::string &value,
                                              const std::vector<std::string> &dictionary_values);

// Optional variant: when ``value`` is std::nullopt, emits a single-row null
// dictionary array; otherwise identical to ``BuildEnumArray``.
ArrowArray BuildOptionalEnumArray(const std::optional<std::string> &value,
                                                      const std::vector<std::string> &dictionary_values);

// Single-row scalar/list/map builders called by the generated
// ``generated/vgi_request_builders.hpp`` (generator: vgi-python
// ``vgi/codegen/cpp_request_builders.py``, which maps fields to these names by a
// fixed table — changing a signature here requires regenerating). Each returns
// a one-row array; non-Optional variants always emit a non-null entry, Optional
// variants emit null for std::nullopt.

// Releases completed request columns if a later field fails validation. On
// success FromColumns moves the vector and takes ownership of every column.
class RequestArrayGuard {
public:
	explicit RequestArrayGuard(std::vector<ArrowArray> &arrays) : arrays_(arrays) {
	}
	~RequestArrayGuard() {
		for (auto &array : arrays_) {
			if (array.release) {
				array.release(&array);
			}
		}
	}
	RequestArrayGuard(const RequestArrayGuard &) = delete;
	RequestArrayGuard &operator=(const RequestArrayGuard &) = delete;

private:
	std::vector<ArrowArray> &arrays_;
};

// Copies one struct value; an empty column is allowed only for nullable fields.
ArrowArray BuildStructScalar(const VgiColumn &value, const ArrowSchema &field, bool nullable);

ArrowArray BuildBinaryScalarRequired(const std::vector<uint8_t> &value);
ArrowArray BuildOptionalBinaryScalar(const std::optional<std::vector<uint8_t>> &value);

ArrowArray BuildStringScalar(const std::string &value);
ArrowArray BuildOptionalStringScalar(const std::optional<std::string> &value);

ArrowArray BuildBoolScalar(bool value);
ArrowArray BuildOptionalBoolScalar(std::optional<bool> value);

ArrowArray BuildInt32Scalar(int32_t value);
ArrowArray BuildOptionalInt32Scalar(std::optional<int32_t> value);

ArrowArray BuildInt64Scalar(int64_t value);
ArrowArray BuildOptionalInt64Scalar(std::optional<int64_t> value);

// list<utf8>, list<binary>, list<int32>, list<int64> single-row lists. The
// Optional/Nullable string variants emit a null list for std::nullopt;
// BuildNullableStringListScalar also allows null elements.
ArrowArray BuildStringListScalar(const std::vector<std::string> &values);
ArrowArray BuildOptionalStringListScalar(const std::optional<std::vector<std::string>> &values);
ArrowArray
BuildNullableStringListScalar(const std::optional<std::vector<std::optional<std::string>>> &values);
ArrowArray BuildBinaryListScalar(const std::vector<std::vector<uint8_t>> &values);
ArrowArray BuildInt32ListScalar(const std::vector<int32_t> &values);
ArrowArray BuildInt64ListScalar(const std::vector<int64_t> &values);

// map<utf8, utf8> single-row scalars.
ArrowArray BuildStringMapScalar(const std::vector<std::pair<std::string, std::string>> &entries);
ArrowArray
BuildOptionalStringMapScalar(const std::optional<std::vector<std::pair<std::string, std::string>>> &entries);

// DuckDB exposes a single schema identifier; these adapt it to the protocol's
// structural schema paths. A path of any depth other than one is rejected,
// never flattened.
std::vector<std::string> SingleSchemaPath(const std::string &schema_name);
std::optional<std::vector<std::string>> OptionalSingleSchemaPath(const std::string &schema_name);
std::string SchemaNameFromPath(const std::vector<std::string> &schema_path, const std::string &field_name);
std::string OptionalSchemaNameFromPath(const std::optional<std::vector<std::string>> &schema_path,
                                       const std::string &field_name);

// BindRequest / BindResponse

// COPY ... FROM context attached to a BindRequest (and so to the InitRequest's
// bind_call). Set only for a COPY-FROM scan. Mirrors generated::CopyFromContext;
// expected_schema_bytes is an IPC-serialized schema.
struct CopyFromBindContext {
	std::string format;
	std::string file_path;
	std::vector<uint8_t> expected_schema_bytes;
};

// COPY ... TO context attached to a BindRequest (and so to the InitRequest's
// bind_call). Set only for a COPY-TO sink. The source columns ride input_schema.
struct CopyToBindContext {
	std::string format;
	std::string file_path;
};

// BindRequest.arguments_location (protocol 3.4, docs/large-values-3.4.md §2):
// the arguments IPC was uploaded once and the worker resolves it from `ref`
// (sealed by its __upload_url__ grant). `size` and `sha256_hex` describe
// exactly the uploaded bytes.
struct ArgumentsLocationRef {
	std::vector<uint8_t> ref;
	int64_t size = 0;
	std::string sha256_hex;
};

// Build a BindRequest batch (wire shape: generated::BindRequest). Empty byte
// vectors / strings are sent as null. `secrets` is a batch with one struct
// column per secret; resolved_secrets_provided is true once scoped secrets have
// been resolved. function_type must be a FunctionType wire name.
VgiBatchPtr
BuildBindRequest(const std::string &function_name, const std::vector<uint8_t> &arguments_ipc_bytes,
                 const std::string &function_type,                    // FunctionType wire name
                 const std::vector<uint8_t> &input_schema_bytes = {}, // Empty = null
                 const std::vector<uint8_t> &settings_bytes = {},     // Empty = null
                 const std::vector<uint8_t> &secrets_bytes = {},      // Empty = null
                 const std::vector<uint8_t> &attach_opaque_data = {},
                 const std::vector<uint8_t> &transaction_opaque_data = {}, bool resolved_secrets_provided = false,
                 const std::string &at_unit = {},                // time travel; empty = null
                 const std::string &at_value = {},               // time travel; empty = null
                 const CopyFromBindContext *copy_from = nullptr, // COPY FROM; null = omit field
                 const CopyToBindContext *copy_to = nullptr,     // COPY TO; null = omit field
                 // Catalog schema that owns this function. A worker may register the same
                 // function name in more than one schema, so the bare name is not a unique
                 // key — the worker resolves (schema_path, function_name). Empty = null,
                 // which makes the worker fall back to a cross-schema name lookup.
                 const std::string &schema_name = {},
                 // Resolved names aligned with the complete logical argument list.
                 // A null list means unavailable; null elements are unnamed varargs.
                 const std::optional<std::vector<std::optional<std::string>>> &argument_names = std::nullopt,
                 // Set: `arguments` is null and this location is sent instead
                 // (arguments_ipc_bytes is then ignored). Exactly one is non-null.
                 const ArgumentsLocationRef *arguments_location = nullptr);

// Parsed BindResponse
struct BindResponseResult {
	VgiSchemaPtr output_schema;
	std::vector<uint8_t> opaque_data;   // Empty if null
	std::vector<std::string> projection_expression_profiles;
	BindRepeatability repeatability = BindRepeatability::UNKNOWN;
};

// Parse a one-row BindResponse; also validates projection_expression_profiles
// and repeatability.
BindResponseResult ParseBindResponse(const VgiBatchPtr &batch,
                                     const std::string &worker_path = "");

// Scoped secret lookup requests carried by a BindResponse.
struct BindSecretScopeResponseResult {
	struct Lookup {
		std::string secret_type;  // Required — C++ enforces type matching
		std::string scope;        // Empty = not specified
		std::string name;         // Empty = not specified
	};
	std::vector<Lookup> lookups;
};

// A BindResponse with a non-empty lookup_secret_types is a secret-scope request:
// returns its lookups. Returns nullopt for a normal BindResponse.
std::optional<BindSecretScopeResponseResult> TryParseBindSecretScopeResponse(
    const VgiBatchPtr &batch);

// InitRequest

// Build an InitRequest batch (wire shape: generated::InitRequest). Empty
// vectors/strings and the negative sentinels below are sent as null.
VgiBatchPtr BuildInitRequest(
    const std::vector<uint8_t> &bind_call_bytes,
    const std::vector<uint8_t> &output_schema_bytes,
    const std::vector<uint8_t> &bind_opaque_data = {},
    const std::vector<int64_t> &projection_ids = {},
    VgiBufferPtr pushdown_filters = nullptr,
    std::vector<VgiBufferPtr> join_keys = {},
    const std::string &phase = "",          // Empty = null, else a VgiInitPhase wire name
    const std::vector<uint8_t> &execution_id = {},
    const std::vector<uint8_t> &init_opaque_data = {},
    const std::string &order_by_column_name = {},   // Empty = null
    const std::string &order_by_direction = {},      // "ASC" or "DESC", empty = null
    const std::string &order_by_null_order = {},     // "NULLS_FIRST" or "NULLS_LAST", empty = null
    int64_t order_by_limit = -1,                     // -1 = null
    double tablesample_percentage = -1.0,            // -1.0 = null (no sample)
    int64_t tablesample_seed = -1,                   // -1 = null (no seed)
    const std::optional<std::vector<uint8_t>> &finalize_state_id = std::nullopt,  // TABLE_BUFFERING_FINALIZE only
    const std::vector<uint8_t> &substream_id = {},           // parallel streaming table-in-out per-substream id
    const std::vector<std::string> &split_tokens = {});  // splits this init redeems (DuckDB sends one)

// GlobalInitResponse

struct GlobalInitResponseResult {
	std::vector<uint8_t> execution_id;
	int64_t max_workers = 1;
	std::vector<uint8_t> opaque_data;   // Empty if null
};

// Parse a one-row GlobalInitResponse; max_workers < 1 is clamped to 1.
GlobalInitResponseResult ParseGlobalInitResponse(const VgiBatchPtr &batch,
                                                  const std::string &worker_path = "");

// Catalog response unwrapping: responses wrap items in {items: list<binary>}
// or {items: list<utf8>}. Null items are skipped; a missing/null `items` yields
// an empty vector.
std::vector<std::vector<uint8_t>> UnwrapBinaryResponseItems(const VgiBatchPtr &batch);

// The list<utf8> form.
std::vector<std::string> UnwrapStringResponseItems(const VgiBatchPtr &batch);

// Unwrap items, deserialize each to a RecordBatch, and validate each item's
// schema against the registered item_schema for `method_name`. Zero-row items
// are skipped. Throws IOException on item schema mismatch (see
// ValidateItemSchema in vgi_schema_registry.hpp for the error format).
std::vector<VgiBatchPtr>
UnwrapAndValidateItems(const VgiBatchPtr &batch,
                       const std::string &method_name,
                       const std::string &worker_path);

// Table function planning / cardinality

//! Build the inner ``TableFunctionPlanRequest`` record (see the .cpp for which
//! fields DuckDB leaves null).
VgiBatchPtr
BuildTableFunctionPlanRequest(const std::vector<uint8_t> &bind_call_bytes,
                              const std::vector<uint8_t> &bind_opaque_data,
                              const std::vector<int32_t> &projection_ids,
                              const std::vector<uint8_t> &pushdown_filters,
                              int64_t min_splits, int64_t target_split_bytes,
                              const std::vector<uint8_t> &cursor);

VgiBatchPtr BuildTableFunctionCardinalityRequest(
    const std::vector<uint8_t> &bind_call_bytes,
    const std::vector<uint8_t> &bind_opaque_data = {});

struct TableFunctionCardinalityResult {
	int64_t estimate = -1;  // -1 = unknown (null from worker)
	int64_t max = -1;       // -1 = unknown (null from worker)
};

// Parse a one-row TableFunctionCardinalityResult; an empty batch is unknown.
TableFunctionCardinalityResult ParseTableFunctionCardinalityResult(
    const VgiBatchPtr &batch,
    const std::string &worker_path = "");

// Same wire shape as the cardinality request: the worker rebuilds the bind
// parameters from bind_call, so statistics() sees the user's original arguments.
VgiBatchPtr BuildTableFunctionStatisticsRequest(
    const std::vector<uint8_t> &bind_call_bytes,
    const std::vector<uint8_t> &bind_opaque_data = {});

// Inner TableFunctionDynamicToStringRequest batch; the caller serialises it and
// wraps it with ``generated::BuildTableFunctionDynamicToStringParams``.
VgiBatchPtr BuildTableFunctionDynamicToStringRequest(
    const std::vector<uint8_t> &bind_call_bytes,
    const std::vector<uint8_t> &bind_opaque_data,
    const std::vector<uint8_t> &global_execution_id);

// Parse the parallel ``keys`` / ``values`` lists, preserving the order the
// worker's dynamic_to_string hook emitted them in.
InsertionOrderPreservingMap<std::string> ParseTableFunctionDynamicToStringResult(
    const VgiBatchPtr &batch,
    const std::string &worker_path = "");

// Inner request builders. The outer ``*Params`` shapes are generated
// (``generated/vgi_request_builders.hpp``); some take an inner request batch,
// which the caller serialises with ``SerializeToIpcBytes`` and passes to
// ``generated::BuildXxxParams(bytes)``.

// Build the inner CatalogAttachRequest batch. Caller serialises and wraps
// with ``generated::BuildCatalogAttachParams`` to produce the wire params.
VgiBatchPtr BuildCatalogAttachRequest(
    const std::string &name,
    const std::vector<uint8_t> &options_ipc_bytes = {},
    const std::string &data_version_spec = "",
    const std::string &implementation_version = "",
    bool query_enabled = false);


// Build the inner TableCreateRequest batch (catalog_table_create's payload);
// serialise + wrap with ``generated::BuildCatalogTableCreateParams``.
VgiBatchPtr BuildTableCreateRequest(
    const std::vector<uint8_t> &attach_opaque_data, const std::vector<std::string> &schema_path,
    const std::string &name, const VgiSchemaPtr &columns_schema, const std::string &on_conflict,
    const std::vector<std::vector<uint8_t>> &constraint_definitions,
    const std::vector<uint8_t> &transaction_opaque_data = {});


} // namespace vgi
} // namespace duckdb
