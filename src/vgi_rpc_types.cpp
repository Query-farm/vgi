// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_rpc_types.hpp"

#include "duckdb/common/exception.hpp"
#include "vgi_arrow_ipc.hpp"
#include "vgi_batch_validation.hpp"
#include "vgi_exception.hpp"
#include "vgi_schema_registry.hpp"

#include "nanoarrow/nanoarrow.h"

#include <cstring>

// Generated request builders — exposes ``duckdb::vgi::generated::Build<Name>Params(...)``
// definitions that mirror the schemas in ``generated/vgi_protocol_schemas.hpp``.
// Including here is the only TU that pulls them in; the file is header-only
// inline functions, so ODR is satisfied by the single inclusion.
#include "generated/vgi_request_builders.hpp"

namespace duckdb {
namespace vgi {

namespace {

// Validate a nested payload decoded from IPC bytes. `what` names it in the
// message ("[worker: nested IPC payload]", unchanged) but is not a worker
// location, so it is reported as `field` rather than `worker_path`/`transport`.
void ValidateNestedBatch(const VgiBatchPtr &batch, WorkerBatchValidation level, const char *what) {
	try {
		ValidateWorkerBatch(batch.get(), level, what);
	} catch (const IOException &e) {
		ErrorInfo info = ExtraInfoOf(e);
		info.erase(error_key::kWorkerPath);
		info.erase(error_key::kTransport);
		info.Set(error_key::kField, what);
		throw IOException(info, RawMessageOf(e));
	}
}

// A one-row array of `type` filled by `fill` (which appends exactly one element).
template <class Fill>
ArrowArray OneRow(const VgiField &type, Fill &&fill) {
	ArrowSchema schema;
	BuildSchema(type, &schema);
	ArrowArray array;
	ArrowError error;
	ArrowErrorInit(&error);
	ArrowErrorCode code = ArrowArrayInitFromSchema(&array, &schema, &error);
	schema.release(&schema);
	if (code != NANOARROW_OK) {
		throw IOException("VGI: failed to build a request field: %s", error.message);
	}
	try {
		if (ArrowArrayStartAppending(&array) != NANOARROW_OK) {
			throw IOException("VGI: failed to build a request field");
		}
		fill(&array);
		if (ArrowArrayFinishBuildingDefault(&array, &error) != NANOARROW_OK) {
			throw IOException("VGI: failed to build a request field: %s", error.message);
		}
	} catch (...) {
		array.release(&array);
		throw;
	}
	return array;
}

VgiField EnumType() {
	return type::Dictionary(type::Int16(), type::Utf8());
}

void FillDictionary(ArrowArray *array, const std::vector<std::string> &dictionary_values) {
	for (const auto &v : dictionary_values) {
		append::String(array->dictionary, v);
	}
}

} // namespace

// ============================================================================
// Single-row builders (declared in vgi_rpc_types.hpp).
// ============================================================================
//
// "Required" non-Optional helpers always emit a non-null entry; the Optional helpers
// are what the codegen calls into.

ArrowArray BuildBinaryScalarRequired(const std::vector<uint8_t> &value) {
	return OneRow(type::Binary(), [&](ArrowArray *a) { append::Bytes(a, value.data(), (int64_t)value.size()); });
}

ArrowArray BuildOptionalBinaryScalar(const std::optional<std::vector<uint8_t>> &value) {
	return OneRow(type::Binary(), [&](ArrowArray *a) {
		if (value) {
			append::Bytes(a, value->data(), (int64_t)value->size());
		} else {
			append::Null(a);
		}
	});
}

ArrowArray BuildStringScalar(const std::string &value) {
	return OneRow(type::Utf8(), [&](ArrowArray *a) { append::String(a, value); });
}

ArrowArray BuildOptionalStringScalar(const std::optional<std::string> &value) {
	return OneRow(type::Utf8(), [&](ArrowArray *a) {
		if (value) {
			append::String(a, *value);
		} else {
			append::Null(a);
		}
	});
}

ArrowArray BuildBoolScalar(bool value) {
	return OneRow(type::Bool(), [&](ArrowArray *a) { append::Bool(a, value); });
}

ArrowArray BuildOptionalBoolScalar(std::optional<bool> value) {
	return OneRow(type::Bool(), [&](ArrowArray *a) {
		if (value) {
			append::Bool(a, *value);
		} else {
			append::Null(a);
		}
	});
}

ArrowArray BuildInt32Scalar(int32_t value) {
	return OneRow(type::Int32(), [&](ArrowArray *a) { append::Int(a, value); });
}

ArrowArray BuildOptionalInt32Scalar(std::optional<int32_t> value) {
	return OneRow(type::Int32(), [&](ArrowArray *a) {
		if (value) {
			append::Int(a, *value);
		} else {
			append::Null(a);
		}
	});
}

ArrowArray BuildInt64Scalar(int64_t value) {
	return OneRow(type::Int64(), [&](ArrowArray *a) { append::Int(a, value); });
}

ArrowArray BuildOptionalInt64Scalar(std::optional<int64_t> value) {
	return OneRow(type::Int64(), [&](ArrowArray *a) {
		if (value) {
			append::Int(a, *value);
		} else {
			append::Null(a);
		}
	});
}

// list<T> single-row builders.

ArrowArray BuildStringListScalar(const std::vector<std::string> &values) {
	return OneRow(type::List(type::Utf8()), [&](ArrowArray *a) {
		for (const auto &v : values) {
			append::String(a->children[0], v);
		}
		append::FinishElement(a);
	});
}

ArrowArray BuildOptionalStringListScalar(const std::optional<std::vector<std::string>> &values) {
	return OneRow(type::List(type::Utf8()), [&](ArrowArray *a) {
		if (!values) {
			append::Null(a);
			return;
		}
		for (const auto &v : *values) {
			append::String(a->children[0], v);
		}
		append::FinishElement(a);
	});
}

ArrowArray BuildNullableStringListScalar(const std::optional<std::vector<std::optional<std::string>>> &values) {
	return OneRow(type::List(type::Utf8()), [&](ArrowArray *a) {
		if (!values) {
			append::Null(a);
			return;
		}
		for (const auto &v : *values) {
			if (v) {
				append::String(a->children[0], *v);
			} else {
				append::Null(a->children[0]);
			}
		}
		append::FinishElement(a);
	});
}

std::vector<std::string> SingleSchemaPath(const std::string &schema_name) {
	if (schema_name.empty()) {
		throw IOException("VGI schema name cannot be empty");
	}
	return {schema_name};
}

std::optional<std::vector<std::string>> OptionalSingleSchemaPath(const std::string &schema_name) {
	if (schema_name.empty()) {
		return std::nullopt;
	}
	return SingleSchemaPath(schema_name);
}

std::string SchemaNameFromPath(const std::vector<std::string> &schema_path, const std::string &field_name) {
	if (schema_path.size() != 1 || schema_path[0].empty()) {
		throw IOException(
		    "DuckDB 1.5 VGI requires %s to contain exactly one non-empty schema component; got depth %llu", field_name,
		    static_cast<unsigned long long>(schema_path.size()));
	}
	return schema_path[0];
}

std::string OptionalSchemaNameFromPath(const std::optional<std::vector<std::string>> &schema_path,
                                       const std::string &field_name) {
	if (!schema_path.has_value()) {
		return {};
	}
	return SchemaNameFromPath(*schema_path, field_name);
}

ArrowArray BuildBinaryListScalar(const std::vector<std::vector<uint8_t>> &values) {
	return OneRow(type::List(type::Binary()), [&](ArrowArray *a) {
		for (const auto &v : values) {
			append::Bytes(a->children[0], v.data(), (int64_t)v.size());
		}
		append::FinishElement(a);
	});
}

ArrowArray BuildInt32ListScalar(const std::vector<int32_t> &values) {
	return OneRow(type::List(type::Int32()), [&](ArrowArray *a) {
		for (auto v : values) {
			append::Int(a->children[0], v);
		}
		append::FinishElement(a);
	});
}

ArrowArray BuildInt64ListScalar(const std::vector<int64_t> &values) {
	return OneRow(type::List(type::Int64()), [&](ArrowArray *a) {
		for (auto v : values) {
			append::Int(a->children[0], v);
		}
		append::FinishElement(a);
	});
}

// map<utf8, utf8> single-row builders.

namespace {
void FillStringMap(ArrowArray *a, const std::vector<std::pair<std::string, std::string>> &entries) {
	ArrowArray *kv = a->children[0];
	for (const auto &[k, v] : entries) {
		append::String(kv->children[0], k);
		append::String(kv->children[1], v);
		append::FinishElement(kv);
	}
	append::FinishElement(a);
}
} // namespace

ArrowArray BuildStringMapScalar(const std::vector<std::pair<std::string, std::string>> &entries) {
	return OneRow(type::Map(type::Utf8(), type::Utf8()), [&](ArrowArray *a) { FillStringMap(a, entries); });
}

ArrowArray BuildOptionalStringMapScalar(const std::optional<std::vector<std::pair<std::string, std::string>>> &entries) {
	return OneRow(type::Map(type::Utf8(), type::Utf8()), [&](ArrowArray *a) {
		if (!entries) {
			append::Null(a);
			return;
		}
		FillStringMap(a, *entries);
	});
}

// ============================================================================
// Enum Serialization
// ============================================================================

ArrowArray BuildEnumArray(const std::string &value, const std::vector<std::string> &dictionary_values) {
	int16_t index = -1;
	for (size_t i = 0; i < dictionary_values.size(); i++) {
		if (dictionary_values[i] == value) {
			index = static_cast<int16_t>(i);
			break;
		}
	}
	if (index < 0) {
		throw IOException("Enum value '%s' not found in dictionary", value);
	}
	return OneRow(EnumType(), [&](ArrowArray *a) {
		FillDictionary(a, dictionary_values);
		append::Int(a, index);
	});
}

ArrowArray BuildOptionalEnumArray(const std::optional<std::string> &value,
                                  const std::vector<std::string> &dictionary_values) {
	if (value) {
		return BuildEnumArray(*value, dictionary_values);
	}
	return OneRow(EnumType(), [&](ArrowArray *a) {
		FillDictionary(a, dictionary_values);
		append::Null(a);
	});
}

// ============================================================================
// IPC Bytes Serialization
// ============================================================================

namespace {
//! Writes straight into a std::vector<uint8_t>.
class VectorOutputStream final : public VgiOutputStream {
public:
	explicit VectorOutputStream(std::vector<uint8_t> &out) : out_(out) {
	}
	void Write(const void *data, int64_t nbytes) override {
		auto *p = static_cast<const uint8_t *>(data);
		out_.insert(out_.end(), p, p + nbytes);
	}
	int64_t Tell() const override {
		return static_cast<int64_t>(out_.size());
	}

private:
	std::vector<uint8_t> &out_;
};
} // namespace

std::vector<uint8_t> SerializeToIpcBytes(const VgiBatchPtr &batch, const VgiMetadataPtr &custom_metadata) {
	std::vector<uint8_t> out;
	auto sink = std::make_shared<VectorOutputStream>(out);
	auto writer = VgiIpcWriter::Open(sink, batch->schema());
	VgiMetadata none;
	writer->Write(*batch, custom_metadata ? custom_metadata.get() : &none);
	writer->Close();
	return out;
}

VgiBatchPtr DeserializeFromIpcBytes(const uint8_t *data, size_t len, WorkerBatchValidation validation) {
	return DeserializeFromIpcBytesWithMetadata(data, len, validation).batch;
}

VgiBatchPtr DeserializeFromIpcBytes(const std::vector<uint8_t> &bytes, WorkerBatchValidation validation) {
	return DeserializeFromIpcBytes(bytes.data(), bytes.size(), validation);
}

VgiBatchPtr DeserializeFromIpcBytesZeroCopy(const VgiBufferPtr &bytes, WorkerBatchValidation validation) {
	VgiBatchPtr batch;
	try {
		batch = DeserializeBatch(bytes);
	} catch (const std::exception &e) {
		throw IOException("Failed to read batch from IPC bytes: %s", RawMessageOf(e));
	}
	ValidateNestedBatch(batch, validation, "nested IPC payload");
	return batch;
}

VgiBatchPtr UnwrapResultEnvelope(const VgiBatchPtr &envelope, const std::string &method,
                                 const std::unordered_map<std::string, std::string> &info, bool null_result_ok,
                                 WorkerBatchValidation validation, VgiMetadataPtr *custom_metadata) {
	auto violation = [&]() {
		return ErrorInfo(error_subtype::kProtocolViolation).Merge(info).Rpc(method);
	};
	// Name the worker in the message too (it is also in extra_info).
	auto worker_it = info.find(error_key::kWorkerPath);
	const std::string where = worker_it == info.end() ? std::string() : " [worker: " + worker_it->second + "]";
	if (!envelope || envelope->num_rows() == 0) {
		throw IOException(violation(), "VGI %s returned an empty response%s", method, where);
	}
	auto column = envelope->GetColumnByName("result");
	if (!column) {
		throw IOException(violation().Set(error_key::kField, "result"),
		                  "VGI %s response is missing its 'result' column%s", method, where);
	}
	if (column.is_dictionary() || column.type().type != NANOARROW_TYPE_BINARY) {
		throw IOException(
		    violation().Set(error_key::kField, "result").ExpectedActual("binary", column.type_string()),
		    "VGI %s response 'result' column has type %s, expected binary%s", method, column.type_string(), where);
	}
	if (column.IsNull(0)) {
		if (null_result_ok) {
			return nullptr;
		}
		throw IOException(violation().Set(error_key::kField, "result"), "VGI %s response has a null 'result'%s",
		                  method, where);
	}
	try {
		// Zero-copy: the slice holds the envelope's buffers alive.
		auto view = column.GetView(0);
		auto bytes = std::make_shared<const VgiBuffer>(envelope, reinterpret_cast<const uint8_t *>(view.data()),
		                                               static_cast<int64_t>(view.size()));
		auto batch = DeserializeFromIpcBytesZeroCopy(bytes, validation);
		if (custom_metadata) {
			*custom_metadata = batch ? batch->metadata() : nullptr;
		}
		return batch;
	} catch (const std::exception &e) {
		throw IOException(violation().Merge(ExtraInfoOf(e)),
		                  "VGI %s response could not be decoded%s: %s. The worker likely returned a malformed or "
		                  "out-of-date response shape for this method.",
		                  method, where, RawMessageOf(e));
	}
}

DeserializedBatch DeserializeFromIpcBytesWithMetadata(const uint8_t *data, size_t len,
                                                      WorkerBatchValidation validation) {
	VgiBatchPtr batch;
	try {
		batch = DeserializeBatch(VgiBuffer::Copy(data, static_cast<int64_t>(len)));
	} catch (const std::exception &e) {
		throw IOException("Failed to read batch from IPC bytes: %s", RawMessageOf(e));
	}
	ValidateNestedBatch(batch, validation, "nested IPC payload");
	DeserializedBatch out;
	out.batch = batch;
	out.custom_metadata = batch ? batch->metadata() : nullptr;
	return out;
}

std::vector<uint8_t> SerializeSchemaToIpcBytes(const VgiSchemaPtr &schema) {
	return SerializeSchema(*schema)->ToVector();
}

std::vector<uint8_t> SerializeForeignKeyToIpcBytes(const std::vector<std::string> &fk_columns,
                                                   const std::vector<std::string> &pk_columns,
                                                   const std::string &referenced_table,
                                                   const std::string &referenced_schema) {
	return SerializeToIpcBytes(
	    generated::BuildForeignKeyInfo(fk_columns, pk_columns, referenced_table, SingleSchemaPath(referenced_schema)));
}

// ============================================================================
// BindResponse Parsing
// ============================================================================

BindResponseResult ParseBindResponse(const VgiBatchPtr &batch, const std::string &worker_path) {
	auto wire = generated::Decode<generated::BindResponse>(batch, 0, worker_path);
	BindResponseResult result;
	try {
		result.output_schema = ReadSchemaBytes(wire.output_schema);
	} catch (const std::exception &e) {
		throw IOException("Failed to deserialize output schema: %s [worker: %s]", RawMessageOf(e), worker_path);
	}
	result.opaque_data = wire.opaque_data.value_or(generated::Bytes {});
	return result;
}

// ============================================================================
// BindSecretScopeResponse Parsing
// ============================================================================

std::optional<BindSecretScopeResponseResult> TryParseBindSecretScopeResponse(const VgiBatchPtr &batch) {
	if (!batch || batch->num_rows() == 0) {
		return std::nullopt;
	}
	auto wire = generated::Decode<generated::BindResponse>(batch, 0, "");
	// A non-empty lookup_secret_types makes this a secret-scope request; the three
	// lists are parallel.
	if (wire.lookup_secret_types.empty()) {
		return std::nullopt;
	}
	BindSecretScopeResponseResult result;
	for (size_t i = 0; i < wire.lookup_secret_types.size(); i++) {
		BindSecretScopeResponseResult::Lookup lookup;
		lookup.secret_type = std::move(wire.lookup_secret_types[i]);
		if (i < wire.lookup_scopes.size()) {
			lookup.scope = std::move(wire.lookup_scopes[i]);
		}
		if (i < wire.lookup_names.size()) {
			lookup.name = std::move(wire.lookup_names[i]);
		}
		result.lookups.push_back(std::move(lookup));
	}
	return result;
}

// ============================================================================
// GlobalInitResponse Parsing
// ============================================================================

GlobalInitResponseResult ParseGlobalInitResponse(const VgiBatchPtr &batch, const std::string &worker_path) {
	auto wire = generated::Decode<generated::GlobalInitResponse>(batch, 0, worker_path);
	GlobalInitResponseResult result;
	result.execution_id = std::move(wire.execution_id);
	result.max_workers = wire.max_workers > 0 ? wire.max_workers : 1;
	result.opaque_data = wire.opaque_data.value_or(generated::Bytes {});
	return result;
}

// ============================================================================
// Response Item Unwrapping
// ============================================================================

namespace {
//! The `items` list column's element range in row 0, or false when absent/null.
bool ItemsRange(const VgiBatchPtr &batch, VgiColumn &values, int64_t &start, int64_t &end) {
	if (!batch || batch->num_rows() == 0) {
		return false;
	}
	auto list = batch->GetColumnByName("items");
	if (!list || list.is_dictionary()) {
		return false;
	}
	auto t = list.type().type;
	if (t != NANOARROW_TYPE_LIST && t != NANOARROW_TYPE_LARGE_LIST) {
		throw IOException("VGI: 'items' column has type %s, expected a list", list.type_string());
	}
	if (list.IsNull(0)) {
		return false;
	}
	list.ListRange(0, start, end);
	values = list.child(0);
	return true;
}
} // namespace

std::vector<std::vector<uint8_t>> UnwrapBinaryResponseItems(const VgiBatchPtr &batch) {
	std::vector<std::vector<uint8_t>> items;
	VgiColumn values;
	int64_t start = 0;
	int64_t end = 0;
	if (!ItemsRange(batch, values, start, end)) {
		return items;
	}
	for (int64_t i = start; i < end; i++) {
		if (!values.IsNull(i)) {
			auto view = values.GetView(i);
			items.emplace_back(view.data(), view.data() + view.size());
		}
	}
	return items;
}

std::vector<VgiBatchPtr> UnwrapAndValidateItems(const VgiBatchPtr &batch, const std::string &method_name,
                                                const std::string &worker_path) {
	std::vector<VgiBatchPtr> items;
	VgiColumn values;
	int64_t start = 0;
	int64_t end = 0;
	if (!ItemsRange(batch, values, start, end)) {
		return items;
	}
	for (int64_t i = start; i < end; i++) {
		if (values.IsNull(i)) {
			continue;
		}
		auto view = values.GetView(i);
		auto bytes = std::make_shared<const VgiBuffer>(batch, reinterpret_cast<const uint8_t *>(view.data()),
		                                               static_cast<int64_t>(view.size()));
		auto info_batch = DeserializeFromIpcBytesZeroCopy(bytes, WorkerBatchValidation::FULL);
		if (!info_batch || info_batch->num_rows() == 0) {
			continue;
		}
		ValidateItemSchema(info_batch, method_name, worker_path, static_cast<size_t>(i - start));
		items.push_back(std::move(info_batch));
	}
	return items;
}

std::vector<std::string> UnwrapStringResponseItems(const VgiBatchPtr &batch) {
	std::vector<std::string> items;
	VgiColumn values;
	int64_t start = 0;
	int64_t end = 0;
	if (!ItemsRange(batch, values, start, end)) {
		return items;
	}
	for (int64_t i = start; i < end; i++) {
		if (!values.IsNull(i)) {
			items.push_back(values.GetString(i));
		}
	}
	return items;
}

// ============================================================================
// TableFunctionCardinalityRequest / TableCardinality
// ============================================================================



//! Build the inner ``TableFunctionPlanRequest`` record for the scan-planning RPC.
//!
//! Only the fields DuckDB can actually supply are populated. Notably ``row_limit``
//! is NOT among them: ``TableFunctionInitInput`` carries no limit, and the Top-N
//! ``order_by_limit`` means something different ("top K by that column"), so
//! sending it here would be a lie. DataFusion supplies row_limit from
//! ``TableProvider::scan(limit)``; DuckDB leaves it null.
//!
//! ``target_split_bytes`` is omitted when 0. DuckDB has no basis to invent a byte
//! target — it claims splits greedily as interchangeable units — and a fabricated
//! default would be actively wrong for a compute-bound worker where bytes do not
//! predict cost. ``min_splits`` IS sent, because thread count is the one sizing
//! fact DuckDB genuinely knows.


// ============================================================================
// TableFunctionDynamicToStringRequest / Response
// ============================================================================


InsertionOrderPreservingMap<std::string>
ParseTableFunctionDynamicToStringResult(const VgiBatchPtr &batch,
                                        const std::string &worker_path) {
	InsertionOrderPreservingMap<std::string> result;
	if (!batch || batch->num_rows() == 0) {
		return result;
	}
	// Parallel keys / values lists, in the order the user's hook emitted them.
	auto wire = generated::Decode<generated::TableFunctionDynamicToStringResult>(batch, 0, worker_path);
	for (size_t i = 0; i < std::min(wire.keys.size(), wire.values.size()); ++i) {
		result.insert(wire.keys[i], wire.values[i]);
	}
	return result;
}

TableFunctionCardinalityResult ParseTableFunctionCardinalityResult(const VgiBatchPtr &batch,
                                                                   const std::string &worker_path) {
	TableFunctionCardinalityResult result;
	if (!batch || batch->num_rows() == 0) {
		return result; // Unknown cardinality
	}
	auto wire = generated::Decode<generated::TableFunctionCardinalityResult>(batch, 0, worker_path);
	result.estimate = wire.estimate.value_or(-1);
	result.max = wire.max_.value_or(-1);
	return result;
}

// ============================================================================
// Inner request builders (Complex bucket — kept hand-coded)
// ============================================================================

//! Describe what THIS client can do, so a worker can tailor what it advertises.
//!
//! Sent on every attach. Without it a worker has to guess, and guessing wrong is
//! expensive in both directions: assume too much and the client cannot read what
//! comes back; assume too little and the worker streams rows it could have handed
//! over as a file reference.
//!
//! ``native_formats`` is the load-bearing one — it is what lets a worker return a
//! FORMAT branch ("read these 40 parquet files") instead of materializing rows.
//! It lists formats DuckDB reads natively; extension-provided readers are omitted
//! because whether they are loadable is a per-session question this static list
//! cannot answer.



// ============================================================================
// Inner request records (generated: src/generated/vgi_protocol_records.hpp)
// ============================================================================
//
// Each builder fills the generated record and encodes it with ToBatch, which
// produces exactly the protocol schema (every declared field, in order, with
// the declared type and nullability). Exactness is load-bearing: vgi-rpc-go
// validates a request against its declared contract with arrow.Schema.Equal, so
// a missing nullable column fails every call. The callers' "empty means absent"
// conventions are mapped to nullopt here.

namespace {

std::optional<int64_t> NonNegativeOrNull(int64_t value) {
	return value < 0 ? std::nullopt : std::make_optional(value);
}

template <class E>
E RequireEnum(std::optional<E> parsed, const std::string &raw, const char *what) {
	if (!parsed) {
		throw InternalException("VGI: '%s' is not a valid %s", raw, what);
	}
	return *parsed;
}

generated::LargeBytes BufferOf(const std::vector<uint8_t> &bytes) {
	return VgiBuffer::FromVector(bytes);
}

} // namespace

VgiBatchPtr
BuildBindRequest(const std::string &function_name, const std::vector<uint8_t> &arguments_ipc_bytes,
                 const std::string &function_type, const std::vector<uint8_t> &input_schema_bytes,
                 const std::vector<uint8_t> &settings_bytes, const std::vector<uint8_t> &secrets_bytes,
                 const std::vector<uint8_t> &attach_opaque_data, const std::vector<uint8_t> &transaction_opaque_data,
                 bool resolved_secrets_provided, const std::string &at_unit, const std::string &at_value,
                 const CopyFromBindContext *copy_from, const CopyToBindContext *copy_to,
                 const std::string &schema_name,
                 const std::optional<std::vector<std::optional<std::string>>> &argument_names) {
	generated::BindRequest request;
	request.function_name = function_name;
	request.arguments = arguments_ipc_bytes;
	request.function_type =
	    RequireEnum(generated::FunctionTypeFromWire(function_type), function_type, "FunctionType");
	request.input_schema = BytesOrNull(input_schema_bytes);
	request.settings = BytesOrNull(settings_bytes);
	request.secrets = BytesOrNull(secrets_bytes);
	request.attach_opaque_data = BytesOrNull(attach_opaque_data);
	request.transaction_opaque_data = BytesOrNull(transaction_opaque_data);
	request.resolved_secrets_provided = resolved_secrets_provided;
	request.at_unit = StringOrNull(at_unit);
	request.at_value = StringOrNull(at_value);
	// copy_from / copy_to are always present as columns, null unless this is a COPY bind.
	if (copy_from) {
		request.copy_from = generated::CopyFromContext {copy_from->format, copy_from->file_path,
		                                                copy_from->expected_schema_bytes};
	}
	if (copy_to) {
		request.copy_to = generated::CopyToContext {copy_to->format, copy_to->file_path};
	}
	// Owning catalog schema; null tells the worker to fall back to a cross-schema
	// lookup by name.
	request.schema_path = OptionalSingleSchemaPath(schema_name);
	request.argument_names = argument_names;
	return generated::ToBatch(request);
}

VgiBatchPtr
BuildInitRequest(const std::vector<uint8_t> &bind_call_bytes, const std::vector<uint8_t> &output_schema_bytes,
                 const std::vector<uint8_t> &bind_opaque_data, const std::vector<int64_t> &projection_ids,
                 VgiBufferPtr pushdown_filters,
                 std::vector<VgiBufferPtr> join_keys, const std::string &phase,
                 const std::vector<uint8_t> &execution_id, const std::vector<uint8_t> &init_opaque_data,
                 const std::string &order_by_column_name, const std::string &order_by_direction,
                 const std::string &order_by_null_order, int64_t order_by_limit, double tablesample_percentage,
                 int64_t tablesample_seed, const std::optional<std::vector<uint8_t>> &finalize_state_id,
                 const std::vector<uint8_t> &substream_id, const std::vector<std::string> &split_tokens) {
	generated::InitRequest request;
	request.bind_call = bind_call_bytes;
	request.output_schema = output_schema_bytes;
	request.bind_opaque_data = BytesOrNull(bind_opaque_data);
	if (!projection_ids.empty()) {
		request.projection_ids = projection_ids;
	}
	if (pushdown_filters && pushdown_filters->size() > 0) {
		request.pushdown_filters = std::move(pushdown_filters);
	}
	if (!join_keys.empty()) {
		request.join_keys = std::move(join_keys);
	}
	// The splits this init redeems. A list because DataFusion bin-packs a group per
	// partition; DuckDB claims greedily and always sends exactly one.
	if (!split_tokens.empty()) {
		request.split_tokens.emplace();
		for (const auto &token : split_tokens) {
			request.split_tokens->push_back(VgiBuffer::FromString(token));
		}
	}
	// row_limit stays null: TableFunctionInitInput carries no limit, and the Top-N
	// order_by_limit means something different ("top K by that column").
	if (!phase.empty()) {
		request.phase = RequireEnum(generated::VgiInitPhaseFromWire(phase), phase, "VgiInitPhase");
	}
	// Opaque worker-chosen bytes from table_buffering_combine (TABLE_BUFFERING_FINALIZE).
	request.finalize_state_id = finalize_state_id;
	request.execution_id = BytesOrNull(execution_id);
	request.init_opaque_data = BytesOrNull(init_opaque_data);
	// Client-minted per-substream id of a parallel streaming table-in-out.
	request.substream_id = BytesOrNull(substream_id);
	request.order_by_column_name = StringOrNull(order_by_column_name);
	if (!order_by_direction.empty()) {
		request.order_by_direction =
		    RequireEnum(generated::VgiOrderByDirectionFromWire(order_by_direction), order_by_direction,
		                "VgiOrderByDirection");
	}
	if (!order_by_null_order.empty()) {
		request.order_by_null_order =
		    RequireEnum(generated::VgiNullOrderFromWire(order_by_null_order), order_by_null_order, "VgiNullOrder");
	}
	request.order_by_limit = NonNegativeOrNull(order_by_limit);
	if (tablesample_percentage >= 0.0) {
		request.tablesample_percentage = tablesample_percentage;
	}
	request.tablesample_seed = NonNegativeOrNull(tablesample_seed);
	return generated::ToBatch(request);
}

VgiBatchPtr BuildTableFunctionCardinalityRequest(const std::vector<uint8_t> &bind_call_bytes,
                                                                         const std::vector<uint8_t> &bind_opaque_data) {
	return generated::ToBatch(generated::TableFunctionCardinalityRequest {bind_call_bytes, BytesOrNull(bind_opaque_data)});
}

//! The inner ``TableFunctionPlanRequest`` for the scan-planning RPC. Only what DuckDB
//! can supply is set; the rest of the twenty declared fields ride as nulls:
//!
//!   row_limit                — TableFunctionInitInput carries no limit
//!   max_splits_per_response  — DuckDB enumerates in one page
//!   join_keys, refined_filters — the plan is built from STATIC filters only; join
//!                              keys arrive after InitGlobal and prune within a split
//!   start/end_position       — no streaming scan
//!   order_by_* / tablesample_* — these reach the worker on the init call
//!
//! ``target_split_bytes`` / ``min_splits`` are null when non-positive: DuckDB claims
//! splits greedily as interchangeable units and has no basis to invent a byte target,
//! while thread count (min_splits) is the one sizing fact it genuinely knows.
//! ``filters_complete`` is always true: no narrower filter is ever coming, so a
//! worker must not hold splits back waiting for one.
VgiBatchPtr
BuildTableFunctionPlanRequest(const std::vector<uint8_t> &bind_call_bytes,
                              const std::vector<uint8_t> &bind_opaque_data,
                              const std::vector<int32_t> &projection_ids,
                              const std::vector<uint8_t> &pushdown_filters, int64_t min_splits,
                              int64_t target_split_bytes, const std::vector<uint8_t> &cursor) {
	generated::TableFunctionPlanRequest request;
	request.bind_call = bind_call_bytes;
	request.bind_opaque_data = BytesOrNull(bind_opaque_data);
	if (!projection_ids.empty()) {
		request.projection_ids = std::vector<int64_t>(projection_ids.begin(), projection_ids.end());
	}
	if (!pushdown_filters.empty()) {
		request.pushdown_filters = BufferOf(pushdown_filters);
	}
	if (target_split_bytes > 0) {
		request.target_split_bytes = target_split_bytes;
	}
	if (min_splits > 0) {
		request.min_splits = min_splits;
	}
	request.cursor = BytesOrNull(cursor);
	request.filters_complete = true;
	return generated::ToBatch(request);
}

VgiBatchPtr BuildTableFunctionStatisticsRequest(const std::vector<uint8_t> &bind_call_bytes,
                                                                        const std::vector<uint8_t> &bind_opaque_data) {
	return generated::ToBatch(generated::TableFunctionStatisticsRequest {bind_call_bytes, BytesOrNull(bind_opaque_data)});
}

VgiBatchPtr
BuildTableFunctionDynamicToStringRequest(const std::vector<uint8_t> &bind_call_bytes,
                                         const std::vector<uint8_t> &bind_opaque_data,
                                         const std::vector<uint8_t> &global_execution_id) {
	return generated::ToBatch(generated::TableFunctionDynamicToStringRequest {
	    bind_call_bytes, BytesOrNull(bind_opaque_data), global_execution_id});
}

//! What THIS client can do, so a worker can tailor what it advertises. Sent on
//! every attach. ``native_formats`` lets a worker return a FORMAT branch ("read
//! these 40 parquet files") instead of materializing rows; it lists only formats
//! DuckDB reads natively, since whether an extension reader is loadable is a
//! per-session question. DuckDB has no streaming scan, so it says so: a worker
//! must not return an unbounded split this client could never terminate.
static std::vector<uint8_t> ClientCapabilitiesBytes() {
	generated::ClientCapabilities caps;
	caps.engine = "duckdb";
	caps.native_formats = {"parquet", "csv", "json"};
	caps.catalogs = {"ducklake", "iceberg", "postgres", "mysql", "sqlite", "duckdb"};
	caps.can_stream = false;
	caps.filter_encodings = {"vgi.filters.v2"};
	return SerializeRecord(caps);
}

VgiBatchPtr BuildCatalogAttachRequest(const std::string &name,
                                                              const std::vector<uint8_t> &options_ipc_bytes,
                                                              const std::string &data_version_spec,
                                                              const std::string &implementation_version) {
	// Empty version constraints are null: the worker treats None as "unconstrained",
	// while "" would be a concrete (and invalid) version string.
	generated::CatalogAttachRequest request;
	request.name = name;
	request.options = BytesOrNull(options_ipc_bytes);
	request.data_version_spec = StringOrNull(data_version_spec);
	request.implementation_version = StringOrNull(implementation_version);
	request.client_capabilities = ClientCapabilitiesBytes();
	return generated::ToBatch(request);
}

VgiBatchPtr BuildTableCreateRequest(
    const std::vector<uint8_t> &attach_opaque_data, const std::vector<std::string> &schema_path,
    const std::string &name, const VgiSchemaPtr &columns_schema, const std::string &on_conflict,
    const std::vector<int> &not_null_constraints, const std::vector<std::vector<int>> &unique_constraints,
    const std::vector<std::string> &check_constraints, const std::vector<std::vector<int>> &primary_key_constraints,
    const std::vector<std::vector<uint8_t>> &foreign_key_constraints,
    const std::vector<uint8_t> &transaction_opaque_data) {
	auto int32_groups = [](const std::vector<std::vector<int>> &groups) {
		std::vector<std::vector<int32_t>> out;
		for (const auto &group : groups) {
			out.emplace_back(group.begin(), group.end());
		}
		return out;
	};
	generated::TableCreateRequest request;
	request.attach_opaque_data = attach_opaque_data;
	request.schema_path = schema_path;
	request.name = name;
	request.columns = SerializeSchemaToIpcBytes(columns_schema);
	request.on_conflict = RequireEnum(generated::OnConflictFromWire(on_conflict), on_conflict, "OnConflict");
	request.not_null_constraints.assign(not_null_constraints.begin(), not_null_constraints.end());
	request.unique_constraints = int32_groups(unique_constraints);
	request.check_constraints = check_constraints;
	request.primary_key_constraints = int32_groups(primary_key_constraints);
	request.foreign_key_constraints = foreign_key_constraints;
	request.transaction_opaque_data = BytesOrNull(transaction_opaque_data);
	return generated::ToBatch(request);
}

} // namespace vgi
} // namespace duckdb
