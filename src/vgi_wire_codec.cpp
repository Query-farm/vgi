// © Copyright 2025, 2026 Query Farm LLC - https://query.farm

#include "vgi_wire_codec.hpp"

#include "vgi_exception.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include "nanoarrow/nanoarrow.h"

#include <cstring>

namespace duckdb {
namespace vgi {
namespace wire {

// ============================================================================
// Errors
// ============================================================================

static const std::string kNoWorker;

static const std::string &WorkerOf(const Ctx &ctx) {
	return ctx.worker_path ? *ctx.worker_path : kNoWorker;
}

static ErrorInfo ProtocolInfo(const Ctx &ctx) {
	return ErrorInfo(error_subtype::kProtocolViolation).Worker(WorkerOf(ctx)).Set(error_key::kField, ctx.field);
}

void ThrowMissing(const Ctx &ctx) {
	throw InvalidInputException(
	    ProtocolInfo(ctx),
	    StringUtil::Format("VGI protocol error: Required column '%s' not found in %s response from worker '%s'",
	                       ctx.field, ctx.record, WorkerOf(ctx)));
}

void ThrowNull(const Ctx &ctx) {
	throw InvalidInputException(
	    ProtocolInfo(ctx),
	    StringUtil::Format(
	        "VGI protocol error: Column '%s' has unexpected null value at row %lld in %s response from worker '%s'",
	        ctx.field, static_cast<long long>(ctx.row), ctx.record, WorkerOf(ctx)));
}

void ThrowType(const Ctx &ctx, const VgiColumn &actual, const char *expected) {
	auto actual_type = actual.type_string();
	throw InvalidInputException(
	    ProtocolInfo(ctx).ExpectedActual(expected, actual_type),
	    StringUtil::Format("VGI protocol error: Column '%s' in %s response from worker '%s' has type %s, expected %s",
	                       ctx.field, ctx.record, WorkerOf(ctx), actual_type, expected));
}

void ThrowBadEnum(const Ctx &ctx, const char *enum_name, const std::string &value) {
	// "unknown <field> '<value>'": the field is what a reader recognizes.
	auto leaf = ctx.field.substr(ctx.field.find_last_of('.') + 1);
	throw InvalidInputException(
	    ProtocolInfo(ctx).Set(error_key::kActual, value),
	    StringUtil::Format("VGI protocol error: worker '%s' returned unknown %s '%s' in %s (not a %s value)",
	                       WorkerOf(ctx), leaf, value, ctx.record, enum_name));
}

void ThrowDuplicateKey(const Ctx &ctx) {
	throw InvalidInputException(
	    ProtocolInfo(ctx),
	    StringUtil::Format("VGI protocol error: Map column '%s' in %s response from worker '%s' repeats a key",
	                       ctx.field, ctx.record, WorkerOf(ctx)));
}

// ============================================================================
// Decoding
// ============================================================================

namespace {

//! The column's logical type (dictionary columns report NANOARROW_TYPE_DICTIONARY).
int LogicalType(const VgiColumn &col) {
	if (col.is_dictionary()) {
		return NANOARROW_TYPE_DICTIONARY;
	}
	return col.type().type;
}

void Expect(const VgiColumn &col, int type, const Ctx &ctx, const char *expected) {
	if (col.is_dictionary() || col.storage_type() != type || col.type().type != type) {
		ThrowType(ctx, col, expected);
	}
}

template <class T>
void ReadInt(const VgiColumn &col, int64_t i, T &out, const Ctx &ctx, int type, const char *expected) {
	Expect(col, type, ctx, expected);
	if (col.IsNull(i)) {
		ThrowNull(ctx);
	}
	out = static_cast<T>(col.GetInt(i));
}

template <class T>
void ReadUInt(const VgiColumn &col, int64_t i, T &out, const Ctx &ctx, int type, const char *expected) {
	Expect(col, type, ctx, expected);
	if (col.IsNull(i)) {
		ThrowNull(ctx);
	}
	out = static_cast<T>(col.GetUInt(i));
}

thread_local const VgiBatch *current_batch = nullptr;

} // namespace

void ReadValue(const VgiColumn &col, int64_t i, bool &out, const Ctx &ctx) {
	Expect(col, NANOARROW_TYPE_BOOL, ctx, "bool");
	if (col.IsNull(i)) {
		ThrowNull(ctx);
	}
	out = col.GetBool(i);
}
void ReadValue(const VgiColumn &col, int64_t i, int8_t &out, const Ctx &ctx) {
	ReadInt(col, i, out, ctx, NANOARROW_TYPE_INT8, "int8");
}
void ReadValue(const VgiColumn &col, int64_t i, int16_t &out, const Ctx &ctx) {
	ReadInt(col, i, out, ctx, NANOARROW_TYPE_INT16, "int16");
}
void ReadValue(const VgiColumn &col, int64_t i, int32_t &out, const Ctx &ctx) {
	ReadInt(col, i, out, ctx, NANOARROW_TYPE_INT32, "int32");
}
void ReadValue(const VgiColumn &col, int64_t i, int64_t &out, const Ctx &ctx) {
	if (!col.is_dictionary() && col.type().type == NANOARROW_TYPE_TIMESTAMP) {
		if (col.type().time_unit != NANOARROW_TIME_UNIT_MICRO) {
			ThrowType(ctx, col, "timestamp[us]");
		}
		if (col.IsNull(i)) {
			ThrowNull(ctx);
		}
		out = col.GetInt(i);
		return;
	}
	ReadInt(col, i, out, ctx, NANOARROW_TYPE_INT64, "int64");
}
void ReadValue(const VgiColumn &col, int64_t i, uint8_t &out, const Ctx &ctx) {
	ReadUInt(col, i, out, ctx, NANOARROW_TYPE_UINT8, "uint8");
}
void ReadValue(const VgiColumn &col, int64_t i, uint16_t &out, const Ctx &ctx) {
	ReadUInt(col, i, out, ctx, NANOARROW_TYPE_UINT16, "uint16");
}
void ReadValue(const VgiColumn &col, int64_t i, uint32_t &out, const Ctx &ctx) {
	ReadUInt(col, i, out, ctx, NANOARROW_TYPE_UINT32, "uint32");
}
void ReadValue(const VgiColumn &col, int64_t i, uint64_t &out, const Ctx &ctx) {
	ReadUInt(col, i, out, ctx, NANOARROW_TYPE_UINT64, "uint64");
}
void ReadValue(const VgiColumn &col, int64_t i, float &out, const Ctx &ctx) {
	Expect(col, NANOARROW_TYPE_FLOAT, ctx, "float");
	if (col.IsNull(i)) {
		ThrowNull(ctx);
	}
	out = static_cast<float>(col.GetDouble(i));
}
void ReadValue(const VgiColumn &col, int64_t i, double &out, const Ctx &ctx) {
	Expect(col, NANOARROW_TYPE_DOUBLE, ctx, "double");
	if (col.IsNull(i)) {
		ThrowNull(ctx);
	}
	out = col.GetDouble(i);
}

static bool IsStringType(int type) {
	return type == NANOARROW_TYPE_STRING || type == NANOARROW_TYPE_LARGE_STRING || type == NANOARROW_TYPE_STRING_VIEW;
}

static bool IsBinaryType(int type) {
	return type == NANOARROW_TYPE_BINARY || type == NANOARROW_TYPE_LARGE_BINARY || type == NANOARROW_TYPE_BINARY_VIEW;
}

void ReadValue(const VgiColumn &col, int64_t i, std::string &out, const Ctx &ctx) {
	if (col.IsNull(i)) {
		ThrowNull(ctx);
	}
	if (col.is_dictionary()) {
		auto values = col.dictionary();
		if (!IsStringType(values.type().type)) {
			ThrowType(ctx, col, "dictionary<utf8>");
		}
		auto index = col.GetInt(i);
		if (index < 0 || index >= values.length()) {
			ThrowType(ctx, col, "dictionary index in range");
		}
		auto view = values.GetView(index);
		out.assign(view.data(), view.size());
		return;
	}
	if (!IsStringType(col.type().type)) {
		ThrowType(ctx, col, "utf8");
	}
	auto view = col.GetView(i);
	out.assign(view.data(), view.size());
}

bool IsNullValue(const VgiColumn &col, int64_t i, const Ctx &ctx) {
	if (col.IsNull(i)) {
		return true;
	}
	if (!col.is_dictionary()) {
		return false;
	}
	auto values = col.dictionary();
	const auto index = col.GetInt(i);
	if (index < 0 || index >= values.length()) {
		ThrowType(ctx, col, "dictionary index in range");
	}
	return IsNullValue(values, index, ctx);
}

void ReadValue(const VgiColumn &col, int64_t i, std::vector<uint8_t> &out, const Ctx &ctx) {
	if (col.IsNull(i)) {
		ThrowNull(ctx);
	}
	if (col.is_dictionary()) {
		auto values = col.dictionary();
		const auto index = col.GetInt(i);
		if (index < 0 || index >= values.length()) {
			ThrowType(ctx, col, "dictionary index in range");
		}
		ReadValue(values, index, out, ctx);
		return;
	}
	if (!IsBinaryType(col.type().type)) {
		ThrowType(ctx, col, "binary");
	}
	auto view = col.GetView(i);
	if (view.empty()) {
		out.clear();
		return;
	}
	out.assign(reinterpret_cast<const uint8_t *>(view.data()),
	           reinterpret_cast<const uint8_t *>(view.data()) + view.size());
}

void ReadValue(const VgiColumn &col, int64_t i, VgiBufferPtr &out, const Ctx &ctx) {
	if (col.IsNull(i)) {
		ThrowNull(ctx);
	}
	int type = col.is_dictionary() ? -1 : col.type().type;
	if (type != NANOARROW_TYPE_LARGE_BINARY && type != NANOARROW_TYPE_BINARY) {
		ThrowType(ctx, col, "large_binary");
	}
	auto view = col.GetView(i);
	if (current_batch) {
		out = std::make_shared<const VgiBuffer>(current_batch->shared_from_this(),
		                                        reinterpret_cast<const uint8_t *>(view.data()),
		                                        static_cast<int64_t>(view.size()));
	} else {
		out = VgiBuffer::Copy(view.data(), static_cast<int64_t>(view.size()));
	}
}

void ListBounds(const VgiColumn &col, int64_t i, const Ctx &ctx, int64_t &begin, int64_t &end) {
	int type = LogicalType(col);
	if (type != NANOARROW_TYPE_LIST && type != NANOARROW_TYPE_LARGE_LIST) {
		ThrowType(ctx, col, "list");
	}
	col.ListRange(i, begin, end);
}

void MapBounds(const VgiColumn &col, int64_t i, const Ctx &ctx, int64_t &begin, int64_t &end) {
	if (LogicalType(col) != NANOARROW_TYPE_MAP) {
		ThrowType(ctx, col, "map");
	}
	col.ListRange(i, begin, end);
}

RowReader::RowReader(const VgiBatchPtr &batch, int64_t row, const char *record, const std::string &worker_path)
    : RecordReader(Ctx {record, &worker_path, std::string(), row}, row), batch_(batch) {
	if (!batch) {
		throw IOException(ErrorInfo(error_subtype::kProtocolViolation).Worker(worker_path),
		                  StringUtil::Format("VGI protocol error: missing %s response from worker '%s'", record,
		                                     worker_path));
	}
	if (row < 0 || row >= batch->num_rows()) {
		throw IOException(ErrorInfo(error_subtype::kProtocolViolation).Worker(worker_path),
		                  StringUtil::Format("VGI protocol error: %s response from worker '%s' has %lld rows; "
		                                     "row %lld was requested",
		                                     record, worker_path, static_cast<long long>(batch->num_rows()),
		                                     static_cast<long long>(row)));
	}
}

VgiColumn RowReader::Column(const char *name) const {
	return batch_->GetColumnByName(name);
}

StructReader::StructReader(const VgiColumn &col, int64_t i, const Ctx &ctx, const char *record)
    : RecordReader(ctx, i), col_(col) {
	(void)record;
	if (LogicalType(col) != NANOARROW_TYPE_STRUCT) {
		ThrowType(ctx, col, "struct");
	}
	if (col.IsNull(i)) {
		ThrowNull(ctx);
	}
}

VgiColumn StructReader::Column(const char *name) const {
	const auto &schema = col_.schema();
	for (int64_t k = 0; k < schema.n_children; k++) {
		const char *child_name = schema.children[k]->name;
		if (child_name && std::strcmp(child_name, name) == 0) {
			return col_.child(k);
		}
	}
	return VgiColumn();
}

const VgiBatch *CurrentBatch() {
	return current_batch;
}

CurrentBatchScope::CurrentBatchScope(const VgiBatch *batch) : previous_(current_batch) {
	current_batch = batch;
}

CurrentBatchScope::~CurrentBatchScope() {
	current_batch = previous_;
}

// ============================================================================
// Encoding
// ============================================================================

void WriteValue(ArrowArray *array, bool value) {
	append::Bool(array, value);
}
void WriteValue(ArrowArray *array, int8_t value) {
	append::Int(array, value);
}
void WriteValue(ArrowArray *array, int16_t value) {
	append::Int(array, value);
}
void WriteValue(ArrowArray *array, int32_t value) {
	append::Int(array, value);
}
void WriteValue(ArrowArray *array, int64_t value) {
	append::Int(array, value);
}
void WriteValue(ArrowArray *array, uint8_t value) {
	append::UInt(array, value);
}
void WriteValue(ArrowArray *array, uint16_t value) {
	append::UInt(array, value);
}
void WriteValue(ArrowArray *array, uint32_t value) {
	append::UInt(array, value);
}
void WriteValue(ArrowArray *array, uint64_t value) {
	append::UInt(array, value);
}
void WriteValue(ArrowArray *array, float value) {
	append::Double(array, value);
}
void WriteValue(ArrowArray *array, double value) {
	append::Double(array, value);
}

void WriteValue(ArrowArray *array, const std::string &value) {
	if (!array->dictionary) {
		append::String(array, value);
		return;
	}
	// A plain string member on a dictionary column: dictionary in first-seen order.
	ArrowArray *dict = array->dictionary;
	auto *offsets = reinterpret_cast<const int32_t *>(ArrowArrayBuffer(dict, 1)->data);
	auto *data = reinterpret_cast<const char *>(ArrowArrayBuffer(dict, 2)->data);
	for (int64_t k = 0; k < dict->length; k++) {
		auto len = static_cast<size_t>(offsets[k + 1] - offsets[k]);
		if (len == value.size() && std::memcmp(data + offsets[k], value.data(), len) == 0) {
			append::Int(array, k);
			return;
		}
	}
	append::String(dict, value);
	append::Int(array, dict->length - 1);
}

void WriteValue(ArrowArray *array, const std::vector<uint8_t> &value) {
	if (!array->dictionary) {
		append::Bytes(array, value.data(), static_cast<int64_t>(value.size()));
		return;
	}
	// Preserve byte identity (including empty values) in first-seen order.
	auto *dictionary = array->dictionary;
	const auto *offsets = reinterpret_cast<const int32_t *>(ArrowArrayBuffer(dictionary, 1)->data);
	const auto *data = ArrowArrayBuffer(dictionary, 2)->data;
	for (int64_t index = 0; index < dictionary->length; index++) {
		const auto size = static_cast<size_t>(offsets[index + 1] - offsets[index]);
		if (size == value.size() && (size == 0 || std::memcmp(data + offsets[index], value.data(), size) == 0)) {
			append::Int(array, index);
			return;
		}
	}
	append::Bytes(dictionary, value.data(), static_cast<int64_t>(value.size()));
	append::Int(array, dictionary->length - 1);
}

void WriteValue(ArrowArray *array, const VgiBufferPtr &value) {
	if (!value) {
		append::Bytes(array, nullptr, 0);
		return;
	}
	append::Bytes(array, value->data(), value->size());
}

void WriteEnum(ArrowArray *array, int16_t index, const char *const *wire_names, int16_t count) {
	ArrowArray *dict = array->dictionary;
	if (!dict) {
		throw InternalException("VGI wire codec: enum member on a non-dictionary column");
	}
	if (dict->length == 0) {
		for (int16_t k = 0; k < count; k++) {
			append::String(dict, wire_names[k]);
		}
	}
	append::Int(array, index);
}

// ============================================================================
// Equality
// ============================================================================

bool Equal(const VgiBufferPtr &a, const VgiBufferPtr &b) {
	int64_t sa = a ? a->size() : 0;
	int64_t sb = b ? b->size() : 0;
	if (sa != sb) {
		return false;
	}
	return sa == 0 || std::memcmp(a->data(), b->data(), static_cast<size_t>(sa)) == 0;
}

} // namespace wire
} // namespace vgi
} // namespace duckdb
