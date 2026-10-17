// © Copyright 2025, 2026 Query Farm LLC - https://query.farm

#include "vgi_wire_codec.hpp"

#include "vgi_exception.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include <arrow/util/bit_util.h>

#include <unordered_map>

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

void ThrowType(const Ctx &ctx, const arrow::DataType &actual, const char *expected) {
	throw InvalidInputException(
	    ProtocolInfo(ctx).ExpectedActual(expected, actual.ToString()),
	    StringUtil::Format("VGI protocol error: Column '%s' in %s response from worker '%s' has type %s, expected %s",
	                       ctx.field, ctx.record, WorkerOf(ctx), actual.ToString(), expected));
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

void CheckArrow(const arrow::Status &status, const char *what) {
	if (!status.ok()) {
		throw IOException("VGI wire codec: %s: %s", what, status.ToString());
	}
}

template <class T>
static T ValueOrThrow(arrow::Result<T> result, const char *what) {
	CheckArrow(result.status(), what);
	return result.MoveValueUnsafe();
}

// ============================================================================
// Decoding
// ============================================================================

template <class ArrayType>
static const ArrayType &Expect(const arrow::Array &array, arrow::Type::type id, const Ctx &ctx, const char *expected) {
	if (array.type_id() != id) {
		ThrowType(ctx, *array.type(), expected);
	}
	return static_cast<const ArrayType &>(array);
}

template <class ArrayType, class T>
static void ReadPrimitive(const arrow::Array &array, int64_t i, T &out, const Ctx &ctx, arrow::Type::type id,
                          const char *expected) {
	const auto &typed = Expect<ArrayType>(array, id, ctx, expected);
	if (typed.IsNull(i)) {
		ThrowNull(ctx);
	}
	out = typed.Value(i);
}

void ReadValue(const arrow::Array &array, int64_t i, bool &out, const Ctx &ctx) {
	ReadPrimitive<arrow::BooleanArray>(array, i, out, ctx, arrow::Type::BOOL, "bool");
}
void ReadValue(const arrow::Array &array, int64_t i, int8_t &out, const Ctx &ctx) {
	ReadPrimitive<arrow::Int8Array>(array, i, out, ctx, arrow::Type::INT8, "int8");
}
void ReadValue(const arrow::Array &array, int64_t i, int16_t &out, const Ctx &ctx) {
	ReadPrimitive<arrow::Int16Array>(array, i, out, ctx, arrow::Type::INT16, "int16");
}
void ReadValue(const arrow::Array &array, int64_t i, int32_t &out, const Ctx &ctx) {
	ReadPrimitive<arrow::Int32Array>(array, i, out, ctx, arrow::Type::INT32, "int32");
}
void ReadValue(const arrow::Array &array, int64_t i, int64_t &out, const Ctx &ctx) {
	if (array.type_id() == arrow::Type::TIMESTAMP) {
		const auto &ts_type = static_cast<const arrow::TimestampType &>(*array.type());
		if (ts_type.unit() != arrow::TimeUnit::MICRO) {
			ThrowType(ctx, *array.type(), "timestamp[us]");
		}
		ReadPrimitive<arrow::TimestampArray>(array, i, out, ctx, arrow::Type::TIMESTAMP, "timestamp[us]");
		return;
	}
	ReadPrimitive<arrow::Int64Array>(array, i, out, ctx, arrow::Type::INT64, "int64");
}
void ReadValue(const arrow::Array &array, int64_t i, uint8_t &out, const Ctx &ctx) {
	ReadPrimitive<arrow::UInt8Array>(array, i, out, ctx, arrow::Type::UINT8, "uint8");
}
void ReadValue(const arrow::Array &array, int64_t i, uint16_t &out, const Ctx &ctx) {
	ReadPrimitive<arrow::UInt16Array>(array, i, out, ctx, arrow::Type::UINT16, "uint16");
}
void ReadValue(const arrow::Array &array, int64_t i, uint32_t &out, const Ctx &ctx) {
	ReadPrimitive<arrow::UInt32Array>(array, i, out, ctx, arrow::Type::UINT32, "uint32");
}
void ReadValue(const arrow::Array &array, int64_t i, uint64_t &out, const Ctx &ctx) {
	ReadPrimitive<arrow::UInt64Array>(array, i, out, ctx, arrow::Type::UINT64, "uint64");
}
void ReadValue(const arrow::Array &array, int64_t i, float &out, const Ctx &ctx) {
	ReadPrimitive<arrow::FloatArray>(array, i, out, ctx, arrow::Type::FLOAT, "float");
}
void ReadValue(const arrow::Array &array, int64_t i, double &out, const Ctx &ctx) {
	ReadPrimitive<arrow::DoubleArray>(array, i, out, ctx, arrow::Type::DOUBLE, "double");
}

void ReadValue(const arrow::Array &array, int64_t i, std::string &out, const Ctx &ctx) {
	if (array.IsNull(i)) {
		ThrowNull(ctx);
	}
	switch (array.type_id()) {
	case arrow::Type::STRING: {
		auto view = static_cast<const arrow::StringArray &>(array).GetView(i);
		out.assign(view.data(), view.size());
		return;
	}
	case arrow::Type::LARGE_STRING: {
		auto view = static_cast<const arrow::LargeStringArray &>(array).GetView(i);
		out.assign(view.data(), view.size());
		return;
	}
	case arrow::Type::STRING_VIEW: {
		auto view = static_cast<const arrow::StringViewArray &>(array).GetView(i);
		out.assign(view.data(), view.size());
		return;
	}
	case arrow::Type::DICTIONARY: {
		const auto &dict = static_cast<const arrow::DictionaryArray &>(array);
		const auto &values = *dict.dictionary();
		auto id = values.type_id();
		if (id != arrow::Type::STRING && id != arrow::Type::LARGE_STRING && id != arrow::Type::STRING_VIEW) {
			ThrowType(ctx, *array.type(), "dictionary<utf8>");
		}
		auto index = dict.GetValueIndex(i);
		if (index < 0 || index >= values.length()) {
			ThrowType(ctx, *array.type(), "dictionary index in range");
		}
		ReadValue(values, index, out, ctx);
		return;
	}
	default:
		ThrowType(ctx, *array.type(), "utf8");
	}
}

void ReadValue(const arrow::Array &array, int64_t i, std::vector<uint8_t> &out, const Ctx &ctx) {
	if (array.IsNull(i)) {
		ThrowNull(ctx);
	}
	std::string_view view;
	switch (array.type_id()) {
	case arrow::Type::BINARY:
		view = static_cast<const arrow::BinaryArray &>(array).GetView(i);
		break;
	case arrow::Type::LARGE_BINARY:
		view = static_cast<const arrow::LargeBinaryArray &>(array).GetView(i);
		break;
	case arrow::Type::BINARY_VIEW:
		view = static_cast<const arrow::BinaryViewArray &>(array).GetView(i);
		break;
	default:
		ThrowType(ctx, *array.type(), "binary");
	}
	out.assign(reinterpret_cast<const uint8_t *>(view.data()),
	           reinterpret_cast<const uint8_t *>(view.data()) + view.size());
}

void ReadValue(const arrow::Array &array, int64_t i, std::shared_ptr<arrow::Buffer> &out, const Ctx &ctx) {
	if (array.IsNull(i)) {
		ThrowNull(ctx);
	}
	switch (array.type_id()) {
	case arrow::Type::LARGE_BINARY: {
		const auto &typed = static_cast<const arrow::LargeBinaryArray &>(array);
		out = arrow::SliceBuffer(typed.value_data(), typed.value_offset(i), typed.value_length(i));
		return;
	}
	case arrow::Type::BINARY: {
		const auto &typed = static_cast<const arrow::BinaryArray &>(array);
		out = arrow::SliceBuffer(typed.value_data(), typed.value_offset(i), typed.value_length(i));
		return;
	}
	default:
		ThrowType(ctx, *array.type(), "large_binary");
	}
}

void ListBounds(const arrow::Array &array, int64_t i, const Ctx &ctx, int64_t &begin, int64_t &end,
                const arrow::Array *&child) {
	switch (array.type_id()) {
	case arrow::Type::LIST: {
		const auto &list = static_cast<const arrow::ListArray &>(array);
		begin = list.value_offset(i);
		end = begin + list.value_length(i);
		child = list.values().get();
		return;
	}
	case arrow::Type::LARGE_LIST: {
		const auto &list = static_cast<const arrow::LargeListArray &>(array);
		begin = list.value_offset(i);
		end = begin + list.value_length(i);
		child = list.values().get();
		return;
	}
	default:
		ThrowType(ctx, *array.type(), "list");
	}
}

void MapBounds(const arrow::Array &array, int64_t i, const Ctx &ctx, int64_t &begin, int64_t &end,
               const arrow::Array *&keys, const arrow::Array *&items) {
	if (array.type_id() != arrow::Type::MAP) {
		ThrowType(ctx, *array.type(), "map");
	}
	const auto &map = static_cast<const arrow::MapArray &>(array);
	begin = map.value_offset(i);
	end = begin + map.value_length(i);
	keys = map.keys().get();
	items = map.items().get();
}

RowReader::RowReader(const std::shared_ptr<arrow::RecordBatch> &batch, int64_t row, const char *record,
                     const std::string &worker_path)
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

std::shared_ptr<arrow::Array> RowReader::Column(const char *name) const {
	auto index = batch_->schema()->GetFieldIndex(name);
	return index < 0 ? nullptr : batch_->column(index);
}

StructReader::StructReader(const arrow::Array &array, int64_t i, const Ctx &ctx, const char *record)
    : RecordReader(ctx, i), array_(nullptr) {
	(void)record;
	if (array.type_id() != arrow::Type::STRUCT) {
		ThrowType(ctx, *array.type(), "struct");
	}
	if (array.IsNull(i)) {
		ThrowNull(ctx);
	}
	array_ = &static_cast<const arrow::StructArray &>(array);
}

std::shared_ptr<arrow::Array> StructReader::Column(const char *name) const {
	auto index = array_->struct_type()->GetFieldIndex(name);
	return index < 0 ? nullptr : array_->field(index);
}

// ============================================================================
// Encoding
// ============================================================================

std::shared_ptr<arrow::Buffer> MakeValidity(const std::vector<bool> &valid, int64_t &null_count) {
	null_count = 0;
	for (bool v : valid) {
		null_count += v ? 0 : 1;
	}
	if (null_count == 0) {
		return nullptr;
	}
	auto bitmap = ValueOrThrow(arrow::AllocateEmptyBitmap(static_cast<int64_t>(valid.size())), "allocate validity");
	for (size_t i = 0; i < valid.size(); i++) {
		if (valid[i]) {
			arrow::bit_util::SetBit(bitmap->mutable_data(), static_cast<int64_t>(i));
		}
	}
	return bitmap;
}

[[noreturn]] static void TypeMismatch(const TypePtr &type, const char *cpp_type) {
	throw InternalException("VGI wire codec: cannot encode a %s member as Arrow type %s", cpp_type, type->ToString());
}

template <class BuilderType, class T>
static ArrayPtr BuildWith(BuilderType &builder, const std::vector<const T *> &values) {
	CheckArrow(builder.Reserve(static_cast<int64_t>(values.size())), "reserve");
	for (auto *v : values) {
		CheckArrow(v ? builder.Append(*v) : builder.AppendNull(), "append");
	}
	return ValueOrThrow(builder.Finish(), "finish");
}

template <class BuilderType, class T>
static ArrayPtr BuildPrimitive(const TypePtr &type, arrow::Type::type id, const std::vector<const T *> &values,
                               const char *cpp_type) {
	if (type->id() != id) {
		TypeMismatch(type, cpp_type);
	}
	BuilderType builder;
	return BuildWith(builder, values);
}

ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const bool *> &values) {
	return BuildPrimitive<arrow::BooleanBuilder>(type, arrow::Type::BOOL, values, "bool");
}
ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const int8_t *> &values) {
	return BuildPrimitive<arrow::Int8Builder>(type, arrow::Type::INT8, values, "int8_t");
}
ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const int16_t *> &values) {
	return BuildPrimitive<arrow::Int16Builder>(type, arrow::Type::INT16, values, "int16_t");
}
ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const int32_t *> &values) {
	return BuildPrimitive<arrow::Int32Builder>(type, arrow::Type::INT32, values, "int32_t");
}
ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const int64_t *> &values) {
	if (type->id() == arrow::Type::TIMESTAMP) {
		arrow::TimestampBuilder builder(type, arrow::default_memory_pool());
		return BuildWith(builder, values);
	}
	return BuildPrimitive<arrow::Int64Builder>(type, arrow::Type::INT64, values, "int64_t");
}
ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const uint8_t *> &values) {
	return BuildPrimitive<arrow::UInt8Builder>(type, arrow::Type::UINT8, values, "uint8_t");
}
ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const uint16_t *> &values) {
	return BuildPrimitive<arrow::UInt16Builder>(type, arrow::Type::UINT16, values, "uint16_t");
}
ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const uint32_t *> &values) {
	return BuildPrimitive<arrow::UInt32Builder>(type, arrow::Type::UINT32, values, "uint32_t");
}
ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const uint64_t *> &values) {
	return BuildPrimitive<arrow::UInt64Builder>(type, arrow::Type::UINT64, values, "uint64_t");
}
ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const float *> &values) {
	return BuildPrimitive<arrow::FloatBuilder>(type, arrow::Type::FLOAT, values, "float");
}
ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const double *> &values) {
	return BuildPrimitive<arrow::DoubleBuilder>(type, arrow::Type::DOUBLE, values, "double");
}

ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const std::string *> &values) {
	switch (type->id()) {
	case arrow::Type::STRING: {
		arrow::StringBuilder builder;
		return BuildWith(builder, values);
	}
	case arrow::Type::LARGE_STRING: {
		arrow::LargeStringBuilder builder;
		return BuildWith(builder, values);
	}
	case arrow::Type::DICTIONARY: {
		const auto &dict_type = static_cast<const arrow::DictionaryType &>(*type);
		if (dict_type.index_type()->id() != arrow::Type::INT16 || dict_type.value_type()->id() != arrow::Type::STRING) {
			TypeMismatch(type, "std::string");
		}
		// Dictionary in first-seen order.
		std::unordered_map<std::string, int16_t> seen;
		arrow::StringBuilder dict_builder;
		arrow::Int16Builder index_builder;
		for (auto *v : values) {
			if (!v) {
				CheckArrow(index_builder.AppendNull(), "append dictionary index");
				continue;
			}
			auto it = seen.find(*v);
			if (it == seen.end()) {
				it = seen.emplace(*v, static_cast<int16_t>(seen.size())).first;
				CheckArrow(dict_builder.Append(*v), "append dictionary value");
			}
			CheckArrow(index_builder.Append(it->second), "append dictionary index");
		}
		auto indices = ValueOrThrow(index_builder.Finish(), "finish dictionary indices");
		auto dictionary = ValueOrThrow(dict_builder.Finish(), "finish dictionary");
		return ValueOrThrow(arrow::DictionaryArray::FromArrays(type, indices, dictionary), "make dictionary array");
	}
	default:
		TypeMismatch(type, "std::string");
	}
}

ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const std::vector<uint8_t> *> &values) {
	auto build = [&](auto &builder) {
		CheckArrow(builder.Reserve(static_cast<int64_t>(values.size())), "reserve");
		for (auto *v : values) {
			CheckArrow(v ? builder.Append(v->data(), static_cast<int64_t>(v->size())) : builder.AppendNull(), "append");
		}
		return ValueOrThrow(builder.Finish(), "finish");
	};
	switch (type->id()) {
	case arrow::Type::BINARY: {
		arrow::BinaryBuilder builder;
		return build(builder);
	}
	case arrow::Type::LARGE_BINARY: {
		arrow::LargeBinaryBuilder builder;
		return build(builder);
	}
	default:
		TypeMismatch(type, "Bytes");
	}
}

ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const std::shared_ptr<arrow::Buffer> *> &values) {
	auto build = [&](auto &builder) {
		CheckArrow(builder.Reserve(static_cast<int64_t>(values.size())), "reserve");
		for (auto *v : values) {
			if (!v) {
				CheckArrow(builder.AppendNull(), "append");
			} else if (!*v) {
				CheckArrow(builder.AppendEmptyValue(), "append");
			} else {
				CheckArrow(builder.Append((*v)->data(), (*v)->size()), "append");
			}
		}
		return ValueOrThrow(builder.Finish(), "finish");
	};
	switch (type->id()) {
	case arrow::Type::LARGE_BINARY: {
		arrow::LargeBinaryBuilder builder;
		return build(builder);
	}
	case arrow::Type::BINARY: {
		arrow::BinaryBuilder builder;
		return build(builder);
	}
	default:
		TypeMismatch(type, "LargeBytes");
	}
}

const TypePtr &ListValueType(const TypePtr &type) {
	switch (type->id()) {
	case arrow::Type::LIST:
	case arrow::Type::LARGE_LIST:
		return static_cast<const arrow::BaseListType &>(*type).value_type();
	default:
		TypeMismatch(type, "std::vector");
	}
}

ArrayPtr FinishList(const TypePtr &type, const std::vector<int64_t> &offsets, const std::vector<bool> &valid,
                    const ArrayPtr &values) {
	int64_t null_count = 0;
	auto validity = MakeValidity(valid, null_count);
	if (type->id() == arrow::Type::LARGE_LIST) {
		arrow::Int64Builder offset_builder;
		CheckArrow(offset_builder.AppendValues(offsets), "append list offsets");
		auto offset_array = ValueOrThrow(offset_builder.Finish(), "finish list offsets");
		return ValueOrThrow(arrow::LargeListArray::FromArrays(type, *offset_array, *values,
		                                                      arrow::default_memory_pool(), validity, null_count),
		                    "make large list");
	}
	arrow::Int32Builder offset_builder;
	for (auto offset : offsets) {
		CheckArrow(offset_builder.Append(static_cast<int32_t>(offset)), "append list offset");
	}
	auto offset_array = ValueOrThrow(offset_builder.Finish(), "finish list offsets");
	return ValueOrThrow(
	    arrow::ListArray::FromArrays(type, *offset_array, *values, arrow::default_memory_pool(), validity, null_count),
	    "make list");
}

ArrayPtr FinishMap(const TypePtr &type, const std::vector<int64_t> &offsets, const std::vector<bool> &valid,
                   const ArrayPtr &keys, const ArrayPtr &items) {
	int64_t null_count = 0;
	auto validity = MakeValidity(valid, null_count);
	arrow::Int32Builder offset_builder;
	for (auto offset : offsets) {
		CheckArrow(offset_builder.Append(static_cast<int32_t>(offset)), "append map offset");
	}
	auto offset_array = ValueOrThrow(offset_builder.Finish(), "finish map offsets");
	return ValueOrThrow(arrow::MapArray::FromArrays(type, offset_array, keys, items, arrow::default_memory_pool(),
	                                                validity),
	                    "make map");
}

ArrayPtr FinishEnum(const TypePtr &type, const std::vector<int16_t> &indices, const char *const *wire_names,
                    int16_t count) {
	if (type->id() != arrow::Type::DICTIONARY) {
		TypeMismatch(type, "enum");
	}
	arrow::StringBuilder dict_builder;
	for (int16_t i = 0; i < count; i++) {
		CheckArrow(dict_builder.Append(wire_names[i]), "append enum dictionary");
	}
	auto dictionary = ValueOrThrow(dict_builder.Finish(), "finish enum dictionary");
	arrow::Int16Builder index_builder;
	for (auto index : indices) {
		if (index < 0) {
			CheckArrow(index_builder.AppendNull(), "append enum index");
		} else if (index >= count) {
			throw InternalException("VGI wire codec: enum index %d out of range (%d enumerators)", index, count);
		} else {
			CheckArrow(index_builder.Append(index), "append enum index");
		}
	}
	auto index_array = ValueOrThrow(index_builder.Finish(), "finish enum indices");
	return ValueOrThrow(arrow::DictionaryArray::FromArrays(type, index_array, dictionary), "make enum array");
}

ArrayPtr FinishStruct(const TypePtr &type, int64_t length, const std::vector<ArrayPtr> &children,
                      const std::vector<bool> &valid) {
	for (size_t i = 0; i < children.size(); i++) {
		if (!children[i]) {
			throw InternalException("VGI wire codec: struct %s field %d was never encoded", type->ToString(),
			                        static_cast<int>(i));
		}
	}
	int64_t null_count = 0;
	auto validity = MakeValidity(valid, null_count);
	return std::make_shared<arrow::StructArray>(type, length, children, validity, null_count);
}

TypePtr StructTypeOf(const std::shared_ptr<arrow::Schema> &schema) {
	return arrow::struct_(schema->fields());
}

std::shared_ptr<arrow::RecordBatch> BatchFromStruct(const std::shared_ptr<arrow::Schema> &schema,
                                                    const ArrayPtr &struct_array) {
	const auto &typed = static_cast<const arrow::StructArray &>(*struct_array);
	std::vector<ArrayPtr> columns;
	columns.reserve(static_cast<size_t>(schema->num_fields()));
	for (int i = 0; i < schema->num_fields(); i++) {
		columns.push_back(typed.field(i));
	}
	return arrow::RecordBatch::Make(schema, typed.length(), std::move(columns));
}

bool Equal(const std::shared_ptr<arrow::Buffer> &a, const std::shared_ptr<arrow::Buffer> &b) {
	auto size_a = a ? a->size() : 0;
	auto size_b = b ? b->size() : 0;
	if (size_a != size_b) {
		return false;
	}
	return size_a == 0 || a->Equals(*b);
}

} // namespace wire
} // namespace vgi
} // namespace duckdb
