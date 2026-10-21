// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// Runtime for the generated protocol-record codecs (src/generated/vgi_protocol_records.cpp).
//
// The generator emits, per record, the column names and member pointers; everything that
// touches an Arrow array lives here. Decoding is by column name and strict about types: a
// missing nullable column reads as absent, a missing or null required column and a column of
// the wrong Arrow type are protocol errors. Encoding builds every array from the record's
// declared Arrow type (never from builder defaults), so the result has exactly the protocol
// schema — field names, nullability and dictionary index width included.
//
// Only the generated .cpp and tests include this header: it pulls in <arrow/api.h>.

#include <arrow/api.h>

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <vector>

namespace duckdb {
namespace vgi {
namespace wire {

//! Where a value being decoded came from, for protocol-error messages.
struct Ctx {
	//! The record being decoded (e.g. "TableInfo").
	std::string record;
	//! The worker that sent it.
	const std::string *worker_path = nullptr;
	//! Dotted path of the value within the record (e.g. "releases[].version").
	std::string field;
	//! Row of the top-level batch.
	int64_t row = 0;
};

[[noreturn]] void ThrowMissing(const Ctx &ctx);
[[noreturn]] void ThrowNull(const Ctx &ctx);
[[noreturn]] void ThrowType(const Ctx &ctx, const arrow::DataType &actual, const char *expected);
[[noreturn]] void ThrowBadEnum(const Ctx &ctx, const char *enum_name, const std::string &value);
[[noreturn]] void ThrowDuplicateKey(const Ctx &ctx);
//! Throw an IOException naming `what` unless `status` is OK.
void CheckArrow(const arrow::Status &status, const char *what);

// ============================================================================
// Decoding
// ============================================================================

//! Logical null, including a valid dictionary index referring to a null value.
bool IsNullValue(const arrow::Array &array, int64_t i, const Ctx &ctx);

template <class T>
struct IsOptional : std::false_type {};
template <class T>
struct IsOptional<std::optional<T>> : std::true_type {};

void ReadValue(const arrow::Array &array, int64_t i, bool &out, const Ctx &ctx);
void ReadValue(const arrow::Array &array, int64_t i, int8_t &out, const Ctx &ctx);
void ReadValue(const arrow::Array &array, int64_t i, int16_t &out, const Ctx &ctx);
void ReadValue(const arrow::Array &array, int64_t i, int32_t &out, const Ctx &ctx);
//! int64 or timestamp[us] (TimestampMicros is an int64).
void ReadValue(const arrow::Array &array, int64_t i, int64_t &out, const Ctx &ctx);
void ReadValue(const arrow::Array &array, int64_t i, uint8_t &out, const Ctx &ctx);
void ReadValue(const arrow::Array &array, int64_t i, uint16_t &out, const Ctx &ctx);
void ReadValue(const arrow::Array &array, int64_t i, uint32_t &out, const Ctx &ctx);
void ReadValue(const arrow::Array &array, int64_t i, uint64_t &out, const Ctx &ctx);
void ReadValue(const arrow::Array &array, int64_t i, float &out, const Ctx &ctx);
void ReadValue(const arrow::Array &array, int64_t i, double &out, const Ctx &ctx);
//! utf8, large_utf8, utf8_view, or a dictionary of any of them.
void ReadValue(const arrow::Array &array, int64_t i, std::string &out, const Ctx &ctx);
//! binary, large_binary, binary_view, or a dictionary of those (copied).
void ReadValue(const arrow::Array &array, int64_t i, std::vector<uint8_t> &out, const Ctx &ctx);
//! binary or large_binary, as a zero-copy slice of the array's data buffer.
void ReadValue(const arrow::Array &array, int64_t i, std::shared_ptr<arrow::Buffer> &out, const Ctx &ctx);

template <class T>
void ReadValue(const arrow::Array &array, int64_t i, std::optional<T> &out, const Ctx &ctx);
template <class T>
void ReadValue(const arrow::Array &array, int64_t i, std::vector<T> &out, const Ctx &ctx);
template <class K, class V>
void ReadValue(const arrow::Array &array, int64_t i, std::map<K, V> &out, const Ctx &ctx);

//! [begin, end) of list element `i` in `*child`. Throws unless `array` is a (large) list.
void ListBounds(const arrow::Array &array, int64_t i, const Ctx &ctx, int64_t &begin, int64_t &end,
                const arrow::Array *&child);
//! [begin, end) of map element `i` in `*keys` / `*items`. Throws unless `array` is a map.
void MapBounds(const arrow::Array &array, int64_t i, const Ctx &ctx, int64_t &begin, int64_t &end,
               const arrow::Array *&keys, const arrow::Array *&items);

template <class T>
void ReadValue(const arrow::Array &array, int64_t i, std::optional<T> &out, const Ctx &ctx) {
	if (IsNullValue(array, i, ctx)) {
		out.reset();
		return;
	}
	T value {};
	ReadValue(array, i, value, ctx);
	out = std::move(value);
}

template <class T>
void ReadValue(const arrow::Array &array, int64_t i, std::vector<T> &out, const Ctx &ctx) {
	if (array.IsNull(i)) {
		ThrowNull(ctx);
	}
	int64_t begin = 0;
	int64_t end = 0;
	const arrow::Array *child = nullptr;
	ListBounds(array, i, ctx, begin, end, child);
	Ctx item = ctx;
	item.field += "[]";
	out.clear();
	out.resize(static_cast<size_t>(end - begin));
	for (int64_t k = begin; k < end; k++) {
		if (!IsOptional<T>::value && child->IsNull(k)) {
			ThrowNull(item);
		}
		ReadValue(*child, k, out[static_cast<size_t>(k - begin)], item);
	}
}

template <class K, class V>
void ReadValue(const arrow::Array &array, int64_t i, std::map<K, V> &out, const Ctx &ctx) {
	if (array.IsNull(i)) {
		ThrowNull(ctx);
	}
	int64_t begin = 0;
	int64_t end = 0;
	const arrow::Array *keys = nullptr;
	const arrow::Array *items = nullptr;
	MapBounds(array, i, ctx, begin, end, keys, items);
	Ctx key_ctx = ctx;
	key_ctx.field += ".key";
	Ctx item_ctx = ctx;
	item_ctx.field += ".value";
	out.clear();
	for (int64_t k = begin; k < end; k++) {
		K key {};
		ReadValue(*keys, k, key, key_ctx);
		V value {};
		ReadValue(*items, k, value, item_ctx);
		// Protocol maps come from dictionaries: a repeated key is a peer bug, not a
		// value to silently overwrite.
		if (!out.emplace(std::move(key), std::move(value)).second) {
			ThrowDuplicateKey(key_ctx);
		}
	}
}

//! Reads the columns of one record by name. The generated Read<Record>() calls
//! Required/Optional/Defaulted once per column.
class RecordReader {
public:
	virtual ~RecordReader() = default;

	template <class T>
	void Required(const char *name, T &out) const {
		auto column = Column(name);
		Ctx ctx = Child(name);
		if (!column) {
			ThrowMissing(ctx);
		}
		ReadValue(*column, row_, out, ctx);
	}

	template <class T>
	void Optional(const char *name, std::optional<T> &out) const {
		auto column = Column(name);
		if (!column) {
			out.reset();
			return;
		}
		ReadValue(*column, row_, out, Child(name));
	}

	//! A nullable column the record holds as a plain value: missing or null keeps the default.
	template <class T>
	void Defaulted(const char *name, T &out) const {
		auto column = Column(name);
		if (!column || column->IsNull(row_)) {
			return;
		}
		ReadValue(*column, row_, out, Child(name));
	}

protected:
	RecordReader(Ctx ctx, int64_t row) : ctx_(std::move(ctx)), row_(row) {
	}
	//! The column called `name`, or null when the record has no such (single) column.
	virtual std::shared_ptr<arrow::Array> Column(const char *name) const = 0;

	Ctx Child(const char *name) const {
		Ctx ctx = ctx_;
		ctx.field = ctx.field.empty() ? std::string(name) : ctx.field + "." + name;
		return ctx;
	}

	Ctx ctx_;
	int64_t row_;
};

//! A top-level record: row `row` of a record batch.
class RowReader : public RecordReader {
public:
	RowReader(const std::shared_ptr<arrow::RecordBatch> &batch, int64_t row, const char *record,
	          const std::string &worker_path);

protected:
	std::shared_ptr<arrow::Array> Column(const char *name) const override;

private:
	const std::shared_ptr<arrow::RecordBatch> &batch_;
};

//! A nested record: element `i` of a struct array.
class StructReader : public RecordReader {
public:
	StructReader(const arrow::Array &array, int64_t i, const Ctx &ctx, const char *record);

protected:
	std::shared_ptr<arrow::Array> Column(const char *name) const override;

private:
	const arrow::StructArray *array_;
};

// ============================================================================
// Encoding: build one column of `type` from one value per row (nullptr = null)
// ============================================================================

using ArrayPtr = std::shared_ptr<arrow::Array>;
using TypePtr = std::shared_ptr<arrow::DataType>;

ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const bool *> &values);
ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const int8_t *> &values);
ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const int16_t *> &values);
ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const int32_t *> &values);
//! int64 or timestamp (TimestampMicros is an int64).
ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const int64_t *> &values);
ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const uint8_t *> &values);
ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const uint16_t *> &values);
ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const uint32_t *> &values);
ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const uint64_t *> &values);
ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const float *> &values);
ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const double *> &values);
//! utf8, large_utf8, or dictionary<int16, utf8> (dictionary in first-seen order).
ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const std::string *> &values);
//! binary, large_binary, or dictionary<int32, binary>.
ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const std::vector<uint8_t> *> &values);
//! large_binary or binary; a null pointer inside a present value is an empty value.
ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const std::shared_ptr<arrow::Buffer> *> &values);

template <class T>
ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const std::optional<T> *> &values);
template <class T>
ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const std::vector<T> *> &values);
template <class K, class V>
ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const std::map<K, V> *> &values);

//! The validity bitmap for `valid` (null when every row is valid) and its null count.
std::shared_ptr<arrow::Buffer> MakeValidity(const std::vector<bool> &valid, int64_t &null_count);
//! The value type of a (large) list type; throws for anything else.
const TypePtr &ListValueType(const TypePtr &type);
ArrayPtr FinishList(const TypePtr &type, const std::vector<int64_t> &offsets, const std::vector<bool> &valid,
                    const ArrayPtr &values);
ArrayPtr FinishMap(const TypePtr &type, const std::vector<int64_t> &offsets, const std::vector<bool> &valid,
                   const ArrayPtr &keys, const ArrayPtr &items);
//! dictionary<int16, utf8> over every enumerator in order, so an index IS the enumerator.
ArrayPtr FinishEnum(const TypePtr &type, const std::vector<int16_t> &indices, const char *const *wire_names,
                    int16_t count);
ArrayPtr FinishStruct(const TypePtr &type, int64_t length, const std::vector<ArrayPtr> &children,
                      const std::vector<bool> &valid);

template <class P>
std::vector<bool> ValidOf(const std::vector<const P *> &values) {
	std::vector<bool> valid(values.size());
	for (size_t i = 0; i < values.size(); i++) {
		valid[i] = values[i] != nullptr;
	}
	return valid;
}

template <class T>
ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const std::optional<T> *> &values) {
	std::vector<const T *> inner(values.size());
	for (size_t i = 0; i < values.size(); i++) {
		inner[i] = values[i] && values[i]->has_value() ? &**values[i] : nullptr;
	}
	return BuildColumn(type, inner);
}

template <class T>
ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const std::vector<T> *> &values) {
	std::vector<const T *> flat;
	std::vector<int64_t> offsets {0};
	offsets.reserve(values.size() + 1);
	for (auto *row : values) {
		if (row) {
			for (auto &item : *row) {
				flat.push_back(&item);
			}
		}
		offsets.push_back(static_cast<int64_t>(flat.size()));
	}
	return FinishList(type, offsets, ValidOf(values), BuildColumn(ListValueType(type), flat));
}

template <class K, class V>
ArrayPtr BuildColumn(const TypePtr &type, const std::vector<const std::map<K, V> *> &values) {
	if (type->id() != arrow::Type::MAP) {
		throw std::logic_error("vgi wire codec: std::map member for non-map type " + type->ToString());
	}
	const auto &map_type = static_cast<const arrow::MapType &>(*type);
	std::vector<const K *> keys;
	std::vector<const V *> items;
	std::vector<int64_t> offsets {0};
	for (auto *row : values) {
		if (row) {
			for (auto &entry : *row) {
				keys.push_back(&entry.first);
				items.push_back(&entry.second);
			}
		}
		offsets.push_back(static_cast<int64_t>(keys.size()));
	}
	return FinishMap(type, offsets, ValidOf(values), BuildColumn(map_type.key_type(), keys),
	                 BuildColumn(map_type.item_type(), items));
}

template <class E>
ArrayPtr BuildEnumColumn(const TypePtr &type, const std::vector<const E *> &values, const char *const *wire_names,
                         int16_t count) {
	std::vector<int16_t> indices(values.size());
	for (size_t i = 0; i < values.size(); i++) {
		indices[i] = values[i] ? static_cast<int16_t>(*values[i]) : int16_t(-1);
	}
	return FinishEnum(type, indices, wire_names, count);
}

//! Builds a struct column from records, one member (= child column) at a time.
template <class R>
class StructColumn {
public:
	StructColumn(const TypePtr &type, const std::vector<const R *> &rows)
	    : type_(type), rows_(rows), children_(static_cast<size_t>(type->num_fields())) {
	}

	template <class M>
	void Put(int index, M R::*member) {
		std::vector<const M *> values(rows_.size());
		for (size_t i = 0; i < rows_.size(); i++) {
			values[i] = rows_[i] ? &(rows_[i]->*member) : nullptr;
		}
		children_[static_cast<size_t>(index)] = BuildColumn(type_->field(index)->type(), values);
	}

	ArrayPtr Finish() const {
		return FinishStruct(type_, static_cast<int64_t>(rows_.size()), children_, ValidOf(rows_));
	}

private:
	const TypePtr &type_;
	const std::vector<const R *> &rows_;
	std::vector<ArrayPtr> children_;
};

//! The struct type whose fields are `schema`'s.
TypePtr StructTypeOf(const std::shared_ptr<arrow::Schema> &schema);
//! A record batch with `schema` whose columns are `struct_array`'s children.
std::shared_ptr<arrow::RecordBatch> BatchFromStruct(const std::shared_ptr<arrow::Schema> &schema,
                                                    const ArrayPtr &struct_array);

// ============================================================================
// Equality (records compare member-wise; large_binary by content)
// ============================================================================

bool Equal(const std::shared_ptr<arrow::Buffer> &a, const std::shared_ptr<arrow::Buffer> &b);
template <class T>
bool Equal(const std::optional<T> &a, const std::optional<T> &b);
template <class T>
bool Equal(const std::vector<T> &a, const std::vector<T> &b);
template <class K, class V>
bool Equal(const std::map<K, V> &a, const std::map<K, V> &b);

template <class T>
bool Equal(const T &a, const T &b) {
	return a == b;
}
template <class T>
bool Equal(const std::optional<T> &a, const std::optional<T> &b) {
	return a.has_value() == b.has_value() && (!a || Equal(*a, *b));
}
template <class T>
bool Equal(const std::vector<T> &a, const std::vector<T> &b) {
	if (a.size() != b.size()) {
		return false;
	}
	for (size_t i = 0; i < a.size(); i++) {
		if (!Equal(a[i], b[i])) {
			return false;
		}
	}
	return true;
}
template <class K, class V>
bool Equal(const std::map<K, V> &a, const std::map<K, V> &b) {
	if (a.size() != b.size()) {
		return false;
	}
	for (auto ia = a.begin(), ib = b.begin(); ia != a.end(); ++ia, ++ib) {
		if (!(ia->first == ib->first) || !Equal(ia->second, ib->second)) {
			return false;
		}
	}
	return true;
}

} // namespace wire
} // namespace vgi
} // namespace duckdb
