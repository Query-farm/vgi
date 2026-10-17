// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// Runtime for the generated protocol-record codecs (src/generated/vgi_protocol_records.cpp).
//
// The generator emits, per record, the column names and member order; everything that
// touches an array lives here. Decoding is by column name and strict: a missing nullable
// column reads as absent; a missing or null required column, a column of the wrong type,
// a null in a non-nullable list/map, a null item in a list of non-optional values, an
// unknown enum value and a repeated map key are protocol errors. Encoding appends to arrays
// built from the record's declared schema, so the result has exactly the protocol schema.
//
// Only the generated .cpp and tests include this header.

#include "vgi_arrow.hpp"

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
[[noreturn]] void ThrowType(const Ctx &ctx, const VgiColumn &actual, const char *expected);
[[noreturn]] void ThrowBadEnum(const Ctx &ctx, const char *enum_name, const std::string &value);
[[noreturn]] void ThrowDuplicateKey(const Ctx &ctx);

// ============================================================================
// Decoding
// ============================================================================

template <class T>
struct IsOptional : std::false_type {};
template <class T>
struct IsOptional<std::optional<T>> : std::true_type {};

void ReadValue(const VgiColumn &col, int64_t i, bool &out, const Ctx &ctx);
void ReadValue(const VgiColumn &col, int64_t i, int8_t &out, const Ctx &ctx);
void ReadValue(const VgiColumn &col, int64_t i, int16_t &out, const Ctx &ctx);
void ReadValue(const VgiColumn &col, int64_t i, int32_t &out, const Ctx &ctx);
//! int64 or timestamp[us] (TimestampMicros is an int64).
void ReadValue(const VgiColumn &col, int64_t i, int64_t &out, const Ctx &ctx);
void ReadValue(const VgiColumn &col, int64_t i, uint8_t &out, const Ctx &ctx);
void ReadValue(const VgiColumn &col, int64_t i, uint16_t &out, const Ctx &ctx);
void ReadValue(const VgiColumn &col, int64_t i, uint32_t &out, const Ctx &ctx);
void ReadValue(const VgiColumn &col, int64_t i, uint64_t &out, const Ctx &ctx);
void ReadValue(const VgiColumn &col, int64_t i, float &out, const Ctx &ctx);
void ReadValue(const VgiColumn &col, int64_t i, double &out, const Ctx &ctx);
//! utf8, large_utf8, utf8_view, or a dictionary of any of them.
void ReadValue(const VgiColumn &col, int64_t i, std::string &out, const Ctx &ctx);
//! binary, large_binary or binary_view (copied).
void ReadValue(const VgiColumn &col, int64_t i, std::vector<uint8_t> &out, const Ctx &ctx);

//! A binary / large_binary value as a zero-copy slice that keeps CurrentBatch() alive.
void ReadValue(const VgiColumn &col, int64_t i, VgiBufferPtr &out, const Ctx &ctx);

template <class T>
void ReadValue(const VgiColumn &col, int64_t i, std::optional<T> &out, const Ctx &ctx);
template <class T>
void ReadValue(const VgiColumn &col, int64_t i, std::vector<T> &out, const Ctx &ctx);
template <class K, class V>
void ReadValue(const VgiColumn &col, int64_t i, std::map<K, V> &out, const Ctx &ctx);

//! [begin, end) of list element `i` in col.child(0). Throws unless `col` is a (large) list.
void ListBounds(const VgiColumn &col, int64_t i, const Ctx &ctx, int64_t &begin, int64_t &end);
//! [begin, end) of map element `i` in the entries column. Throws unless `col` is a map.
void MapBounds(const VgiColumn &col, int64_t i, const Ctx &ctx, int64_t &begin, int64_t &end);

template <class T>
void ReadValue(const VgiColumn &col, int64_t i, std::optional<T> &out, const Ctx &ctx) {
	if (col.IsNull(i)) {
		out.reset();
		return;
	}
	T value {};
	ReadValue(col, i, value, ctx);
	out = std::move(value);
}

template <class T>
void ReadValue(const VgiColumn &col, int64_t i, std::vector<T> &out, const Ctx &ctx) {
	if (col.IsNull(i)) {
		ThrowNull(ctx);
	}
	int64_t begin = 0;
	int64_t end = 0;
	ListBounds(col, i, ctx, begin, end);
	auto child = col.child(0);
	Ctx item = ctx;
	item.field += "[]";
	out.clear();
	out.resize(static_cast<size_t>(end - begin));
	for (int64_t k = begin; k < end; k++) {
		if (!IsOptional<T>::value && child.IsNull(k)) {
			ThrowNull(item);
		}
		ReadValue(child, k, out[static_cast<size_t>(k - begin)], item);
	}
}

template <class K, class V>
void ReadValue(const VgiColumn &col, int64_t i, std::map<K, V> &out, const Ctx &ctx) {
	if (col.IsNull(i)) {
		ThrowNull(ctx);
	}
	int64_t begin = 0;
	int64_t end = 0;
	MapBounds(col, i, ctx, begin, end);
	auto entries = col.child(0);
	auto keys = entries.child(0);
	auto items = entries.child(1);
	Ctx key_ctx = ctx;
	key_ctx.field += ".key";
	Ctx item_ctx = ctx;
	item_ctx.field += ".value";
	out.clear();
	for (int64_t k = begin; k < end; k++) {
		K key {};
		ReadValue(keys, k, key, key_ctx);
		V value {};
		ReadValue(items, k, value, item_ctx);
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
		ReadValue(column, row_, out, ctx);
	}

	template <class T>
	void Optional(const char *name, std::optional<T> &out) const {
		auto column = Column(name);
		if (!column) {
			out.reset();
			return;
		}
		ReadValue(column, row_, out, Child(name));
	}

	//! A nullable column the record holds as a plain value: missing or null keeps the default.
	template <class T>
	void Defaulted(const char *name, T &out) const {
		auto column = Column(name);
		if (!column || column.IsNull(row_)) {
			return;
		}
		ReadValue(column, row_, out, Child(name));
	}

protected:
	RecordReader(Ctx ctx, int64_t row) : ctx_(std::move(ctx)), row_(row) {
	}
	//! The column called `name`, or an empty column when the record has none.
	virtual VgiColumn Column(const char *name) const = 0;

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
	RowReader(const VgiBatchPtr &batch, int64_t row, const char *record, const std::string &worker_path);

protected:
	VgiColumn Column(const char *name) const override;

private:
	const VgiBatchPtr &batch_;
};

//! A nested record: element `i` of a struct column.
class StructReader : public RecordReader {
public:
	StructReader(const VgiColumn &col, int64_t i, const Ctx &ctx, const char *record);

protected:
	VgiColumn Column(const char *name) const override;

private:
	VgiColumn col_;
};

//! The batch whose buffers zero-copy reads (VgiBufferPtr members) share. Set by
//! generated FromBatch for the duration of a decode.
const VgiBatch *CurrentBatch();
class CurrentBatchScope {
public:
	explicit CurrentBatchScope(const VgiBatch *batch);
	~CurrentBatchScope();

private:
	const VgiBatch *previous_;
};

// ============================================================================
// Encoding: append one value to an array being built from the declared schema
// ============================================================================

void WriteValue(ArrowArray *array, bool value);
void WriteValue(ArrowArray *array, int8_t value);
void WriteValue(ArrowArray *array, int16_t value);
void WriteValue(ArrowArray *array, int32_t value);
void WriteValue(ArrowArray *array, int64_t value);
void WriteValue(ArrowArray *array, uint8_t value);
void WriteValue(ArrowArray *array, uint16_t value);
void WriteValue(ArrowArray *array, uint32_t value);
void WriteValue(ArrowArray *array, uint64_t value);
void WriteValue(ArrowArray *array, float value);
void WriteValue(ArrowArray *array, double value);
//! utf8 / large_utf8, or a dictionary<int, utf8> column (dictionary in first-seen order).
void WriteValue(ArrowArray *array, const std::string &value);
void WriteValue(ArrowArray *array, const std::vector<uint8_t> &value);
//! A null pointer inside a present value is an empty value.
void WriteValue(ArrowArray *array, const VgiBufferPtr &value);

template <class T>
void WriteValue(ArrowArray *array, const std::optional<T> &value);
template <class T>
void WriteValue(ArrowArray *array, const std::vector<T> &value);
template <class K, class V>
void WriteValue(ArrowArray *array, const std::map<K, V> &value);

template <class T>
void WriteValue(ArrowArray *array, const std::optional<T> &value) {
	if (!value) {
		append::Null(array);
		return;
	}
	WriteValue(array, *value);
}

template <class T>
void WriteValue(ArrowArray *array, const std::vector<T> &value) {
	for (const auto &item : value) {
		WriteValue(array->children[0], item);
	}
	append::FinishElement(array);
}

template <class K, class V>
void WriteValue(ArrowArray *array, const std::map<K, V> &value) {
	ArrowArray *entries = array->children[0];
	for (const auto &kv : value) {
		WriteValue(entries->children[0], kv.first);
		WriteValue(entries->children[1], kv.second);
		append::FinishElement(entries);
	}
	append::FinishElement(array);
}

//! A dictionary<int16, utf8> enum column: the dictionary holds every enumerator in order,
//! so an index IS the enumerator.
void WriteEnum(ArrowArray *array, int16_t index, const char *const *wire_names, int16_t count);

// ============================================================================
// Equality (records compare member-wise; large_binary by content)
// ============================================================================

bool Equal(const VgiBufferPtr &a, const VgiBufferPtr &b);
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
