// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// The extension's Arrow data types, on the Arrow C data interface and nanoarrow.
//
//   VgiBuffer    shared, immutable bytes (an IPC body, a serialized message, ...)
//   VgiMetadata  key/value metadata (message custom_metadata, schema/field metadata)
//   VgiSchema    shared, immutable owner of a struct ArrowSchema (the batch schema)
//   VgiColumn    a borrowed, typed read view of one column (or nested child)
//   VgiBatch     shared, immutable owner of one struct ArrowArray + its schema
//
// Everything is immutable once built and safe to read from several threads.
// A VgiBatch's buffers stay alive while any VgiBatch, slice or exported ArrowArray
// referencing them does: DuckDB releases exported arrays on any thread, long after
// the scan that produced them.
//
// This header is cheap to include: it needs only DuckDB's C data interface structs.

#include "duckdb/common/arrow/arrow.hpp"

#include <cstdint>
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

struct ArrowArrayView;
struct ArrowSchemaView;

namespace duckdb {
struct ArrowArrayWrapper;
struct ArrowSchemaWrapper;

namespace vgi {

class VgiBuffer;
class VgiMetadata;
class VgiSchema;
class VgiBatch;
using VgiBufferPtr = std::shared_ptr<const VgiBuffer>;
using VgiMetadataPtr = std::shared_ptr<const VgiMetadata>;
using VgiSchemaPtr = std::shared_ptr<const VgiSchema>;
using VgiBatchPtr = std::shared_ptr<const VgiBatch>;

// ============================================================================
// VgiBuffer
// ============================================================================

class VgiBuffer {
public:
	//! `data` stays valid while `owner` is alive.
	VgiBuffer(std::shared_ptr<const void> owner, const uint8_t *data, int64_t size)
	    : owner_(std::move(owner)), data_(data), size_(size) {
	}

	static VgiBufferPtr FromString(std::string bytes);
	static VgiBufferPtr FromVector(std::vector<uint8_t> bytes);
	static VgiBufferPtr Copy(const void *data, int64_t size);
	//! Borrow `data` without owning it; the caller keeps it alive.
	static VgiBufferPtr Wrap(const void *data, int64_t size);
	//! A new zeroed buffer of `size` bytes, written through `*out` before it is shared.
	static VgiBufferPtr Allocate(int64_t size, uint8_t **out);

	const uint8_t *data() const {
		return data_;
	}
	int64_t size() const {
		return size_;
	}
	std::string_view view() const {
		return std::string_view(reinterpret_cast<const char *>(data_), static_cast<size_t>(size_));
	}
	std::string ToString() const {
		return std::string(view());
	}
	std::vector<uint8_t> ToVector() const {
		return std::vector<uint8_t>(data_, data_ + size_);
	}
	bool Equals(const VgiBuffer &other) const {
		return view() == other.view();
	}
	//! [offset, offset + length) sharing this buffer's owner.
	VgiBufferPtr Slice(int64_t offset, int64_t length) const;
	const std::shared_ptr<const void> &owner() const {
		return owner_;
	}

private:
	std::shared_ptr<const void> owner_;
	const uint8_t *data_;
	int64_t size_;
};

// ============================================================================
// VgiMetadata
// ============================================================================

class VgiMetadata {
public:
	VgiMetadata() = default;
	explicit VgiMetadata(std::vector<std::pair<std::string, std::string>> pairs) : pairs_(std::move(pairs)) {
	}
	VgiMetadata(std::initializer_list<std::pair<std::string, std::string>> pairs) : pairs_(pairs) {
	}
	static VgiMetadataPtr Make(std::vector<std::pair<std::string, std::string>> pairs) {
		return std::make_shared<const VgiMetadata>(std::move(pairs));
	}
	//! Decode the C data interface's binary metadata encoding (NULL -> empty).
	static VgiMetadata FromC(const char *metadata);
	//! The C data interface binary encoding ("" for none).
	std::string ToC() const;

	int64_t size() const {
		return static_cast<int64_t>(pairs_.size());
	}
	bool empty() const {
		return pairs_.empty();
	}
	const std::string &key(int64_t i) const {
		return pairs_[static_cast<size_t>(i)].first;
	}
	const std::string &value(int64_t i) const {
		return pairs_[static_cast<size_t>(i)].second;
	}
	//! Index of the first `key`, or -1.
	int64_t FindKey(std::string_view key) const;
	bool Contains(std::string_view key) const {
		return FindKey(key) >= 0;
	}
	std::optional<std::string> Get(std::string_view key) const;
	//! Replace the first `key` or append it.
	void Set(const std::string &key, std::string value);
	void Append(std::string key, std::string value) {
		pairs_.emplace_back(std::move(key), std::move(value));
	}
	const std::vector<std::pair<std::string, std::string>> &pairs() const {
		return pairs_;
	}
	bool operator==(const VgiMetadata &other) const {
		return pairs_ == other.pairs_;
	}

private:
	std::vector<std::pair<std::string, std::string>> pairs_;
};

// ============================================================================
// Type descriptions
// ============================================================================

//! The parts of a (field) type the extension inspects: nanoarrow's ArrowType for the
//! logical type (dictionary columns report NANOARROW_TYPE_DICTIONARY here and the
//! index type in `storage`), plus the parameters a few types carry.
struct VgiTypeInfo {
	int type = 0;    //!< enum ArrowType
	int storage = 0; //!< enum ArrowType of the physical storage (the index type for dictionaries)
	int time_unit = 0; //!< enum ArrowTimeUnit (timestamp/time/duration)
	std::string timezone;
	int32_t decimal_precision = 0;
	int32_t decimal_scale = 0;
	int32_t fixed_size = 0; //!< fixed_size_binary width / fixed_size_list size
	bool is_dictionary = false;
	std::string extension_name; //!< ARROW:extension:name, "" for none
};

//! Describe an ArrowSchema node. Throws InvalidInputException on an unparseable format.
VgiTypeInfo DescribeType(const ArrowSchema &schema);
//! Human-readable type, close to Arrow C++'s DataType::ToString ("int64", "list<item: string>", ...).
std::string TypeToString(const ArrowSchema &schema);
//! Format, flags, children and dictionary equal (names compared iff `check_names`).
bool TypesEqual(const ArrowSchema &a, const ArrowSchema &b, bool check_names = true, bool check_metadata = false);

// ============================================================================
// VgiSchema
// ============================================================================

class VgiSchema : public std::enable_shared_from_this<VgiSchema> {
public:
	//! Takes ownership of a released-on-destruction struct schema (moved out of `schema`).
	static VgiSchemaPtr Adopt(ArrowSchema *schema);
	//! Deep copy of `schema`.
	static VgiSchemaPtr Copy(const ArrowSchema &schema);
	//! A struct schema with no fields.
	static VgiSchemaPtr Empty();

	~VgiSchema();
	VgiSchema(const VgiSchema &) = delete;
	VgiSchema &operator=(const VgiSchema &) = delete;

	const ArrowSchema &c_schema() const {
		return schema_;
	}
	int num_fields() const {
		return static_cast<int>(schema_.n_children);
	}
	const ArrowSchema &field(int i) const {
		return *schema_.children[i];
	}
	std::string field_name(int i) const;
	const std::vector<std::string> &field_names() const {
		return names_;
	}
	bool field_nullable(int i) const;
	//! Index of the first field called `name`, or -1.
	int GetFieldIndex(std::string_view name) const;
	//! Schema-level metadata.
	const VgiMetadata &metadata() const {
		return metadata_;
	}
	//! Metadata of field `i`.
	VgiMetadata field_metadata(int i) const;

	std::string ToString() const;
	bool Equals(const VgiSchema &other, bool check_metadata = false) const;

	//! This schema with different schema-level metadata.
	VgiSchemaPtr WithMetadata(const VgiMetadata &metadata) const;
	//! The schema of fields `indices`, in that order.
	VgiSchemaPtr Select(const std::vector<int> &indices) const;
	//! This schema with field `i` removed / with `field` (deep-copied) appended or set at `i`.
	VgiSchemaPtr RemoveField(int i) const;
	VgiSchemaPtr AddField(int i, const ArrowSchema &field) const;
	VgiSchemaPtr SetField(int i, const ArrowSchema &field) const;

	//! Deep-copy into `out` (caller releases).
	void ExportTo(ArrowSchema *out) const;
	void ExportTo(ArrowSchemaWrapper &out) const;

private:
	VgiSchema();
	ArrowSchema schema_;
	std::vector<std::string> names_;
	VgiMetadata metadata_;
};

// ============================================================================
// VgiColumn
// ============================================================================

//! A borrowed read view of one array (a batch column or a nested child). Valid while the
//! batch it came from is alive. Row arguments are logical (the view's offset applied).
class VgiColumn {
public:
	VgiColumn() = default;
	//! `shift`: rows to skip before this view's own offset (a struct child inherits its
	//! parent's offset, which nanoarrow views do not apply to children).
	VgiColumn(const ArrowArrayView *view, const ArrowSchema *schema, int64_t shift = 0)
	    : view_(view), schema_(schema), shift_(shift) {
	}
	explicit operator bool() const {
		return view_ != nullptr;
	}

	const ArrowArrayView &view() const {
		return *view_;
	}
	const ArrowSchema &schema() const {
		return *schema_;
	}
	VgiTypeInfo type() const {
		return DescribeType(*schema_);
	}
	//! enum ArrowType of the physical storage (the index type for dictionary columns).
	int storage_type() const;
	bool is_dictionary() const;
	std::string type_string() const {
		return TypeToString(*schema_);
	}
	std::string name() const;

	int64_t length() const;
	int64_t offset() const;
	bool IsNull(int64_t i) const;
	int64_t null_count() const;

	int64_t GetInt(int64_t i) const;
	uint64_t GetUInt(int64_t i) const;
	double GetDouble(int64_t i) const;
	bool GetBool(int64_t i) const {
		return GetInt(i) != 0;
	}
	//! utf8 / large_utf8 / utf8_view / binary / large_binary / binary_view bytes.
	std::string_view GetView(int64_t i) const;
	std::string GetString(int64_t i) const {
		return std::string(GetView(i));
	}

	int64_t num_children() const;
	VgiColumn child(int64_t i) const;
	//! The dictionary values of a dictionary column.
	VgiColumn dictionary() const;
	//! [begin, end) of list/map element `i` in child(0). Throws unless a (large) list or map.
	void ListRange(int64_t i, int64_t &begin, int64_t &end) const;

	int64_t shift() const {
		return shift_;
	}

private:
	const ArrowArrayView *view_ = nullptr;
	const ArrowSchema *schema_ = nullptr;
	int64_t shift_ = 0;
};

// ============================================================================
// VgiBatch
// ============================================================================

//! How much to check when a batch is built from foreign buffers (worker data).
enum class VgiValidation : uint8_t {
	NONE,       //!< trust the producer (our own encoders)
	MINIMAL,    //!< O(1) structural checks
	DEFAULT,    //!< + buffer sizes vs. offsets (O(columns))
	FULL,       //!< + every offset, union id and dictionary index (O(rows))
};

class VgiBatch : public std::enable_shared_from_this<VgiBatch> {
public:
	//! Takes ownership of a struct `array` (moved out) matching `schema`.
	static VgiBatchPtr Adopt(VgiSchemaPtr schema, ArrowArray *array, VgiMetadataPtr metadata = nullptr,
	                         VgiValidation validation = VgiValidation::NONE);
	//! A batch of `columns` (each moved out) with `schema`; every column has `num_rows` rows.
	static VgiBatchPtr FromColumns(VgiSchemaPtr schema, int64_t num_rows, std::vector<ArrowArray> columns,
	                               VgiMetadataPtr metadata = nullptr);
	//! A zero-row batch of `schema`.
	static VgiBatchPtr MakeEmpty(VgiSchemaPtr schema, VgiMetadataPtr metadata = nullptr);

	~VgiBatch();
	VgiBatch(const VgiBatch &) = delete;
	VgiBatch &operator=(const VgiBatch &) = delete;

	const VgiSchemaPtr &schema() const {
		return schema_;
	}
	int64_t num_rows() const {
		return num_rows_;
	}
	int num_columns() const {
		return schema_->num_fields();
	}
	VgiColumn column(int i) const;
	//! The first column called `name`; an empty VgiColumn when there is none.
	VgiColumn GetColumnByName(std::string_view name) const;
	const std::string &column_name(int i) const {
		return schema_->field_names()[static_cast<size_t>(i)];
	}

	//! The message's custom_metadata (never null; empty when none).
	const VgiMetadataPtr &metadata() const {
		return metadata_;
	}
	VgiBatchPtr WithMetadata(VgiMetadataPtr metadata) const;
	//! The same rows under `schema` (same physical layout; only names/flags/metadata differ).
	VgiBatchPtr WithSchema(VgiSchemaPtr schema) const;

	//! Rows [offset, offset + length), zero-copy.
	VgiBatchPtr Slice(int64_t offset, int64_t length) const;
	//! Columns `indices`, zero-copy.
	VgiBatchPtr SelectColumns(const std::vector<int> &indices) const;

	//! A struct ArrowArray for consumers (DuckDB's ArrowToDuckDB) holding a reference to
	//! this batch: every child has a release callback and the parent's length.
	void ExportTo(ArrowArray *out) const;
	void ExportTo(ArrowArrayWrapper &out) const;

	//! Bytes referenced by this batch's buffers (shared buffers counted once per use).
	int64_t TotalBufferSize() const;
	//! The underlying struct array (top-level offset always 0).
	const ArrowArray &c_array() const;
	const ArrowArrayView &c_view() const;

	//! Set when the IPC reader found this batch's buffers inconsistent with its offsets and
	//! lengths (checked at decode time, where the real body sizes are known; the C data
	//! interface carries none). ValidateWorkerBatch rejects such a batch at `structural`
	//! and `full`; "" when the batch decoded cleanly or was not decoded from IPC.
	const std::string &structural_error() const {
		return structural_error_;
	}

private:
	friend class VgiIpcReader;
	std::string structural_error_;
	struct Holder;
	VgiBatch();
	std::shared_ptr<Holder> holder_;
	VgiSchemaPtr schema_;
	VgiMetadataPtr metadata_;
	int64_t num_rows_ = 0;
	std::unique_ptr<ArrowArrayView> view_;
};

// ============================================================================
// Building schemas
// ============================================================================

//! A (named) type description, built with the helpers in vgi::type and turned into an
//! ArrowSchema by VgiSchema::Make / BuildSchema.
struct VgiField {
	std::string name;
	std::string format; //!< C data interface format string
	bool nullable = true;
	std::vector<VgiField> children;
	std::shared_ptr<VgiField> dictionary; //!< value type when this is dictionary-encoded (format = index)
	VgiMetadata metadata;
	bool map_keys_sorted = false;

	VgiField Named(std::string field_name, bool is_nullable = true) const {
		VgiField out = *this;
		out.name = std::move(field_name);
		out.nullable = is_nullable;
		return out;
	}
	VgiField WithMetadata(VgiMetadata field_metadata) const {
		VgiField out = *this;
		out.metadata = std::move(field_metadata);
		return out;
	}
};

namespace type {
VgiField Null();
VgiField Bool();
VgiField Int8();
VgiField Int16();
VgiField Int32();
VgiField Int64();
VgiField UInt8();
VgiField UInt16();
VgiField UInt32();
VgiField UInt64();
VgiField Float32();
VgiField Float64();
VgiField Utf8();
VgiField LargeUtf8();
VgiField Binary();
VgiField LargeBinary();
//! unit: 's', 'm', 'u' or 'n'; timezone "" for none.
VgiField Timestamp(char unit, const std::string &timezone = "");
VgiField Date32();
VgiField List(VgiField item, bool item_nullable = true);
VgiField LargeList(VgiField item, bool item_nullable = true);
//! map<key, value> with the canonical entries/key/value names; keys non-null.
VgiField Map(VgiField key, VgiField value, bool value_nullable = true);
VgiField Struct(std::vector<VgiField> fields);
//! dictionary(index, value); `index` is an integer type.
VgiField Dictionary(VgiField index, VgiField value);
} // namespace type

//! Build an ArrowSchema (caller releases) from a description.
void BuildSchema(const VgiField &field, ArrowSchema *out);
//! A struct schema of `fields` with schema-level `metadata`.
VgiSchemaPtr MakeSchema(const std::vector<VgiField> &fields, const VgiMetadata &metadata = VgiMetadata());

// ============================================================================
// Building arrays
// ============================================================================

//! Typed appends to a nanoarrow array that is being built (ArrowArrayStartAppending
//! has been called). Every helper throws InternalException on failure (allocation).
namespace append {
void Null(ArrowArray *array, int64_t n = 1);
void Int(ArrowArray *array, int64_t value);
void UInt(ArrowArray *array, uint64_t value);
void Double(ArrowArray *array, double value);
void Bool(ArrowArray *array, bool value);
void Bytes(ArrowArray *array, const void *data, int64_t size);
inline void String(ArrowArray *array, std::string_view value) {
	Bytes(array, value.data(), static_cast<int64_t>(value.size()));
}
//! Finish one struct/list/map element whose children were appended.
void FinishElement(ArrowArray *array);
//! Append `n` empty elements (an empty list / a struct of the children appended so far).
void Empty(ArrowArray *array, int64_t n = 1);
} // namespace append

//! Builds a batch row by row (or column by column) for `schema`.
class VgiBatchBuilder {
public:
	explicit VgiBatchBuilder(VgiSchemaPtr schema);
	~VgiBatchBuilder();
	VgiBatchBuilder(const VgiBatchBuilder &) = delete;
	VgiBatchBuilder &operator=(const VgiBatchBuilder &) = delete;

	//! The array being built for column `i`; append to it with vgi::append.
	ArrowArray *column(int i);
	int num_columns() const;
	//! Record that every column received one more row.
	void FinishRow();
	//! Set the row count directly (column-wise building).
	void SetRows(int64_t rows);
	VgiBatchPtr Finish(VgiMetadataPtr metadata = nullptr);

private:
	VgiSchemaPtr schema_;
	std::unique_ptr<ArrowArray> array_;
	int64_t rows_ = 0;
};

// ============================================================================
// Row selection
// ============================================================================

//! Rows `rows[0..n)` of `batch` (any order, repeats allowed; no bounds check beyond an
//! assertion-level check), as a new batch with the same schema and metadata.
VgiBatchPtr TakeRows(const VgiBatchPtr &batch, const int64_t *rows, int64_t n);
inline VgiBatchPtr TakeRows(const VgiBatchPtr &batch, const std::vector<int64_t> &rows) {
	return TakeRows(batch, rows.data(), static_cast<int64_t>(rows.size()));
}
//! The rows of `batches` in order (all with equal schemas; the first one's schema and
//! metadata are kept). A single batch is returned as is. Dictionary columns whose
//! dictionaries differ are merged (indices remapped).
VgiBatchPtr ConcatBatches(const std::vector<VgiBatchPtr> &batches);
//! Append element `row` of `src` to `dst` (built from a schema equal to src's).
void AppendElement(ArrowArray *dst, const VgiColumn &src, int64_t row);

// ============================================================================
// Validation / errors
// ============================================================================

//! Throws InternalException unless nanoarrow was built with atomic shared-buffer
//! refcounts (decoded batches are released on DuckDB worker threads). Called at LOAD.
void CheckArrowRuntime();

//! Check `batch` at `level`; throws InvalidInputException with nanoarrow's message.
void ValidateBatch(const VgiBatch &batch, VgiValidation level);

} // namespace vgi
} // namespace duckdb
