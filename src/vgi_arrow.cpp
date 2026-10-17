// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_arrow.hpp"

#include <atomic>
#include <cstring>
#include <unordered_map>

#include "duckdb.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/exception.hpp"

#include "nanoarrow/nanoarrow.h"

namespace duckdb {
namespace vgi {

namespace {

[[noreturn]] void ThrowNano(ArrowErrorCode code, const ArrowError *error, const char *what) {
	const char *detail = error && error->message[0] ? error->message : std::strerror(code);
	throw InvalidInputException("%s: %s", what, detail);
}

inline void Check(ArrowErrorCode code, const char *what) {
	if (code != NANOARROW_OK) {
		throw InternalException("vgi arrow: %s failed (%s)", what, std::strerror(code));
	}
}

inline void Check(ArrowErrorCode code, const ArrowError &error, const char *what) {
	if (code != NANOARROW_OK) {
		ThrowNano(code, &error, what);
	}
}

} // namespace

// ============================================================================
// VgiBuffer
// ============================================================================

VgiBufferPtr VgiBuffer::FromString(std::string bytes) {
	auto owner = std::make_shared<std::string>(std::move(bytes));
	auto *data = reinterpret_cast<const uint8_t *>(owner->data());
	auto size = static_cast<int64_t>(owner->size());
	return std::make_shared<const VgiBuffer>(std::move(owner), data, size);
}

VgiBufferPtr VgiBuffer::FromVector(std::vector<uint8_t> bytes) {
	auto owner = std::make_shared<std::vector<uint8_t>>(std::move(bytes));
	auto *data = owner->data();
	auto size = static_cast<int64_t>(owner->size());
	return std::make_shared<const VgiBuffer>(std::move(owner), data, size);
}

VgiBufferPtr VgiBuffer::Copy(const void *data, int64_t size) {
	return FromString(std::string(static_cast<const char *>(data), static_cast<size_t>(size)));
}

VgiBufferPtr VgiBuffer::Wrap(const void *data, int64_t size) {
	return std::make_shared<const VgiBuffer>(nullptr, static_cast<const uint8_t *>(data), size);
}

VgiBufferPtr VgiBuffer::Allocate(int64_t size, uint8_t **out) {
	// Uninitialized: callers overwrite every byte (reads off the wire).
	std::shared_ptr<uint8_t> owner(new uint8_t[static_cast<size_t>(size > 0 ? size : 1)],
	                               std::default_delete<uint8_t[]>());
	*out = owner.get();
	auto *data = owner.get();
	return std::make_shared<const VgiBuffer>(std::move(owner), data, size);
}

VgiBufferPtr VgiBuffer::Slice(int64_t offset, int64_t length) const {
	if (offset < 0 || length < 0 || offset + length > size_) {
		throw InternalException("VgiBuffer::Slice(%lld, %lld) out of range (size %lld)", (long long)offset,
		                        (long long)length, (long long)size_);
	}
	return std::make_shared<const VgiBuffer>(owner_, data_ + offset, length);
}

// ============================================================================
// VgiMetadata
// ============================================================================

VgiMetadata VgiMetadata::FromC(const char *metadata) {
	VgiMetadata out;
	if (!metadata) {
		return out;
	}
	ArrowMetadataReader reader;
	Check(ArrowMetadataReaderInit(&reader, metadata), "metadata read");
	ArrowStringView key;
	ArrowStringView value;
	while (reader.remaining_keys > 0) {
		Check(ArrowMetadataReaderRead(&reader, &key, &value), "metadata read");
		out.pairs_.emplace_back(std::string(key.data, static_cast<size_t>(key.size_bytes)),
		                        std::string(value.data, static_cast<size_t>(value.size_bytes)));
	}
	return out;
}

std::string VgiMetadata::ToC() const {
	if (pairs_.empty()) {
		return std::string();
	}
	ArrowBuffer buffer;
	Check(ArrowMetadataBuilderInit(&buffer, nullptr), "metadata build");
	for (const auto &kv : pairs_) {
		ArrowStringView k {kv.first.data(), static_cast<int64_t>(kv.first.size())};
		ArrowStringView v {kv.second.data(), static_cast<int64_t>(kv.second.size())};
		ArrowErrorCode code = ArrowMetadataBuilderAppend(&buffer, k, v);
		if (code != NANOARROW_OK) {
			ArrowBufferReset(&buffer);
			Check(code, "metadata build");
		}
	}
	std::string out(reinterpret_cast<const char *>(buffer.data), static_cast<size_t>(buffer.size_bytes));
	ArrowBufferReset(&buffer);
	return out;
}

int64_t VgiMetadata::FindKey(std::string_view key) const {
	for (size_t i = 0; i < pairs_.size(); i++) {
		if (pairs_[i].first == key) {
			return static_cast<int64_t>(i);
		}
	}
	return -1;
}

std::optional<std::string> VgiMetadata::Get(std::string_view key) const {
	auto i = FindKey(key);
	if (i < 0) {
		return std::nullopt;
	}
	return pairs_[static_cast<size_t>(i)].second;
}

void VgiMetadata::Set(const std::string &key, std::string value) {
	auto i = FindKey(key);
	if (i < 0) {
		pairs_.emplace_back(key, std::move(value));
	} else {
		pairs_[static_cast<size_t>(i)].second = std::move(value);
	}
}

// ============================================================================
// Type descriptions
// ============================================================================

VgiTypeInfo DescribeType(const ArrowSchema &schema) {
	ArrowSchemaView view;
	std::memset(&view, 0, sizeof(view));
	ArrowError error;
	ArrowErrorInit(&error);
	Check(ArrowSchemaViewInit(&view, &schema, &error), error, "VGI: unsupported Arrow type");
	VgiTypeInfo out;
	out.type = view.type;
	out.storage = view.storage_type;
	out.time_unit = view.time_unit;
	out.timezone = view.timezone ? view.timezone : "";
	out.decimal_precision = view.decimal_precision;
	out.decimal_scale = view.decimal_scale;
	out.fixed_size = view.fixed_size;
	out.is_dictionary = schema.dictionary != nullptr;
	if (view.extension_name.data) {
		out.extension_name.assign(view.extension_name.data, static_cast<size_t>(view.extension_name.size_bytes));
	}
	return out;
}

namespace {

const char *UnitName(ArrowTimeUnit unit) {
	switch (unit) {
	case NANOARROW_TIME_UNIT_SECOND:
		return "s";
	case NANOARROW_TIME_UNIT_MILLI:
		return "ms";
	case NANOARROW_TIME_UNIT_MICRO:
		return "us";
	case NANOARROW_TIME_UNIT_NANO:
		return "ns";
	}
	return "?";
}

void AppendTypeString(const ArrowSchema &schema, std::string &out) {
	ArrowSchemaView view;
	std::memset(&view, 0, sizeof(view));
	ArrowError error;
	ArrowErrorInit(&error);
	if (ArrowSchemaViewInit(&view, &schema, &error) != NANOARROW_OK) {
		out += "<invalid:";
		out += schema.format ? schema.format : "null";
		out += ">";
		return;
	}
	auto child = [&](int64_t i, bool with_name) {
		const ArrowSchema &c = *schema.children[i];
		if (with_name) {
			out += c.name ? c.name : "";
			out += ": ";
		}
		AppendTypeString(c, out);
		if (!(c.flags & ARROW_FLAG_NULLABLE)) {
			out += " not null";
		}
	};
	switch (view.type) {
	case NANOARROW_TYPE_DICTIONARY:
		out += "dictionary<values=";
		AppendTypeString(*schema.dictionary, out);
		out += ", indices=";
		out += ArrowTypeString(view.storage_type);
		out += ">";
		return;
	case NANOARROW_TYPE_TIMESTAMP:
		out += "timestamp[";
		out += UnitName(view.time_unit);
		if (view.timezone && view.timezone[0]) {
			out += ", tz=";
			out += view.timezone;
		}
		out += "]";
		return;
	case NANOARROW_TYPE_TIME32:
	case NANOARROW_TYPE_TIME64:
	case NANOARROW_TYPE_DURATION:
		out += ArrowTypeString(view.type);
		out += "[";
		out += UnitName(view.time_unit);
		out += "]";
		return;
	case NANOARROW_TYPE_DECIMAL128:
	case NANOARROW_TYPE_DECIMAL256:
		out += "decimal(" + std::to_string(view.decimal_precision) + ", " + std::to_string(view.decimal_scale) + ")";
		return;
	case NANOARROW_TYPE_FIXED_SIZE_BINARY:
		out += "fixed_size_binary[" + std::to_string(view.fixed_size) + "]";
		return;
	case NANOARROW_TYPE_LIST:
	case NANOARROW_TYPE_LARGE_LIST:
		out += view.type == NANOARROW_TYPE_LIST ? "list<" : "large_list<";
		child(0, true);
		out += ">";
		return;
	case NANOARROW_TYPE_FIXED_SIZE_LIST:
		out += "fixed_size_list<";
		child(0, true);
		out += ">[" + std::to_string(view.fixed_size) + "]";
		return;
	case NANOARROW_TYPE_MAP: {
		const ArrowSchema &entries = *schema.children[0];
		out += "map<";
		AppendTypeString(*entries.children[0], out);
		out += ", ";
		AppendTypeString(*entries.children[1], out);
		out += ">";
		return;
	}
	case NANOARROW_TYPE_STRUCT:
		out += "struct<";
		for (int64_t i = 0; i < schema.n_children; i++) {
			if (i) {
				out += ", ";
			}
			child(i, true);
		}
		out += ">";
		return;
	default:
		break;
	}
	switch (view.type) {
	case NANOARROW_TYPE_STRING:
		out += "string";
		return;
	case NANOARROW_TYPE_LARGE_STRING:
		out += "large_string";
		return;
	case NANOARROW_TYPE_BOOL:
		out += "bool";
		return;
	default:
		out += ArrowTypeString(view.type);
		return;
	}
}

} // namespace

std::string TypeToString(const ArrowSchema &schema) {
	std::string out;
	AppendTypeString(schema, out);
	return out;
}

namespace {
bool StrEq(const char *a, const char *b) {
	if (!a || !b) {
		return (a ? *a : 0) == (b ? *b : 0);
	}
	return std::strcmp(a, b) == 0;
}

bool MetadataEq(const char *a, const char *b) {
	return VgiMetadata::FromC(a) == VgiMetadata::FromC(b);
}
} // namespace

bool TypesEqual(const ArrowSchema &a, const ArrowSchema &b, bool check_names, bool check_metadata) {
	if (!StrEq(a.format, b.format) || a.n_children != b.n_children) {
		return false;
	}
	if ((a.flags & (ARROW_FLAG_NULLABLE | ARROW_FLAG_DICTIONARY_ORDERED | ARROW_FLAG_MAP_KEYS_SORTED)) !=
	    (b.flags & (ARROW_FLAG_NULLABLE | ARROW_FLAG_DICTIONARY_ORDERED | ARROW_FLAG_MAP_KEYS_SORTED))) {
		return false;
	}
	if (check_names && !StrEq(a.name, b.name)) {
		return false;
	}
	if (check_metadata && !MetadataEq(a.metadata, b.metadata)) {
		return false;
	}
	for (int64_t i = 0; i < a.n_children; i++) {
		if (!TypesEqual(*a.children[i], *b.children[i], check_names, check_metadata)) {
			return false;
		}
	}
	if ((a.dictionary == nullptr) != (b.dictionary == nullptr)) {
		return false;
	}
	return !a.dictionary || TypesEqual(*a.dictionary, *b.dictionary, check_names, check_metadata);
}

// ============================================================================
// VgiSchema
// ============================================================================

VgiSchema::VgiSchema() {
	schema_.release = nullptr;
}

VgiSchema::~VgiSchema() {
	if (schema_.release) {
		schema_.release(&schema_);
	}
}

VgiSchemaPtr VgiSchema::Adopt(ArrowSchema *schema) {
	if (!schema || !schema->release) {
		throw InternalException("VgiSchema::Adopt: released schema");
	}
	if (!StrEq(schema->format, "+s")) {
		schema->release(schema);
		throw InvalidInputException("VGI: expected a struct schema for a record batch");
	}
	std::shared_ptr<VgiSchema> out(new VgiSchema());
	out->schema_ = *schema;
	schema->release = nullptr;
	out->names_.reserve(static_cast<size_t>(out->schema_.n_children));
	for (int64_t i = 0; i < out->schema_.n_children; i++) {
		const char *name = out->schema_.children[i]->name;
		out->names_.emplace_back(name ? name : "");
	}
	out->metadata_ = VgiMetadata::FromC(out->schema_.metadata);
	return out;
}

VgiSchemaPtr VgiSchema::Copy(const ArrowSchema &schema) {
	ArrowSchema copy;
	Check(ArrowSchemaDeepCopy(&schema, &copy), "schema copy");
	return Adopt(&copy);
}

VgiSchemaPtr VgiSchema::Empty() {
	static const VgiSchemaPtr empty = MakeSchema({});
	return empty;
}

std::string VgiSchema::field_name(int i) const {
	return names_[static_cast<size_t>(i)];
}

bool VgiSchema::field_nullable(int i) const {
	return (field(i).flags & ARROW_FLAG_NULLABLE) != 0;
}

int VgiSchema::GetFieldIndex(std::string_view name) const {
	for (size_t i = 0; i < names_.size(); i++) {
		if (names_[i] == name) {
			return static_cast<int>(i);
		}
	}
	return -1;
}

VgiMetadata VgiSchema::field_metadata(int i) const {
	return VgiMetadata::FromC(field(i).metadata);
}

std::string VgiSchema::ToString() const {
	std::string out;
	for (int i = 0; i < num_fields(); i++) {
		if (i) {
			out += "\n";
		}
		out += names_[static_cast<size_t>(i)];
		out += ": ";
		AppendTypeString(field(i), out);
		if (!field_nullable(i)) {
			out += " not null";
		}
	}
	return out;
}

bool VgiSchema::Equals(const VgiSchema &other, bool check_metadata) const {
	return TypesEqual(schema_, other.schema_, /*check_names=*/true, check_metadata);
}

void VgiSchema::ExportTo(ArrowSchema *out) const {
	Check(ArrowSchemaDeepCopy(&schema_, out), "schema export");
}

void VgiSchema::ExportTo(ArrowSchemaWrapper &out) const {
	if (out.arrow_schema.release) {
		out.arrow_schema.release(&out.arrow_schema);
	}
	ExportTo(&out.arrow_schema);
}

namespace {

// A struct schema whose children are deep copies of `fields` (in order), with `metadata`.
VgiSchemaPtr StructOf(const std::vector<const ArrowSchema *> &fields, const std::string &metadata) {
	ArrowSchema out;
	ArrowSchemaInit(&out);
	ArrowErrorCode code = ArrowSchemaSetTypeStruct(&out, static_cast<int64_t>(fields.size()));
	if (code == NANOARROW_OK && !metadata.empty()) {
		code = ArrowSchemaSetMetadata(&out, metadata.data());
	}
	for (size_t i = 0; code == NANOARROW_OK && i < fields.size(); i++) {
		// ArrowSchemaSetTypeStruct initialized the child; replace it with the copy.
		out.children[i]->release(out.children[i]);
		code = ArrowSchemaDeepCopy(fields[i], out.children[i]);
	}
	if (code != NANOARROW_OK) {
		out.release(&out);
		Check(code, "schema build");
	}
	return VgiSchema::Adopt(&out);
}

std::string MetadataBytes(const char *metadata) {
	if (!metadata) {
		return std::string();
	}
	ArrowMetadataReader reader;
	if (ArrowMetadataReaderInit(&reader, metadata) != NANOARROW_OK) {
		return std::string();
	}
	// The binary encoding: int32 count, then (int32 len, bytes) pairs.
	int64_t size = 4;
	ArrowStringView key;
	ArrowStringView value;
	while (reader.remaining_keys > 0) {
		ArrowMetadataReaderRead(&reader, &key, &value);
		size += 8 + key.size_bytes + value.size_bytes;
	}
	return std::string(metadata, static_cast<size_t>(size));
}

} // namespace

VgiSchemaPtr VgiSchema::WithMetadata(const VgiMetadata &metadata) const {
	std::vector<const ArrowSchema *> fields;
	for (int i = 0; i < num_fields(); i++) {
		fields.push_back(&field(i));
	}
	auto bytes = metadata.ToC();
	return StructOf(fields, bytes);
}

VgiSchemaPtr VgiSchema::Select(const std::vector<int> &indices) const {
	std::vector<const ArrowSchema *> fields;
	for (int i : indices) {
		fields.push_back(&field(i));
	}
	return StructOf(fields, MetadataBytes(schema_.metadata));
}

VgiSchemaPtr VgiSchema::RemoveField(int i) const {
	std::vector<const ArrowSchema *> fields;
	for (int k = 0; k < num_fields(); k++) {
		if (k != i) {
			fields.push_back(&field(k));
		}
	}
	return StructOf(fields, MetadataBytes(schema_.metadata));
}

VgiSchemaPtr VgiSchema::AddField(int i, const ArrowSchema &new_field) const {
	std::vector<const ArrowSchema *> fields;
	for (int k = 0; k < num_fields(); k++) {
		if (k == i) {
			fields.push_back(&new_field);
		}
		fields.push_back(&field(k));
	}
	if (i >= num_fields()) {
		fields.push_back(&new_field);
	}
	return StructOf(fields, MetadataBytes(schema_.metadata));
}

VgiSchemaPtr VgiSchema::SetField(int i, const ArrowSchema &new_field) const {
	std::vector<const ArrowSchema *> fields;
	for (int k = 0; k < num_fields(); k++) {
		fields.push_back(k == i ? &new_field : &field(k));
	}
	return StructOf(fields, MetadataBytes(schema_.metadata));
}

// ============================================================================
// VgiColumn
// ============================================================================

int VgiColumn::storage_type() const {
	return view_->storage_type;
}

bool VgiColumn::is_dictionary() const {
	return view_->dictionary != nullptr;
}

std::string VgiColumn::name() const {
	return schema_->name ? schema_->name : "";
}

int64_t VgiColumn::length() const {
	return view_->length;
}

int64_t VgiColumn::offset() const {
	return view_->offset + shift_;
}

bool VgiColumn::IsNull(int64_t i) const {
	return ArrowArrayViewIsNull(view_, i + shift_);
}

int64_t VgiColumn::null_count() const {
	if (shift_ == 0) {
		return ArrowArrayViewComputeNullCount(view_);
	}
	int64_t n = 0;
	for (int64_t i = 0; i < view_->length; i++) {
		n += IsNull(i) ? 1 : 0;
	}
	return n;
}

int64_t VgiColumn::GetInt(int64_t i) const {
	return ArrowArrayViewGetIntUnsafe(view_, i + shift_);
}

uint64_t VgiColumn::GetUInt(int64_t i) const {
	return ArrowArrayViewGetUIntUnsafe(view_, i + shift_);
}

double VgiColumn::GetDouble(int64_t i) const {
	return ArrowArrayViewGetDoubleUnsafe(view_, i + shift_);
}

std::string_view VgiColumn::GetView(int64_t i) const {
	switch (view_->storage_type) {
	case NANOARROW_TYPE_STRING:
	case NANOARROW_TYPE_LARGE_STRING:
	case NANOARROW_TYPE_STRING_VIEW: {
		auto s = ArrowArrayViewGetStringUnsafe(view_, i + shift_);
		return std::string_view(s.data, static_cast<size_t>(s.size_bytes));
	}
	case NANOARROW_TYPE_BINARY:
	case NANOARROW_TYPE_LARGE_BINARY:
	case NANOARROW_TYPE_BINARY_VIEW:
	case NANOARROW_TYPE_FIXED_SIZE_BINARY: {
		auto b = ArrowArrayViewGetBytesUnsafe(view_, i + shift_);
		return std::string_view(reinterpret_cast<const char *>(b.data.as_uint8), static_cast<size_t>(b.size_bytes));
	}
	default:
		throw InternalException("VgiColumn::GetView on a %s column", TypeToString(*schema_));
	}
}

int64_t VgiColumn::num_children() const {
	return view_->n_children;
}

VgiColumn VgiColumn::child(int64_t i) const {
	int64_t child_shift = view_->storage_type == NANOARROW_TYPE_STRUCT ? shift_ + view_->offset : 0;
	return VgiColumn(view_->children[i], schema_->children[i], child_shift);
}

VgiColumn VgiColumn::dictionary() const {
	if (!view_->dictionary) {
		throw InternalException("VgiColumn::dictionary on a non-dictionary column");
	}
	return VgiColumn(view_->dictionary, schema_->dictionary, 0);
}

void VgiColumn::ListRange(int64_t i, int64_t &begin, int64_t &end) const {
	int64_t k = view_->offset + shift_ + i;
	switch (view_->storage_type) {
	case NANOARROW_TYPE_LIST:
	case NANOARROW_TYPE_MAP:
		begin = view_->buffer_views[1].data.as_int32[k];
		end = view_->buffer_views[1].data.as_int32[k + 1];
		return;
	case NANOARROW_TYPE_LARGE_LIST:
		begin = view_->buffer_views[1].data.as_int64[k];
		end = view_->buffer_views[1].data.as_int64[k + 1];
		return;
	case NANOARROW_TYPE_FIXED_SIZE_LIST: {
		int64_t size = view_->layout.child_size_elements;
		begin = k * size;
		end = begin + size;
		return;
	}
	default:
		throw InternalException("VgiColumn::ListRange on a %s column", TypeToString(*schema_));
	}
}

// ============================================================================
// VgiBatch
// ============================================================================

//! Owns the struct ArrowArray a batch (and its slices) read from.
struct VgiBatch::Holder {
	ArrowArray array;
	//! For a slice: the shallow child copies `array` points at, and the batch they came from.
	std::vector<ArrowArray> children;
	std::vector<ArrowArray *> child_ptrs;
	const void *buffers[1] = {nullptr};
	std::shared_ptr<Holder> base;

	Holder() {
		std::memset(&array, 0, sizeof(array));
	}
	~Holder() {
		if (!base && array.release) {
			array.release(&array);
		}
	}
};

VgiBatch::VgiBatch() = default;

VgiBatch::~VgiBatch() {
	if (view_) {
		ArrowArrayViewReset(view_.get());
	}
}


VgiBatchPtr VgiBatch::Adopt(VgiSchemaPtr schema, ArrowArray *array, VgiMetadataPtr metadata,
                            VgiValidation validation) {
	if (!array || !array->release) {
		throw InternalException("VgiBatch::Adopt: released array");
	}
	auto holder = std::make_shared<Holder>();
	holder->array = *array;
	array->release = nullptr;

	std::shared_ptr<VgiBatch> out(new VgiBatch());
	out->holder_ = std::move(holder);
	out->schema_ = std::move(schema);
	out->metadata_ = metadata ? std::move(metadata) : std::make_shared<const VgiMetadata>();
	const ArrowArray &a = out->holder_->array;
	if (a.n_children != out->schema_->num_fields()) {
		throw InvalidInputException("VGI: record batch has %lld columns but its schema has %d", (long long)a.n_children,
		                            out->schema_->num_fields());
	}
	if (a.offset != 0) {
		throw InvalidInputException("VGI: record batch with a non-zero top-level offset");
	}
	out->num_rows_ = a.length;

	out->view_ = make_uniq<ArrowArrayView>();
	ArrowError error;
	ArrowErrorInit(&error);
	Check(ArrowArrayViewInitFromSchema(out->view_.get(), &out->schema_->c_schema(), &error), error,
	      "VGI: record batch schema");
	if (validation == VgiValidation::NONE) {
		Check(ArrowArrayViewSetArrayMinimal(out->view_.get(), &a, &error), error, "VGI: malformed record batch");
	} else {
		Check(ArrowArrayViewSetArray(out->view_.get(), &a, &error), error, "VGI: malformed record batch");
		if (validation == VgiValidation::FULL) {
			Check(ArrowArrayViewValidate(out->view_.get(), NANOARROW_VALIDATION_LEVEL_FULL, &error), error,
			      "VGI: malformed record batch");
		}
	}
	return out;
}

namespace {
void ReleaseColumnsParent(ArrowArray *array) {
	for (int64_t i = 0; i < array->n_children; i++) {
		if (array->children[i]->release) {
			array->children[i]->release(array->children[i]);
		}
		delete array->children[i];
	}
	delete[] array->children;
	delete[] array->buffers;
	array->release = nullptr;
}
} // namespace

VgiBatchPtr VgiBatch::FromColumns(VgiSchemaPtr schema, int64_t num_rows, std::vector<ArrowArray> columns,
                                  VgiMetadataPtr metadata) {
	ArrowArray parent;
	std::memset(&parent, 0, sizeof(parent));
	parent.length = num_rows;
	parent.null_count = 0;
	parent.n_buffers = 1;
	parent.buffers = new const void *[1] {nullptr};
	parent.n_children = static_cast<int64_t>(columns.size());
	parent.children = new ArrowArray *[columns.size()];
	for (size_t i = 0; i < columns.size(); i++) {
		parent.children[i] = new ArrowArray(columns[i]);
		columns[i].release = nullptr;
	}
	parent.release = &ReleaseColumnsParent;
	return Adopt(std::move(schema), &parent, std::move(metadata));
}

VgiBatchPtr VgiBatch::MakeEmpty(VgiSchemaPtr schema, VgiMetadataPtr metadata) {
	VgiBatchBuilder builder(std::move(schema));
	return builder.Finish(std::move(metadata));
}

VgiColumn VgiBatch::column(int i) const {
	return VgiColumn(view_->children[i], &schema_->field(i), 0);
}

VgiColumn VgiBatch::GetColumnByName(std::string_view name) const {
	int i = schema_->GetFieldIndex(name);
	return i < 0 ? VgiColumn() : column(i);
}

VgiBatchPtr VgiBatch::WithMetadata(VgiMetadataPtr metadata) const {
	auto out = std::const_pointer_cast<VgiBatch>(Slice(0, num_rows_));
	out->metadata_ = metadata ? std::move(metadata) : std::make_shared<const VgiMetadata>();
	return out;
}

VgiBatchPtr VgiBatch::WithSchema(VgiSchemaPtr schema) const {
	if (schema->num_fields() != num_columns()) {
		throw InternalException("VgiBatch::WithSchema: %d fields for %d columns", schema->num_fields(), num_columns());
	}
	auto out = std::const_pointer_cast<VgiBatch>(Slice(0, num_rows_));
	// Same physical layout: rebuild the view against the new schema.
	ArrowArrayViewReset(out->view_.get());
	out->schema_ = std::move(schema);
	ArrowError error;
	ArrowErrorInit(&error);
	Check(ArrowArrayViewInitFromSchema(out->view_.get(), &out->schema_->c_schema(), &error), error,
	      "VGI: record batch schema");
	Check(ArrowArrayViewSetArrayMinimal(out->view_.get(), &out->holder_->array, &error), error,
	      "VGI: record batch does not match its schema");
	return out;
}

VgiBatchPtr VgiBatch::Slice(int64_t offset, int64_t length) const {
	if (offset < 0 || length < 0 || offset + length > num_rows_) {
		throw InternalException("VgiBatch::Slice(%lld, %lld) out of range (%lld rows)", (long long)offset,
		                        (long long)length, (long long)num_rows_);
	}
	auto holder = std::make_shared<Holder>();
	holder->base = holder_->base ? holder_->base : holder_;
	const ArrowArray &src = holder_->array;
	holder->children.resize(static_cast<size_t>(src.n_children));
	holder->child_ptrs.resize(static_cast<size_t>(src.n_children));
	for (int64_t i = 0; i < src.n_children; i++) {
		ArrowArray child = *src.children[i];
		child.offset += offset;
		child.length = length;
		child.null_count = (child.null_count == 0) ? 0 : -1;
		child.release = nullptr; // owned by the base holder
		holder->children[static_cast<size_t>(i)] = child;
		holder->child_ptrs[static_cast<size_t>(i)] = &holder->children[static_cast<size_t>(i)];
	}
	holder->array = src;
	holder->array.length = length;
	holder->array.offset = 0;
	holder->array.null_count = 0;
	holder->array.n_buffers = 1;
	holder->array.buffers = holder->buffers;
	holder->array.children = holder->child_ptrs.data();
	holder->array.dictionary = nullptr;
	holder->array.release = nullptr;

	std::shared_ptr<VgiBatch> out(new VgiBatch());
	out->holder_ = std::move(holder);
	out->schema_ = schema_;
	out->metadata_ = metadata_;
	out->num_rows_ = length;
	out->view_ = make_uniq<ArrowArrayView>();
	ArrowError error;
	ArrowErrorInit(&error);
	Check(ArrowArrayViewInitFromSchema(out->view_.get(), &schema_->c_schema(), &error), error,
	      "VGI: record batch schema");
	Check(ArrowArrayViewSetArrayMinimal(out->view_.get(), &out->holder_->array, &error), error,
	      "VGI: record batch slice");
	return out;
}

VgiBatchPtr VgiBatch::SelectColumns(const std::vector<int> &indices) const {
	auto schema = schema_->Select(indices);
	auto holder = std::make_shared<Holder>();
	holder->base = holder_->base ? holder_->base : holder_;
	const ArrowArray &src = holder_->array;
	holder->children.resize(indices.size());
	holder->child_ptrs.resize(indices.size());
	for (size_t k = 0; k < indices.size(); k++) {
		ArrowArray child = *src.children[indices[k]];
		child.release = nullptr;
		holder->children[k] = child;
		holder->child_ptrs[k] = &holder->children[k];
	}
	holder->array = src;
	holder->array.n_children = static_cast<int64_t>(indices.size());
	holder->array.n_buffers = 1;
	holder->array.buffers = holder->buffers;
	holder->array.children = holder->child_ptrs.data();
	holder->array.dictionary = nullptr;
	holder->array.release = nullptr;

	std::shared_ptr<VgiBatch> out(new VgiBatch());
	out->holder_ = std::move(holder);
	out->schema_ = std::move(schema);
	out->metadata_ = metadata_;
	out->num_rows_ = num_rows_;
	out->view_ = make_uniq<ArrowArrayView>();
	ArrowError error;
	ArrowErrorInit(&error);
	Check(ArrowArrayViewInitFromSchema(out->view_.get(), &out->schema_->c_schema(), &error), error,
	      "VGI: record batch schema");
	Check(ArrowArrayViewSetArrayMinimal(out->view_.get(), &out->holder_->array, &error), error,
	      "VGI: record batch columns");
	return out;
}

namespace {

//! private_data of an exported array: a reference to the batch's buffers.
struct ExportedArray {
	std::shared_ptr<const void> keep;
	std::vector<ArrowArray> children;
	std::vector<ArrowArray *> child_ptrs;
	const void *buffers[1] = {nullptr};
};

void ReleaseExportedChild(ArrowArray *array) {
	delete static_cast<std::shared_ptr<const void> *>(array->private_data);
	array->release = nullptr;
}

void ReleaseExported(ArrowArray *array) {
	auto *priv = static_cast<ExportedArray *>(array->private_data);
	for (auto &child : priv->children) {
		if (child.release) {
			child.release(&child);
		}
	}
	delete priv;
	array->release = nullptr;
}

} // namespace

void VgiBatch::ExportTo(ArrowArray *out) const {
	auto self = shared_from_this();
	auto *priv = new ExportedArray();
	priv->keep = self;
	const ArrowArray &src = holder_->array;
	priv->children.resize(static_cast<size_t>(src.n_children));
	priv->child_ptrs.resize(static_cast<size_t>(src.n_children));
	for (int64_t i = 0; i < src.n_children; i++) {
		ArrowArray child = *src.children[i];
		child.private_data = new std::shared_ptr<const void>(self);
		child.release = &ReleaseExportedChild;
		priv->children[static_cast<size_t>(i)] = child;
		priv->child_ptrs[static_cast<size_t>(i)] = &priv->children[static_cast<size_t>(i)];
	}
	std::memset(out, 0, sizeof(*out));
	out->length = num_rows_;
	out->null_count = 0;
	out->offset = 0;
	out->n_buffers = 1;
	out->buffers = priv->buffers;
	out->n_children = src.n_children;
	out->children = priv->child_ptrs.data();
	out->dictionary = nullptr;
	out->private_data = priv;
	out->release = &ReleaseExported;
}

void VgiBatch::ExportTo(ArrowArrayWrapper &out) const {
	if (out.arrow_array.release) {
		out.arrow_array.release(&out.arrow_array);
	}
	ExportTo(&out.arrow_array);
}

namespace {
int64_t ViewBytes(const ArrowArrayView *view) {
	int64_t total = 0;
	for (int i = 0; i < NANOARROW_MAX_FIXED_BUFFERS; i++) {
		if (view->buffer_views[i].size_bytes > 0) {
			total += view->buffer_views[i].size_bytes;
		}
	}
	for (int32_t i = 0; i < view->n_variadic_buffers; i++) {
		total += view->variadic_buffer_sizes[i];
	}
	for (int64_t i = 0; i < view->n_children; i++) {
		total += ViewBytes(view->children[i]);
	}
	if (view->dictionary) {
		total += ViewBytes(view->dictionary);
	}
	return total;
}
} // namespace

int64_t VgiBatch::TotalBufferSize() const {
	// SetArrayMinimal leaves variable-size data buffers' sizes unknown (-1); compute them.
	ArrowArrayView view;
	ArrowError error;
	ArrowErrorInit(&error);
	if (ArrowArrayViewInitFromSchema(&view, &schema_->c_schema(), &error) != NANOARROW_OK) {
		return 0;
	}
	int64_t total = 0;
	if (ArrowArrayViewSetArray(&view, &holder_->array, &error) == NANOARROW_OK) {
		total = ViewBytes(&view);
	}
	ArrowArrayViewReset(&view);
	return total;
}

const ArrowArray &VgiBatch::c_array() const {
	return holder_->array;
}

const ArrowArrayView &VgiBatch::c_view() const {
	return *view_;
}

// ============================================================================
// Row selection
// ============================================================================

namespace {

//! Where each source dictionary's values start in the destination's dictionary (a
//! source's dictionary is appended whole, so its indices shift by that offset).
using DictOffsets = std::unordered_map<const ArrowArrayView *, int64_t>;

//! Append rows[0..n) of `src` to `dst`; the column types are equal.
void AppendRows(ArrowArray *dst, const VgiColumn &src, const int64_t *rows, int64_t n, const DictOffsets &offsets);

void AppendOne(ArrowArray *dst, const VgiColumn &src, int64_t row, const DictOffsets &offsets) {
	AppendRows(dst, src, &row, 1, offsets);
}

void AppendRows(ArrowArray *dst, const VgiColumn &src, const int64_t *rows, int64_t n, const DictOffsets &offsets) {
	const ArrowArrayView &view = src.view();
	if (src.is_dictionary()) {
		auto it = offsets.find(&view);
		const int64_t dict_offset = it == offsets.end() ? 0 : it->second;
		for (int64_t k = 0; k < n; k++) {
			if (src.IsNull(rows[k])) {
				append::Null(dst);
			} else {
				append::Int(dst, src.GetInt(rows[k]) + dict_offset);
			}
		}
		return;
	}
	switch (view.storage_type) {
	case NANOARROW_TYPE_NA:
		append::Null(dst, n);
		return;
	case NANOARROW_TYPE_BOOL:
	case NANOARROW_TYPE_INT8:
	case NANOARROW_TYPE_INT16:
	case NANOARROW_TYPE_INT32:
	case NANOARROW_TYPE_INT64:
		for (int64_t k = 0; k < n; k++) {
			if (src.IsNull(rows[k])) {
				append::Null(dst);
			} else {
				append::Int(dst, src.GetInt(rows[k]));
			}
		}
		return;
	case NANOARROW_TYPE_UINT8:
	case NANOARROW_TYPE_UINT16:
	case NANOARROW_TYPE_UINT32:
	case NANOARROW_TYPE_UINT64:
		for (int64_t k = 0; k < n; k++) {
			if (src.IsNull(rows[k])) {
				append::Null(dst);
			} else {
				append::UInt(dst, src.GetUInt(rows[k]));
			}
		}
		return;
	case NANOARROW_TYPE_HALF_FLOAT:
	case NANOARROW_TYPE_FLOAT:
	case NANOARROW_TYPE_DOUBLE:
		for (int64_t k = 0; k < n; k++) {
			if (src.IsNull(rows[k])) {
				append::Null(dst);
			} else {
				append::Double(dst, src.GetDouble(rows[k]));
			}
		}
		return;
	case NANOARROW_TYPE_STRING:
	case NANOARROW_TYPE_LARGE_STRING:
	case NANOARROW_TYPE_STRING_VIEW:
	case NANOARROW_TYPE_BINARY:
	case NANOARROW_TYPE_LARGE_BINARY:
	case NANOARROW_TYPE_BINARY_VIEW:
	case NANOARROW_TYPE_FIXED_SIZE_BINARY:
		for (int64_t k = 0; k < n; k++) {
			if (src.IsNull(rows[k])) {
				append::Null(dst);
			} else {
				auto v = src.GetView(rows[k]);
				append::Bytes(dst, v.data(), static_cast<int64_t>(v.size()));
			}
		}
		return;
	case NANOARROW_TYPE_STRUCT:
		for (int64_t k = 0; k < n; k++) {
			if (src.IsNull(rows[k])) {
				append::Null(dst);
				continue;
			}
			for (int64_t c = 0; c < src.num_children(); c++) {
				AppendOne(dst->children[c], src.child(c), rows[k], offsets);
			}
			append::FinishElement(dst);
		}
		return;
	case NANOARROW_TYPE_LIST:
	case NANOARROW_TYPE_LARGE_LIST:
	case NANOARROW_TYPE_MAP:
	case NANOARROW_TYPE_FIXED_SIZE_LIST: {
		auto child = src.child(0);
		std::vector<int64_t> items;
		for (int64_t k = 0; k < n; k++) {
			if (src.IsNull(rows[k])) {
				append::Null(dst);
				continue;
			}
			int64_t begin = 0;
			int64_t end = 0;
			src.ListRange(rows[k], begin, end);
			items.clear();
			for (int64_t i = begin; i < end; i++) {
				items.push_back(i);
			}
			AppendRows(dst->children[0], child, items.data(), static_cast<int64_t>(items.size()), offsets);
			append::FinishElement(dst);
		}
		return;
	}
	case NANOARROW_TYPE_DECIMAL32:
	case NANOARROW_TYPE_DECIMAL64:
	case NANOARROW_TYPE_DECIMAL128:
	case NANOARROW_TYPE_DECIMAL256: {
		auto info = DescribeType(src.schema());
		int bits = view.storage_type == NANOARROW_TYPE_DECIMAL32    ? 32
		           : view.storage_type == NANOARROW_TYPE_DECIMAL64  ? 64
		           : view.storage_type == NANOARROW_TYPE_DECIMAL128 ? 128
		                                                            : 256;
		ArrowDecimal dec;
		ArrowDecimalInit(&dec, bits, info.decimal_precision, info.decimal_scale);
		for (int64_t k = 0; k < n; k++) {
			if (src.IsNull(rows[k])) {
				append::Null(dst);
				continue;
			}
			ArrowArrayViewGetDecimalUnsafe(&view, rows[k] + src.shift(), &dec);
			if (ArrowArrayAppendDecimal(dst, &dec) != NANOARROW_OK) {
				throw InternalException("vgi arrow: append decimal failed");
			}
		}
		return;
	}
	case NANOARROW_TYPE_INTERVAL_MONTHS:
	case NANOARROW_TYPE_INTERVAL_DAY_TIME:
	case NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO: {
		ArrowInterval interval;
		ArrowIntervalInit(&interval, view.storage_type);
		for (int64_t k = 0; k < n; k++) {
			if (src.IsNull(rows[k])) {
				append::Null(dst);
				continue;
			}
			ArrowArrayViewGetIntervalUnsafe(&view, rows[k] + src.shift(), &interval);
			if (ArrowArrayAppendInterval(dst, &interval) != NANOARROW_OK) {
				throw InternalException("vgi arrow: append interval failed");
			}
		}
		return;
	}
	default:
		throw NotImplementedException("VGI: row selection on Arrow type %s is not supported",
		                              TypeToString(src.schema()));
	}
}

//! Append every dictionary of `src` (at any depth) to the matching dictionary of `dst`,
//! recording where each one starts. Dictionaries of dictionaries are not supported.
void PrepareDictionaries(ArrowArray *dst, const VgiColumn &src, DictOffsets &offsets) {
	if (src.is_dictionary()) {
		auto dict = src.dictionary();
		if (dict.is_dictionary() || dict.num_children() > 0) {
			throw NotImplementedException("VGI: row selection on a dictionary of nested values (%s)",
			                              TypeToString(src.schema()));
		}
		offsets[&src.view()] = dst->dictionary->length;
		std::vector<int64_t> all(static_cast<size_t>(dict.length()));
		for (int64_t i = 0; i < dict.length(); i++) {
			all[static_cast<size_t>(i)] = i;
		}
		static const DictOffsets none;
		AppendRows(dst->dictionary, dict, all.data(), dict.length(), none);
	}
	for (int64_t c = 0; c < src.num_children(); c++) {
		PrepareDictionaries(dst->children[c], src.child(c), offsets);
	}
}

} // namespace

void AppendElement(ArrowArray *dst, const VgiColumn &src, int64_t row) {
	DictOffsets offsets;
	PrepareDictionaries(dst, src, offsets);
	AppendOne(dst, src, row, offsets);
}

VgiBatchPtr TakeRows(const VgiBatchPtr &batch, const int64_t *rows, int64_t n) {
	VgiBatchBuilder builder(batch->schema());
	for (int c = 0; c < batch->num_columns(); c++) {
		auto col = batch->column(c);
		DictOffsets offsets;
		PrepareDictionaries(builder.column(c), col, offsets);
		AppendRows(builder.column(c), col, rows, n, offsets);
	}
	builder.SetRows(n);
	return builder.Finish(batch->metadata());
}

VgiBatchPtr ConcatBatches(const std::vector<VgiBatchPtr> &batches) {
	if (batches.empty()) {
		throw InternalException("ConcatBatches of no batches");
	}
	if (batches.size() == 1) {
		return batches[0];
	}
	const auto &schema = batches[0]->schema();
	int64_t total = 0;
	for (const auto &b : batches) {
		if (b->num_columns() != schema->num_fields()) {
			throw InvalidInputException("ConcatBatches: batches have different column counts");
		}
		total += b->num_rows();
	}
	VgiBatchBuilder builder(schema);
	std::vector<int64_t> rows;
	for (int c = 0; c < schema->num_fields(); c++) {
		for (const auto &b : batches) {
			auto col = b->column(c);
			// Merge dictionaries: append this batch's and shift its indices past the
			// values appended so far.
			DictOffsets offsets;
			PrepareDictionaries(builder.column(c), col, offsets);
			rows.resize(static_cast<size_t>(b->num_rows()));
			for (int64_t r = 0; r < b->num_rows(); r++) {
				rows[static_cast<size_t>(r)] = r;
			}
			AppendRows(builder.column(c), col, rows.data(), b->num_rows(), offsets);
		}
	}
	builder.SetRows(total);
	return builder.Finish(batches[0]->metadata());
}

void CheckArrowRuntime() {
	if (!ArrowSharedBufferIsThreadSafe()) {
		throw InternalException("VGI was built with a nanoarrow whose shared buffers are not thread-safe "
		                        "(C11 atomics unavailable); decoded batches would be released unsafely");
	}
}

void ValidateBatch(const VgiBatch &batch, VgiValidation level) {
	if (level == VgiValidation::NONE) {
		return;
	}
	ArrowArrayView view;
	ArrowError error;
	ArrowErrorInit(&error);
	Check(ArrowArrayViewInitFromSchema(&view, &batch.schema()->c_schema(), &error), error, "VGI: batch schema");
	ArrowErrorCode code = ArrowArrayViewSetArray(&view, &batch.c_array(), &error);
	if (code == NANOARROW_OK && level == VgiValidation::FULL) {
		code = ArrowArrayViewValidate(&view, NANOARROW_VALIDATION_LEVEL_FULL, &error);
	}
	ArrowArrayViewReset(&view);
	if (code != NANOARROW_OK) {
		ThrowNano(code, &error, "VGI: malformed worker batch");
	}
}

// ============================================================================
// Building schemas
// ============================================================================

namespace type {
namespace {
VgiField Fmt(const char *format) {
	VgiField f;
	f.format = format;
	return f;
}
} // namespace
VgiField Null() {
	return Fmt("n");
}
VgiField Bool() {
	return Fmt("b");
}
VgiField Int8() {
	return Fmt("c");
}
VgiField Int16() {
	return Fmt("s");
}
VgiField Int32() {
	return Fmt("i");
}
VgiField Int64() {
	return Fmt("l");
}
VgiField UInt8() {
	return Fmt("C");
}
VgiField UInt16() {
	return Fmt("S");
}
VgiField UInt32() {
	return Fmt("I");
}
VgiField UInt64() {
	return Fmt("L");
}
VgiField Float32() {
	return Fmt("f");
}
VgiField Float64() {
	return Fmt("g");
}
VgiField Utf8() {
	return Fmt("u");
}
VgiField LargeUtf8() {
	return Fmt("U");
}
VgiField Binary() {
	return Fmt("z");
}
VgiField LargeBinary() {
	return Fmt("Z");
}
VgiField Timestamp(char unit, const std::string &timezone) {
	VgiField f;
	f.format = std::string("ts") + unit + ":" + timezone;
	return f;
}
VgiField Date32() {
	return Fmt("tdD");
}
VgiField List(VgiField item, bool item_nullable) {
	VgiField f = Fmt("+l");
	f.children.push_back(item.Named("item", item_nullable));
	return f;
}
VgiField LargeList(VgiField item, bool item_nullable) {
	VgiField f = Fmt("+L");
	f.children.push_back(item.Named("item", item_nullable));
	return f;
}
VgiField Map(VgiField key, VgiField value, bool value_nullable) {
	VgiField entries = Fmt("+s");
	entries.children.push_back(key.Named("key", false));
	entries.children.push_back(value.Named("value", value_nullable));
	VgiField f = Fmt("+m");
	f.children.push_back(entries.Named("entries", false));
	return f;
}
VgiField Struct(std::vector<VgiField> fields) {
	VgiField f = Fmt("+s");
	f.children = std::move(fields);
	return f;
}
VgiField Dictionary(VgiField index, VgiField value) {
	VgiField f = index;
	f.dictionary = std::make_shared<VgiField>(std::move(value));
	return f;
}
} // namespace type

namespace {
ArrowErrorCode BuildInto(const VgiField &field, ArrowSchema *out) {
	ArrowSchemaInit(out);
	NANOARROW_RETURN_NOT_OK(ArrowSchemaSetFormat(out, field.format.c_str()));
	NANOARROW_RETURN_NOT_OK(ArrowSchemaSetName(out, field.name.c_str()));
	out->flags = (field.nullable ? ARROW_FLAG_NULLABLE : 0) | (field.map_keys_sorted ? ARROW_FLAG_MAP_KEYS_SORTED : 0);
	if (!field.metadata.empty()) {
		auto bytes = field.metadata.ToC();
		NANOARROW_RETURN_NOT_OK(ArrowSchemaSetMetadata(out, bytes.data()));
	}
	NANOARROW_RETURN_NOT_OK(ArrowSchemaAllocateChildren(out, static_cast<int64_t>(field.children.size())));
	for (size_t i = 0; i < field.children.size(); i++) {
		NANOARROW_RETURN_NOT_OK(BuildInto(field.children[i], out->children[i]));
	}
	if (field.dictionary) {
		NANOARROW_RETURN_NOT_OK(ArrowSchemaAllocateDictionary(out));
		NANOARROW_RETURN_NOT_OK(BuildInto(*field.dictionary, out->dictionary));
	}
	return NANOARROW_OK;
}
} // namespace

void BuildSchema(const VgiField &field, ArrowSchema *out) {
	ArrowErrorCode code = BuildInto(field, out);
	if (code != NANOARROW_OK) {
		if (out->release) {
			out->release(out);
		}
		Check(code, "schema build");
	}
}

VgiSchemaPtr MakeSchema(const std::vector<VgiField> &fields, const VgiMetadata &metadata) {
	VgiField root = type::Struct(fields);
	root.nullable = false;
	root.metadata = metadata;
	ArrowSchema schema;
	BuildSchema(root, &schema);
	return VgiSchema::Adopt(&schema);
}

// ============================================================================
// Building arrays
// ============================================================================

namespace append {
void Null(ArrowArray *array, int64_t n) {
	Check(ArrowArrayAppendNull(array, n), "append null");
}
void Int(ArrowArray *array, int64_t value) {
	Check(ArrowArrayAppendInt(array, value), "append int");
}
void UInt(ArrowArray *array, uint64_t value) {
	Check(ArrowArrayAppendUInt(array, value), "append uint");
}
void Double(ArrowArray *array, double value) {
	Check(ArrowArrayAppendDouble(array, value), "append double");
}
void Bool(ArrowArray *array, bool value) {
	Check(ArrowArrayAppendInt(array, value ? 1 : 0), "append bool");
}
void Bytes(ArrowArray *array, const void *data, int64_t size) {
	ArrowBufferView view;
	view.data.data = data;
	view.size_bytes = size;
	Check(ArrowArrayAppendBytes(array, view), "append bytes");
}
void FinishElement(ArrowArray *array) {
	Check(ArrowArrayFinishElement(array), "finish element");
}
void Empty(ArrowArray *array, int64_t n) {
	Check(ArrowArrayAppendEmpty(array, n), "append empty");
}
} // namespace append

VgiBatchBuilder::VgiBatchBuilder(VgiSchemaPtr schema) : schema_(std::move(schema)), array_(make_uniq<ArrowArray>()) {
	ArrowError error;
	ArrowErrorInit(&error);
	Check(ArrowArrayInitFromSchema(array_.get(), &schema_->c_schema(), &error), error, "VGI: batch builder");
	Check(ArrowArrayStartAppending(array_.get()), "start appending");
}

VgiBatchBuilder::~VgiBatchBuilder() {
	if (array_ && array_->release) {
		array_->release(array_.get());
	}
}

ArrowArray *VgiBatchBuilder::column(int i) {
	return array_->children[i];
}

int VgiBatchBuilder::num_columns() const {
	return static_cast<int>(array_->n_children);
}

void VgiBatchBuilder::FinishRow() {
	rows_++;
}

void VgiBatchBuilder::SetRows(int64_t rows) {
	rows_ = rows;
}

VgiBatchPtr VgiBatchBuilder::Finish(VgiMetadataPtr metadata) {
	array_->length = rows_;
	for (int64_t i = 0; i < array_->n_children; i++) {
		if (array_->children[i]->length != rows_) {
			throw InternalException("VgiBatchBuilder: column %lld has %lld rows, expected %lld", (long long)i,
			                        (long long)array_->children[i]->length, (long long)rows_);
		}
	}
	ArrowError error;
	ArrowErrorInit(&error);
	Check(ArrowArrayFinishBuildingDefault(array_.get(), &error), error, "VGI: batch builder");
	return VgiBatch::Adopt(schema_, array_.get(), std::move(metadata));
}

} // namespace vgi
} // namespace duckdb
