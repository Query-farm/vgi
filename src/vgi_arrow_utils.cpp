// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_arrow_utils.hpp"
#include "vgi_column_expression.hpp"
#include "vgi_column_metadata.hpp"
#include "vgi_protocol_constants.hpp"
#include "vgi_rpc_types.hpp"

#include "nanoarrow/nanoarrow.h"

#include <cstring>

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/column_list.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// Settings Extraction
// ============================================================================

std::map<std::string, Value> ExtractVgiSettings(ClientContext &context,
                                                const std::vector<std::string> &setting_names) {
	std::map<std::string, Value> settings;
	for (const auto &name : setting_names) {
		Value value;
		if (context.TryGetCurrentSetting(Identifier(name), value)) {
			settings[name] = value;
		}
	}
	return settings;
}

// ============================================================================
// Schema Conversions
// ============================================================================

static void RejectUnsupportedLogicalField(const ArrowSchema &field) {
	const auto metadata = VgiMetadata::FromC(field.metadata);
	for (const auto &item : metadata.pairs()) {
		const auto &key = item.first;
		if (key.compare(0, 19, "vgi.column.default.") == 0 ||
		    key.compare(0, 21, "vgi.column.generated.") == 0 ||
		    key.compare(0, 22, "vgi.column.generation.") == 0) {
			throw NotImplementedException(
			    "VGI column expressions require a qualified native adapter; field '%s' is unsupported",
			    field.name ? field.name : "");
		}
		if (key.compare(0, 17, "vgi.logical_type.") == 0 ||
		    (key == "ARROW:extension:name" && item.second == "arrow.parquet.variant")) {
			throw NotImplementedException(
			    "VGI logical type annotations require a qualified native adapter; field '%s' is unsupported",
			    field.name ? field.name : "");
		}
	}
	if (field.dictionary) {
		RejectUnsupportedLogicalField(*field.dictionary);
	}
	for (int64_t i = 0; i < field.n_children; ++i) {
		RejectUnsupportedLogicalField(*field.children[i]);
	}
}

void ExportSchema(const VgiSchemaPtr &schema, ArrowSchemaWrapper &out) {
	// Preserve protocol semantic guards on every Arrow-to-DuckDB conversion.
	for (int i = 0; i < schema->num_fields(); ++i) {
		RejectUnsupportedLogicalField(schema->field(i));
	}
	// ExportTo releases a previous export before replacing its ownership.
	schema->ExportTo(out);
}

void ExportRecordBatch(const VgiBatchPtr &batch, ArrowArrayWrapper &out) {
	batch->ExportTo(out);
}

// ============================================================================
// DuckDB Type Extraction
// ============================================================================

void GetDuckDBTypesFromArrowTable(const ArrowTableSchema &arrow_table, const ArrowSchema &c_schema,
                                  vector<LogicalType> &types, vector<string> &names) {
	types.clear();
	names.clear();

	auto &columns = arrow_table.GetColumns();
	for (int64_t i = 0; i < c_schema.n_children; i++) {
		auto &child = *c_schema.children[i];

		// Get column name (with fallback)
		string col_name = child.name ? child.name : "column" + to_string(i);
		names.push_back(col_name);

		// Get DuckDB type from ArrowTableSchema
		auto arrow_type = columns.at(i);
		types.push_back(arrow_type->GetDuckType(true));
	}
}

// ============================================================================
// High-Level Conversion Utilities
// ============================================================================

void ArrowSchemaToDuckDBTypes(ClientContext &context, const VgiSchemaPtr &schema, ArrowSchemaWrapper &c_schema_out,
                              ArrowTableSchema &arrow_table_out, vector<LogicalType> &types, vector<string> &names) {
	ExportSchema(schema, c_schema_out);
	ArrowTableFunction::PopulateArrowTableSchema(context, arrow_table_out, c_schema_out.arrow_schema);
	GetDuckDBTypesFromArrowTable(arrow_table_out, c_schema_out.arrow_schema, types, names);
}

void ArrowSchemaToDuckDBTypes(ClientContext &context, const VgiSchemaPtr &schema, ArrowSchemaWrapper &c_schema_out,
                             ArrowTableSchema &arrow_table_out, vector<LogicalType> &types, vector<Identifier> &names) {
	vector<string> raw_names;
	ArrowSchemaToDuckDBTypes(context, schema, c_schema_out, arrow_table_out, types, raw_names);
	names = StringsToIdentifiers(raw_names);
}

VgiSchemaPtr BuildArrowSchemaFromDuckDB(ClientContext &context, const vector<LogicalType> &types,
                                      const vector<Identifier> &names) {
	return BuildArrowSchemaFromDuckDB(context, types, IdentifiersToStrings(names));
}

void ArrowSchemaToColumnList(ClientContext &context, const VgiSchemaPtr &schema, ColumnList &columns) {
	ValidateNativeColumnMetadata(schema);
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
	vector<LogicalType> types;
	vector<string> names;

	ArrowSchemaToDuckDBTypes(context, schema, c_schema, arrow_table, types, names);
	for (idx_t i = 0; i < types.size(); i++) {
		ColumnDefinition column(Identifier(names[i]), types[i]);
		auto metadata = ReadNativeColumnMetadata(schema->field(static_cast<int>(i)));
		if (metadata.comment)
			column.SetComment(Value(*metadata.comment));
		// DuckDB's column tag map is case-insensitive; VGI tags preserve raw
		// keys, including distinct "A" and "a". Keep the authoritative tags
		// on the Arrow schema until a lossless native mapping is qualified.
		columns.AddColumn(std::move(column));
	}
}

VgiSchemaPtr DuckDBColumnsToArrowSchema(ClientContext &context, const ColumnList &columns,
                                       const std::string &owner_schema) {
	vector<LogicalType> types;
	vector<string> names;
	for (auto &col : columns.Logical()) {
		types.push_back(col.GetType());
		names.push_back(col.GetName().GetIdentifierName());
	}
	auto schema = BuildArrowSchemaFromDuckDB(context, types, names);
	ArrowSchemaWrapper result_schema;
	schema->ExportTo(result_schema);
	idx_t index = 0;
	for (auto &col : columns.Logical()) {
		if (col.CompressionType() != CompressionType::COMPRESSION_AUTO) {
			throw NotImplementedException("VGI does not support non-default column compression");
		}
		auto &field = *result_schema.arrow_schema.children[index++];
		auto metadata = VgiMetadata::FromC(field.metadata);
		if (!col.Comment().IsNull()) {
			if (col.Comment().type() != LogicalType::VARCHAR) {
				throw InvalidInputException("VGI column metadata: native column comment must be a string");
			}
			metadata.Append("comment", col.Comment().GetValue<std::string>());
		}
		if (col.Generated()) {
			auto expression = BuildNativeGeneratedExpression(col.GeneratedExpression().ToString(), owner_schema,
			                                                 NativeColumnEvaluationContext(context));
			metadata.Append(VGI_COLUMN_GENERATED, EncodeNativeColumnDefaultMetadata(expression));
			metadata.Append(VGI_COLUMN_GENERATION, "virtual");
		}
		if (col.HasDefaultValue()) {
			auto expression = BuildNativeColumnExpression(col.DefaultValue().ToString(), owner_schema,
			                                              NativeColumnEvaluationContext(context), false);
			metadata.Append(VGI_COLUMN_DEFAULT, EncodeNativeColumnDefaultMetadata(expression));
		}
		auto encoded_metadata = metadata.ToC();
		if (ArrowSchemaSetMetadata(&field, encoded_metadata.empty() ? nullptr : encoded_metadata.data())) {
			throw InternalException("VGI failed to copy column metadata");
		}
	}
	auto result = VgiSchema::Adopt(&result_schema.arrow_schema);
	ValidateNativeColumnMetadata(result);
	ColumnList validated;
	idx_t validated_index = 0;
	for (const auto &column : columns.Logical()) {
		auto expression = ReadNativeColumnGenerated(result->field(static_cast<int>(validated_index++)), owner_schema);
		if (expression) {
			validated.AddColumn(
			    ColumnDefinition(column.Name(), column.Type(), std::move(expression), TableColumnType::GENERATED));
		} else {
			validated.AddColumn(column.Copy());
		}
	}
	ValidateNativeGeneratedColumns(context, validated);
	return result;
}

// ============================================================================
// DuckDB DataChunk / Type to Arrow Conversion
// ============================================================================

namespace {
VgiBatchPtr AppendChunk(DataChunk &chunk, const vector<LogicalType> &types, const vector<string> &names,
                        ClientProperties &props,
                        const unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> &extension_types) {
	ArrowAppender appender(types, chunk.size(), props, extension_types);
	appender.Append(chunk, 0, chunk.size(), chunk.size());
	ArrowArray arr = appender.Finalize();
	ArrowSchema c_schema;
	ArrowConverter::ToArrowSchema(&c_schema, types, names, props);
	VgiSchemaPtr schema;
	try {
		schema = VgiSchema::Adopt(&c_schema);
	} catch (...) {
		if (arr.release) {
			arr.release(&arr);
		}
		throw;
	}
	return VgiBatch::Adopt(std::move(schema), &arr);
}
} // namespace

VgiBatchPtr DataChunkToArrow(ClientContext &context, DataChunk &chunk, const VgiSchemaPtr &schema) {
	if (chunk.size() == 0) {
		return VgiBatch::MakeEmpty(schema);
	}
	vector<LogicalType> types;
	vector<string> names;
	for (idx_t i = 0; i < chunk.ColumnCount(); i++) {
		types.push_back(chunk.data[i].GetType());
		names.push_back(schema->field_name(static_cast<int>(i)));
	}
	ClientProperties client_props = context.GetClientProperties();
	return AppendChunk(chunk, types, names, client_props, ArrowTypeExtensionData::GetExtensionTypes(context, types));
}

// Cached-input fast path: see header for contract.
VgiBatchPtr DataChunkToArrowCached(
    ClientContext &context, DataChunk &chunk, const VgiSchemaPtr &schema, const vector<LogicalType> &cached_types,
    const vector<string> &cached_names, const ClientProperties &cached_client_props,
    const unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> &cached_extension_types) {
	(void)context;
	if (chunk.size() == 0) {
		return VgiBatch::MakeEmpty(schema);
	}
	// ArrowConverter::ToArrowSchema takes its ClientProperties by non-const reference.
	ClientProperties props_copy = cached_client_props;
	return AppendChunk(chunk, cached_types, cached_names, props_copy, cached_extension_types);
}

VgiSchemaPtr BuildArrowSchemaFromDuckDB(ClientContext &context, const vector<LogicalType> &types,
                                        const vector<string> &names) {
	ArrowSchema c_schema;
	ClientProperties client_props = context.GetClientProperties();
	ArrowConverter::ToArrowSchema(&c_schema, types, names, client_props);
	return VgiSchema::Adopt(&c_schema);
}

// ============================================================================
// DuckDB Value Settings to a batch
// ============================================================================

VgiBatchPtr BuildSettingsBatch(ClientContext &context, const std::map<std::string, Value> &settings) {
	vector<LogicalType> field_types;
	vector<string> field_names;
	for (const auto &[name, value] : settings) {
		field_names.push_back(name);
		field_types.push_back(value.type());
	}

	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), field_types);
	chunk.SetCardinality(1);
	idx_t col_idx = 0;
	for (const auto &[name, value] : settings) {
		chunk.SetValue(col_idx++, 0, value);
	}

	auto schema = BuildArrowSchemaFromDuckDB(context, field_types, field_names);
	return DataChunkToArrow(context, chunk, schema);
}

// ============================================================================
// DuckDB Value Secrets to a batch
// ============================================================================

namespace {

void ReleaseStructOfChildren(ArrowArray *array) {
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

//! A struct ArrowArray of `length` rows whose children are moved out of `children`.
ArrowArray StructOfChildren(int64_t length, std::vector<ArrowArray> children) {
	ArrowArray out;
	std::memset(&out, 0, sizeof(out));
	out.length = length;
	out.n_buffers = 1;
	out.buffers = new const void *[1] {nullptr};
	out.n_children = static_cast<int64_t>(children.size());
	out.children = new ArrowArray *[children.size()];
	for (size_t i = 0; i < children.size(); i++) {
		out.children[i] = new ArrowArray(children[i]);
		children[i].release = nullptr;
	}
	out.release = &ReleaseStructOfChildren;
	return out;
}

//! Move a batch's columns out as a struct ArrowArray (the batch's buffers stay shared).
ArrowArray BatchAsStruct(const VgiBatchPtr &batch) {
	ArrowArray exported;
	batch->ExportTo(&exported);
	return exported; // a struct array: the batch's columns are its children
}

//! A one-row-per-batch schema: struct field `name` holding `schema`'s fields.
void StructFieldFromSchema(const VgiSchema &schema, const std::string &name, bool nullable, ArrowSchema *out) {
	schema.ExportTo(out);
	ArrowSchemaSetName(out, name.c_str());
	out->flags = nullable ? ARROW_FLAG_NULLABLE : 0;
}

} // namespace

std::vector<uint8_t> BuildSecretsBatch(ClientContext &context,
                                       const std::map<std::string, std::map<std::string, Value>> &secrets) {
	if (secrets.empty()) {
		return {};
	}

	ArrowSchema outer_schema;
	ArrowSchemaInit(&outer_schema);
	std::vector<ArrowSchema> fields;
	std::vector<ArrowArray> columns;

	for (const auto &[secret_type, kv_pairs] : secrets) {
		if (kv_pairs.empty()) {
			continue;
		}
		vector<LogicalType> field_types;
		vector<string> field_names;
		for (const auto &[key, value] : kv_pairs) {
			field_names.push_back(key);
			field_types.push_back(value.type());
		}
		DataChunk chunk;
		chunk.Initialize(Allocator::DefaultAllocator(), field_types);
		chunk.SetCardinality(1);
		idx_t col_idx = 0;
		for (const auto &[key, value] : kv_pairs) {
			chunk.SetValue(col_idx++, 0, value);
		}

		// One struct column per secret (preserves DuckDB types as Arrow types).
		auto inner_schema = BuildArrowSchemaFromDuckDB(context, field_types, field_names);
		auto inner_batch = DataChunkToArrow(context, chunk, inner_schema);
		ArrowSchema field;
		StructFieldFromSchema(*inner_batch->schema(), secret_type, true, &field);
		fields.push_back(field);
		columns.push_back(BatchAsStruct(inner_batch));
	}

	if (fields.empty()) {
		outer_schema.release(&outer_schema);
		return {};
	}

	ArrowSchemaSetTypeStruct(&outer_schema, static_cast<int64_t>(fields.size()));
	for (size_t i = 0; i < fields.size(); i++) {
		outer_schema.children[i]->release(outer_schema.children[i]);
		*outer_schema.children[i] = fields[i];
	}
	auto schema = VgiSchema::Adopt(&outer_schema);
	auto batch = VgiBatch::FromColumns(std::move(schema), 1, std::move(columns));
	return SerializeToIpcBytes(batch);
}

// ============================================================================
// Function Argument Schema Parsing
// ============================================================================

FunctionArgumentTypes ParseFunctionArgumentSchema(ClientContext &context, const VgiSchemaPtr &schema) {
	FunctionArgumentTypes result;

	if (!schema) {
		return result;
	}

	// Export schema to C ABI and get DuckDB types
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
	ExportSchema(schema, c_schema);
	ArrowTableFunction::PopulateArrowTableSchema(context, arrow_table, c_schema.arrow_schema);

	auto &columns = arrow_table.GetColumns();
	int num_fields = schema->num_fields();
	for (int field_idx = 0; field_idx < num_fields; field_idx++) {
		auto metadata = schema->field_metadata(field_idx);
		auto &child = c_schema.arrow_schema.children[field_idx];

		// Get column name
		string col_name = child->name ? child->name : "column" + to_string(field_idx);

		// Get DuckDB type
		auto arrow_type = columns.at(field_idx);
		LogicalType duckdb_type = arrow_type->GetDuckType();

		// Check metadata for special markers
		bool is_named = false;
		bool is_varargs = false;
		bool is_table_input = false;
		bool is_any_type = false;
		bool is_const = false;
		if (!metadata.empty()) {
			// Check for named argument marker
			auto named_idx = metadata.FindKey(VGI_ARG_METADATA_KEY);
			if (named_idx >= 0) {
				is_named = (metadata.value(named_idx) == VGI_ARG_NAMED_VALUE);
			}

			// Check for varargs marker (on field, not schema)
			auto varargs_idx = metadata.FindKey(VGI_VARARGS_METADATA_KEY);
			if (varargs_idx >= 0) {
				is_varargs = (metadata.value(varargs_idx) == VGI_VARARGS_TRUE_VALUE);
			}

			// Check for vgi_type metadata (table input or any type)
			auto type_idx = metadata.FindKey(VGI_TYPE_METADATA_KEY);
			if (type_idx >= 0) {
				auto &value = metadata.value(type_idx);
				is_table_input = (value == VGI_TYPE_TABLE_VALUE);
				is_any_type = (value == VGI_TYPE_ANY_VALUE);
			}

			// Check for const parameter marker
			auto const_idx = metadata.FindKey(VGI_CONST_METADATA_KEY);
			if (const_idx >= 0) {
				is_const = (metadata.value(const_idx) == VGI_CONST_TRUE_VALUE);
			}
		}

		// Override DuckDB type if this is an "any" type field
		if (is_any_type) {
			duckdb_type = LogicalType::ANY;
		}

		if (is_varargs) {
			// Varargs field - set flag and use declared type (or ANY for untyped/any)
			result.has_varargs = true;
			result.varargs_type = (is_any_type || duckdb_type == LogicalType::SQLNULL)
			                          ? LogicalType::ANY
			                          : duckdb_type;
			// Don't add varargs to positional_types - it's a sentinel
		} else if (is_table_input) {
			// Table input field - record position and name for table-in-out function registration
			// Store the current position (in terms of positional args only)
			result.table_input_position = static_cast<int>(result.positional_types.size());
			result.table_input_name = col_name;
			// Add TABLE type placeholder - this will be replaced with actual TABLE type
			// during function registration
			result.positional_types.push_back(LogicalType::TABLE);
			result.positional_names.push_back(col_name);
			result.positional_is_const.push_back(false); // Table inputs are never const
		} else if (is_named) {
			// Named argument - add to named_parameters map
			result.named_parameters[col_name] = duckdb_type;
		} else {
			// Positional argument
			result.positional_types.push_back(duckdb_type);
			result.positional_names.push_back(col_name);
			result.positional_is_const.push_back(is_const);
		}
	}

	return result;
}

// ============================================================================
// DuckDB Value to Arrow Conversion (using ArrowAppender)
// ============================================================================

ArrowArguments BuildArgumentsFromValues(ClientContext &context, const vector<Value> &positional_args,
                                        const vector<std::pair<string, Value>> &named_args) {
	// Build the struct type with named fields
	vector<LogicalType> field_types;
	vector<string> field_names;

	// Add positional arguments (prefixed with VGI_POSITIONAL_PREFIX per VGI protocol)
	for (idx_t i = 0; i < positional_args.size(); i++) {
		field_names.push_back(VGI_POSITIONAL_PREFIX + to_string(i));
		field_types.push_back(positional_args[i].type());
	}

	// Add named arguments (prefixed with VGI_NAMED_PREFIX per VGI protocol)
	for (auto &[name, value] : named_args) {
		field_names.push_back(VGI_NAMED_PREFIX + name);
		field_types.push_back(value.type());
	}

	// Handle empty arguments case - a null struct<> row: table-in-out functions that only
	// have a TABLE argument.
	if (field_types.empty()) {
		auto schema = MakeSchema({type::Struct({}).Named("args")});
		VgiBatchBuilder builder(schema);
		append::Null(builder.column(0));
		builder.FinishRow();
		return ArrowArguments {builder.Finish()};
	}

	// Create a single-row DataChunk with the values
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), field_types);
	chunk.SetCardinality(1);

	idx_t col_idx = 0;
	for (idx_t i = 0; i < positional_args.size(); i++) {
		chunk.SetValue(col_idx++, 0, positional_args[i]);
	}
	for (auto &[name, value] : named_args) {
		chunk.SetValue(col_idx++, 0, value);
	}

	// ArrowAppender gives a struct array of the fields; it becomes the "args" column.
	ClientProperties client_props = context.GetClientProperties();
	ArrowAppender appender(field_types, 1, client_props, ArrowTypeExtensionData::GetExtensionTypes(context, field_types));
	appender.Append(chunk, 0, 1, 1);
	std::vector<ArrowArray> columns;
	columns.push_back(appender.Finalize());

	// nanoarrow's ArrowSchemaSet* may only touch schemas nanoarrow allocated: rename a copy.
	ArrowSchema duck_schema;
	ArrowConverter::ToArrowSchema(&duck_schema, field_types, field_names, client_props);
	ArrowSchema args_field;
	ArrowErrorCode copied = ArrowSchemaDeepCopy(&duck_schema, &args_field);
	duck_schema.release(&duck_schema);
	if (copied != NANOARROW_OK) {
		throw InternalException("VGI: failed to copy the arguments schema");
	}
	ArrowSchemaSetName(&args_field, "args");
	args_field.flags = ARROW_FLAG_NULLABLE;
	ArrowSchema outer;
	ArrowSchemaInit(&outer);
	ArrowSchemaSetTypeStruct(&outer, 1);
	outer.children[0]->release(outer.children[0]);
	*outer.children[0] = args_field;
	return ArrowArguments {VgiBatch::FromColumns(VgiSchema::Adopt(&outer), 1, std::move(columns))};
}

// ============================================================================
// Schema Reconciliation
// ============================================================================

namespace {

ArrowSchemaView ViewOf(const ArrowSchema &schema) {
	ArrowSchemaView view;
	std::memset(&view, 0, sizeof(view));
	ArrowError error;
	ArrowErrorInit(&error);
	if (ArrowSchemaViewInit(&view, &schema, &error) != NANOARROW_OK) {
		throw IOException("VGI schema reconciliation: unsupported Arrow type '%s': %s",
		                  schema.format ? schema.format : "", error.message);
	}
	return view;
}

bool HasUnit(::ArrowType type) {
	return type == NANOARROW_TYPE_TIMESTAMP || type == NANOARROW_TYPE_TIME32 || type == NANOARROW_TYPE_TIME64 ||
	       type == NANOARROW_TYPE_DURATION;
}

// Whether `src`'s buffers can be read as `dst` unchanged: everything but names,
// nullability flags, metadata, a timestamp's timezone and a decimal's precision may differ.
bool SameLayout(const ArrowSchema &src, const ArrowSchema &dst) {
	if ((src.dictionary == nullptr) != (dst.dictionary == nullptr) || src.n_children != dst.n_children) {
		return false;
	}
	auto sv = ViewOf(src);
	auto dv = ViewOf(dst);
	if (sv.type != dv.type || sv.storage_type != dv.storage_type) {
		return false;
	}
	if (HasUnit(sv.type) && sv.time_unit != dv.time_unit) {
		return false;
	}
	if ((sv.type == NANOARROW_TYPE_DECIMAL128 || sv.type == NANOARROW_TYPE_DECIMAL256) &&
	    sv.decimal_scale != dv.decimal_scale) {
		return false;
	}
	if (sv.fixed_size != dv.fixed_size) {
		return false;
	}
	for (int64_t i = 0; i < src.n_children; i++) {
		if (!SameLayout(*src.children[i], *dst.children[i])) {
			return false;
		}
	}
	return !src.dictionary || SameLayout(*src.dictionary, *dst.dictionary);
}

int64_t UnitsPerSecond(ArrowTimeUnit unit) {
	switch (unit) {
	case NANOARROW_TIME_UNIT_SECOND:
		return 1;
	case NANOARROW_TIME_UNIT_MILLI:
		return 1000;
	case NANOARROW_TIME_UNIT_MICRO:
		return 1000000;
	case NANOARROW_TIME_UNIT_NANO:
		return 1000000000;
	}
	return 1;
}

int64_t Rescale(int64_t value, ArrowTimeUnit from, ArrowTimeUnit to) {
	int64_t f = UnitsPerSecond(from);
	int64_t t = UnitsPerSecond(to);
	if (f == t) {
		return value;
	}
	if (t > f) {
		return value * (t / f);
	}
	// Truncate toward negative infinity, as Arrow's cast does for safe=false.
	int64_t d = f / t;
	int64_t q = value / d;
	if ((value % d) != 0 && value < 0) {
		q--;
	}
	return q;
}

[[noreturn]] void CannotReconcile(const ArrowSchema &src, const ArrowSchema &dst) {
	throw IOException("VGI schema reconciliation: failed to cast %s -> %s", TypeToString(src), TypeToString(dst));
}

// Append element `i` of `src` to `dst` (built from `dst_schema`), converting leaves.
void AppendConverted(ArrowArray *dst, const ArrowSchema &dst_schema, const VgiColumn &src, int64_t i) {
	if (src.IsNull(i)) {
		append::Null(dst);
		return;
	}
	if (dst_schema.dictionary) {
		CannotReconcile(src.schema(), dst_schema);
	}
	auto dv = ViewOf(dst_schema);
	switch (dv.type) {
	case NANOARROW_TYPE_STRUCT:
		if (src.num_children() != dst_schema.n_children) {
			CannotReconcile(src.schema(), dst_schema);
		}
		for (int64_t k = 0; k < dst_schema.n_children; k++) {
			AppendConverted(dst->children[k], *dst_schema.children[k], src.child(k), i);
		}
		append::FinishElement(dst);
		return;
	case NANOARROW_TYPE_LIST:
	case NANOARROW_TYPE_LARGE_LIST:
	case NANOARROW_TYPE_MAP:
	case NANOARROW_TYPE_FIXED_SIZE_LIST: {
		int st = src.is_dictionary() ? -1 : src.type().type;
		if (st != NANOARROW_TYPE_LIST && st != NANOARROW_TYPE_LARGE_LIST && st != NANOARROW_TYPE_MAP &&
		    st != NANOARROW_TYPE_FIXED_SIZE_LIST) {
			CannotReconcile(src.schema(), dst_schema);
		}
		int64_t begin = 0;
		int64_t end = 0;
		src.ListRange(i, begin, end);
		auto child = src.child(0);
		for (int64_t k = begin; k < end; k++) {
			AppendConverted(dst->children[0], *dst_schema.children[0], child, k);
		}
		append::FinishElement(dst);
		return;
	}
	default:
		break;
	}

	// A leaf. A dictionary source reads through its dictionary.
	VgiColumn values = src;
	int64_t row = i;
	if (src.is_dictionary()) {
		values = src.dictionary();
		row = src.GetInt(i);
	}
	auto sv = ViewOf(values.schema());
	switch (dv.type) {
	case NANOARROW_TYPE_BOOL:
	case NANOARROW_TYPE_INT8:
	case NANOARROW_TYPE_INT16:
	case NANOARROW_TYPE_INT32:
	case NANOARROW_TYPE_INT64:
	case NANOARROW_TYPE_DATE32:
	case NANOARROW_TYPE_DATE64:
		switch (sv.type) {
		case NANOARROW_TYPE_UINT8:
		case NANOARROW_TYPE_UINT16:
		case NANOARROW_TYPE_UINT32:
		case NANOARROW_TYPE_UINT64:
			append::Int(dst, static_cast<int64_t>(values.GetUInt(row)));
			return;
		case NANOARROW_TYPE_BOOL:
		case NANOARROW_TYPE_INT8:
		case NANOARROW_TYPE_INT16:
		case NANOARROW_TYPE_INT32:
		case NANOARROW_TYPE_INT64:
			append::Int(dst, values.GetInt(row));
			return;
		case NANOARROW_TYPE_DATE32:
		case NANOARROW_TYPE_DATE64:
			if (sv.type != dv.type) {
				CannotReconcile(values.schema(), dst_schema);
			}
			append::Int(dst, values.GetInt(row));
			return;
		default:
			CannotReconcile(values.schema(), dst_schema);
		}
	case NANOARROW_TYPE_UINT8:
	case NANOARROW_TYPE_UINT16:
	case NANOARROW_TYPE_UINT32:
	case NANOARROW_TYPE_UINT64:
		switch (sv.type) {
		case NANOARROW_TYPE_UINT8:
		case NANOARROW_TYPE_UINT16:
		case NANOARROW_TYPE_UINT32:
		case NANOARROW_TYPE_UINT64:
			append::UInt(dst, values.GetUInt(row));
			return;
		case NANOARROW_TYPE_INT8:
		case NANOARROW_TYPE_INT16:
		case NANOARROW_TYPE_INT32:
		case NANOARROW_TYPE_INT64:
			append::UInt(dst, static_cast<uint64_t>(values.GetInt(row)));
			return;
		default:
			CannotReconcile(values.schema(), dst_schema);
		}
	case NANOARROW_TYPE_HALF_FLOAT:
	case NANOARROW_TYPE_FLOAT:
	case NANOARROW_TYPE_DOUBLE:
		switch (sv.type) {
		case NANOARROW_TYPE_HALF_FLOAT:
		case NANOARROW_TYPE_FLOAT:
		case NANOARROW_TYPE_DOUBLE:
			append::Double(dst, values.GetDouble(row));
			return;
		case NANOARROW_TYPE_INT8:
		case NANOARROW_TYPE_INT16:
		case NANOARROW_TYPE_INT32:
		case NANOARROW_TYPE_INT64:
			append::Double(dst, static_cast<double>(values.GetInt(row)));
			return;
		default:
			CannotReconcile(values.schema(), dst_schema);
		}
	case NANOARROW_TYPE_STRING:
	case NANOARROW_TYPE_LARGE_STRING:
	case NANOARROW_TYPE_STRING_VIEW:
	case NANOARROW_TYPE_BINARY:
	case NANOARROW_TYPE_LARGE_BINARY:
	case NANOARROW_TYPE_BINARY_VIEW:
	case NANOARROW_TYPE_FIXED_SIZE_BINARY:
		switch (sv.type) {
		case NANOARROW_TYPE_STRING:
		case NANOARROW_TYPE_LARGE_STRING:
		case NANOARROW_TYPE_STRING_VIEW:
		case NANOARROW_TYPE_BINARY:
		case NANOARROW_TYPE_LARGE_BINARY:
		case NANOARROW_TYPE_BINARY_VIEW:
		case NANOARROW_TYPE_FIXED_SIZE_BINARY: {
			auto view = values.GetView(row);
			append::Bytes(dst, view.data(), static_cast<int64_t>(view.size()));
			return;
		}
		default:
			CannotReconcile(values.schema(), dst_schema);
		}
	case NANOARROW_TYPE_TIMESTAMP:
	case NANOARROW_TYPE_TIME32:
	case NANOARROW_TYPE_TIME64:
	case NANOARROW_TYPE_DURATION:
		if (sv.type != dv.type && !(sv.type == NANOARROW_TYPE_TIME32 && dv.type == NANOARROW_TYPE_TIME64) &&
		    !(sv.type == NANOARROW_TYPE_TIME64 && dv.type == NANOARROW_TYPE_TIME32)) {
			CannotReconcile(values.schema(), dst_schema);
		}
		append::Int(dst, Rescale(values.GetInt(row), sv.time_unit, dv.time_unit));
		return;
	default:
		CannotReconcile(values.schema(), dst_schema);
	}
}

} // namespace

VgiBatchPtr ReconcileBatchToSchema(const VgiBatchPtr &batch, const VgiSchemaPtr &target_schema) {
	if (!batch || !target_schema) {
		return batch;
	}
	// Fast path: schemas already equal (excluding metadata, since ArrowConverter
	// doesn't preserve it).
	if (batch->schema().get() == target_schema.get() || batch->schema()->Equals(*target_schema)) {
		return batch;
	}
	if (batch->num_columns() != target_schema->num_fields()) {
		throw IOException("VGI schema reconciliation: column count mismatch (batch=%d target=%d)",
		                  batch->num_columns(), target_schema->num_fields());
	}
	bool relabel = true;
	for (int i = 0; i < batch->num_columns() && relabel; i++) {
		relabel = SameLayout(batch->schema()->field(i), target_schema->field(i));
	}
	if (relabel) {
		return batch->WithSchema(target_schema);
	}
	VgiBatchBuilder builder(target_schema);
	for (int c = 0; c < batch->num_columns(); c++) {
		auto column = batch->column(c);
		const auto &field = target_schema->field(c);
		for (int64_t r = 0; r < batch->num_rows(); r++) {
			AppendConverted(builder.column(c), field, column, r);
		}
	}
	builder.SetRows(batch->num_rows());
	return builder.Finish(batch->metadata());
}

} // namespace vgi
} // namespace duckdb
