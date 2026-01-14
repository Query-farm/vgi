#include "vgi_arrow_convert.hpp"

#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// Arrow to DuckDB Type Conversion
// ============================================================================

LogicalType ArrowTypeToDuckDB(const std::shared_ptr<arrow::DataType> &arrow_type) {
	switch (arrow_type->id()) {
	// Boolean
	case arrow::Type::BOOL:
		return LogicalType::BOOLEAN;

	// Signed integers
	case arrow::Type::INT8:
		return LogicalType::TINYINT;
	case arrow::Type::INT16:
		return LogicalType::SMALLINT;
	case arrow::Type::INT32:
		return LogicalType::INTEGER;
	case arrow::Type::INT64:
		return LogicalType::BIGINT;

	// Unsigned integers
	case arrow::Type::UINT8:
		return LogicalType::UTINYINT;
	case arrow::Type::UINT16:
		return LogicalType::USMALLINT;
	case arrow::Type::UINT32:
		return LogicalType::UINTEGER;
	case arrow::Type::UINT64:
		return LogicalType::UBIGINT;

	// Floating point
	case arrow::Type::HALF_FLOAT:
	case arrow::Type::FLOAT:
		return LogicalType::FLOAT;
	case arrow::Type::DOUBLE:
		return LogicalType::DOUBLE;

	// Strings
	case arrow::Type::STRING:
	case arrow::Type::LARGE_STRING:
		return LogicalType::VARCHAR;

	// Binary
	case arrow::Type::BINARY:
	case arrow::Type::LARGE_BINARY:
	case arrow::Type::FIXED_SIZE_BINARY:
		return LogicalType::BLOB;

	// Date/Time
	case arrow::Type::DATE32:
	case arrow::Type::DATE64:
		return LogicalType::DATE;

	case arrow::Type::TIMESTAMP:
		return LogicalType::TIMESTAMP;

	case arrow::Type::TIME32:
	case arrow::Type::TIME64:
		return LogicalType::TIME;

	// Decimal
	case arrow::Type::DECIMAL128:
	case arrow::Type::DECIMAL256: {
		auto dec_type = std::static_pointer_cast<arrow::DecimalType>(arrow_type);
		return LogicalType::DECIMAL(dec_type->precision(), dec_type->scale());
	}

	// Nested types
	case arrow::Type::LIST:
	case arrow::Type::LARGE_LIST: {
		auto list_type = std::static_pointer_cast<arrow::BaseListType>(arrow_type);
		return LogicalType::LIST(ArrowTypeToDuckDB(list_type->value_type()));
	}

	case arrow::Type::FIXED_SIZE_LIST: {
		auto list_type = std::static_pointer_cast<arrow::FixedSizeListType>(arrow_type);
		return LogicalType::LIST(ArrowTypeToDuckDB(list_type->value_type()));
	}

	case arrow::Type::STRUCT: {
		auto struct_type = std::static_pointer_cast<arrow::StructType>(arrow_type);
		child_list_t<LogicalType> children;
		for (int i = 0; i < struct_type->num_fields(); i++) {
			auto field = struct_type->field(i);
			children.push_back({field->name(), ArrowTypeToDuckDB(field->type())});
		}
		return LogicalType::STRUCT(std::move(children));
	}

	case arrow::Type::MAP: {
		auto map_type = std::static_pointer_cast<arrow::MapType>(arrow_type);
		return LogicalType::MAP(ArrowTypeToDuckDB(map_type->key_type()), ArrowTypeToDuckDB(map_type->item_type()));
	}

	// Dictionary - unwrap to value type
	case arrow::Type::DICTIONARY: {
		auto dict_type = std::static_pointer_cast<arrow::DictionaryType>(arrow_type);
		return ArrowTypeToDuckDB(dict_type->value_type());
	}

	// Null type
	case arrow::Type::NA:
		return LogicalType::SQLNULL;

	default:
		throw NotImplementedException("Arrow type '%s' is not supported for DuckDB conversion", arrow_type->ToString());
	}
}

void ArrowSchemaToDuckDB(const std::shared_ptr<arrow::Schema> &schema, vector<LogicalType> &types,
                         vector<string> &names) {
	types.clear();
	names.clear();
	for (int i = 0; i < schema->num_fields(); i++) {
		auto &field = schema->field(i);
		types.push_back(ArrowTypeToDuckDB(field->type()));
		names.push_back(field->name());
	}
}

// ============================================================================
// Arrow to DuckDB Data Conversion - Primitive Types
// ============================================================================

template <typename ARROW_TYPE, typename DUCKDB_TYPE>
static void ConvertPrimitiveArray(const std::shared_ptr<arrow::Array> &array, idx_t offset, idx_t count, Vector &target,
                                  idx_t target_offset) {
	using ArrayType = typename arrow::TypeTraits<ARROW_TYPE>::ArrayType;
	auto typed_array = std::static_pointer_cast<ArrayType>(array);
	auto target_data = FlatVector::GetData<DUCKDB_TYPE>(target);
	auto &validity = FlatVector::Validity(target);

	for (idx_t i = 0; i < count; i++) {
		if (typed_array->IsNull(offset + i)) {
			validity.SetInvalid(target_offset + i);
		} else {
			target_data[target_offset + i] = static_cast<DUCKDB_TYPE>(typed_array->Value(offset + i));
		}
	}
}

static void ConvertBooleanArray(const std::shared_ptr<arrow::Array> &array, idx_t offset, idx_t count, Vector &target,
                                idx_t target_offset) {
	auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(array);
	auto target_data = FlatVector::GetData<bool>(target);
	auto &validity = FlatVector::Validity(target);

	for (idx_t i = 0; i < count; i++) {
		if (bool_array->IsNull(offset + i)) {
			validity.SetInvalid(target_offset + i);
		} else {
			target_data[target_offset + i] = bool_array->Value(offset + i);
		}
	}
}

// ============================================================================
// Arrow to DuckDB Data Conversion - String/Binary Types
// ============================================================================

static void ConvertStringArray(const std::shared_ptr<arrow::Array> &array, idx_t offset, idx_t count, Vector &target,
                               idx_t target_offset) {
	auto string_array = std::static_pointer_cast<arrow::StringArray>(array);
	auto &validity = FlatVector::Validity(target);

	for (idx_t i = 0; i < count; i++) {
		if (string_array->IsNull(offset + i)) {
			validity.SetInvalid(target_offset + i);
		} else {
			auto value = string_array->GetView(offset + i);
			FlatVector::GetData<string_t>(target)[target_offset + i] =
			    StringVector::AddString(target, value.data(), value.size());
		}
	}
}

static void ConvertLargeStringArray(const std::shared_ptr<arrow::Array> &array, idx_t offset, idx_t count,
                                    Vector &target, idx_t target_offset) {
	auto string_array = std::static_pointer_cast<arrow::LargeStringArray>(array);
	auto &validity = FlatVector::Validity(target);

	for (idx_t i = 0; i < count; i++) {
		if (string_array->IsNull(offset + i)) {
			validity.SetInvalid(target_offset + i);
		} else {
			auto value = string_array->GetView(offset + i);
			FlatVector::GetData<string_t>(target)[target_offset + i] =
			    StringVector::AddString(target, value.data(), value.size());
		}
	}
}

static void ConvertBinaryArray(const std::shared_ptr<arrow::Array> &array, idx_t offset, idx_t count, Vector &target,
                               idx_t target_offset) {
	auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(array);
	auto &validity = FlatVector::Validity(target);

	for (idx_t i = 0; i < count; i++) {
		if (binary_array->IsNull(offset + i)) {
			validity.SetInvalid(target_offset + i);
		} else {
			auto value = binary_array->GetView(offset + i);
			FlatVector::GetData<string_t>(target)[target_offset + i] =
			    StringVector::AddStringOrBlob(target, value.data(), value.size());
		}
	}
}

// ============================================================================
// Arrow to DuckDB Data Conversion - Temporal Types
// ============================================================================

static void ConvertDate32Array(const std::shared_ptr<arrow::Array> &array, idx_t offset, idx_t count, Vector &target,
                               idx_t target_offset) {
	auto date_array = std::static_pointer_cast<arrow::Date32Array>(array);
	auto target_data = FlatVector::GetData<date_t>(target);
	auto &validity = FlatVector::Validity(target);

	for (idx_t i = 0; i < count; i++) {
		if (date_array->IsNull(offset + i)) {
			validity.SetInvalid(target_offset + i);
		} else {
			// Arrow Date32 is days since Unix epoch, same as DuckDB
			target_data[target_offset + i] = date_t(date_array->Value(offset + i));
		}
	}
}

static void ConvertDate64Array(const std::shared_ptr<arrow::Array> &array, idx_t offset, idx_t count, Vector &target,
                               idx_t target_offset) {
	auto date_array = std::static_pointer_cast<arrow::Date64Array>(array);
	auto target_data = FlatVector::GetData<date_t>(target);
	auto &validity = FlatVector::Validity(target);

	for (idx_t i = 0; i < count; i++) {
		if (date_array->IsNull(offset + i)) {
			validity.SetInvalid(target_offset + i);
		} else {
			// Arrow Date64 is milliseconds since Unix epoch
			int64_t ms = date_array->Value(offset + i);
			target_data[target_offset + i] = date_t(static_cast<int32_t>(ms / (1000 * 60 * 60 * 24)));
		}
	}
}

static void ConvertTimestampArray(const std::shared_ptr<arrow::Array> &array, idx_t offset, idx_t count, Vector &target,
                                  idx_t target_offset) {
	auto ts_array = std::static_pointer_cast<arrow::TimestampArray>(array);
	auto ts_type = std::static_pointer_cast<arrow::TimestampType>(array->type());
	auto target_data = FlatVector::GetData<timestamp_t>(target);
	auto &validity = FlatVector::Validity(target);

	for (idx_t i = 0; i < count; i++) {
		if (ts_array->IsNull(offset + i)) {
			validity.SetInvalid(target_offset + i);
		} else {
			int64_t value = ts_array->Value(offset + i);
			// Convert to microseconds (DuckDB's timestamp unit)
			switch (ts_type->unit()) {
			case arrow::TimeUnit::SECOND:
				value *= 1000000;
				break;
			case arrow::TimeUnit::MILLI:
				value *= 1000;
				break;
			case arrow::TimeUnit::MICRO:
				// Already in microseconds
				break;
			case arrow::TimeUnit::NANO:
				value /= 1000;
				break;
			}
			target_data[target_offset + i] = timestamp_t(value);
		}
	}
}

// ============================================================================
// Arrow to DuckDB Data Conversion - Nested Types
// ============================================================================

static void ConvertListArray(const std::shared_ptr<arrow::Array> &array, idx_t offset, idx_t count, Vector &target,
                             idx_t target_offset, const LogicalType &type) {
	auto list_array = std::static_pointer_cast<arrow::ListArray>(array);
	(void)type; // Used for child type in recursive calls

	auto list_size = ListVector::GetListSize(target);
	auto &child_vector = ListVector::GetEntry(target);
	auto list_data = FlatVector::GetData<list_entry_t>(target);
	auto &validity = FlatVector::Validity(target);

	for (idx_t i = 0; i < count; i++) {
		if (list_array->IsNull(offset + i)) {
			validity.SetInvalid(target_offset + i);
			list_data[target_offset + i].offset = list_size;
			list_data[target_offset + i].length = 0;
		} else {
			auto start = list_array->value_offset(offset + i);
			auto length = list_array->value_length(offset + i);

			list_data[target_offset + i].offset = list_size;
			list_data[target_offset + i].length = length;

			// Resize child vector if needed
			ListVector::Reserve(target, list_size + length);

			// Convert child elements
			ArrowArrayToVector(list_array->values(), start, length, child_vector, list_size);

			list_size += length;
		}
	}

	ListVector::SetListSize(target, list_size);
}

static void ConvertStructArray(const std::shared_ptr<arrow::Array> &array, idx_t offset, idx_t count, Vector &target,
                               idx_t target_offset, const LogicalType &type) {
	auto struct_array = std::static_pointer_cast<arrow::StructArray>(array);
	auto &child_types = StructType::GetChildTypes(type);
	auto &child_vectors = StructVector::GetEntries(target);
	auto &validity = FlatVector::Validity(target);

	// Set struct-level nulls
	for (idx_t i = 0; i < count; i++) {
		if (struct_array->IsNull(offset + i)) {
			validity.SetInvalid(target_offset + i);
		}
	}

	// Convert each child field
	for (idx_t child_idx = 0; child_idx < child_types.size(); child_idx++) {
		auto arrow_child = struct_array->field(static_cast<int>(child_idx));
		ArrowArrayToVector(arrow_child, offset, count, *child_vectors[child_idx], target_offset);
	}
}

// ============================================================================
// Main Conversion Entry Points
// ============================================================================

void ArrowArrayToVector(const std::shared_ptr<arrow::Array> &array, idx_t offset, idx_t count, Vector &target,
                        idx_t target_offset) {
	auto &type = target.GetType();

	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		ConvertBooleanArray(array, offset, count, target, target_offset);
		break;

	case LogicalTypeId::TINYINT:
		ConvertPrimitiveArray<arrow::Int8Type, int8_t>(array, offset, count, target, target_offset);
		break;
	case LogicalTypeId::SMALLINT:
		ConvertPrimitiveArray<arrow::Int16Type, int16_t>(array, offset, count, target, target_offset);
		break;
	case LogicalTypeId::INTEGER:
		ConvertPrimitiveArray<arrow::Int32Type, int32_t>(array, offset, count, target, target_offset);
		break;
	case LogicalTypeId::BIGINT:
		ConvertPrimitiveArray<arrow::Int64Type, int64_t>(array, offset, count, target, target_offset);
		break;

	case LogicalTypeId::UTINYINT:
		ConvertPrimitiveArray<arrow::UInt8Type, uint8_t>(array, offset, count, target, target_offset);
		break;
	case LogicalTypeId::USMALLINT:
		ConvertPrimitiveArray<arrow::UInt16Type, uint16_t>(array, offset, count, target, target_offset);
		break;
	case LogicalTypeId::UINTEGER:
		ConvertPrimitiveArray<arrow::UInt32Type, uint32_t>(array, offset, count, target, target_offset);
		break;
	case LogicalTypeId::UBIGINT:
		ConvertPrimitiveArray<arrow::UInt64Type, uint64_t>(array, offset, count, target, target_offset);
		break;

	case LogicalTypeId::FLOAT:
		ConvertPrimitiveArray<arrow::FloatType, float>(array, offset, count, target, target_offset);
		break;
	case LogicalTypeId::DOUBLE:
		ConvertPrimitiveArray<arrow::DoubleType, double>(array, offset, count, target, target_offset);
		break;

	case LogicalTypeId::VARCHAR:
		if (array->type_id() == arrow::Type::LARGE_STRING) {
			ConvertLargeStringArray(array, offset, count, target, target_offset);
		} else {
			ConvertStringArray(array, offset, count, target, target_offset);
		}
		break;

	case LogicalTypeId::BLOB:
		ConvertBinaryArray(array, offset, count, target, target_offset);
		break;

	case LogicalTypeId::DATE:
		if (array->type_id() == arrow::Type::DATE64) {
			ConvertDate64Array(array, offset, count, target, target_offset);
		} else {
			ConvertDate32Array(array, offset, count, target, target_offset);
		}
		break;

	case LogicalTypeId::TIMESTAMP:
		ConvertTimestampArray(array, offset, count, target, target_offset);
		break;

	case LogicalTypeId::LIST:
		ConvertListArray(array, offset, count, target, target_offset, type);
		break;

	case LogicalTypeId::STRUCT:
		ConvertStructArray(array, offset, count, target, target_offset, type);
		break;

	default:
		throw NotImplementedException("Arrow to DuckDB conversion not implemented for type: %s", type.ToString());
	}
}

void ArrowBatchToDataChunk(const std::shared_ptr<arrow::RecordBatch> &batch, idx_t offset, idx_t count,
                           DataChunk &output, idx_t output_offset) {
	for (idx_t col_idx = 0; col_idx < static_cast<idx_t>(batch->num_columns()); col_idx++) {
		auto arrow_col = batch->column(static_cast<int>(col_idx));
		ArrowArrayToVector(arrow_col, offset, count, output.data[col_idx], output_offset);
	}
}

} // namespace vgi
} // namespace duckdb
