// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

#include "vgi_arrow.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/column_list.hpp"

namespace duckdb {
namespace vgi {

// Fetch the named settings from the client context, skipping any that are unset.
std::map<std::string, Value> ExtractVgiSettings(ClientContext &context,
                                                const std::vector<std::string> &setting_names);

// Export a schema to C ABI format (deep copy, owned by `out`; a previous export is released).
void ExportSchema(const VgiSchemaPtr &schema, ArrowSchemaWrapper &out);

// Export a batch to C ABI format; the array keeps the batch's buffers alive.
void ExportRecordBatch(const VgiBatchPtr &batch, ArrowArrayWrapper &out);

// `arrow_table` must already be populated via ArrowTableFunction::PopulateArrowTableSchema.
void GetDuckDBTypesFromArrowTable(const ArrowTableSchema &arrow_table, const ArrowSchema &c_schema,
                                  vector<LogicalType> &types, vector<string> &names);

// The one Arrow -> DuckDB type conversion (never hand-switch on Arrow type ids):
// exports `schema` to C ABI, populates `arrow_table_out` via DuckDB's own
// conversion and extracts types and names. `c_schema_out`/`arrow_table_out`
// are kept for later ArrowToDuckDB data conversion. Rejects Arrow fields VGI
// does not support (NotImplementedException).
void ArrowSchemaToDuckDBTypes(ClientContext &context, const VgiSchemaPtr &schema,
                              ArrowSchemaWrapper &c_schema_out, ArrowTableSchema &arrow_table_out,
                              vector<LogicalType> &types, vector<string> &names);

void ArrowSchemaToDuckDBTypes(ClientContext &context, const VgiSchemaPtr &schema,
                             ArrowSchemaWrapper &c_schema_out, ArrowTableSchema &arrow_table_out,
                             vector<LogicalType> &types, vector<Identifier> &names);

// Column definitions only (e.g. for CreateTableInfo), when the ArrowTableSchema
// is not needed for data conversion.
void ArrowSchemaToColumnList(ClientContext &context, const VgiSchemaPtr &schema, ColumnList &columns);

// Inverse of ArrowSchemaToColumnList, via DuckDB's ArrowConverter.
VgiSchemaPtr DuckDBColumnsToArrowSchema(ClientContext &context, const ColumnList &columns,
                                       const std::string &owner_schema = "");

// Arrow export options for anything sent to a worker. The session's own
// options can ask for view layouts (BLOB/BIT export as binary_view from
// arrow_output_version 1.4, plus produce_arrow_string_view and
// arrow_output_list_view), which the IPC encoder cannot write and workers need not
// read. Timezone, offset size and lossless conversion follow the session.
ClientProperties VgiWireClientProperties(ClientContext &context);

// Convert DuckDB DataChunk to a batch named after `schema`'s fields (types are DuckDB's
// ArrowConverter output for the chunk's types; a 0-row chunk yields an empty batch of `schema`)
VgiBatchPtr DataChunkToArrow(ClientContext &context, DataChunk &chunk, const VgiSchemaPtr &schema);

// DataChunkToArrow for hot-path callers whose batches all share types, names,
// extension types and client properties (e.g. a scalar's input after bind):
// the caller caches those (typically in local state) instead of rebuilding
// them per batch. The cached vectors MUST match `chunk`'s column layout; a
// mismatch is undefined behaviour (no runtime check).
VgiBatchPtr DataChunkToArrowCached(
    ClientContext &context, DataChunk &chunk,
    const VgiSchemaPtr &schema,
    const vector<LogicalType> &cached_types,
    const vector<string> &cached_names,
    const ClientProperties &cached_client_props,
    const unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> &cached_extension_types);

VgiSchemaPtr BuildArrowSchemaFromDuckDB(ClientContext &context, const vector<LogicalType> &types,
                                        const vector<string> &names);
VgiSchemaPtr BuildArrowSchemaFromDuckDB(ClientContext &context, const vector<LogicalType> &types,
                                      const vector<Identifier> &names);

// Reconcile a batch DataChunkToArrow produced to the schema a worker declared.
// DuckDB's LogicalType loses Arrow attributes on the round trip (nullability:
// ArrowConverter always emits nullable; timestamp unit/timezone; decimal
// precision). Returns a batch whose schema is ``target_schema``:
//   * unchanged when the schemas are equal (ignoring metadata);
//   * relabeled zero-copy when every node has the same physical layout (only
//     names, nullability, metadata, a timestamp's timezone or a decimal's
//     precision differ);
//   * otherwise rebuilt, converting leaves: timestamp/time/duration unit
//     rescale, integer width/sign changes, integer -> floating point,
//     string/binary variants, decimal bit-width changes (32/64/128/256, same
//     scale), dictionary -> its value type.
// Throws IOException for any other difference (including a decimal scale
// change, which DuckDB's write path never produces).
VgiBatchPtr ReconcileBatchToSchema(const VgiBatchPtr &batch, const VgiSchemaPtr &target_schema);

// Function arguments for the bind call: a one-row batch whose single column "args" is
// a struct with positional_0, positional_1, ..., named_<name> fields.
struct ArrowArguments {
	VgiBatchPtr batch;
	//! Rows of the arguments batch (1), or 0 when there are none.
	int64_t length() const {
		return batch ? batch->num_rows() : 0;
	}
	explicit operator bool() const {
		return batch != nullptr;
	}
};

// Each field keeps its DuckDB Value's type (via ArrowAppender).
ArrowArguments BuildArgumentsFromValues(ClientContext &context, const vector<Value> &positional_args,
                                        const vector<std::pair<string, Value>> &named_args = {});

// A single-row batch with one column per setting, each keeping its DuckDB type.
VgiBatchPtr BuildSettingsBatch(ClientContext &context, const std::map<std::string, Value> &settings);

// IPC bytes of a single-row batch with one struct column per secret type
// (secret_type → {key → Value}, DuckDB types preserved); empty when there are
// no non-empty secrets.
std::vector<uint8_t> BuildSecretsBatch(ClientContext &context,
                                        const std::map<std::string, std::map<std::string, Value>> &secrets);

// A function's arguments schema split by field metadata: "vgi_arg"="named" ->
// named_parameters; "vgi_varargs"="true" -> varargs (not a positional slot);
// "vgi_type"="table" -> a TABLE positional slot; the rest positional in order.
struct FunctionArgumentTypes {
	vector<LogicalType> positional_types;
	vector<string> positional_names;
	vector<bool> positional_is_const;  // Parallel to positional_types: true if param is a constant
	case_insensitive_map_t<LogicalType> named_parameters;
	bool has_varargs = false;
	LogicalType varargs_type = LogicalType::ANY;
	// Index among the positional args of the TABLE input, or -1.
	int table_input_position = -1;
	string table_input_name;

	bool HasTableInput() const {
		return table_input_position >= 0;
	}
};

FunctionArgumentTypes ParseFunctionArgumentSchema(ClientContext &context, const VgiSchemaPtr &schema);

} // namespace vgi
} // namespace duckdb
