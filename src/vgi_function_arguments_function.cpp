// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
// =============================================================================
// vgi_function_arguments() — diagnostic table function. One row per
// (catalog, schema, function, argument) across every attached VGI catalog.
//
// Surfaces per-argument metadata that DuckDB's duckdb_functions() flattens
// away: the named-vs-positional split, const, varargs, table-input, and
// any-type markers, plus the per-argument description (the vgi_doc field
// metadata). Lets an agent that discovered a function via the catalog tags
// learn how to actually call it.
//
// Performance: issues a fresh catalog_schema_contents_functions RPC per
// (schema, function-type) per attached catalog. Acceptable for a diagnostic;
// not for a hot path. Reports each catalog's current/attached data version
// (no time travel).
// =============================================================================

#include <arrow/api.h>

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "storage/vgi_catalog.hpp"
#include "storage/vgi_transaction.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_function_docs.hpp"
#include "vgi_logging.hpp"
#include "vgi_protocol_constants.hpp"

namespace duckdb {
namespace vgi {

namespace {

struct ArgRow {
	std::string catalog_name;
	std::string schema_name;
	std::string function_name;
	std::string function_type;
	int64_t arg_position;
	bool arg_position_present; // false -> NULL (named / varargs)
	int64_t field_index;
	std::string arg_name;
	std::string arg_type;
	std::string arg_description; // empty -> NULL (presence-only vgi_doc)
	bool arg_description_present;
	bool is_named;
	bool is_positional;
	bool is_const;
	bool is_varargs;
	bool is_table_input;
	bool is_any_type;
};

struct VgiFunctionArgumentsData : public TableFunctionData {
	std::vector<ArgRow> rows;
	mutable idx_t current_idx = 0;
};

// Read a presence-only string field-metadata value; returns "" when absent.
static std::string MetadataValue(const std::shared_ptr<const arrow::KeyValueMetadata> &md, const char *key) {
	if (!md) {
		return "";
	}
	int idx = md->FindKey(key);
	if (idx < 0) {
		return "";
	}
	return md->value(idx);
}

// Expand an arguments_schema (from a function OR a macro) into per-argument
// rows. function_type is the display string (e.g. "scalar", "table_macro").
static void EmitArgsFromSchema(ClientContext &context, const std::string &catalog_name, const std::string &schema_name,
                               const std::string &function_name, const std::string &function_type,
                               const std::shared_ptr<arrow::Schema> &arguments_schema, std::vector<ArgRow> &out) {
	if (!arguments_schema || arguments_schema->num_fields() == 0) {
		return; // No declared arguments — emit no rows.
	}

	// Resolve each field's DuckDB type in field order (reuses the shared
	// Arrow->DuckDB bridge; never a manual arrow::Type switch).
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
	vector<LogicalType> types;
	vector<string> names;
	ArrowSchemaToDuckDBTypes(context, arguments_schema, c_schema, arrow_table, types, names);

	const int num_fields = arguments_schema->num_fields();
	int64_t positional_ordinal = 0;

	for (int i = 0; i < num_fields; i++) {
		auto field = arguments_schema->field(i);
		auto md = field->HasMetadata() ? field->metadata() : nullptr;

		const bool is_named = MetadataValue(md, VGI_ARG_METADATA_KEY) == VGI_ARG_NAMED_VALUE;
		const bool is_varargs = MetadataValue(md, VGI_VARARGS_METADATA_KEY) == VGI_VARARGS_TRUE_VALUE;
		const auto type_marker = MetadataValue(md, VGI_TYPE_METADATA_KEY);
		const bool is_table_input = type_marker == VGI_TYPE_TABLE_VALUE;
		const bool is_any_type = type_marker == VGI_TYPE_ANY_VALUE;
		const bool is_const = MetadataValue(md, VGI_CONST_METADATA_KEY) == VGI_CONST_TRUE_VALUE;
		const auto doc = MetadataValue(md, VGI_DOC_METADATA_KEY);

		// Type display: ANY for any-typed args, TABLE for table inputs, else
		// the resolved DuckDB type.
		LogicalType arg_type;
		if (is_any_type) {
			arg_type = LogicalType::ANY;
		} else if (is_table_input) {
			arg_type = LogicalType::TABLE;
		} else {
			arg_type = (i < static_cast<int>(types.size())) ? types[static_cast<size_t>(i)] : LogicalType::ANY;
		}

		// Positional ordinal: assigned to every arg that occupies a SQL
		// positional slot (positional value, const, or table input); named
		// and varargs args have no stable position.
		const bool has_position = !is_named && !is_varargs;

		ArgRow row;
		row.catalog_name = catalog_name;
		row.schema_name = schema_name;
		row.function_name = function_name;
		row.function_type = function_type;
		row.field_index = i;
		row.arg_position = has_position ? positional_ordinal : 0;
		row.arg_position_present = has_position;
		if (has_position) {
			positional_ordinal++;
		}
		row.arg_name = field->name();
		row.arg_type = arg_type.ToString();
		row.arg_description = doc;
		row.arg_description_present = !doc.empty();
		row.is_named = is_named;
		row.is_positional = !is_named && !is_varargs && !is_table_input;
		row.is_const = is_const;
		row.is_varargs = is_varargs;
		row.is_table_input = is_table_input;
		row.is_any_type = is_any_type;
		out.push_back(std::move(row));
	}
}

// Expand one function's arguments_schema into per-argument rows.
static void EmitFunctionArgs(ClientContext &context, const std::string &catalog_name, const std::string &schema_name,
                             const VgiFunctionInfo &func, std::vector<ArgRow> &out) {
	EmitArgsFromSchema(context, catalog_name, schema_name, func.name, VgiFunctionTypeToString(func.function_type),
	                   func.arguments_schema, out);
}

// Expand one macro's arguments_schema into per-argument rows. macro_type is the
// wire value "scalar"/"table"; surface it as "scalar_macro"/"table_macro" so the
// function_type column distinguishes macros from same-named functions.
static void EmitMacroArgs(ClientContext &context, const std::string &catalog_name, const std::string &schema_name,
                          const VgiMacroInfo &macro, std::vector<ArgRow> &out) {
	// macro_type arrives as the MacroType enum name ("SCALAR"/"TABLE"); accept the
	// lowercase value form too for resilience.
	const bool is_table = macro.macro_type == "TABLE" || macro.macro_type == "table";
	const std::string function_type = is_table ? "table_macro" : "scalar_macro";
	EmitArgsFromSchema(context, catalog_name, schema_name, macro.name, function_type, macro.arguments_schema, out);
}

static unique_ptr<FunctionData> VgiFunctionArgumentsBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
	return_types = {
	    LogicalType::VARCHAR, // catalog_name
	    LogicalType::VARCHAR, // schema_name
	    LogicalType::VARCHAR, // function_name
	    LogicalType::VARCHAR, // function_type
	    LogicalType::BIGINT,  // arg_position (NULL for named / varargs)
	    LogicalType::BIGINT,  // field_index
	    LogicalType::VARCHAR, // arg_name
	    LogicalType::VARCHAR, // arg_type
	    LogicalType::VARCHAR, // arg_description (NULL when undocumented)
	    LogicalType::BOOLEAN, // is_named
	    LogicalType::BOOLEAN, // is_positional
	    LogicalType::BOOLEAN, // is_const
	    LogicalType::BOOLEAN, // is_varargs
	    LogicalType::BOOLEAN, // is_table_input
	    LogicalType::BOOLEAN, // is_any_type
	};
	names = {"catalog_name", "schema_name",   "function_name", "function_type", "arg_position",
	         "field_index",  "arg_name",      "arg_type",      "arg_description", "is_named",
	         "is_positional", "is_const",     "is_varargs",    "is_table_input", "is_any_type"};

	auto data = make_uniq<VgiFunctionArgumentsData>();

	static const char *kFunctionTypeRequests[] = {"SCALAR_FUNCTION", "TABLE_FUNCTION", "AGGREGATE_FUNCTION"};

	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db : databases) {
		auto &catalog = db->GetCatalog();
		if (catalog.GetCatalogType() != "vgi") {
			continue;
		}
		auto &vgi_catalog = catalog.Cast<VgiCatalog>();
		auto &attach_params = vgi_catalog.attach_parameters();
		auto &attach_result = vgi_catalog.attach_result();
		if (!attach_params || !attach_result) {
			continue; // Not fully attached — skip (same guard as LoadEntries).
		}
		const auto catalog_name = vgi_catalog.GetName();
		auto &vgi_tx = VgiTransaction::Get(context, catalog);
		const auto transaction_opaque = vgi_tx.GetTransactionOpaqueData();

		vgi_catalog.ScanSchemas(context, [&](SchemaCatalogEntry &schema) {
			const auto schema_name = schema.name;
			for (const char *request_type : kFunctionTypeRequests) {
				try {
					CatalogRpcContext rpc_ctx{attach_params, attach_result->attach_opaque_data, transaction_opaque};
					rpc_ctx.entity_kind = "schema";
					rpc_ctx.entity_qualifier = schema_name;
					auto function_list =
					    InvokeCatalogSchemaContentsFunctions(rpc_ctx, schema_name, request_type, context);
					for (auto &func : function_list) {
						EmitFunctionArgs(context, catalog_name, schema_name, func, data->rows);
					}
				} catch (const std::exception &e) {
					// One failing (schema, function-type) shouldn't abort the
					// whole diagnostic; the skip is observable via duckdb_logs.
					VGI_LOG(context, "vgi.function_arguments.skip_failed",
					        {{"catalog", catalog_name},
					         {"schema", schema_name},
					         {"function_type", request_type},
					         {"error", e.what()}});
				}
			}

			// Macros: enumerate scalar + table macros and surface their per-
			// parameter docs (arguments_schema vgi_doc field metadata).
			static const char *kMacroTypeRequests[] = {"SCALAR_MACRO", "TABLE_MACRO"};
			for (const char *macro_type : kMacroTypeRequests) {
				try {
					CatalogRpcContext rpc_ctx{attach_params, attach_result->attach_opaque_data, transaction_opaque};
					rpc_ctx.entity_kind = "schema";
					rpc_ctx.entity_qualifier = schema_name;
					auto macro_list = InvokeCatalogSchemaContentsMacros(rpc_ctx, schema_name, macro_type, context);
					for (auto &macro : macro_list) {
						EmitMacroArgs(context, catalog_name, schema_name, macro, data->rows);
					}
				} catch (const std::exception &e) {
					VGI_LOG(context, "vgi.function_arguments.skip_failed",
					        {{"catalog", catalog_name},
					         {"schema", schema_name},
					         {"function_type", macro_type},
					         {"error", e.what()}});
				}
			}
		});
	}

	return data;
}

static void VgiFunctionArgumentsScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.bind_data->Cast<VgiFunctionArgumentsData>();
	idx_t count = 0;
	while (data.current_idx < data.rows.size() && count < STANDARD_VECTOR_SIZE) {
		const auto &row = data.rows[data.current_idx++];
		output.SetValue(0, count, Value(row.catalog_name));
		output.SetValue(1, count, Value(row.schema_name));
		output.SetValue(2, count, Value(row.function_name));
		output.SetValue(3, count, Value(row.function_type));
		output.SetValue(4, count, row.arg_position_present ? Value::BIGINT(row.arg_position) : Value());
		output.SetValue(5, count, Value::BIGINT(row.field_index));
		output.SetValue(6, count, Value(row.arg_name));
		output.SetValue(7, count, Value(row.arg_type));
		output.SetValue(8, count, row.arg_description_present ? Value(row.arg_description) : Value());
		output.SetValue(9, count, Value::BOOLEAN(row.is_named));
		output.SetValue(10, count, Value::BOOLEAN(row.is_positional));
		output.SetValue(11, count, Value::BOOLEAN(row.is_const));
		output.SetValue(12, count, Value::BOOLEAN(row.is_varargs));
		output.SetValue(13, count, Value::BOOLEAN(row.is_table_input));
		output.SetValue(14, count, Value::BOOLEAN(row.is_any_type));
		count++;
	}
	output.SetCardinality(count);
}

} // anonymous namespace

void RegisterVgiFunctionArgumentsFunction(ExtensionLoader &loader) {
	TableFunction func("vgi_function_arguments", {}, VgiFunctionArgumentsScan, VgiFunctionArgumentsBind);
	CreateTableFunctionInfo info(func);
	info.descriptions.push_back(MakeFunctionDescription(
	    "Inspect the arguments of every function and macro exposed by every attached VGI catalog, one row per "
	    "argument. function_type is scalar/table/aggregate for functions and scalar_macro/table_macro for macros. "
	    "Surfaces per-argument detail that duckdb_functions() flattens away: named-vs-positional, const, "
	    "varargs, table-input, any-type, and the per-argument description (vgi_doc). Filter with "
	    "WHERE catalog_name = '...'. Reports each catalog's current data version; does not honor time travel.",
	    {}, {},
	    {"SELECT * FROM vgi_function_arguments();",
	     "SELECT arg_name, arg_type, is_const, arg_description FROM vgi_function_arguments() "
	     "WHERE catalog_name = 'example' AND function_name = 'multiply' ORDER BY arg_position;"}));
	loader.RegisterFunction(std::move(info));
}

} // namespace vgi
} // namespace duckdb
