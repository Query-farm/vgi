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
// Reuses the schema's function/macro inventories, including catalog_contents
// seeds. The optional catalog parameter limits discovery before any RPCs.
// Reports each catalog's current/attached data version (no time travel).
// =============================================================================

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "storage/vgi_catalog.hpp"
#include "storage/vgi_schema_entry.hpp"
#include "storage/vgi_transaction.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_rpc.hpp"
#include "vgi_macro_contract.hpp"
#include "vgi_function_docs.hpp"
#include "vgi_logging.hpp"
#include "vgi_protocol_constants.hpp"

namespace duckdb {
namespace vgi {

namespace {

struct ArgRow {
	std::optional<int64_t> overload_index;
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
	// Function-level capability repeated on each argument row. True only for
	// row-transform table functions whose positional arguments may be bound to
	// columns from a correlated LATERAL driver.
	bool input_from_args;
	// Per-argument constraint metadata (agent discovery); empty -> NULL.
	std::string arg_default; // functions: vgi_default JSON; macros: imported literal rendered as SQL
	bool arg_default_present;
	std::string arg_choices; // presence-only vgi_choices (JSON array text)
	bool arg_choices_present;
	std::string arg_range; // presence-only vgi_range (interval notation, e.g. "[0, 100]")
	bool arg_range_present;
	std::string arg_pattern; // presence-only vgi_pattern (regex)
	bool arg_pattern_present;
};

struct VgiFunctionArgumentsData : public TableFunctionData {
	std::vector<ArgRow> rows;
	mutable idx_t current_idx = 0;
};

// Read a presence-only string field-metadata value; returns "" when absent.
static std::string MetadataValue(const VgiMetadata &md, const char *key) {
	return md.Get(key).value_or("");
}

// Expand an arguments_schema (from a function OR a macro) into per-argument
// rows. function_type is the display string (e.g. "scalar", "table_macro").
static void EmitArgsFromSchema(ClientContext &context, const std::string &catalog_name, const std::string &schema_name,
                               const std::string &function_name, const std::string &function_type,
                               const VgiSchemaPtr &arguments_schema, bool input_from_args,
                               std::vector<ArgRow> &out) {
	if (!arguments_schema || arguments_schema->num_fields() == 0) {
		return; // No declared arguments — emit no rows.
	}

	// Resolve each field's DuckDB type in field order (reuses the shared
	// Arrow->DuckDB bridge; never a manual Arrow type switch).
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
	vector<LogicalType> types;
	vector<string> names;
	ArrowSchemaToDuckDBTypes(context, arguments_schema, c_schema, arrow_table, types, names);

	const int num_fields = arguments_schema->num_fields();
	int64_t positional_ordinal = 0;

	for (int i = 0; i < num_fields; i++) {
		const auto md = arguments_schema->field_metadata(i);

		const bool is_named = MetadataValue(md, VGI_ARG_METADATA_KEY) == VGI_ARG_NAMED_VALUE;
		const bool is_varargs = MetadataValue(md, VGI_VARARGS_METADATA_KEY) == VGI_VARARGS_TRUE_VALUE;
		const auto type_marker = MetadataValue(md, VGI_TYPE_METADATA_KEY);
		const bool is_table_input = type_marker == VGI_TYPE_TABLE_VALUE;
		const bool is_any_type = type_marker == VGI_TYPE_ANY_VALUE;
		const bool is_const = MetadataValue(md, VGI_CONST_METADATA_KEY) == VGI_CONST_TRUE_VALUE;
		const auto doc = MetadataValue(md, VGI_DOC_METADATA_KEY);
		// Constraint metadata: value-encoded UTF-8, empty == absent (JSON/interval
		// encodings are never empty), so the emptiness test is a safe presence check.
		const auto arg_default = MetadataValue(md, VGI_DEFAULT_METADATA_KEY);
		const auto arg_choices = MetadataValue(md, VGI_CHOICES_METADATA_KEY);
		const auto arg_range = MetadataValue(md, VGI_RANGE_METADATA_KEY);
		const auto arg_pattern = MetadataValue(md, VGI_PATTERN_METADATA_KEY);

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
		row.arg_name = arguments_schema->field_names()[static_cast<size_t>(i)];
		row.arg_type = arg_type.ToString();
		row.arg_description = doc;
		row.arg_description_present = !doc.empty();
		row.is_named = is_named;
		row.is_positional = !is_named && !is_varargs && !is_table_input;
		row.is_const = is_const;
		row.is_varargs = is_varargs;
		row.is_table_input = is_table_input;
		row.is_any_type = is_any_type;
		row.input_from_args = input_from_args;
		row.arg_default = arg_default;
		row.arg_default_present = !arg_default.empty();
		row.arg_choices = arg_choices;
		row.arg_choices_present = !arg_choices.empty();
		row.arg_range = arg_range;
		row.arg_range_present = !arg_range.empty();
		row.arg_pattern = arg_pattern;
		row.arg_pattern_present = !arg_pattern.empty();
		out.push_back(std::move(row));
	}
}

// Expand one function's arguments_schema into per-argument rows.
static void EmitFunctionArgs(ClientContext &context, const std::string &catalog_name, const std::string &schema_name,
                             const VgiFunctionInfo &func, std::vector<ArgRow> &out) {
	EmitArgsFromSchema(context, catalog_name, schema_name, func.name, VgiFunctionTypeToString(func.function_type),
	                   func.arguments_schema, func.input_from_args, out);
}

// Canonical macro documentation is separate from declared type identity.
static void EmitMacroArgs(ClientContext &context, const std::string &catalog_name, const std::string &schema_name,
                          const VgiMacroInfo &macro, std::vector<ArgRow> &out) {
	for (size_t overload = 0; overload < macro.overloads.size(); ++overload) {
		const auto &parameters = macro.overloads[overload].parameters;
		for (size_t index = 0; index < parameters.size(); ++index) {
			const auto &parameter = parameters[index];
			ArgRow row {};
			row.overload_index = overload;
			row.catalog_name = catalog_name;
			row.schema_name = schema_name;
			row.function_name = macro.name;
			row.function_type = macro.macro_type == "table" ? "table_macro" : "scalar_macro";
			row.arg_position = index;
			row.arg_position_present = true;
			row.field_index = index;
			row.arg_name = parameter.name;
			row.is_any_type = !parameter.declared_type;
			row.arg_type = row.is_any_type ? "ANY" : NativeMacroParameterType(context, parameter).ToString();
			row.arg_description_present = parameter.documentation.has_value();
			row.arg_description = parameter.documentation.value_or("");
			row.is_positional = true;
			row.is_named = true; // Pinned MacroFunction accepts named required and defaulted parameters.
			row.arg_default_present = bool(parameter.default_value);
			if (parameter.default_value)
				row.arg_default = NativeMacroDefaultValue(context, parameter).ToSQLString();
			out.push_back(std::move(row));
		}
	}
}

static unique_ptr<FunctionData> VgiFunctionArgumentsBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<Identifier> &names) {
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
	    LogicalType::VARCHAR, // arg_default (functions: JSON scalar; macros: SQL literal)
	    LogicalType::VARCHAR, // arg_choices (NULL when unconstrained; JSON array)
	    LogicalType::VARCHAR, // arg_range (NULL when unbounded; interval notation)
	    LogicalType::VARCHAR, // arg_pattern (NULL when no pattern; regex)
	    LogicalType::BOOLEAN, // input_from_args (function-level capability)
	    LogicalType::BIGINT,  // overload_index (zero-based; NULL for functions)
	};
	names = {"catalog_name",  "schema_name", "function_name", "function_type",   "arg_position",
	         "field_index",   "arg_name",    "arg_type",      "arg_description", "is_named",
	         "is_positional", "is_const",    "is_varargs",    "is_table_input",  "is_any_type",
	         "arg_default",   "arg_choices", "arg_range",     "arg_pattern",     "input_from_args", "overload_index"};

	auto data = make_uniq<VgiFunctionArgumentsData>();

	static const CatalogType kFunctionTypes[] = {CatalogType::SCALAR_FUNCTION_ENTRY,
	                                             CatalogType::TABLE_FUNCTION_ENTRY,
	                                             CatalogType::AGGREGATE_FUNCTION_ENTRY};
	const auto selected_catalog = input.named_parameters.find("catalog");

	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db : databases) {
		auto &catalog = db->GetCatalog();
		if (catalog.GetCatalogType() != "vgi") {
			continue;
		}
		if (selected_catalog != input.named_parameters.end() && !selected_catalog->second.IsNull() &&
		    !StringUtil::CIEquals(catalog.GetName().GetIdentifierName(), selected_catalog->second.ToString())) {
			continue;
		}
		auto &vgi_catalog = catalog.Cast<VgiCatalog>();
		auto &attach_params = vgi_catalog.attach_parameters();
		auto &attach_result = vgi_catalog.attach_result();
		if (!attach_params || !attach_result) {
			continue; // Not fully attached — skip (same guard as LoadEntries).
		}
		const auto catalog_name = vgi_catalog.GetName().GetIdentifierName();
		auto &vgi_tx = VgiTransaction::Get(context, catalog);
		const auto transaction_opaque = vgi_tx.GetTransactionOpaqueData();

		vgi_catalog.ScanSchemas(context, [&](SchemaCatalogEntry &schema) {
			const auto schema_name = schema.name.GetIdentifierName();
			auto &vgi_schema = schema.Cast<VgiSchemaEntry>();
			for (const auto function_type : kFunctionTypes) {
				try {
					CatalogRpcContext rpc_ctx {attach_params, attach_result->attach_opaque_data, transaction_opaque};
					rpc_ctx.entity_kind = "schema";
					rpc_ctx.entity_qualifier = schema_name;
					auto function_list = vgi_schema.GetFunctionInventory(rpc_ctx, function_type, context);
					for (auto &func : function_list) {
						EmitFunctionArgs(context, catalog_name, schema_name, func, data->rows);
					}
				} catch (const std::exception &e) {
					// One failing (schema, function-type) shouldn't abort the
					// whole diagnostic; the skip is observable via duckdb_logs.
					VGI_LOG(context, "vgi.function_arguments.skip_failed",
					        {{"catalog", catalog_name},
					         {"schema", schema_name},
					         {"function_type", EnumUtil::ToString(function_type)},
					         {"error", e.what()}});
				}
			}

			// Macros: enumerate scalar + table macros and surface their per-
			// parameter documentation from each ordered overload.
			static const CatalogType kMacroTypes[] = {CatalogType::MACRO_ENTRY, CatalogType::TABLE_MACRO_ENTRY};
			for (const auto macro_type : kMacroTypes) {
				try {
					CatalogRpcContext rpc_ctx {attach_params, attach_result->attach_opaque_data, transaction_opaque};
					rpc_ctx.entity_kind = "schema";
					rpc_ctx.entity_qualifier = schema_name;
					auto macro_list = vgi_schema.GetMacroInventory(rpc_ctx, macro_type, context);
					for (auto &macro : macro_list) {
						EmitMacroArgs(context, catalog_name, schema_name, macro, data->rows);
					}
				} catch (const std::exception &e) {
					VGI_LOG(context, "vgi.function_arguments.skip_failed",
					        {{"catalog", catalog_name},
					         {"schema", schema_name},
					         {"function_type", EnumUtil::ToString(macro_type)},
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
		output.SetValue(15, count, row.arg_default_present ? Value(row.arg_default) : Value());
		output.SetValue(16, count, row.arg_choices_present ? Value(row.arg_choices) : Value());
		output.SetValue(17, count, row.arg_range_present ? Value(row.arg_range) : Value());
		output.SetValue(18, count, row.arg_pattern_present ? Value(row.arg_pattern) : Value());
		output.SetValue(19, count, Value::BOOLEAN(row.input_from_args));
		output.SetValue(20, count, row.overload_index ? Value::BIGINT(*row.overload_index) : Value());
		count++;
	}
	output.SetCardinality(count);
}

} // anonymous namespace

void RegisterVgiFunctionArgumentsFunction(ExtensionLoader &loader) {
	TableFunction func("vgi_function_arguments", {}, VgiFunctionArgumentsScan, VgiFunctionArgumentsBind);
	AddVgiFunctionOption(func, "catalog", LogicalType::VARCHAR);
	CreateTableFunctionInfo info(func);
	info.descriptions.push_back(MakeFunctionDescription(
	    "Inspect the arguments of every function and macro exposed by every attached VGI catalog, one row per "
	    "argument. function_type is scalar/table/aggregate for functions and scalar_macro/table_macro for macros. "
	    "Surfaces per-argument detail that duckdb_functions() flattens away: named-vs-positional, const, "
	    "varargs, table-input, any-type, function-level input_from_args correlated-input capability, the "
	    "per-argument description (vgi_doc), and discovery-facing "
	    "constraints (arg_default, arg_choices, arg_range, arg_pattern) so an agent can read valid inputs "
	    "before calling. Pass catalog := '...' to inspect one attachment. Reuses cached catalog metadata. "
	    "Reports each catalog's current data version; does not honor time travel. Macro rows include "
	    "overload_index and preserve explicitly empty documentation; their arg_default is a SQL literal "
	    "display of the resolved typed value.",
	    {}, {},
	    {"SELECT * FROM vgi_function_arguments();",
	     "SELECT arg_name, arg_type, is_const, arg_description FROM vgi_function_arguments(catalog := 'example') "
	     "WHERE catalog_name = 'example' AND function_name = 'multiply' ORDER BY arg_position;",
	     "SELECT arg_name, arg_range, arg_choices, arg_pattern FROM vgi_function_arguments() "
	     "WHERE catalog_name = 'example' AND function_name = 'format_number' ORDER BY field_index;"}));
	loader.RegisterFunction(std::move(info));
}

} // namespace vgi
} // namespace duckdb
