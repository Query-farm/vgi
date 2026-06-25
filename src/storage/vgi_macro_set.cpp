// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "storage/vgi_macro_set.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp"
#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/function/table_macro_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"

#include "storage/vgi_catalog.hpp"
#include "storage/vgi_schema_entry.hpp"
#include "storage/vgi_transaction.hpp"
#include "vgi_catalog_rpc.hpp"
#include "vgi_logging.hpp"
#include "vgi_rpc_types.hpp"

namespace duckdb {

VgiMacroSet::VgiMacroSet(Catalog &catalog, VgiSchemaEntry &schema, CatalogType macro_type)
    : VgiCatalogSet(catalog, &schema), schema_(schema), macro_catalog_type_(macro_type) {
}

// Helper to extract a DuckDB Value from an Arrow array at a given row index
static Value ExtractDefaultValue(const std::shared_ptr<arrow::Array> &array, int64_t row_idx) {
	if (!array || array->IsNull(row_idx)) {
		return Value();
	}

	switch (array->type()->id()) {
	case arrow::Type::BOOL: {
		auto typed = std::static_pointer_cast<arrow::BooleanArray>(array);
		return Value::BOOLEAN(typed->Value(row_idx));
	}
	case arrow::Type::INT8:
		return Value::TINYINT(std::static_pointer_cast<arrow::Int8Array>(array)->Value(row_idx));
	case arrow::Type::INT16:
		return Value::SMALLINT(std::static_pointer_cast<arrow::Int16Array>(array)->Value(row_idx));
	case arrow::Type::INT32:
		return Value::INTEGER(std::static_pointer_cast<arrow::Int32Array>(array)->Value(row_idx));
	case arrow::Type::INT64:
		return Value::BIGINT(std::static_pointer_cast<arrow::Int64Array>(array)->Value(row_idx));
	case arrow::Type::FLOAT:
		return Value::FLOAT(std::static_pointer_cast<arrow::FloatArray>(array)->Value(row_idx));
	case arrow::Type::DOUBLE:
		return Value::DOUBLE(std::static_pointer_cast<arrow::DoubleArray>(array)->Value(row_idx));
	case arrow::Type::STRING: {
		auto typed = std::static_pointer_cast<arrow::StringArray>(array);
		return Value(typed->GetString(row_idx));
	}
	case arrow::Type::LARGE_STRING: {
		auto typed = std::static_pointer_cast<arrow::LargeStringArray>(array);
		return Value(typed->GetString(row_idx));
	}
	default: {
		auto scalar_result = array->GetScalar(row_idx);
		if (scalar_result.ok()) {
			return Value(scalar_result.ValueUnsafe()->ToString());
		}
		return Value();
	}
	}
}

void VgiMacroSet::LoadEntries(ClientContext &context, const std::lock_guard<std::mutex> &/*_load_lock*/) {
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();

	if (!attach_params || !attach_result) {
		return;
	}

	// Determine the RPC type string based on catalog type
	std::string rpc_type = (macro_catalog_type_ == CatalogType::MACRO_ENTRY) ? "SCALAR_MACRO" : "TABLE_MACRO";

	auto &vgi_tx_load = VgiTransaction::Get(context, catalog_);
	vgi::CatalogRpcContext rpc_ctx{attach_params, attach_result->attach_opaque_data, vgi_tx_load.GetTransactionOpaqueData()};
	rpc_ctx.entity_kind = "schema";
	rpc_ctx.entity_qualifier = schema_.name;

	auto macros = vgi::InvokeCatalogSchemaContentsMacros(rpc_ctx, schema_.name, rpc_type, context);

	for (auto &macro_info : macros) {
		try {
			unique_ptr<MacroFunction> macro_func;

			if (macro_catalog_type_ == CatalogType::MACRO_ENTRY) {
				// Scalar macro: parse expression
				auto expressions = Parser::ParseExpressionList(macro_info.definition);
				if (expressions.empty()) {
					VGI_STDERR_DEBUG("[VGI] macro.parse_warning name=%s error=empty expression\n",
					                 macro_info.name.c_str());
					continue;
				}
				macro_func = make_uniq<ScalarMacroFunction>(std::move(expressions[0]));
			} else {
				// Table macro: parse query
				Parser parser;
				parser.ParseQuery(macro_info.definition);
				if (parser.statements.empty() ||
				    parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
					VGI_STDERR_DEBUG("[VGI] macro.parse_warning name=%s error=not a select statement\n",
					                 macro_info.name.c_str());
					continue;
				}
				auto &select = parser.statements[0]->Cast<SelectStatement>();
				macro_func = make_uniq<TableMacroFunction>(std::move(select.node));
			}

			// Add parameters as ColumnRefExpression
			for (const auto &param_name : macro_info.parameters) {
				macro_func->parameters.push_back(make_uniq<ColumnRefExpression>(param_name));
			}

			// Parse default values from IPC bytes if present
			if (!macro_info.parameter_default_values_bytes.empty()) {
				auto defaults_batch =
				    vgi::DeserializeFromIpcBytes(macro_info.parameter_default_values_bytes);
				if (defaults_batch && defaults_batch->num_rows() > 0) {
					for (int i = 0; i < defaults_batch->num_columns(); i++) {
						auto col_name = defaults_batch->schema()->field(i)->name();
						auto value = ExtractDefaultValue(defaults_batch->column(i), 0);
						macro_func->default_parameters[col_name] =
						    make_uniq<ConstantExpression>(std::move(value));
					}
				}
			}

			// Create the catalog entry
			CreateMacroInfo info(macro_catalog_type_);
			info.name = macro_info.name;
			info.schema = schema_.name;
			info.internal = true;
			// Propagate the worker's object-level metadata so it reaches
			// duckdb_functions().comment / .tags (the macro entry constructor
			// copies comment/tags from the CreateInfo, unlike FunctionEntry).
			// Without this, macros surface with NULL comment + empty tags and
			// fail the metadata linter even when the worker supplies them.
			if (!macro_info.comment.empty()) {
				info.comment = Value(macro_info.comment);
			}
			for (const auto &[key, val] : macro_info.tags) {
				info.tags[key] = val;
			}
			info.macros.push_back(std::move(macro_func));

			unique_ptr<CatalogEntry> entry;
			if (macro_catalog_type_ == CatalogType::MACRO_ENTRY) {
				entry = make_uniq<ScalarMacroCatalogEntry>(catalog_, schema_, info);
			} else {
				entry = make_uniq<TableMacroCatalogEntry>(catalog_, schema_, info);
			}

			{ std::lock_guard<std::mutex> __entry_lk(entry_lock_); CreateEntryLocked(std::move(entry)); }
		} catch (const std::exception &e) {
			VGI_STDERR_DEBUG("[VGI] macro.parse_error name=%s error=%s\n", macro_info.name.c_str(), e.what());
		} catch (...) {
			VGI_STDERR_DEBUG("[VGI] macro.parse_error name=%s error=unknown\n", macro_info.name.c_str());
		}
	}
}

} // namespace duckdb
