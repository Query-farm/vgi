// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "storage/vgi_macro_set.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/function/table_macro_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/type_expression.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"

#include "storage/vgi_catalog.hpp"
#include "storage/vgi_schema_entry.hpp"
#include "storage/vgi_transaction.hpp"
#include "vgi_catalog_rpc.hpp"
#include "vgi_macro_contract.hpp"
#include "vgi_logging.hpp"
#include "vgi_rpc_types.hpp"

namespace duckdb {

static void QualifyWorkerFunctions(QueryNode &node, const case_insensitive_set_t &worker_functions,
                                   const string &catalog_name, const string &schema_name);

static void QualifyWorkerFunctions(ParsedExpression &expression, const case_insensitive_set_t &worker_functions,
                                   const string &catalog_name, const string &schema_name) {
	if (expression.HasParameter())
		throw InvalidInputException("VGI macro bodies cannot contain query parameters");
	if (expression.GetExpressionClass() == ExpressionClass::WINDOW) {
		throw NotImplementedException("VGI native macro window expressions require a qualified adapter");
	}
	if (expression.GetExpressionClass() == ExpressionClass::FUNCTION) {
		auto &function = expression.Cast<FunctionExpression>();
		if (!function.catalog.empty())
			throw InvalidInputException("VGI macro body cannot name a client catalog");
		if (!function.schema.empty()) {
			function.catalog = catalog_name;
		} else if (worker_functions.find(function.function_name) != worker_functions.end()) {
			function.catalog = catalog_name;
			function.schema = schema_name;
		} else {
			// Built-ins must not be rebound to a caller's temporary macro.
			function.catalog = SYSTEM_CATALOG;
			function.schema = DEFAULT_SCHEMA;
		}
	} else if (expression.GetExpressionClass() == ExpressionClass::CAST) {
		auto &cast = expression.Cast<CastExpression>();
		if (cast.cast_type.id() == LogicalTypeId::UNBOUND) {
			auto &type = UnboundType::GetTypeExpression(cast.cast_type);
			if (type->GetExpressionClass() != ExpressionClass::TYPE ||
			    !type->Cast<TypeExpression>().GetCatalog().empty() ||
			    !type->Cast<TypeExpression>().GetSchema().empty()) {
				throw NotImplementedException("VGI macro cast requires an unqualified built-in type");
			}
			cast.cast_type = UnboundType::TryDefaultBind(cast.cast_type);
			if (cast.cast_type.IsUnbound())
				throw NotImplementedException("VGI macro cast requires a built-in type");
		}
	} else if (expression.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		auto &subquery = expression.Cast<SubqueryExpression>();
		QualifyWorkerFunctions(*subquery.subquery->node, worker_functions, catalog_name, schema_name);
	}

	ParsedExpressionIterator::EnumerateChildren(expression, [&](ParsedExpression &child) {
		QualifyWorkerFunctions(child, worker_functions, catalog_name, schema_name);
	});
}

static void QualifyWorkerFunctions(QueryNode &node, const case_insensitive_set_t &worker_functions,
                                   const string &catalog_name, const string &schema_name) {
	if (node.type != QueryNodeType::SELECT_NODE || !node.cte_map.map.empty()) {
		throw NotImplementedException(
		    "VGI native macro query qualification currently supports SELECT without CTEs or set operations");
	}
	ParsedExpressionIterator::EnumerateQueryNodeChildren(
	    node,
	    [&](unique_ptr<ParsedExpression> &expression) {
		    QualifyWorkerFunctions(*expression, worker_functions, catalog_name, schema_name);
	    },
	    [&](TableRef &ref) {
		    if (ref.type == TableReferenceType::BASE_TABLE) {
			    auto &table = ref.Cast<BaseTableRef>();
			    if (!table.catalog_name.empty())
				    throw InvalidInputException("VGI macro body cannot name a client catalog");
			    table.catalog_name = catalog_name;
			    if (table.schema_name.empty())
				    table.schema_name = schema_name;
		    } else if (ref.type == TableReferenceType::SUBQUERY) {
			    auto &query = ref.Cast<SubqueryRef>().subquery->node;
			    if (query->type != QueryNodeType::SELECT_NODE || !query->cte_map.map.empty()) {
				    throw NotImplementedException(
				        "VGI native macro subquery qualification requires SELECT without CTEs or set operations");
			    }
		    }
	    });
}

VgiMacroSet::VgiMacroSet(Catalog &catalog, VgiSchemaEntry &schema, CatalogType macro_type)
    : VgiCatalogSet(catalog, &schema), schema_(schema), macro_catalog_type_(macro_type) {
}

void VgiMacroSet::LoadEntries(ClientContext &context, const std::lock_guard<std::mutex> & /*_load_lock*/) {
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();

	if (!attach_params || !attach_result) {
		return;
	}

	auto &vgi_tx_load = VgiTransaction::Get(context, catalog_);
	vgi::CatalogRpcContext rpc_ctx {attach_params, attach_result->attach_opaque_data,
	                                vgi_tx_load.GetTransactionOpaqueData()};
	rpc_ctx.entity_kind = "schema";
	rpc_ctx.entity_qualifier = schema_.name;

	auto macros = schema_.GetMacroInventory(rpc_ctx, macro_catalog_type_, context);
	if (macros.empty()) {
		return;
	}
	auto worker_functions = schema_.GetMacroCallableNames(rpc_ctx, ReadTrustEmptyKinds(context), context);

	vgi::RequireNativeMacroContext(context);
	std::vector<unique_ptr<CatalogEntry>> pending;
	case_insensitive_set_t family_names;
	for (auto &macro_info : macros) {
		if (macro_info.schema_name != schema_.name ||
		    macro_info.macro_type != (macro_catalog_type_ == CatalogType::MACRO_ENTRY ? "scalar" : "table")) {
			throw InvalidInputException("VGI macro family kind or owner does not match its catalog listing");
		}
		if (!family_names.insert(macro_info.name).second) {
			throw InvalidInputException("VGI macro listing contains a duplicate family name");
		}
		CreateMacroInfo info(macro_catalog_type_);
		info.name = macro_info.name;
		info.catalog = catalog_.GetName();
		info.schema = schema_.name;
		info.internal = true;
		if (!macro_info.comment.empty())
			info.comment = Value(macro_info.comment);
		for (const auto &[key, val] : macro_info.tags)
			info.tags[key] = val;
		std::vector<vector<LogicalType>> signatures;
		for (const auto &overload : macro_info.overloads) {
			unique_ptr<MacroFunction> function;
			if (macro_catalog_type_ == CatalogType::MACRO_ENTRY) {
				auto expressions = Parser::ParseExpressionList(overload.definition);
				if (expressions.size() != 1)
					throw InvalidInputException("VGI scalar macro requires exactly one expression");
				QualifyWorkerFunctions(*expressions[0], *worker_functions, catalog_.GetName(), schema_.name);
				function = make_uniq<ScalarMacroFunction>(std::move(expressions[0]));
			} else {
				Parser parser;
				parser.ParseQuery(overload.definition);
				if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
					throw InvalidInputException("VGI table macro requires exactly one SELECT statement");
				}
				auto &select = parser.statements[0]->Cast<SelectStatement>();
				QualifyWorkerFunctions(*select.node, *worker_functions, catalog_.GetName(), schema_.name);
				function = make_uniq<TableMacroFunction>(std::move(select.node));
			}
			for (const auto &parameter : overload.parameters) {
				function->parameters.push_back(make_uniq<ColumnRefExpression>(parameter.name));
				auto type = vgi::NativeMacroParameterType(context, parameter);
				function->types.push_back(type);
				if (parameter.default_value) {
					auto value = vgi::NativeMacroDefaultValue(context, parameter);
					if (type != LogicalType::UNKNOWN &&
					    CastFunctionSet::ImplicitCastCost(context, value.type(), type) < 0) {
						throw InvalidInputException("VGI macro literal default is incompatible with its declared type");
					}
					function->default_parameters[parameter.name] = make_uniq<ConstantExpression>(std::move(value));
				}
			}
			for (const auto &signature : signatures) {
				if (signature == function->types)
					throw InvalidInputException("VGI macro family has duplicate resolved signatures");
			}
			signatures.push_back(function->types);
			info.macros.push_back(std::move(function));
		}
		if (macro_catalog_type_ == CatalogType::MACRO_ENTRY) {
			pending.push_back(make_uniq<ScalarMacroCatalogEntry>(catalog_, schema_, info));
		} else {
			pending.push_back(make_uniq<TableMacroCatalogEntry>(catalog_, schema_, info));
		}
	}
	// Parsing/importing every member succeeds before any family is visible.
	// In particular an invalid later overload cannot install its first member.
	std::lock_guard<std::mutex> lock(entry_lock_);
	for (auto &entry : pending)
		CreateEntryLocked(std::move(entry));
}

optional_ptr<CatalogEntry> VgiMacroSet::GetEntry(ClientContext &context, const std::string &name) {
	auto entry = VgiCatalogSet::GetEntry(context, name);
	if (entry)
		vgi::RequireNativeMacroContext(context);
	return entry;
}

} // namespace duckdb
