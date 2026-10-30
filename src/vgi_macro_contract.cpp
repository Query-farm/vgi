// © Copyright 2026 Query Farm LLC - https://query.farm
#include "vgi_macro_contract.hpp"
#include "generated/vgi_protocol_schemas.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_record_access.hpp"
#include "vgi_column_expression.hpp"
#include "vgi_query_function.hpp"
#include "vgi_rpc_types.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "vgi_ipc.hpp"
#include "nanoarrow/nanoarrow.hpp"

#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/type_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/tableref/list.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {
static void QualifyWorkerFunctions(QueryNode &node, const case_insensitive_set_t &worker_functions,
                                   const string &catalog_name, const string &schema_name,
                                   const case_insensitive_set_t &ctes);

static void QualifyWorkerFunctions(ParsedExpression &expression, const case_insensitive_set_t &worker_functions,
                                   const string &catalog_name, const string &schema_name,
                                   const case_insensitive_set_t &ctes) {
	if (expression.HasParameter())
		throw InvalidInputException("VGI macro bodies cannot contain query parameters");
	if (expression.GetExpressionClass() == ExpressionClass::WINDOW) {
		throw NotImplementedException("VGI native macro window expressions require a qualified adapter");
	}
	if (expression.GetExpressionClass() == ExpressionClass::FUNCTION) {
		auto &function = expression.Cast<FunctionExpression>();
		const auto &qualified = function.GetQualifiedName();
		if (!qualified.Catalog().empty())
			throw InvalidInputException("VGI macro body cannot name a client catalog");
		if (!qualified.Schema().empty()) {
			function.SetQualifiedName(qualified.WithCatalog(Identifier(catalog_name)));
		} else if (worker_functions.count(function.FunctionName().GetIdentifierName())) {
			function.SetQualifiedName(Identifier(catalog_name), Identifier(schema_name), function.FunctionName());
		} else {
			// Built-ins must not be rebound to a caller's temporary macro.
			function.SetQualifiedName(SYSTEM_CATALOG, DEFAULT_SCHEMA, function.FunctionName());
		}
	} else if (expression.GetExpressionClass() == ExpressionClass::CAST) {
		auto &cast = expression.Cast<CastExpression>();
		const auto &target = cast.TargetType();
		if (target.GetQualifiedName().Path().size() != 1)
			throw NotImplementedException("VGI macro cast requires an unqualified built-in type");
		if (UnboundType::TryDefaultBind(target).IsUnbound())
			throw NotImplementedException("VGI macro cast requires a built-in type");
		vgi::QualifyNativeBuiltinType(cast.TargetTypeMutable());
	} else if (expression.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		auto &subquery = expression.Cast<SubqueryExpression>();
		QualifyWorkerFunctions(*subquery.Subquery()->node, worker_functions, catalog_name, schema_name, ctes);
	}

	ParsedExpressionIterator::EnumerateChildren(expression, [&](ParsedExpression &child) {
		QualifyWorkerFunctions(child, worker_functions, catalog_name, schema_name, ctes);
	});
}

static void QualifyWorkerTable(TableRef &ref, const case_insensitive_set_t &worker_functions,
                               const string &catalog_name, const string &schema_name,
                               const case_insensitive_set_t &ctes) {
	auto expression = [&](unique_ptr<ParsedExpression> &expr) {
		if (expr)
			QualifyWorkerFunctions(*expr, worker_functions, catalog_name, schema_name, ctes);
	};
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE: {
		auto &table = ref.Cast<BaseTableRef>();
		if (table.at_clause)
			expression(table.at_clause->ExpressionMutable());
		const auto &qualified = table.GetQualifiedName();
		if (!qualified.Catalog().empty())
			throw InvalidInputException("VGI macro body cannot name a client catalog");
		if (qualified.Schema().empty() && ctes.count(table.Table().GetIdentifierName()))
			return;
		if (qualified.Schema().empty())
			table.SetQualifiedName(Identifier(catalog_name), Identifier(schema_name), table.Table());
		else
			table.SetQualifiedName(qualified.WithCatalog(Identifier(catalog_name)));
		break;
	}
	case TableReferenceType::SUBQUERY:
		QualifyWorkerFunctions(*ref.Cast<SubqueryRef>().subquery->node, worker_functions, catalog_name, schema_name,
		                       ctes);
		break;
	case TableReferenceType::JOIN: {
		auto &join = ref.Cast<JoinRef>();
		QualifyWorkerTable(*join.left, worker_functions, catalog_name, schema_name, ctes);
		QualifyWorkerTable(*join.right, worker_functions, catalog_name, schema_name, ctes);
		expression(join.condition);
		break;
	}
	case TableReferenceType::TABLE_FUNCTION:
		expression(ref.Cast<TableFunctionRef>().function);
		break;
	case TableReferenceType::EXPRESSION_LIST:
		for (auto &row : ref.Cast<ExpressionListRef>().values)
			for (auto &value : row)
				expression(value);
		break;
	case TableReferenceType::EMPTY_FROM:
		break;
	default:
		throw NotImplementedException("VGI native macro table reference requires a qualified adapter");
	}
}

static void QualifyWorkerFunctions(QueryNode &node, const case_insensitive_set_t &worker_functions,
                                   const string &catalog_name, const string &schema_name,
                                   const case_insensitive_set_t &outer_ctes) {
	if (node.type != QueryNodeType::SELECT_NODE && node.type != QueryNodeType::SET_OPERATION_NODE)
		throw NotImplementedException("VGI native macro query qualification requires SELECT or set operations");
	// CTE relation names occupy a separate namespace from callable functions.
	// Definitions see prior CTEs and outer scopes; the body sees all local CTEs.
	auto ctes = outer_ctes;
	for (auto &entry : node.cte_map.map) {
		QualifyWorkerFunctions(*entry.second->query_node, worker_functions, catalog_name, schema_name, ctes);
		ctes.insert(entry.first.GetIdentifierName());
	}
	auto expression = [&](unique_ptr<ParsedExpression> &expr) {
		if (expr)
			QualifyWorkerFunctions(*expr, worker_functions, catalog_name, schema_name, ctes);
	};
	if (node.type == QueryNodeType::SET_OPERATION_NODE) {
		for (auto &child : node.Cast<SetOperationNode>().children)
			QualifyWorkerFunctions(*child, worker_functions, catalog_name, schema_name, ctes);
	} else {
		auto &select = node.Cast<SelectNode>();
		for (auto &expr : select.select_list)
			expression(expr);
		for (auto &expr : select.groups.group_expressions)
			expression(expr);
		expression(select.where_clause);
		expression(select.having);
		expression(select.qualify);
		QualifyWorkerTable(*select.from_table, worker_functions, catalog_name, schema_name, ctes);
	}
	ParsedExpressionIterator::EnumerateQueryNodeModifiers(node, expression);
}

void vgi::QualifyNativeMacroQuery(QueryNode &node, const case_insensitive_set_t &worker_functions,
                                  const string &catalog_name, const string &schema_name) {
	QualifyWorkerFunctions(node, worker_functions, catalog_name, schema_name, {});
}

void vgi::QualifyNativeMacroExpression(ParsedExpression &expression, const case_insensitive_set_t &worker_functions,
                                       const string &catalog_name, const string &schema_name) {
	QualifyWorkerFunctions(expression, worker_functions, catalog_name, schema_name, {});
}

namespace vgi {
namespace {
void Require(bool condition, const char *message) {
	if (!condition)
		throw InvalidInputException("VGI macro family: %s", message);
}
void Identifier(const std::string &name) {
	Require(!name.empty() && name.size() <= 4096 && name.find('\0') == std::string::npos, "invalid identifier");
}
void Strict(const VgiBatchPtr &batch, const VgiSchemaPtr &schema) {
	Require(batch && batch->num_rows() == 1, "record requires exactly one row");
	Require(batch->TotalBufferSize() <= 8 * 1024 * 1024, "record is too large");
	ValidateVgiQueryResultBatch(batch, schema);
}
VgiBatchPtr StructRow(const VgiColumn &array, int64_t index, const VgiSchemaPtr &schema) {
	Require(!array.IsNull(index), "NULL record in family");
	VgiBatchBuilder builder(schema);
	for (int i = 0; i < builder.num_columns(); ++i)
		AppendElement(builder.column(i), array.child(i), index);
	builder.FinishRow();
	auto result = builder.Finish();
	Strict(result, schema);
	return result;
}
void TypeMetadata(const VgiSchemaPtr &schema) {
	Require(schema->metadata().empty(), "schema type metadata requires a qualified adapter");
	std::vector<const ArrowSchema *> pending;
	for (int i = 0; i < schema->num_fields(); ++i)
		pending.push_back(&schema->field(i));
	while (!pending.empty()) {
		auto field = pending.back();
		pending.pop_back();
		Require(VgiMetadata::FromC(field->metadata).empty(), "field type metadata requires a qualified adapter");
		if (field->dictionary)
			pending.push_back(field->dictionary);
		for (int64_t i = 0; i < field->n_children; ++i)
			pending.push_back(field->children[i]);
	}
}
VgiSchemaPtr TypeSchema(const std::vector<uint8_t> &bytes) {
	Require(!bytes.empty() && bytes.size() <= 1024 * 1024, "invalid declared type schema size");
	auto input = std::make_shared<VgiBufferReader>(VgiBuffer::FromVector(bytes));
	auto reader = VgiIpcReader::Open(input);
	Require(input->remaining() == 0, "trailing declared type schema bytes");
	return reader->schema();
}
} // namespace

VgiBatchPtr DecodeMacroInfoRecord(const uint8_t *data, size_t size) {
	Require(size > 0 && size <= 8 * 1024 * 1024, "invalid macro record size");
	auto record = DeserializeVgiQueryRecord(std::vector<uint8_t>(data, data + size));
	Strict(record, generated::MacroInfoSchema());
	return record;
}

VgiMacroInfo ParseMacroInfo(const VgiBatchPtr &batch, const std::string &worker_path) {
	Strict(batch, generated::MacroInfoSchema());
	RecordBatchSingleRow row(batch, 0, "MacroInfo", worker_path);
	VgiMacroInfo info;
	info.name = row["name"].value_not_null<std::string>();
	Identifier(info.name);
	auto path = row["schema_path"].value_not_null<std::vector<std::string>>();
	Require(path.size() == 1, "native adapter requires a raw root owner schema");
	Identifier(path[0]);
	info.schema_name = path[0];
	info.macro_type = StringUtil::Lower(row["macro_type"].value_not_null<std::string>());
	Require(info.macro_type == "scalar" || info.macro_type == "table", "invalid macro kind");
	info.comment = row["comment"].value_or("");
	info.tags = row["tags"].value_not_null<std::map<std::string, std::string>>();
	auto list = batch->GetColumnByName("overloads");
	int64_t begin = 0, end = 0;
	list.ListRange(0, begin, end);
	Require(end > begin && end - begin <= 256, "family must have 1 to 256 overloads");
	auto values = list.child(0);
	for (int64_t index = begin; index < end; ++index) {
		auto record = StructRow(values, index, generated::MacroOverloadSchema());
		RecordBatchSingleRow overload(record, 0, "MacroOverload", worker_path);
		Require(overload["profile"].value_not_null<std::string>() == "duckdb.macro.v1",
		        "unsupported definition profile");
		Require(overload["macro_type"].value_not_null<std::string>() == info.macro_type,
		        "overload kind differs from family");
		Require(overload["schema_path"].value_not_null<std::vector<std::string>>() == path,
		        "overload owner differs from family");
		auto settings =
		    StructRow(record->GetColumnByName("evaluation_context"), 0, generated::QueryEvaluationContextSchema());
		RecordBatchSingleRow context(settings, 0, "Macro context", worker_path);
		Require(context["timezone"].value_not_null<std::string>() == "UTC" &&
		            context["collation"].value_not_null<std::string>() == "binary" &&
		            context["default_order"].value_not_null<std::string>() == "asc" &&
		            context["default_null_order"].value_not_null<std::string>() == "nulls_last" &&
		            context["preserve_insertion_order"].value_not_null<bool>(),
		        "unsupported evaluation context");
		VgiMacroOverload item;
		item.definition = overload["definition"].value_not_null<std::string>();
		Require(!item.definition.empty() && item.definition.size() <= 1024 * 1024 &&
		            item.definition.find('\0') == std::string::npos,
		        "invalid definition SQL");
		auto parameters = record->GetColumnByName("parameters");
		int64_t first = 0, last = 0;
		parameters.ListRange(0, first, last);
		Require(last - first <= 1024, "too many parameters");
		auto parameter_values = parameters.child(0);
		case_insensitive_set_t names;
		bool seen_default = false;
		for (int64_t p = first; p < last; ++p) {
			auto parameter_record = StructRow(parameter_values, p, generated::MacroParameterSchema());
			RecordBatchSingleRow parameter(parameter_record, 0, "MacroParameter", worker_path);
			VgiMacroParameter value;
			value.name = parameter["name"].value_not_null<std::string>();
			Identifier(value.name);
			Require(names.insert(value.name).second, "duplicate parameter name");
			Require(parameter["default_expression"].is_null(), "discovery must return resolved literal defaults");
			if (auto declared = parameter["declared_type"].as<std::vector<uint8_t>>()) {
				value.declared_type = TypeSchema(*declared);
				Require(value.declared_type->num_fields() == 1 && value.declared_type->field(0).name == value.name &&
				            value.declared_type->field_nullable(0) &&
				            DescribeType(value.declared_type->field(0)).type != NANOARROW_TYPE_NA,
				        "invalid declared parameter type");
				TypeMetadata(value.declared_type);
			}
			if (auto literal = parameter["default_value"].as<std::vector<uint8_t>>()) {
				value.default_value = DeserializeVgiQueryRecord(*literal);
				Strict(value.default_value, value.default_value->schema());
				Require(value.default_value->num_columns() == 1 &&
				            value.default_value->schema()->field(0).name == value.name,
				        "literal default must contain its single named parameter");
				TypeMetadata(value.default_value->schema());
			}
			Require(!seen_default || value.default_value, "required parameter follows a default");
			seen_default = seen_default || bool(value.default_value);
			value.documentation = parameter["documentation"].as<std::string>();
			Require(!value.documentation || (value.documentation->size() <= 64 * 1024 &&
			                                 value.documentation->find('\0') == std::string::npos),
			        "invalid parameter documentation");
			item.parameters.push_back(std::move(value));
		}
		info.overloads.push_back(std::move(item));
	}
	return info;
}

LogicalType NativeMacroParameterType(ClientContext &context, const VgiMacroParameter &parameter) {
	if (!parameter.declared_type)
		return LogicalType::UNKNOWN;
	ArrowSchemaWrapper exported;
	ArrowTableSchema table;
	vector<LogicalType> types;
	vector<string> names;
	ArrowSchemaToDuckDBTypes(context, parameter.declared_type, exported, table, types, names);
	return types[0];
}

Value NativeMacroDefaultValue(ClientContext &context, const VgiMacroParameter &parameter) {
	Require(bool(parameter.default_value), "missing literal default");
	ArrowSchemaWrapper exported;
	ArrowTableSchema table;
	vector<LogicalType> types;
	vector<string> names;
	ArrowSchemaToDuckDBTypes(context, parameter.default_value->schema(), exported, table, types, names);
	auto array = make_uniq<ArrowArrayWrapper>();
	ExportRecordBatch(parameter.default_value, *array);
	ArrowScanLocalState state(std::move(array), context);
	DataChunk chunk;
	chunk.Initialize(Allocator::Get(context), types);
	chunk.SetCardinality(1);
	ArrowTableFunction::ArrowToDuckDB(state, table.GetColumns(), chunk, false);
	return chunk.GetValue(0, 0);
}
} // namespace vgi
} // namespace duckdb
