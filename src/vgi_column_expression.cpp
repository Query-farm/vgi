// © Copyright 2026 Query Farm LLC - https://query.farm
#include "vgi_column_expression.hpp"

#include "generated/vgi_protocol_schemas.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_record_access.hpp"
#include "vgi_constraint_contract.hpp"
#include "vgi_query_function.hpp"
#include "vgi_rpc_types.hpp"
#include "nanoarrow/nanoarrow.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_properties.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/type_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/catalog/catalog_entry/column_dependency_manager.hpp"
#include "duckdb/parser/column_list.hpp"
#include <set>

namespace duckdb {
namespace vgi {
namespace {
constexpr size_t MAX_SQL = 1024 * 1024;
constexpr size_t MAX_RECORD = 8 * 1024 * 1024;

void Require(bool condition, const std::string &message) {
	if (!condition)
		throw InvalidInputException("VGI column expression: %s", message);
}

void Text(const std::string &text, size_t limit, const char *label) {
	Require(!text.empty() && text.size() <= limit && text.find('\0') == std::string::npos,
	        std::string("invalid ") + label);
}

void Context(const VgiQueryEvaluationContext &context) {
	Require(context.timezone == "UTC" && context.collation == "binary" && context.default_order == "asc" &&
	            context.default_null_order == "nulls_last" && context.preserve_insertion_order,
	        "native adapter supports only the default explicit evaluation context");
}

bool Primitive(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::SQLNULL:
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::VARCHAR:
		return true;
	default:
		return false;
	}
}

bool GeneratedPrimitive(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::SQLNULL:
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::VARCHAR:
		return true;
	default:
		return false;
	}
}

void Scalar(ParsedExpression &expression, bool using_expression, bool qualify_functions, size_t depth, size_t &nodes,
            bool generated = false) {
	Require(depth <= 64 && ++nodes <= 4096, "scalar expression is too complex");
	Require(!expression.HasAlias(), "scalar aliases are unsupported");
	switch (expression.GetExpressionClass()) {
	case ExpressionClass::CONSTANT:
		Require((generated ? GeneratedPrimitive
		                   : Primitive)(expression.Cast<ConstantExpression>().GetLiteral().ToValue().type()),
		        "unsupported scalar literal type");
		break;
	case ExpressionClass::COLUMN_REF: {
		auto &column = expression.Cast<ColumnRefExpression>();
		Require(using_expression && column.ColumnNames().size() == 1,
		        "column references are unsupported in this scope");
		Text(column.ColumnNames()[0].GetIdentifierName(), 4096, "column name");
		break;
	}
	case ExpressionClass::CAST: {
		auto &cast = expression.Cast<CastExpression>();
		const auto &target = cast.TargetType();
		Require(target.GetQualifiedName().Path().size() == 1, "qualified scalar cast types are unsupported");
		auto cast_type = UnboundType::TryDefaultBind(target);
		Require(!cast.IsTryCast() && (generated ? GeneratedPrimitive : Primitive)(cast_type),
		        "unsupported scalar cast");
		if (qualify_functions)
			QualifyNativeBuiltinType(cast.TargetTypeMutable());
		break;
	}
	case ExpressionClass::COMPARISON:
		switch (expression.GetExpressionType()) {
		case ExpressionType::COMPARE_EQUAL:
		case ExpressionType::COMPARE_NOTEQUAL:
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			break;
		default:
			throw InvalidInputException("VGI column expression: unsupported comparison");
		}
		break;
	case ExpressionClass::CONJUNCTION:
		Require(expression.GetExpressionType() == ExpressionType::CONJUNCTION_AND ||
		            expression.GetExpressionType() == ExpressionType::CONJUNCTION_OR,
		        "unsupported conjunction");
		break;
	case ExpressionClass::OPERATOR:
		Require(expression.GetExpressionType() == ExpressionType::OPERATOR_COALESCE ||
		            expression.GetExpressionType() == ExpressionType::OPERATOR_IS_NULL ||
		            expression.GetExpressionType() == ExpressionType::OPERATOR_IS_NOT_NULL ||
		            expression.GetExpressionType() == ExpressionType::OPERATOR_NOT,
		        "unsupported scalar operator");
		break;
	case ExpressionClass::FUNCTION: {
		auto &function = expression.Cast<FunctionExpression>();
		for (const auto &argument : function.GetArguments())
			Require(!argument.HasName(), "named scalar arguments are unsupported");
		static const std::set<std::string> allowed {"+", "-", "*", "%", "abs", "lower", "upper"};
		Require(function.GetQualifiedName().Path().size() == 1 && !function.Distinct() && !function.Filter() &&
		            !function.ExportState() && (!function.OrderBy() || function.OrderBy()->orders.empty()) &&
		            (allowed.count(StringUtil::Lower(function.FunctionName().GetIdentifierName())) ||
		             (using_expression && !generated && function.FunctionName().GetIdentifierName() == "/") ||
		             (generated && function.FunctionName().GetIdentifierName() == "||")),
		        "unsupported scalar function");
		if (qualify_functions) {
			// These names identify built-ins in the bounded profile, never a macro
			// supplied by the caller's search path when the default is later bound.
			function.SetQualifiedName("system", "main", function.FunctionName());
		}
		break;
	}
	default:
		throw InvalidInputException(
		    "VGI column expression: expression is outside the supported deterministic scalar grammar");
	}
	ParsedExpressionIterator::EnumerateChildren(expression, [&](ParsedExpression &child) {
		Scalar(child, using_expression, qualify_functions, depth + 1, nodes, generated);
	});
}

unique_ptr<ParsedExpression> Parse(const std::string &sql, bool using_expression, bool qualify_functions,
                                   bool generated = false) {
	Text(sql, MAX_SQL, "SQL");
	Parser parser {ParserOptions::Builtin()};
	parser.ParseQuery("SELECT (" + sql + ")");
	Require(parser.statements.size() == 1 && parser.statements[0]->type == StatementType::SELECT_STATEMENT,
	        "expected exactly one scalar expression");
	auto &node = parser.statements[0]->Cast<SelectStatement>().node;
	Require(node->type == QueryNodeType::SELECT_NODE, "query expressions are unsupported");
	auto &select = node->Cast<SelectNode>();
	Require(select.select_list.size() == 1 && select.from_table &&
	            select.from_table->type == TableReferenceType::EMPTY_FROM && select.modifiers.empty() &&
	            select.cte_map.map.empty() && !select.where_clause && select.groups.group_expressions.empty() &&
	            select.groups.grouping_sets.empty() && !select.having && !select.qualify && !select.sample,
	        "query clauses are unsupported");
	auto expression = std::move(select.select_list[0]);
	size_t nodes = 0;
	Scalar(*expression, using_expression, qualify_functions, 0, nodes, generated);
	return expression;
}

std::string ReadExpression(const std::vector<uint8_t> &bytes, const std::string &schema_name) {
	auto record = DeserializeVgiQueryRecord(bytes);
	Require(record->num_rows() == 1, "record must contain exactly one row");
	ValidateVgiQueryResultBatch(record, generated::SqlExpressionSchema());
	RecordBatchSingleRow row(record, 0, "SqlExpression", "column");
	Require(row["profile"].value_not_null<std::string>() == VGI_CONSTRAINT_SCALAR_READ, "unsupported scalar profile");
	auto path = record->GetColumnByName("schema_path");
	int64_t begin = 0, end = 0;
	path.ListRange(0, begin, end);
	Require(end - begin == 1 && !path.child(0).IsNull(begin), "invalid owner schema path");
	auto owner = row["schema_path"].value_not_null<std::vector<std::string>>();
	Require(StringUtil::CIEquals(owner[0], schema_name), "expression belongs to another owner schema");
	auto settings = record->GetColumnByName("evaluation_context");
	VgiBatchBuilder builder(generated::QueryEvaluationContextSchema());
	for (int i = 0; i < builder.num_columns(); ++i)
		AppendElement(builder.column(i), settings.child(i), 0);
	builder.FinishRow();
	auto batch = builder.Finish();
	ValidateVgiQueryResultBatch(batch, generated::QueryEvaluationContextSchema());
	RecordBatchSingleRow values(batch, 0, "QueryEvaluationContext", "column");
	VgiQueryEvaluationContext context;
	context.timezone = values["timezone"].value_not_null<std::string>();
	context.collation = values["collation"].value_not_null<std::string>();
	context.default_order = values["default_order"].value_not_null<std::string>();
	context.default_null_order = values["default_null_order"].value_not_null<std::string>();
	context.preserve_insertion_order = values["preserve_insertion_order"].value_not_null<bool>();
	Context(context);
	return row["sql"].value_not_null<std::string>();
}
} // namespace

void QualifyNativeBuiltinType(TypeExpression &type) {
	type.SetQualifiedName(SYSTEM_CATALOG, DEFAULT_SCHEMA, type.GetTypeName());
	for (auto &child : type.GetChildren()) {
		if (child->GetExpressionClass() == ExpressionClass::TYPE)
			QualifyNativeBuiltinType(child->Cast<TypeExpression>());
	}
}

unique_ptr<ParsedExpression> NativeLiteralExpression(const Value &value) {
	auto result = ConstantExpression::FromValue(value);
	std::function<void(ParsedExpression &)> qualify = [&](ParsedExpression &expression) {
		if (expression.GetExpressionClass() == ExpressionClass::CAST)
			QualifyNativeBuiltinType(expression.Cast<CastExpression>().TargetTypeMutable());
		else if (expression.GetExpressionClass() == ExpressionClass::FUNCTION) {
			auto &function = expression.Cast<FunctionExpression>();
			function.SetQualifiedName(SYSTEM_CATALOG, DEFAULT_SCHEMA, function.FunctionName());
		}
		ParsedExpressionIterator::EnumerateChildren(expression, qualify);
	};
	qualify(*result);
	return result;
}

VgiQueryEvaluationContext PushdownEvaluationContext(ClientContext &context) {
	VgiQueryEvaluationContext result;
	auto setting = [&](const char *name) {
		Value value;
		Require(context.TryGetCurrentSetting(name, value) && !value.IsNull(),
		        std::string("missing session setting ") + name);
		return value;
	};
	// Use the engine's effective timezone (UTC when ICU has not registered the
	// optional setting), rather than inventing a different adapter fallback.
	result.timezone = context.GetClientProperties().time_zone;
	if (result.timezone == "Etc/UTC" || result.timezone == "Etc/GMT" || result.timezone == "GMT")
		result.timezone = "UTC";
	result.collation = setting("default_collation").GetValue<std::string>();
	if (result.collation.empty())
		result.collation = "binary";
	result.default_order = StringUtil::Lower(setting("default_order").GetValue<std::string>());
	if (result.default_order == "ascending")
		result.default_order = "asc";
	if (result.default_order == "descending")
		result.default_order = "desc";
	result.default_null_order = StringUtil::Lower(setting("default_null_order").GetValue<std::string>());
	result.preserve_insertion_order = setting("preserve_insertion_order").GetValue<bool>();
	Require(!setting("integer_division").GetValue<bool>() && setting("ieee_floating_point_ops").GetValue<bool>() &&
	            !setting("old_implicit_casting").GetValue<bool>(),
	        "native adapter requires default division and implicit-cast settings");
	Context(result);
	return result;
}

static std::vector<uint8_t> EncodeColumnExpression(const std::string &sql, const std::string &schema_name,
                                                   const VgiQueryEvaluationContext &context) {
	Text(schema_name, 4096, "owner schema");
	Context(context);
	VgiBatchBuilder builder(generated::SqlExpressionSchema());
	append::String(builder.column(0), sql);
	append::String(builder.column(1), VGI_CONSTRAINT_SCALAR_READ);
	append::String(builder.column(2)->children[0], schema_name);
	append::FinishElement(builder.column(2));
	auto settings = builder.column(3);
	append::String(settings->children[0], context.timezone);
	append::String(settings->children[1], context.collation);
	append::String(settings->children[2], context.default_order);
	append::String(settings->children[3], context.default_null_order);
	append::Bool(settings->children[4], context.preserve_insertion_order);
	append::FinishElement(settings);
	builder.FinishRow();
	return SerializeToIpcBytes(builder.Finish());
}

std::vector<uint8_t> BuildNativeColumnExpression(const std::string &sql, const std::string &schema_name,
                                                 const VgiQueryEvaluationContext &context, bool using_expression) {
	Parse(sql, using_expression, false);
	return EncodeColumnExpression(sql, schema_name, context);
}

std::vector<uint8_t> BuildNativeGeneratedExpression(const std::string &sql, const std::string &schema_name,
                                                    const VgiQueryEvaluationContext &context) {
	Parse(sql, true, false, true);
	return EncodeColumnExpression(sql, schema_name, context);
}

unique_ptr<ParsedExpression> ReadNativeColumnGenerated(const ArrowSchema &field, const std::string &schema_name) {
	if (!field.metadata)
		return nullptr;
	auto metadata = VgiMetadata::FromC(field.metadata);
	std::set<std::string> keys;
	for (int64_t i = 0; i < metadata.size(); ++i) {
		const auto &key = metadata.key(i);
		Require(keys.insert(key).second, "duplicate column metadata key");
		Require((key.compare(0, 21, "vgi.column.generated.") != 0 || key == VGI_COLUMN_GENERATED) &&
		            (key.compare(0, 22, "vgi.column.generation.") != 0 || key == VGI_COLUMN_GENERATION),
		        "unknown generated metadata version");
	}
	Require(!keys.count("generated_expression"),
	        "legacy generated_expression requires canonical SqlExpression metadata");
	auto generated = metadata.FindKey(VGI_COLUMN_GENERATED);
	auto category = metadata.FindKey(VGI_COLUMN_GENERATION);
	Require((generated >= 0) == (category >= 0), "generated expression and category must occur together");
	if (generated < 0)
		return nullptr;
	Require(metadata.value(category) == "virtual", "native adapter supports only VIRTUAL generated columns");
	Require(!keys.count(VGI_COLUMN_DEFAULT) && !keys.count("default"),
	        "generated column conflicts with default metadata");
	Require((field.flags & ARROW_FLAG_NULLABLE) != 0, "native generated columns must be nullable");
	if (!DescribeType(field).extension_name.empty())
		throw NotImplementedException("VGI column expressions require a qualified adapter for extension types");
	switch (DescribeType(field).type) {
	case NANOARROW_TYPE_BOOL:
	case NANOARROW_TYPE_INT8:
	case NANOARROW_TYPE_INT16:
	case NANOARROW_TYPE_INT32:
	case NANOARROW_TYPE_INT64:
	case NANOARROW_TYPE_STRING:
		break;
	default:
		throw NotImplementedException("VGI generated columns require signed integer, Boolean or string types");
	}
	const auto &encoded = metadata.value(generated);
	Require(!encoded.empty() && encoded.size() <= ((MAX_RECORD + 2) / 3) * 4, "invalid generated metadata size");
	auto decoded = Blob::FromBase64(string_t(encoded));
	Require(Blob::ToBase64(string_t(decoded)) == encoded, "generated metadata requires canonical base64");
	auto sql = ReadExpression(std::vector<uint8_t>(decoded.begin(), decoded.end()), schema_name);
	return Parse(sql, true, true, true);
}

void ValidateNativeGeneratedColumns(ClientContext &context, const ColumnList &columns) {
	bool generated = false;
	for (const auto &column : columns.Logical())
		generated = generated || column.Generated();
	if (!generated)
		return;
	Require(columns.PhysicalColumnCount() > 0, "generated table requires a physical column");
	ColumnDependencyManager dependencies;
	vector<Identifier> names;
	vector<LogicalType> types;
	for (const auto &column : columns.Logical()) {
		names.push_back(column.Name());
		types.push_back(column.Type());
		if (!column.Generated())
			continue;
		Require(GeneratedPrimitive(column.Type()) && column.Type() != LogicalType::SQLNULL,
		        "unsupported generated column type");
		dependencies.AddGeneratedColumn(column, columns);
		vector<string> referenced;
		column.GetListOfDependencies(referenced);
		for (const auto &name : referenced)
			Require(GeneratedPrimitive(columns.GetColumn(duckdb::Identifier(name)).Type()),
			        "unsupported generated operand type");
	}
	auto binder = Binder::CreateBinder(context);
	binder->bind_context.AddGenericBinding(binder->GenerateTableIndex(), "__vgi_generated", names, types);
	ExpressionBinder expressions(*binder, context);
	for (const auto &column : columns.Logical()) {
		if (!column.Generated())
			continue;
		auto expression = column.GeneratedExpression().Copy();
		auto bound = expressions.Bind(expression);
		Require(bound->GetReturnType() == column.Type(), "generated result type differs from its column");
		if (bound->IsFoldable())
			ExpressionExecutor::EvaluateScalar(context, *bound);
	}
}

std::string EncodeNativeColumnDefaultMetadata(const std::vector<uint8_t> &expression) {
	Require(!expression.empty() && expression.size() <= MAX_RECORD, "invalid default record size");
	return Blob::ToBase64(string_t(reinterpret_cast<const char *>(expression.data()), expression.size()));
}

unique_ptr<ParsedExpression> ReadNativeColumnDefault(const ArrowSchema &field, const std::string &schema_name) {
	if (!field.metadata)
		return nullptr;
	auto metadata = VgiMetadata::FromC(field.metadata);
	std::set<std::string> keys;
	for (int64_t i = 0; i < metadata.size(); ++i) {
		Require(keys.insert(metadata.key(i)).second, "duplicate column metadata key");
	}
	auto index = metadata.FindKey(VGI_COLUMN_DEFAULT);
	if (index < 0)
		return nullptr;
	if (!DescribeType(field).extension_name.empty())
		throw NotImplementedException("VGI column expressions require a qualified adapter for extension types");
	switch (DescribeType(field).type) {
	case NANOARROW_TYPE_BOOL:
	case NANOARROW_TYPE_INT8:
	case NANOARROW_TYPE_INT16:
	case NANOARROW_TYPE_INT32:
	case NANOARROW_TYPE_INT64:
	case NANOARROW_TYPE_FLOAT:
	case NANOARROW_TYPE_DOUBLE:
	case NANOARROW_TYPE_DECIMAL128:
	case NANOARROW_TYPE_STRING:
		break;
	default:
		throw NotImplementedException(
		    "VGI column expression: native defaults require primitive signed numeric, Boolean or string columns");
	}
	Require(!keys.count("default") && !keys.count("generated_expression") && !keys.count("vgi.column.generated.v1") &&
	            !keys.count("vgi.column.generation.v1"),
	        "conflicting column expression metadata");
	const auto &encoded = metadata.value(index);
	Require(!encoded.empty() && encoded.size() <= ((MAX_RECORD + 2) / 3) * 4, "invalid default metadata size");
	auto decoded = Blob::FromBase64(string_t(encoded));
	Require(Blob::ToBase64(string_t(decoded)) == encoded, "default metadata requires canonical base64");
	auto sql = ReadExpression(std::vector<uint8_t>(decoded.begin(), decoded.end()), schema_name);
	return Parse(sql, false, true);
}

unique_ptr<ParsedExpression> BindNativeColumnDefault(ClientContext &context, const ArrowSchema &field,
                                                     const std::string &schema_name, const LogicalType &target_type) {
	auto expression = ReadNativeColumnDefault(field, schema_name);
	if (!expression)
		return nullptr;
	auto binder = Binder::CreateBinder(context);
	ConstantBinder constants(*binder, context, "VGI column default");
	constants.target_type = target_type;
	auto bound = constants.Bind(expression);
	Require(bound->IsFoldable(), "column default must be deterministic and constant");
	// Cached table entries must not rebind expression functions/collation against
	// a later INSERT session. Keep the authoritative SQL/context in table metadata
	// and install the resolved literal for this bounded constant-only profile.
	return NativeLiteralExpression(ExpressionExecutor::EvaluateScalar(context, *bound));
}

VgiSchemaPtr NativeColumnTypeSchema(const VgiSchemaPtr &schema, const std::string &schema_name) {
	nanoarrow::UniqueSchema output;
	schema->ExportTo(output.get());
	for (int i = 0; i < schema->num_fields(); ++i) {
		const auto &field = schema->field(i);
		auto generated = ReadNativeColumnGenerated(field, schema_name);
		if (!ReadNativeColumnDefault(field, schema_name) && !generated)
			continue;
		auto metadata = VgiMetadata::FromC(field.metadata);
		VgiMetadata filtered;
		for (const auto &entry : metadata.pairs()) {
			if ((generated && (entry.first == VGI_COLUMN_GENERATED || entry.first == VGI_COLUMN_GENERATION)) ||
			    (!generated && entry.first == VGI_COLUMN_DEFAULT))
				continue;
			filtered.Append(entry.first, entry.second);
		}
		auto encoded = filtered.ToC();
		Require(ArrowSchemaSetMetadata(output->children[i], encoded.empty() ? nullptr : encoded.c_str()) ==
		            NANOARROW_OK,
		        "failed to preserve column metadata");
	}
	return VgiSchema::Adopt(output.get());
}
} // namespace vgi
} // namespace duckdb
