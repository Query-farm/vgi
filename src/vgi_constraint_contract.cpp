// © Copyright 2026 Query Farm LLC - https://query.farm
#include "vgi_constraint_contract.hpp"
#include "vgi_index_contract.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_record_access.hpp"
#include "vgi_query_function.hpp"
#include "vgi_rpc_types.hpp"
#include "generated/vgi_protocol_schemas.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/parser/constraints/check_constraint.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/check_binder.hpp"
#include "vgi_ipc.hpp"
#include "nanoarrow/nanoarrow.hpp"
#include <set>
#include <algorithm>
#include "duckdb/common/string_util.hpp"

namespace duckdb {
namespace vgi {
namespace {
constexpr size_t MAX_BYTES = 8 * 1024 * 1024;
void Require(bool value, const std::string &message) {
	if (!value)
		throw InvalidInputException("VGI constraint protocol: %s", message);
}
void Identifier(const std::string &value) {
	Require(!value.empty() && value.size() <= 4096 && value.find('\0') == std::string::npos, "invalid identifier");
}
void Token(const std::vector<uint8_t> &value) {
	Require(!value.empty() && value.size() <= 65536, "missing or oversized definition revision");
}
void NoListNulls(const VgiColumn &array, int64_t begin, int64_t end) {
	if (array.type().type != NANOARROW_TYPE_LIST)
		return;
	auto values = array.child(0);
	for (int64_t i = begin; i < end; ++i) {
		if (array.IsNull(i))
			continue;
		int64_t first = 0, last = 0;
		array.ListRange(i, first, last);
		for (auto child = first; child < last; ++child)
			Require(!values.IsNull(child), "NULL list elements are not permitted");
		NoListNulls(values, first, last);
	}
}
void Strict(const VgiBatchPtr &batch, const VgiSchemaPtr &schema) {
	Require(batch && batch->num_rows() == 1, "record must contain exactly one row");
	Require(batch->TotalBufferSize() <= MAX_BYTES, "record exceeds size limit");
	ValidateVgiQueryResultBatch(batch, schema);
	for (int i = 0; i < batch->num_columns(); ++i)
		NoListNulls(batch->column(i), 0, 1);
}
VgiBatchPtr Decode(const std::vector<uint8_t> &bytes, const VgiSchemaPtr &schema) {
	Require(!bytes.empty() && bytes.size() <= MAX_BYTES, "invalid record size");
	// Returned Arrow arrays may retain slices of the IPC buffer. Nested callers
	// pass temporary vectors, so the reader must own its storage beyond this call.
	auto input = std::make_shared<VgiBufferReader>(VgiBuffer::FromVector(bytes));
	auto reader = VgiIpcReader::Open(input);
	auto batch = reader->ReadNext();
	Strict(batch, schema);
	Require(!reader->ReadNext(), "unexpected extra batch");
	Require(input->remaining() == 0, "trailing IPC bytes");
	return batch;
}
std::vector<uint8_t> Encode(const VgiSchemaPtr &schema, std::vector<ArrowArray> columns) {
	return SerializeToIpcBytes(VgiBatch::FromColumns(schema, 1, std::move(columns)));
}
std::vector<uint8_t> Ref(const std::string &schema, const std::string &table) {
	Identifier(schema);
	Identifier(table);
	return Encode(generated::CatalogObjectRefSchema(),
	              {BuildStringScalar("table"), BuildStringListScalar({schema}), BuildStringScalar(table),
	               BuildOptionalStringScalar(std::nullopt)});
}
struct Reference {
	std::string name, schema, owner;
};
Reference ReadRef(const std::vector<uint8_t> &bytes, const std::string &kind) {
	auto batch = Decode(bytes, generated::CatalogObjectRefSchema());
	RecordBatchSingleRow row(batch, 0, "CatalogObjectRef", "constraint");
	Require(row["kind"].value_not_null<std::string>() == kind, "wrong object kind");
	auto path = row["schema_path"].value_not_null<std::vector<std::string>>();
	Require(path.size() == 1, "native adapter requires one-component schema paths");
	Reference result {row["name"].value_not_null<std::string>(), path[0], row["owner_table"].value_or(std::string())};
	Identifier(result.name);
	Identifier(result.schema);
	Require(kind == "constraint" ? !result.owner.empty() : batch->GetColumnByName("owner_table").IsNull(0),
	        "invalid constraint owner");
	if (!result.owner.empty())
		Identifier(result.owner);
	return result;
}
using ColumnPath = std::vector<std::string>;
std::vector<ColumnPath> ReadPaths(RecordBatchSingleRow &row, const char *field) {
	auto paths = row[field].value_not_null<std::vector<std::vector<std::string>>>();
	Require(paths.size() <= 1024, "too many column paths");
	std::set<ColumnPath> seen;
	for (const auto &path : paths) {
		Require(!path.empty() && path.size() <= 64, "empty or oversized constraint column path");
		for (const auto &component : path)
			Identifier(component);
		Require(seen.insert(path).second, "duplicate constraint path");
	}
	return paths;
}
std::vector<std::string> TopLevelNames(const std::vector<ColumnPath> &paths) {
	std::vector<std::string> names;
	for (const auto &path : paths) {
		Require(path.size() == 1, "native adapter supports nested constraint paths only for NOT_NULL facts");
		names.push_back(path[0]);
	}
	return names;
}
bool OrdinaryStruct(const ArrowSchema &field) {
	if (DescribeType(field).type != NANOARROW_TYPE_STRUCT)
		return false;
	auto metadata = VgiMetadata::FromC(field.metadata);
	for (const auto &entry : metadata.pairs()) {
		if (entry.first == "ARROW:extension:name" || entry.first.compare(0, 17, "vgi.logical_type.") == 0)
			return false;
	}
	return true;
}
int ChildIndex(const ArrowSchema &parent, const std::string &name) {
	int index = -1;
	for (int64_t i = 0; i < parent.n_children; ++i) {
		if (name == parent.children[i]->name) {
			if (index >= 0)
				return -1;
			index = static_cast<int>(i);
		}
	}
	return index;
}
void ResolveNotNullPath(const VgiTableInfo &info, const ColumnPath &path) {
	auto index = info.arrow_schema->GetFieldIndex(path[0]);
	Require(index >= 0 && index != info.row_id_column,
	        "constraint references a missing, ambiguous, or synthetic column");
	auto field = &info.arrow_schema->field(index);
	for (size_t depth = 1; depth < path.size(); ++depth) {
		Require(OrdinaryStruct(*field), "constraint path must traverse ordinary STRUCT fields, not storage members");
		index = ChildIndex(*field, path[depth]);
		Require(index >= 0, "constraint references a missing or ambiguous nested field");
		field = field->children[index];
	}
}
void ValidateNullability(const ArrowSchema &parent, const ColumnPath &prefix, const std::set<ColumnPath> &required,
                         int row_id_column) {
	for (int64_t i = 0; i < parent.n_children; ++i) {
		if (prefix.empty() && i == row_id_column)
			continue;
		const auto &field = *parent.children[i];
		auto path = prefix;
		path.push_back(field.name);
		Require(((field.flags & ARROW_FLAG_NULLABLE) != 0) == !required.count(path),
		        "Arrow nullability disagrees with canonical NOT_NULL/PRIMARY_KEY");
		if (OrdinaryStruct(field))
			ValidateNullability(field, path, required, row_id_column);
	}
}
ArrowArray PathArray(const std::vector<std::string> &names) {
	VgiBatchBuilder builder(MakeSchema({type::List(type::List(type::Utf8()))}));
	auto list = builder.column(0);
	for (const auto &name : names) {
		Identifier(name);
		append::String(list->children[0]->children[0], name);
		append::FinishElement(list->children[0]);
	}
	append::FinishElement(list);
	builder.FinishRow();
	nanoarrow::UniqueArray array;
	builder.Finish()->ExportTo(array.get());
	ArrowArray result;
	ArrowArrayMove(array->children[0], &result);
	return result;
}
const std::vector<std::string> KINDS {"NOT_NULL", "UNIQUE", "PRIMARY_KEY", "CHECK", "FOREIGN_KEY"};
std::vector<uint8_t> Definition(const std::string &kind, const std::vector<std::string> &names,
                                const std::vector<uint8_t> &foreign = {}) {
	return Encode(generated::ConstraintDefinitionSchema(),
	              {BuildEnumArray(kind, KINDS), PathArray(names), BuildOptionalStringScalar(std::nullopt),
	               BuildOptionalBinaryScalar(std::nullopt),
	               (foreign.empty() ? BuildOptionalBinaryScalar(std::nullopt) : BuildBinaryScalarRequired(foreign))});
}
void Records(const std::vector<std::vector<uint8_t>> &records) {
	Require(records.size() <= 1024, "too many constraints");
	size_t size = 0;
	for (const auto &record : records) {
		Require(!record.empty() && record.size() <= MAX_BYTES, "invalid nested constraint size");
		size += record.size();
		Require(size <= MAX_BYTES, "constraint collection exceeds size limit");
	}
}

// The read profile is deliberately bounded to numeric comparisons. No function,
// cast, collation, subquery, parameter or external name resolution is qualified.
// These expressions are independent of the reader's session evaluation settings.
void NumericOperand(const ParsedExpression &expression, const VgiTableInfo &info) {
	Require(!expression.HasAlias(), "CHECK expressions cannot have aliases");
	if (expression.GetExpressionClass() == ExpressionClass::CONSTANT) {
		const auto &value = expression.Cast<ConstantExpression>().GetLiteral().ToValue();
		Require(!value.IsNull() && value.type().IsNumeric(), "CHECK requires numeric operands");
		return;
	}
	Require(expression.GetExpressionClass() == ExpressionClass::COLUMN_REF,
	        "native CHECK read supports only numeric column/constant comparisons");
	const auto &names = expression.Cast<ColumnRefExpression>().ColumnNames();
	Require(names.size() == 1, "CHECK requires unqualified column references");
	int index = -1;
	for (int i = 0; i < info.arrow_schema->num_fields(); ++i) {
		if (StringUtil::CIEquals(info.arrow_schema->field(i).name, names[0].GetIdentifierName())) {
			Require(index < 0, "ambiguous CHECK column reference");
			index = i;
		}
	}
	Require(index >= 0 && index != info.row_id_column, "CHECK references a missing or synthetic column");
	const auto &field = info.arrow_schema->field(index);
	Require(!VgiMetadata::FromC(field.metadata).Contains("ARROW:extension:name"),
	        "CHECK extension-typed operands are unsupported");
	switch (DescribeType(field).type) {
	case NANOARROW_TYPE_INT8:
	case NANOARROW_TYPE_INT16:
	case NANOARROW_TYPE_INT32:
	case NANOARROW_TYPE_INT64:
	case NANOARROW_TYPE_FLOAT:
	case NANOARROW_TYPE_DOUBLE:
		return;
	default:
		throw InvalidInputException("VGI constraint protocol: CHECK requires primitive signed numeric columns");
	}
}
unique_ptr<ParsedExpression> ParseNumericCheck(const std::string &sql, const VgiTableInfo &info) {
	Require(!sql.empty() && sql.size() <= 1024 * 1024 && sql.find('\0') == std::string::npos,
	        "invalid CHECK SQL size or text");
	Parser parser {ParserOptions::Builtin()};
	parser.ParseQuery("SELECT " + sql);
	Require(parser.statements.size() == 1 && parser.statements[0]->type == StatementType::SELECT_STATEMENT,
	        "CHECK requires exactly one scalar expression");
	auto &node = parser.statements[0]->Cast<SelectStatement>().node;
	Require(node->type == QueryNodeType::SELECT_NODE, "CHECK requires exactly one scalar expression");
	auto &select = node->Cast<SelectNode>();
	Require(select.select_list.size() == 1 && select.from_table &&
	            select.from_table->type == TableReferenceType::EMPTY_FROM && select.modifiers.empty() &&
	            select.cte_map.map.empty() && !select.where_clause && select.groups.group_expressions.empty() &&
	            select.groups.grouping_sets.empty() && !select.having && !select.qualify && !select.sample,
	        "CHECK requires exactly one scalar expression without query clauses");
	auto expression = std::move(select.select_list[0]);
	Require(!expression->HasAlias() && expression->GetExpressionClass() == ExpressionClass::COMPARISON,
	        "native CHECK read supports only Boolean numeric comparisons");
	switch (expression->GetExpressionType()) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		break;
	default:
		throw InvalidInputException("VGI constraint protocol: unsupported CHECK comparison");
	}
	auto &comparison = expression->Cast<ComparisonExpression>();
	NumericOperand(comparison.Left(), info);
	NumericOperand(comparison.Right(), info);
	return expression;
}
std::string ReadCheck(const std::vector<uint8_t> &bytes, const VgiTableInfo &info) {
	auto expression = Decode(bytes, generated::SqlExpressionSchema());
	RecordBatchSingleRow row(expression, 0, "SqlExpression", "constraint");
	Require(row["profile"].value_not_null<std::string>() == VGI_CONSTRAINT_SCALAR_READ,
	        "unsupported CHECK expression profile");
	Require(row["schema_path"].value_not_null<std::vector<std::string>>() ==
	            std::vector<std::string> {info.schema_name},
	        "CHECK expression belongs to another schema");
	auto context = expression->GetColumnByName("evaluation_context");
	Require(!context.IsNull(0), "missing evaluation context");
	VgiBatchBuilder builder(generated::QueryEvaluationContextSchema());
	for (int i = 0; i < builder.num_columns(); ++i)
		AppendElement(builder.column(i), context.child(i), 0);
	builder.FinishRow();
	auto batch = builder.Finish();
	RecordBatchSingleRow settings(batch, 0, "QueryEvaluationContext", "constraint");
	Require(settings["timezone"].value_not_null<std::string>() == "UTC" &&
	            settings["collation"].value_not_null<std::string>() == "binary" &&
	            settings["default_order"].value_not_null<std::string>() == "asc" &&
	            settings["default_null_order"].value_not_null<std::string>() == "nulls_last" &&
	            settings["preserve_insertion_order"].value_not_null<bool>(),
	        "unsupported CHECK evaluation context");
	auto sql = row["sql"].value_not_null<std::string>();
	ParseNumericCheck(sql, info);
	return sql;
}
} // namespace

std::vector<uint8_t> BuildNativeConstraintCapabilities() {
	return Encode(
	    generated::CatalogCapabilitiesSchema(),
	    {BuildStringListScalar({VGI_CONSTRAINT_READ, VGI_CONSTRAINT_SCALAR_READ, VGI_INDEX_READ}),
	     BuildStringListScalar({VGI_CONSTRAINT_CREATE, VGI_CONSTRAINT_MUTATE, VGI_INDEX_CREATE, VGI_INDEX_DROP}),
	     BuildStringListScalar({VGI_CONSTRAINT_ENFORCE}), BuildStringListScalar({"vgi.transaction.ddl.v1"})});
}
void ParseNativeConstraintCapabilities(CatalogAttachResult &result, const std::vector<uint8_t> &bytes) {
	if (bytes.empty())
		return;
	auto batch = Decode(bytes, generated::CatalogCapabilitiesSchema());
	RecordBatchSingleRow row(batch, 0, "CatalogCapabilities", "constraint");
	std::map<std::string, std::set<std::string>> offered {
	    {"read", {VGI_CONSTRAINT_READ, VGI_CONSTRAINT_SCALAR_READ, VGI_INDEX_READ}},
	    {"ddl", {VGI_CONSTRAINT_CREATE, VGI_CONSTRAINT_MUTATE, VGI_INDEX_CREATE, VGI_INDEX_DROP}},
	    {"write", {VGI_CONSTRAINT_ENFORCE}},
	    {"transaction", {"vgi.transaction.ddl.v1"}}};
	for (const auto &category : offered) {
		std::set<std::string> seen;
		for (const auto &profile : row[category.first].value_not_null<std::vector<std::string>>()) {
			Require(category.second.count(profile) && seen.insert(profile).second,
			        "unoffered or duplicate catalog profile");
			result.constraint_profiles.insert(profile);
		}
	}
	Require(!(result.constraint_profiles.count(VGI_INDEX_CREATE) || result.constraint_profiles.count(VGI_INDEX_DROP)) ||
	            result.constraint_profiles.count(VGI_INDEX_READ),
	        "index DDL requires typed index read support");
	Require(!result.constraint_profiles.count("vgi.transaction.ddl.v1") || result.supports_transactions,
	        "DDL transaction profile requires transaction support");
	if (result.constraint_profiles.count(VGI_CONSTRAINT_CREATE) ||
	    result.constraint_profiles.count(VGI_CONSTRAINT_MUTATE) ||
	    result.constraint_profiles.count(VGI_CONSTRAINT_ENFORCE)) {
		Require(result.constraint_profiles.count(VGI_CONSTRAINT_READ),
		        "constraint profiles require canonical read support");
	}
}
void RequireNativeConstraintCapabilities(const CatalogAttachResult &result, const char *operation) {
	Require(result.constraint_profiles.count(VGI_CONSTRAINT_READ), "constraint read profile was not negotiated");
	if (std::string(operation) != "read") {
		Require(result.constraint_profiles.count(VGI_CONSTRAINT_ENFORCE),
		        "constraint enforcement profile was not negotiated");
		Require(result.constraint_profiles.count(operation), "constraint operation profile was not negotiated");
	}
}
void RequireNativeConstraintRead(const CatalogAttachResult &result, const VgiTableInfo &info) {
	if (!info.constraints.empty())
		RequireNativeConstraintCapabilities(result, "read");
	if (!info.check_constraints.empty())
		Require(result.constraint_profiles.count(VGI_CONSTRAINT_SCALAR_READ),
		        "CHECK scalar read profile was not negotiated");
}
void RequireNativeConstraintWrite(const CatalogAttachResult &result, const VgiTableInfo &info) {
	if (!info.constraints.empty())
		RequireNativeConstraintCapabilities(result, VGI_CONSTRAINT_ENFORCE);
}

void ValidateNativeConstraintList(const VgiColumn &array, int64_t row) {
	Require(array && array.type().type == NANOARROW_TYPE_LIST && array.child(0).type().type == NANOARROW_TYPE_BINARY &&
	            row >= 0 && row < array.length() && !array.IsNull(row),
	        "constraints must be a non-NULL binary list");
	ArrowError error;
	ArrowErrorInit(&error);
	// Validation may fill inferred sizes: use local views rather than mutating
	// the immutable batch shared with other catalog readers. This shape has
	// exactly one binary child and no further descendants.
	auto view = array.view();
	auto values = array.child(0).view();
	ArrowArrayView *children[] = {&values};
	view.children = children;
	Require(ArrowArrayViewValidate(&view, NANOARROW_VALIDATION_LEVEL_FULL, &error) == NANOARROW_OK,
	        "invalid constraint list buffers");
	NoListNulls(array, row, row + 1);
}

void DecodeNativeConstraints(VgiTableInfo &info, bool check_nullability) {
	Records(info.constraints);
	if (!info.definition_revision.empty())
		Token(info.definition_revision);
	info.not_null_constraints.clear();
	info.unique_constraints.clear();
	info.primary_key_constraints.clear();
	info.check_constraints.clear();
	info.foreign_key_constraints.clear();
	info.not_null_constraint_refs.clear();
	std::set<std::string> keys, labels;
	std::set<ColumnPath> required;
	for (const auto &bytes : info.constraints) {
		auto batch = Decode(bytes, generated::ConstraintInfoSchema());
		RecordBatchSingleRow row(batch, 0, "ConstraintInfo", "constraint");
		auto ref_bytes = row["ref"].value_not_null<std::vector<uint8_t>>();
		auto ref = ReadRef(ref_bytes, "constraint");
		Require(ref.schema == info.schema_name && ref.owner == info.name,
		        "constraint identity belongs to another table");
		Require(keys.insert(ref.name).second, "duplicate constraint identity");
		Require(row["enforcement"].value_not_null<std::string>() == "ENFORCED" &&
		            row["validation"].value_not_null<std::string>() == "ALL_ROWS",
		        "native constraints require ENFORCED + ALL_ROWS");
		auto definition =
		    Decode(row["definition"].value_not_null<std::vector<uint8_t>>(), generated::ConstraintDefinitionSchema());
		RecordBatchSingleRow def(definition, 0, "ConstraintDefinition", "constraint");
		auto kind = def["kind"].value_not_null<std::string>();
		auto paths = ReadPaths(def, "column_paths");
		if (!definition->GetColumnByName("declared_name").IsNull(0)) {
			auto label = def["declared_name"].value_not_null<std::string>();
			Identifier(label);
			Require(labels.insert(StringUtil::Lower(label)).second, "duplicate SQL constraint label");
		}
		auto foreign = def["foreign_key"].value_or(std::vector<uint8_t>());
		auto target_bytes = row["referenced_constraint"].value_or(std::vector<uint8_t>());
		Require((kind == "CHECK") == !definition->GetColumnByName("check_expression").IsNull(0),
		        "inconsistent CHECK payload");
		Require(kind == "CHECK" ? paths.empty() : !paths.empty(), "invalid constraint column paths");
		Require(kind == "NOT_NULL" || kind == "UNIQUE" || kind == "PRIMARY_KEY" || kind == "FOREIGN_KEY" ||
		            kind == "CHECK",
		        "unknown constraint kind");
		Require((kind == "FOREIGN_KEY") == !definition->GetColumnByName("foreign_key").IsNull(0) &&
		            (kind == "FOREIGN_KEY") == !batch->GetColumnByName("referenced_constraint").IsNull(0),
		        "inconsistent FK payload");
		if (kind == "CHECK") {
			info.check_constraints.push_back(
			    ReadCheck(def["check_expression"].value_not_null<std::vector<uint8_t>>(), info));
			continue;
		}
		if (kind == "NOT_NULL") {
			Require(paths.size() == 1, "NOT_NULL requires one column");
			const auto &path = paths[0];
			ResolveNotNullPath(info, path);
			required.insert(path);
			// DuckDB 1.5 represents NOT NULL only on top-level columns. Preserve
			// nested facts in info.constraints and Arrow nullability; the negotiated
			// provider enforces writes. A required child must never make its parent
			// nonnullable or supply the identity for ALTER COLUMN parent DROP NOT NULL.
			if (path.size() == 1) {
				info.not_null_constraints.push_back(info.arrow_schema->GetFieldIndex(path[0]));
				info.not_null_constraint_refs[path[0]].push_back(ref_bytes);
			}
			continue;
		}
		auto names = TopLevelNames(paths);
		std::vector<int> indices;
		for (const auto &name : names) {
			auto index = info.arrow_schema->GetFieldIndex(name);
			Require(index >= 0 && index != info.row_id_column,
			        "constraint references a missing, ambiguous, or synthetic column");
			indices.push_back(index);
		}
		if (kind == "PRIMARY_KEY") {
			Require(info.primary_key_constraints.empty(), "multiple primary keys");
			info.primary_key_constraints.push_back(indices);
			required.insert(paths.begin(), paths.end());
		} else if (kind == "UNIQUE") {
			info.unique_constraints.push_back(indices);
		} else {
			auto fk_batch = Decode(foreign, generated::ForeignKeyDefinitionSchema());
			RecordBatchSingleRow fk(fk_batch, 0, "ForeignKeyDefinition", "constraint");
			auto table = ReadRef(fk["referenced_table"].value_not_null<std::vector<uint8_t>>(), "table");
			auto target = ReadRef(target_bytes, "constraint");
			Require(table.schema == info.schema_name && target.schema == table.schema && target.owner == table.name,
			        "FK must reference an identified key in the same schema");
			auto pk_names = TopLevelNames(ReadPaths(fk, "referenced_column_paths"));
			Require(pk_names.size() == names.size(), "FK arity mismatch");
			Require(fk["match"].value_not_null<std::string>() == "SIMPLE", "native adapter requires MATCH SIMPLE");
			for (const char *action : {"on_update", "on_delete"}) {
				auto value = fk[action].value_not_null<std::string>();
				Require(value == "NO_ACTION" || value == "RESTRICT",
				        "native adapter does not support this referential action");
			}
			info.foreign_key_constraints.push_back({names, pk_names, table.name, table.schema});
		}
	}
	if (check_nullability) {
		ValidateNullability(info.arrow_schema->c_schema(), {}, required, info.row_id_column);
	}
}

void AppendNativeCheckConstraints(ClientContext &context, const VgiTableInfo &info, CreateTableInfo &create) {
	for (const auto &sql : info.check_constraints) {
		auto expression = ParseNumericCheck(sql, info);
		// Bind only against this table's physical primitive columns. No generated
		// expression expansion or session/catalog function lookup is qualified.
		auto &comparison = expression->Cast<ComparisonExpression>();
		for (const auto *operand : {&comparison.Left(), &comparison.Right()}) {
			if (operand->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
				const auto &name = operand->Cast<ColumnRefExpression>().ColumnNames()[0];
				const auto &column = create.columns.GetColumn(name);
				Require(!column.Generated() && column.Type().IsNumeric(), "unsupported bound CHECK column type");
			}
		}
		auto binder = Binder::CreateBinder(context);
		physical_index_set_t columns;
		CheckBinder check_binder(*binder, context, create.GetTableName(), create.columns, columns);
		// DuckDB's CHECK binder normally casts to INTEGER; require an actual Boolean.
		check_binder.target_type = LogicalType::INVALID;
		auto copy = expression->Copy();
		auto bound = check_binder.Bind(copy);
		Require(bound->GetReturnType() == LogicalType::BOOLEAN, "CHECK must bind to BOOLEAN");
		create.constraints.push_back(make_uniq<CheckConstraint>(std::move(expression)));
	}
}

std::vector<std::vector<uint8_t>> EncodeNativeConstraints(const CreateTableInfo &info, VgiSchemaPtr &schema) {
	std::vector<std::vector<uint8_t>> result;
	std::set<int> required;
	auto column = [&](idx_t index) {
		Require(index < static_cast<idx_t>(schema->num_fields()), "logical column index out of range");
		return schema->field(index).name;
	};
	for (const auto &constraint : info.constraints) {
		std::vector<std::string> names;
		if (constraint->type == ConstraintType::NOT_NULL) {
			auto index = constraint->Cast<NotNullConstraint>().index.index;
			result.push_back(Definition("NOT_NULL", {column(index)}));
			required.insert(index);
		} else if (constraint->type == ConstraintType::UNIQUE) {
			auto &unique = constraint->Cast<UniqueConstraint>();
			if (unique.HasIndex())
				names.push_back(column(unique.GetIndex().index));
			else {
				for (const auto &name : unique.GetColumnNames()) {
					auto spelling = name;
					names.push_back(column(info.columns.GetColumnIndex(spelling).index));
				}
			}
			for (const auto &name : names) {
				int index = schema->GetFieldIndex(name);
				Require(index >= 0, "unique column was not resolved exactly");
				if (unique.IsPrimaryKey())
					required.insert(index);
			}
			result.push_back(Definition(unique.IsPrimaryKey() ? "PRIMARY_KEY" : "UNIQUE", names));
		} else if (constraint->type == ConstraintType::FOREIGN_KEY) {
			auto &fk = constraint->Cast<ForeignKeyConstraint>();
			Require(fk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE ||
			            fk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE,
			        "cannot create an engine-internal FK counterpart");
			auto target_schema = fk.info.schema.empty() ? info.GetQualifiedName().Schema() : fk.info.schema;
			Require(target_schema == info.GetQualifiedName().Schema(), "native FK must reference the same schema");
			auto pk = IdentifiersToStrings(fk.pk_columns);
			std::vector<std::string> local;
			for (const auto &name : fk.fk_columns) {
				auto spelling = name;
				local.push_back(column(info.columns.GetColumnIndex(spelling).index));
			}
			Require(!local.empty() && local.size() == pk.size(), "unresolved FK columns");
			auto foreign = Encode(
			    generated::ForeignKeyDefinitionSchema(),
			    {BuildBinaryScalarRequired(Ref(target_schema.GetIdentifierName(), fk.info.table.GetIdentifierName())),
			     PathArray(pk), BuildEnumArray("SIMPLE", {"SIMPLE", "FULL"}),
			     BuildEnumArray("NO_ACTION", {"NO_ACTION", "RESTRICT", "CASCADE", "SET_NULL", "SET_DEFAULT"}),
			     BuildEnumArray("NO_ACTION", {"NO_ACTION", "RESTRICT", "CASCADE", "SET_NULL", "SET_DEFAULT"})});
			result.push_back(Definition("FOREIGN_KEY", local, foreign));
		} else {
			throw NotImplementedException(
			    "Native VGI canonical CHECK and other expression constraints are not yet supported");
		}
	}
	Records(result);
	nanoarrow::UniqueSchema output;
	schema->ExportTo(output.get());
	for (int64_t i = 0; i < output->n_children; ++i) {
		if (required.count(i))
			output->children[i]->flags &= ~ARROW_FLAG_NULLABLE;
		else
			output->children[i]->flags |= ARROW_FLAG_NULLABLE;
	}
	schema = VgiSchema::Adopt(output.get());
	return result;
}

std::vector<uint8_t> BuildNativeNotNullChange(const VgiTableInfo &info, const std::string &column, bool add) {
	int index = -1;
	for (int i = 0; i < info.arrow_schema->num_fields(); ++i) {
		if (StringUtil::CIEquals(info.arrow_schema->field(i).name, column)) {
			Require(index < 0, "ambiguous SQL column spelling");
			index = i;
		}
	}
	Require(index >= 0 && index != info.row_id_column, "missing or synthetic NOT NULL column");
	const auto &raw_name = info.arrow_schema->field(index).name;
	for (const auto &key : info.primary_key_constraints) {
		if (std::find(key.begin(), key.end(), index) != key.end()) {
			Require(add, "cannot drop PRIMARY KEY implied NOT NULL");
			return {};
		}
	}
	auto existing = info.not_null_constraint_refs.find(raw_name);
	if (add && existing != info.not_null_constraint_refs.end())
		return {};
	if (!add && existing == info.not_null_constraint_refs.end())
		return {};
	if (!add)
		Require(existing->second.size() == 1, "multiple NOT_NULL identities require an explicit constraint operation");
	return Encode(
	    generated::ConstraintChangeSchema(),
	    {BuildEnumArray(add ? "ADD" : "DROP", {"ADD", "DROP", "RENAME", "REPLACE"}),
	     add ? BuildOptionalBinaryScalar(std::nullopt) : BuildBinaryScalarRequired(existing->second[0]),
	     add ? BuildBinaryScalarRequired(Definition("NOT_NULL", {raw_name})) : BuildOptionalBinaryScalar(std::nullopt),
	     BuildOptionalEnumArray(add ? std::optional<std::string>("ENFORCED") : std::nullopt,
	                            {"ENFORCED", "METADATA_ONLY"}),
	     BuildOptionalEnumArray(add ? std::optional<std::string>("ALL_ROWS") : std::nullopt,
	                            {"ALL_ROWS", "FUTURE_WRITES", "UNVALIDATED"}),
	     add ? BuildOptionalBoolScalar(std::nullopt) : BuildBoolScalar(false),
	     BuildOptionalStringScalar(std::nullopt)});
}
std::vector<uint8_t> BuildNativeConstraintsApply(const std::vector<uint8_t> &attachment,
                                                 const std::vector<uint8_t> &transaction, const VgiTableInfo &info,
                                                 const std::vector<uint8_t> &change) {
	Token(info.definition_revision);
	Require(!change.empty(), "empty mutation");
	return Encode(
	    generated::ConstraintsApplyRequestSchema(),
	    {BuildBinaryScalarRequired(attachment), BuildBinaryScalarRequired(Ref(info.schema_name, info.name)),
	     BuildBinaryScalarRequired(info.definition_revision), BuildBinaryListScalar({change}),
	     (transaction.empty() ? BuildOptionalBinaryScalar(std::nullopt) : BuildBinaryScalarRequired(transaction))});
}
void ValidateNativeConstraintSnapshot(const std::vector<uint8_t> &bytes, const VgiTableInfo &before) {
	auto batch = Decode(bytes, generated::ConstraintSnapshotSchema());
	RecordBatchSingleRow row(batch, 0, "ConstraintSnapshot", "constraint");
	auto table = ReadRef(row["table"].value_not_null<std::vector<uint8_t>>(), "table");
	Require(table.schema == before.schema_name && table.name == before.name,
	        "mutation snapshot belongs to another table");
	auto revision = row["definition_revision"].value_not_null<std::vector<uint8_t>>();
	Token(revision);
	Require(revision != before.definition_revision,
	        "successful constraint mutation did not rotate the definition revision");
	auto after = before;
	after.definition_revision = revision;
	after.constraints = row["constraints"].value_not_null<std::vector<std::vector<uint8_t>>>();
	DecodeNativeConstraints(after, false);
}
} // namespace vgi
} // namespace duckdb
