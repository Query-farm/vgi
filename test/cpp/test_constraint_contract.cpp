// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/constraints/check_constraint.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "vgi_constraint_contract.hpp"
#include "vgi_catalog_rpc.hpp"
#include "vgi_rpc_types.hpp"
#include "vgi_arrow_utils.hpp"
#include "../../src/generated/vgi_protocol_schemas.hpp"

using namespace duckdb;
namespace {
std::vector<uint8_t> ConstraintRecord(const std::vector<uint8_t> &definition, const std::string &key = "stable-key",
                                      const std::string &state = "ENFORCED", const std::string &validation = "ALL_ROWS",
                                      const std::string &owner = "t") {
	auto ref = vgi::SerializeToIpcBytes(
	    arrow::RecordBatch::Make(vgi::generated::CatalogObjectRefSchema(), 1,
	                             {vgi::BuildStringScalar("constraint"), vgi::BuildStringListScalar({"main"}),
	                              vgi::BuildStringScalar(key), vgi::BuildStringScalar(owner)}));
	return vgi::SerializeToIpcBytes(
	    arrow::RecordBatch::Make(vgi::generated::ConstraintInfoSchema(), 1,
	                             {vgi::BuildBinaryScalarRequired(ref), vgi::BuildBinaryScalarRequired(definition),
	                              vgi::BuildEnumArray(state, {"ENFORCED", "METADATA_ONLY", "unknown"}),
	                              vgi::BuildEnumArray(validation, {"ALL_ROWS", "FUTURE_WRITES", "UNVALIDATED"}),
	                              vgi::BuildOptionalBinaryScalar(std::nullopt)}));
}
vgi::VgiTableInfo NativeTable(bool primary = false) {
	CreateTableInfo create;
	create.schema = "main";
	create.table = "t";
	create.columns.AddColumn(ColumnDefinition("a.b", LogicalType::INTEGER));
	if (primary)
		create.constraints.push_back(make_uniq<UniqueConstraint>(LogicalIndex(0), true));
	else
		create.constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(0)));
	auto schema = arrow::schema({arrow::field("a.b", arrow::int32())});
	auto definitions = vgi::EncodeNativeConstraints(create, schema);
	vgi::VgiTableInfo table;
	table.schema_name = "main";
	table.name = "t";
	table.arrow_schema = schema;
	table.definition_revision = {1, 0, 2};
	table.constraints = {ConstraintRecord(definitions[0])};
	return table;
}
std::vector<uint8_t> GetDefinition(const vgi::VgiTableInfo &table) {
	auto batch = vgi::DeserializeFromIpcBytes(table.constraints[0]);
	vgi::RecordBatchSingleRow row(batch, 0, "ConstraintInfo", "test");
	return row["definition"].value_not_null<std::vector<uint8_t>>();
}
std::vector<uint8_t> CheckExpression(const std::string &sql = "budget >= 0") {
	auto context = arrow::StructArray::Make(
	    {vgi::BuildStringScalar("UTC"), vgi::BuildStringScalar("binary"), vgi::BuildStringScalar("asc"),
	     vgi::BuildStringScalar("nulls_last"), vgi::BuildBoolScalar(true)},
	    vgi::generated::QueryEvaluationContextSchema()->fields()).ValueOrDie();
	return vgi::SerializeToIpcBytes(arrow::RecordBatch::Make(
	    vgi::generated::SqlExpressionSchema(), 1,
	    {vgi::BuildStringScalar(sql), vgi::BuildStringScalar(vgi::VGI_CONSTRAINT_SCALAR_READ),
	     vgi::BuildStringListScalar({"main"}), context}));
}
vgi::VgiTableInfo CheckTable(const std::vector<uint8_t> &expression = CheckExpression()) {
	auto strings = std::make_shared<arrow::StringBuilder>();
	auto inner = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(), strings);
	arrow::ListBuilder paths(arrow::default_memory_pool(), inner);
	REQUIRE(paths.Append().ok());
	auto definition = vgi::SerializeToIpcBytes(arrow::RecordBatch::Make(
	    vgi::generated::ConstraintDefinitionSchema(), 1,
	    {vgi::BuildEnumArray("CHECK", {"NOT_NULL", "UNIQUE", "PRIMARY_KEY", "CHECK", "FOREIGN_KEY"}),
	     paths.Finish().ValueOrDie(), arrow::MakeArrayOfNull(arrow::utf8(), 1).ValueOrDie(),
	     vgi::BuildBinaryScalarRequired(expression), vgi::BuildOptionalBinaryScalar(std::nullopt)}));
	vgi::VgiTableInfo table;
	table.schema_name = "main";
	table.name = "t";
	table.arrow_schema = arrow::schema({arrow::field("budget", arrow::float64())});
	table.constraints = {ConstraintRecord(definition)};
	return table;
}
} // namespace

TEST_CASE("Native canonical constraints preserve raw names and logical rowid mapping", "[constraint-contract]") {
	auto table = NativeTable();
	REQUIRE_FALSE(table.arrow_schema->field(0)->nullable());
	vgi::DecodeNativeConstraints(table);
	REQUIRE(table.not_null_constraints == std::vector<int> {0});
	auto definitions = vgi::DeserializeFromIpcBytes(GetDefinition(table));
	vgi::RecordBatchSingleRow row(definitions, 0, "ConstraintDefinition", "test");
	REQUIRE(row["column_paths"].value_not_null<std::vector<std::vector<std::string>>>() ==
	        std::vector<std::vector<std::string>> {{"a.b"}});
	table.arrow_schema = arrow::schema({arrow::field("row_id", arrow::int64(), false), table.arrow_schema->field(0)});
	table.row_id_column = 0;
	vgi::DecodeNativeConstraints(table);
	CHECK(table.not_null_constraints == std::vector<int> {1});
	table.row_id_column = 1;
	CHECK_THROWS(vgi::DecodeNativeConstraints(table));
}

TEST_CASE("Native constraint facts reject downgrade identity ambiguity and nullability drift",
          "[constraint-contract]") {
	for (const auto &state : {"METADATA_ONLY", "unknown"}) {
		auto table = NativeTable();
		table.constraints = {ConstraintRecord(GetDefinition(table), "key", state)};
		CHECK_THROWS(vgi::DecodeNativeConstraints(table));
	}
	for (const auto &state : {"FUTURE_WRITES", "UNVALIDATED"}) {
		auto table = NativeTable();
		table.constraints = {ConstraintRecord(GetDefinition(table), "key", "ENFORCED", state)};
		CHECK_THROWS(vgi::DecodeNativeConstraints(table));
	}
	auto table = NativeTable();
	table.constraints.push_back(table.constraints[0]);
	CHECK_THROWS(vgi::DecodeNativeConstraints(table));
	table = NativeTable();
	table.constraints = {ConstraintRecord(GetDefinition(table), "key", "ENFORCED", "ALL_ROWS", "other")};
	CHECK_THROWS(vgi::DecodeNativeConstraints(table));
	table = NativeTable();
	table.arrow_schema = arrow::schema({arrow::field("a.b", arrow::int32(), true)});
	CHECK_THROWS(vgi::DecodeNativeConstraints(table));
	table = NativeTable();
	table.constraints.clear();
	CHECK_THROWS(vgi::DecodeNativeConstraints(table));
	table = NativeTable();
	table.constraints[0].push_back(0);
	CHECK_THROWS(vgi::DecodeNativeConstraints(table));
}

TEST_CASE("Native NOT NULL mutation uses stable identity and binary revision", "[constraint-contract]") {
	auto table = NativeTable();
	vgi::DecodeNativeConstraints(table);
	CHECK(vgi::BuildNativeNotNullChange(table, "a.b", true).empty());
	auto change = vgi::BuildNativeNotNullChange(table, "A.B", false);
	auto batch = vgi::DeserializeFromIpcBytes(change);
	vgi::RecordBatchSingleRow row(batch, 0, "ConstraintChange", "test");
	CHECK(row["operation"].value_not_null<std::string>() == "DROP");
	CHECK(row["ref"].value_not_null<std::vector<uint8_t>>() == table.not_null_constraint_refs["a.b"][0]);
	auto request = vgi::DeserializeFromIpcBytes(vgi::BuildNativeConstraintsApply({4}, {9}, table, change));
	vgi::RecordBatchSingleRow request_row(request, 0, "ConstraintsApplyRequest", "test");
	CHECK(request_row["expected_revision"].value_not_null<std::vector<uint8_t>>() == table.definition_revision);
	CHECK(request_row["transaction_opaque_data"].value_not_null<std::vector<uint8_t>>() == std::vector<uint8_t> {9});
	table.definition_revision.clear();
	CHECK_THROWS(vgi::BuildNativeConstraintsApply({4}, {}, table, change));
	table = NativeTable(true);
	vgi::DecodeNativeConstraints(table);
	CHECK_THROWS(vgi::BuildNativeNotNullChange(table, "a.b", false));
}

TEST_CASE("Native constraint capability selection rejects unoffered profiles", "[constraint-contract]") {
	vgi::CatalogAttachResult result;
	result.supports_transactions = true;
	vgi::ParseNativeConstraintCapabilities(result, vgi::BuildNativeConstraintCapabilities());
	CHECK_NOTHROW(vgi::RequireNativeConstraintCapabilities(result, vgi::VGI_CONSTRAINT_MUTATE));
	auto batch = arrow::RecordBatch::Make(vgi::generated::CatalogCapabilitiesSchema(), 1,
	                                      {vgi::BuildStringListScalar({vgi::VGI_CONSTRAINT_READ}),
	                                       vgi::BuildStringListScalar({"vgi.expression.duckdb.scalar.v1"}),
	                                       vgi::BuildStringListScalar({}), vgi::BuildStringListScalar({})});
	CHECK_THROWS(vgi::ParseNativeConstraintCapabilities(result, vgi::SerializeToIpcBytes(batch)));
	vgi::CatalogAttachResult empty;
	CHECK_THROWS(vgi::RequireNativeConstraintCapabilities(empty, "read"));
}

TEST_CASE("Canonical read facts do not grant constrained write or mutation authority", "[constraint-contract]") {
	auto table = NativeTable();
	vgi::DecodeNativeConstraints(table);
	vgi::CatalogAttachResult attached;
	attached.constraint_profiles = {vgi::VGI_CONSTRAINT_READ};
	CHECK_NOTHROW(vgi::RequireNativeConstraintRead(attached, table));
	// The count-mode fallback and an inlined function must not confer authority.
	table.write_result_modes["insert"] = "count";
	for (const auto *operation : {vgi::VGI_CONSTRAINT_CREATE, vgi::VGI_CONSTRAINT_MUTATE}) {
		attached.constraint_profiles.insert(operation);
		CHECK_THROWS(vgi::RequireNativeConstraintCapabilities(attached, operation));
	}
	CHECK_THROWS(vgi::RequireNativeConstraintWrite(attached, table));
	attached.constraint_profiles.insert(vgi::VGI_CONSTRAINT_ENFORCE);
	CHECK_NOTHROW(vgi::RequireNativeConstraintWrite(attached, table));
	CHECK_NOTHROW(vgi::RequireNativeConstraintCapabilities(attached, vgi::VGI_CONSTRAINT_MUTATE));
	table.constraints.clear();
	CHECK_NOTHROW(vgi::RequireNativeConstraintWrite({}, table));
}

TEST_CASE("Canonical numeric CHECK read requires the selected profile and binds Boolean", "[constraint-contract]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_FALSE(connection.Query("BEGIN")->HasError());
	for (const auto &sql : {"budget >= 0", "BUDGET < 1.5", "budget = budget", "budget <> 0", "0 <= budget"}) {
		auto table = CheckTable(CheckExpression(sql));
		vgi::DecodeNativeConstraints(table);
		REQUIRE(table.check_constraints == std::vector<std::string> {sql});
		vgi::CatalogAttachResult attached;
		attached.constraint_profiles = {vgi::VGI_CONSTRAINT_READ};
		CHECK_THROWS(vgi::RequireNativeConstraintRead(attached, table));
		attached.constraint_profiles.insert(vgi::VGI_CONSTRAINT_SCALAR_READ);
		CHECK_NOTHROW(vgi::RequireNativeConstraintRead(attached, table));
		auto create = vgi::CreateTableInfoFromVgiTable(*connection.context, table, "main");
		REQUIRE(create.constraints.size() == 1);
		CHECK(create.constraints[0]->type == ConstraintType::CHECK);
		CHECK_THROWS(vgi::EncodeNativeConstraints(create, table.arrow_schema));
	}
	auto table = CheckTable();
	vgi::DecodeNativeConstraints(table);
	table.arrow_schema = arrow::schema({arrow::field("budget", arrow::float64())->WithMetadata(
	    arrow::key_value_metadata({"generated_expression"}, {"1.0"}))});
	CHECK_THROWS(vgi::CreateTableInfoFromVgiTable(*connection.context, table, "main"));
	REQUIRE_FALSE(connection.Query("ROLLBACK")->HasError());
}

TEST_CASE("Canonical CHECK rejects unqualified SQL semantics and malformed envelopes", "[constraint-contract]") {
	for (const auto &sql : {"budget", "42", "true", "budget + 1 > 0", "abs(budget) > 0",
	                        "budget >= CAST(0 AS DOUBLE)", "budget > '0'", "budget > ?", "t.budget > 0",
	                        "missing > 0", "(SELECT 1) > 0", "budget > 0 AS ok", "budget > 0, 1 = 1",
	                        "budget > 0; SELECT 1", "budget > 0 FROM other", "budget > 0 WHERE true",
	                        "budget > 0 ORDER BY 1", "budget > 0 AND budget < 10"}) {
		INFO(sql);
		auto table = CheckTable(CheckExpression(sql));
		CHECK_THROWS(vgi::DecodeNativeConstraints(table));
	}
	for (const auto &type : {arrow::utf8(), arrow::boolean(), arrow::timestamp(arrow::TimeUnit::SECOND)}) {
		auto table = CheckTable();
		table.arrow_schema = arrow::schema({arrow::field("budget", type)});
		CHECK_THROWS(vgi::DecodeNativeConstraints(table));
	}
	auto table = CheckTable();
	table.row_id_column = 0;
	CHECK_THROWS(vgi::DecodeNativeConstraints(table));
	table = CheckTable();
	table.arrow_schema = arrow::schema({arrow::field("budget", arrow::float64()), arrow::field("BUDGET", arrow::float64())});
	CHECK_THROWS(vgi::DecodeNativeConstraints(table));
	auto expression = vgi::DeserializeFromIpcBytes(CheckExpression());
	for (int index : {1, 2}) {
		auto columns = expression->columns();
		columns[index] = index == 1 ? vgi::BuildStringScalar("vgi.expression.unsupported.v1")
		                           : vgi::BuildStringListScalar({"other"});
		table = CheckTable(vgi::SerializeToIpcBytes(arrow::RecordBatch::Make(expression->schema(), 1, columns)));
		CHECK_THROWS(vgi::DecodeNativeConstraints(table));
	}
	auto context = std::static_pointer_cast<arrow::StructArray>(expression->column(3));
	for (int index = 0; index < 5; ++index) {
		auto settings = context->fields();
		settings[index] = index == 4 ? vgi::BuildBoolScalar(false) : vgi::BuildStringScalar("unsupported");
		auto columns = expression->columns();
		columns[3] = arrow::StructArray::Make(settings, vgi::generated::QueryEvaluationContextSchema()->fields()).ValueOrDie();
		table = CheckTable(vgi::SerializeToIpcBytes(arrow::RecordBatch::Make(expression->schema(), 1, columns)));
		CHECK_THROWS(vgi::DecodeNativeConstraints(table));
	}
	auto bytes = CheckExpression();
	bytes.push_back(0);
	table = CheckTable(bytes);
	CHECK_THROWS(vgi::DecodeNativeConstraints(table));
	table = CheckTable();
	table.constraints = {ConstraintRecord(GetDefinition(table), "key", "METADATA_ONLY", "UNVALIDATED")};
	CHECK_THROWS(vgi::DecodeNativeConstraints(table));
}

TEST_CASE("Canonical constraint paths cannot discard NULL components", "[constraint-contract]") {
	auto table = NativeTable();
	auto definition = vgi::DeserializeFromIpcBytes(GetDefinition(table));
	auto strings = std::make_shared<arrow::StringBuilder>();
	auto inner = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(), strings);
	arrow::ListBuilder paths(arrow::default_memory_pool(), inner);
	REQUIRE(paths.Append().ok());
	REQUIRE(inner->Append().ok());
	REQUIRE(strings->Append("a.b").ok());
	REQUIRE(strings->AppendNull().ok());
	auto columns = definition->columns();
	columns[1] = paths.Finish().ValueOrDie();
	table.constraints = {
	    ConstraintRecord(vgi::SerializeToIpcBytes(arrow::RecordBatch::Make(definition->schema(), 1, columns)))};
	CHECK_THROWS(vgi::DecodeNativeConstraints(table));

	auto values = std::make_shared<arrow::BinaryBuilder>();
	arrow::ListBuilder list(arrow::default_memory_pool(), values);
	REQUIRE(list.Append().ok());
	REQUIRE(values->AppendNull().ok());
	CHECK_THROWS(vgi::ValidateNativeConstraintList(list.Finish().ValueOrDie(), 0));
}

TEST_CASE("Constraint mutation snapshots require exact bounded single-record IPC", "[constraint-contract]") {
	auto table = NativeTable();
	vgi::DecodeNativeConstraints(table);
	auto change = vgi::BuildNativeNotNullChange(table, "a.b", false);
	auto request = vgi::DeserializeFromIpcBytes(vgi::BuildNativeConstraintsApply({1}, {}, table, change));
	vgi::RecordBatchSingleRow request_row(request, 0, "ConstraintsApplyRequest", "test");
	auto snapshot = arrow::RecordBatch::Make(
	    vgi::generated::ConstraintSnapshotSchema(), 1,
	    {vgi::BuildBinaryScalarRequired(request_row["table"].value_not_null<std::vector<uint8_t>>()),
	     vgi::BuildBinaryScalarRequired({3}), vgi::BuildBinaryListScalar({})});
	auto bytes = vgi::SerializeToIpcBytes(snapshot);
	CHECK_NOTHROW(vgi::ValidateNativeConstraintSnapshot(bytes, table));
	bytes.push_back(0);
	CHECK_THROWS(vgi::ValidateNativeConstraintSnapshot(bytes, table));
	auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
	auto writer = arrow::ipc::MakeStreamWriter(sink, snapshot->schema()).ValueOrDie();
	REQUIRE(writer->WriteRecordBatch(*snapshot).ok());
	REQUIRE(writer->WriteRecordBatch(*snapshot).ok());
	REQUIRE(writer->Close().ok());
	auto buffer = sink->Finish().ValueOrDie();
	CHECK_THROWS(vgi::ValidateNativeConstraintSnapshot(
	    std::vector<uint8_t>(buffer->data(), buffer->data() + buffer->size()), table));
	auto metadata_schema = snapshot->schema()->WithMetadata(arrow::key_value_metadata({"unexpected"}, {"value"}));
	CHECK_THROWS(vgi::ValidateNativeConstraintSnapshot(
	    vgi::SerializeToIpcBytes(arrow::RecordBatch::Make(metadata_schema, 1, snapshot->columns())), table));
}

TEST_CASE("Native CREATE resolves SQL spelling before emitting raw unique paths", "[constraint-contract]") {
	CreateTableInfo create;
	create.schema = "main";
	create.table = "t";
	create.columns.AddColumn(ColumnDefinition("Mixed.Name", LogicalType::INTEGER));
	create.constraints.push_back(make_uniq<UniqueConstraint>(vector<string> {"mixed.name"}, false));
	auto schema = arrow::schema({arrow::field("Mixed.Name", arrow::int32())});
	auto bytes = vgi::EncodeNativeConstraints(create, schema);
	auto definition = vgi::DeserializeFromIpcBytes(bytes[0]);
	vgi::RecordBatchSingleRow row(definition, 0, "ConstraintDefinition", "test");
	CHECK(row["column_paths"].value_not_null<std::vector<std::vector<std::string>>>() ==
	      std::vector<std::vector<std::string>> {{"Mixed.Name"}});
}

TEST_CASE("Native FK discovery retains nested IPC storage while decoding raw paths", "[constraint-contract]") {
	CreateTableInfo create;
	create.schema = "main";
	create.table = "t";
	create.columns.AddColumn(ColumnDefinition("Child.Key", LogicalType::INTEGER));
	ForeignKeyInfo fk;
	fk.type = ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE;
	fk.schema = "main";
	fk.table = "parent";
	create.constraints.push_back(
	    make_uniq<ForeignKeyConstraint>(vector<string> {"Parent.Key"}, vector<string> {"child.key"}, fk));
	auto schema = arrow::schema({arrow::field("Child.Key", arrow::int32())});
	auto definitions = vgi::EncodeNativeConstraints(create, schema);
	auto record = vgi::DeserializeFromIpcBytes(ConstraintRecord(definitions[0]));
	auto target = vgi::SerializeToIpcBytes(
	    arrow::RecordBatch::Make(vgi::generated::CatalogObjectRefSchema(), 1,
	                             {vgi::BuildStringScalar("constraint"), vgi::BuildStringListScalar({"main"}),
	                              vgi::BuildStringScalar("persisted-parent-key"), vgi::BuildStringScalar("parent")}));
	auto columns = record->columns();
	columns[4] = vgi::BuildBinaryScalarRequired(target);
	vgi::VgiTableInfo table;
	table.schema_name = "main";
	table.name = "t";
	table.arrow_schema = schema;
	table.constraints = {vgi::SerializeToIpcBytes(arrow::RecordBatch::Make(record->schema(), 1, columns))};
	for (int iteration = 0; iteration < 16; ++iteration) {
		vgi::DecodeNativeConstraints(table);
		REQUIRE(table.foreign_key_constraints.size() == 1);
		const auto &result = table.foreign_key_constraints[0];
		CHECK(result.fk_columns == std::vector<std::string> {"Child.Key"});
		CHECK(result.pk_columns == std::vector<std::string> {"Parent.Key"});
		CHECK(result.referenced_schema == "main");
		CHECK(result.referenced_table == "parent");
	}
}
