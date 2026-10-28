// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catch.hpp"
#include "../support/vgi_contract_fixtures.hpp"
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
#include "vgi_record_access.hpp"
#include "../../src/generated/vgi_protocol_schemas.hpp"

using namespace duckdb;
namespace {
std::vector<uint8_t> ConstraintRecord(const std::vector<uint8_t> &definition, const std::string &key = "stable-key",
                                      const std::string &state = "ENFORCED", const std::string &validation = "ALL_ROWS",
                                      const std::string &owner = "t") {
	auto ref = vgi::SerializeToIpcBytes(
	    vgi::VgiBatch::FromColumns(vgi::generated::CatalogObjectRefSchema(), 1,
	                               {vgi::BuildStringScalar("constraint"), vgi::BuildStringListScalar({"main"}),
	                                vgi::BuildStringScalar(key), vgi::BuildStringScalar(owner)}));
	return vgi::SerializeToIpcBytes(
	    vgi::VgiBatch::FromColumns(vgi::generated::ConstraintInfoSchema(), 1,
	                               {vgi::BuildBinaryScalarRequired(ref), vgi::BuildBinaryScalarRequired(definition),
	                                vgi::BuildEnumArray(state, {"ENFORCED", "METADATA_ONLY", "unknown"}),
	                                vgi::BuildEnumArray(validation, {"ALL_ROWS", "FUTURE_WRITES", "UNVALIDATED"}),
	                                vgi::BuildOptionalBinaryScalar(std::nullopt)}));
}
vgi::VgiTableInfo NativeTable(bool primary = false) {
	CreateTableInfo create;
	create.SetQualifiedName(QualifiedName(Identifier(), "main", "t"));
	create.columns.AddColumn(ColumnDefinition("a.b", LogicalType::INTEGER));
	if (primary)
		create.constraints.push_back(make_uniq<UniqueConstraint>(LogicalIndex(0), true));
	else
		create.constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(0)));
	auto schema = vgi::MakeSchema({vgi_test::Field("a.b", vgi::type::Int32())});
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
std::vector<uint8_t> PathDefinition(const std::vector<std::vector<std::string>> &values,
                                    const std::string &kind = "NOT_NULL") {
	std::vector<std::vector<std::optional<std::string>>> paths;
	for (const auto &path : values)
		paths.emplace_back(path.begin(), path.end());
	return vgi::SerializeToIpcBytes(vgi::VgiBatch::FromColumns(
	    vgi::generated::ConstraintDefinitionSchema(), 1,
	    {vgi::BuildEnumArray(kind, {"NOT_NULL", "UNIQUE", "PRIMARY_KEY", "CHECK", "FOREIGN_KEY"}),
	     vgi_test::PathList(paths), vgi::BuildOptionalStringScalar(std::nullopt),
	     vgi::BuildOptionalBinaryScalar(std::nullopt), vgi::BuildOptionalBinaryScalar(std::nullopt)}));
}
vgi::VgiTableInfo NestedTable() {
	vgi::VgiTableInfo table;
	table.schema_name = "main";
	table.name = "t";
	table.arrow_schema = vgi::MakeSchema(
	    {vgi_test::Field("record", vgi::type::Struct({vgi_test::Field("a.b", vgi::type::Int32(), false)})),
	     vgi_test::Field("record.a.b", vgi::type::Int32(), false)});
	table.constraints = {ConstraintRecord(PathDefinition({{"record", "a.b"}}), "nested-key"),
	                     ConstraintRecord(PathDefinition({{"record.a.b"}}), "literal-key")};
	return table;
}
std::vector<uint8_t> CheckExpression(const std::string &sql = "budget >= 0") {
	auto context = vgi_test::Struct(vgi::generated::QueryEvaluationContextSchema(),
	                                {vgi::BuildStringScalar("UTC"), vgi::BuildStringScalar("binary"),
	                                 vgi::BuildStringScalar("asc"), vgi::BuildStringScalar("nulls_last"),
	                                 vgi::BuildBoolScalar(true)});
	return vgi::SerializeToIpcBytes(vgi::VgiBatch::FromColumns(vgi::generated::SqlExpressionSchema(), 1,
	                                                           {vgi::BuildStringScalar(sql),
	                                                            vgi::BuildStringScalar(vgi::VGI_CONSTRAINT_SCALAR_READ),
	                                                            vgi::BuildStringListScalar({"main"}), context}));
}
vgi::VgiTableInfo CheckTable(const std::vector<uint8_t> &expression = CheckExpression()) {
	auto definition = vgi::SerializeToIpcBytes(vgi::VgiBatch::FromColumns(
	    vgi::generated::ConstraintDefinitionSchema(), 1,
	    {vgi::BuildEnumArray("CHECK", {"NOT_NULL", "UNIQUE", "PRIMARY_KEY", "CHECK", "FOREIGN_KEY"}),
	     vgi_test::PathList({}), vgi::BuildOptionalStringScalar(std::nullopt),
	     vgi::BuildBinaryScalarRequired(expression), vgi::BuildOptionalBinaryScalar(std::nullopt)}));
	vgi::VgiTableInfo table;
	table.schema_name = "main";
	table.name = "t";
	table.arrow_schema = vgi::MakeSchema({vgi_test::Field("budget", vgi::type::Float64())});
	table.constraints = {ConstraintRecord(definition)};
	return table;
}
} // namespace

TEST_CASE("Native canonical constraints preserve raw names and logical rowid mapping", "[constraint-contract]") {
	auto table = NativeTable();
	REQUIRE_FALSE(table.arrow_schema->field(0).flags & ARROW_FLAG_NULLABLE);
	vgi::DecodeNativeConstraints(table);
	REQUIRE(table.not_null_constraints == std::vector<int> {0});
	auto definitions = vgi::DeserializeFromIpcBytes(GetDefinition(table));
	vgi::RecordBatchSingleRow row(definitions, 0, "ConstraintDefinition", "test");
	REQUIRE(row["column_paths"].value_not_null<std::vector<std::vector<std::string>>>() ==
	        std::vector<std::vector<std::string>> {{"a.b"}});
	table.arrow_schema = vgi::MakeSchema(
	    {vgi_test::Field("row_id", vgi::type::Int64(), false), vgi_test::CopyField(table.arrow_schema->field(0))});
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
	table.arrow_schema = vgi::MakeSchema({vgi_test::Field("a.b", vgi::type::Int32(), true)});
	CHECK_THROWS(vgi::DecodeNativeConstraints(table));
	table = NativeTable();
	table.constraints.clear();
	CHECK_THROWS(vgi::DecodeNativeConstraints(table));
	table = NativeTable();
	table.constraints[0].push_back(0);
	CHECK_THROWS(vgi::DecodeNativeConstraints(table));
}

TEST_CASE("Nested NOT NULL facts preserve nullable parents and raw path identities", "[constraint-contract]") {
	auto table = NestedTable();
	auto original = table.constraints;
	vgi::DecodeNativeConstraints(table);
	CHECK(table.constraints == original);
	CHECK(table.arrow_schema->field(0).flags & ARROW_FLAG_NULLABLE);
	CHECK(table.not_null_constraints == std::vector<int> {1});
	REQUIRE(table.not_null_constraint_refs.size() == 1);
	CHECK(table.not_null_constraint_refs.count("record.a.b") == 1);
	CHECK(vgi::BuildNativeNotNullChange(table, "record", false).empty());
	CHECK_FALSE(vgi::BuildNativeNotNullChange(table, "record", true).empty());
	// Facts alone do not grant write authority, even when all constraints are nested.
	table.arrow_schema = table.arrow_schema->Select({0});
	table.constraints.resize(1);
	vgi::DecodeNativeConstraints(table);
	CHECK(table.not_null_constraints.empty());
	CHECK(table.not_null_constraint_refs.empty());
	vgi::CatalogAttachResult attached;
	attached.constraint_profiles = {vgi::VGI_CONSTRAINT_READ};
	CHECK_NOTHROW(vgi::RequireNativeConstraintRead(attached, table));
	CHECK_THROWS(vgi::RequireNativeConstraintWrite(attached, table));
	attached.constraint_profiles.insert(vgi::VGI_CONSTRAINT_ENFORCE);
	CHECK_NOTHROW(vgi::RequireNativeConstraintWrite(attached, table));
	DuckDB db(nullptr);
	Connection connection(db);
	auto create = vgi::CreateTableInfoFromVgiTable(*connection.context, table, "main");
	CHECK(create.constraints.empty());
}

TEST_CASE("Nested NOT NULL facts require exact fields and recursive nullability agreement", "[constraint-contract]") {
	for (const auto &path : std::vector<std::vector<std::string>> {{},
	                                                               {"record", "missing"},
	                                                               {"record", "a", "b"},
	                                                               {"record.a.b", "child"},
	                                                               {"record", ""},
	                                                               std::vector<std::string>(65, "record")}) {
		auto table = NestedTable();
		table.constraints[0] = ConstraintRecord(PathDefinition({path}), "nested-key");
		CHECK_THROWS(vgi::DecodeNativeConstraints(table));
	}
	auto table = NestedTable();
	table.arrow_schema =
	    vgi::MakeSchema({vgi_test::Field("record", vgi::type::Struct({vgi_test::Field("a.b", vgi::type::Int32())})),
	                     vgi_test::CopyField(table.arrow_schema->field(1))});
	CHECK_THROWS(vgi::DecodeNativeConstraints(table));
	// A mutation snapshot is checked against the pre-mutation Arrow schema.
	CHECK_NOTHROW(vgi::DecodeNativeConstraints(table, false));
	table = NestedTable();
	table.constraints.erase(table.constraints.begin());
	CHECK_THROWS(vgi::DecodeNativeConstraints(table));
	table = NestedTable();
	table.arrow_schema = vgi::MakeSchema(
	    {vgi_test::Field("record", vgi::type::Struct({vgi_test::Field("a.b", vgi::type::Int32(), false),
	                                                  vgi_test::Field("a.b", vgi::type::Int32(), false)})),
	     vgi_test::CopyField(table.arrow_schema->field(1))});
	CHECK_THROWS(vgi::DecodeNativeConstraints(table));
	table = NestedTable();
	table.row_id_column = 0;
	CHECK_THROWS(vgi::DecodeNativeConstraints(table));
	for (const auto &kind : {"UNIQUE", "PRIMARY_KEY"}) {
		table = NestedTable();
		table.constraints[0] = ConstraintRecord(PathDefinition({{"record", "a.b"}}, kind), "nested-key");
		CHECK_THROWS(vgi::DecodeNativeConstraints(table));
	}
}

TEST_CASE("Constraint nullability does not expose collection or logical type storage", "[constraint-contract]") {
	for (const auto &metadata : {vgi_test::Metadata({"ARROW:extension:name"}, {"example"}),
	                             vgi_test::Metadata({"vgi.logical_type.name"}, {"variant"})}) {
		auto table = NestedTable();
		table.arrow_schema =
		    vgi::MakeSchema({vgi_test::CopyField(table.arrow_schema->field(0)).WithMetadata(metadata)});
		table.constraints.clear();
		CHECK_NOTHROW(vgi::DecodeNativeConstraints(table));
		table.constraints = {ConstraintRecord(PathDefinition({{"record", "a.b"}}))};
		CHECK_THROWS(vgi::DecodeNativeConstraints(table));
	}
	auto table = NestedTable();
	table.arrow_schema = vgi::MakeSchema({vgi_test::Field(
	    "items", vgi::type::List(vgi_test::Field(
	                 "item", vgi::type::Struct({vgi_test::Field("k", vgi::type::Utf8(), false)}), false)))});
	table.constraints.clear();
	CHECK_NOTHROW(vgi::DecodeNativeConstraints(table));
	table.constraints = {ConstraintRecord(PathDefinition({{"items", "k"}}))};
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
	auto batch = vgi::VgiBatch::FromColumns(vgi::generated::CatalogCapabilitiesSchema(), 1,
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
	table.arrow_schema = vgi::MakeSchema({vgi_test::Field("budget", vgi::type::Float64())
	                                          .WithMetadata(vgi_test::Metadata({"generated_expression"}, {"1.0"}))});
	CHECK_THROWS(vgi::CreateTableInfoFromVgiTable(*connection.context, table, "main"));
	REQUIRE_FALSE(connection.Query("ROLLBACK")->HasError());
}

TEST_CASE("Canonical CHECK rejects unqualified SQL semantics and malformed envelopes", "[constraint-contract]") {
	for (const auto &sql : {"budget", "42", "true", "budget + 1 > 0", "abs(budget) > 0", "budget >= CAST(0 AS DOUBLE)",
	                        "budget > '0'", "budget > ?", "t.budget > 0", "missing > 0", "(SELECT 1) > 0",
	                        "budget > 0 AS ok", "budget > 0, 1 = 1", "budget > 0; SELECT 1", "budget > 0 FROM other",
	                        "budget > 0 WHERE true", "budget > 0 ORDER BY 1", "budget > 0 AND budget < 10"}) {
		INFO(sql);
		auto table = CheckTable(CheckExpression(sql));
		CHECK_THROWS(vgi::DecodeNativeConstraints(table));
	}
	for (const auto &type : {vgi::type::Utf8(), vgi::type::Bool(), vgi::type::Timestamp('s')}) {
		auto table = CheckTable();
		table.arrow_schema = vgi::MakeSchema({vgi_test::Field("budget", type)});
		CHECK_THROWS(vgi::DecodeNativeConstraints(table));
	}
	auto table = CheckTable();
	table.row_id_column = 0;
	CHECK_THROWS(vgi::DecodeNativeConstraints(table));
	table = CheckTable();
	table.arrow_schema = vgi::MakeSchema(
	    {vgi_test::Field("budget", vgi::type::Float64()), vgi_test::Field("BUDGET", vgi::type::Float64())});
	CHECK_THROWS(vgi::DecodeNativeConstraints(table));
	auto expression = vgi::DeserializeFromIpcBytes(CheckExpression());
	for (int index : {1, 2}) {
		auto value = index == 1 ? vgi::BuildStringScalar("vgi.expression.unsupported.v1")
		                        : vgi::BuildStringListScalar({"other"});
		table = CheckTable(vgi::SerializeToIpcBytes(vgi_test::ReplaceColumn(expression, index, value)));
		CHECK_THROWS(vgi::DecodeNativeConstraints(table));
	}
	auto context = vgi_test::StructRow(expression->column(3), vgi::generated::QueryEvaluationContextSchema());
	for (int index = 0; index < 5; ++index) {
		auto settings = vgi_test::ReplaceColumn(
		    context, index, index == 4 ? vgi::BuildBoolScalar(false) : vgi::BuildStringScalar("unsupported"));
		ArrowArray values;
		settings->ExportTo(&values);
		table = CheckTable(vgi::SerializeToIpcBytes(vgi_test::ReplaceColumn(expression, 3, values)));
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
	auto paths = vgi_test::PathList({{"a.b", std::nullopt}});
	table.constraints = {ConstraintRecord(vgi::SerializeToIpcBytes(vgi_test::ReplaceColumn(definition, 1, paths)))};
	CHECK_THROWS(vgi::DecodeNativeConstraints(table));

	vgi::VgiBatchBuilder builder(vgi::MakeSchema({vgi::type::List(vgi::type::Binary())}));
	vgi::append::Null(builder.column(0)->children[0]);
	vgi::append::FinishElement(builder.column(0));
	builder.FinishRow();
	auto batch = builder.Finish();
	CHECK_THROWS(vgi::ValidateNativeConstraintList(batch->column(0), 0));
}

TEST_CASE("Constraint mutation snapshots require exact bounded single-record IPC", "[constraint-contract]") {
	auto table = NativeTable();
	vgi::DecodeNativeConstraints(table);
	auto change = vgi::BuildNativeNotNullChange(table, "a.b", false);
	auto request = vgi::DeserializeFromIpcBytes(vgi::BuildNativeConstraintsApply({1}, {}, table, change));
	vgi::RecordBatchSingleRow request_row(request, 0, "ConstraintsApplyRequest", "test");
	auto snapshot = vgi::VgiBatch::FromColumns(
	    vgi::generated::ConstraintSnapshotSchema(), 1,
	    {vgi::BuildBinaryScalarRequired(request_row["table"].value_not_null<std::vector<uint8_t>>()),
	     vgi::BuildBinaryScalarRequired({3}), vgi::BuildBinaryListScalar({})});
	auto bytes = vgi::SerializeToIpcBytes(snapshot);
	CHECK_NOTHROW(vgi::ValidateNativeConstraintSnapshot(bytes, table));
	bytes.push_back(0);
	CHECK_THROWS(vgi::ValidateNativeConstraintSnapshot(bytes, table));
	auto sink = std::make_shared<vgi::VgiBufferOutputStream>();
	auto writer = vgi::VgiIpcWriter::Open(sink, snapshot->schema());
	writer->Write(*snapshot);
	writer->Write(*snapshot);
	writer->Close();
	auto buffer = sink->Finish();
	CHECK_THROWS(vgi::ValidateNativeConstraintSnapshot(
	    std::vector<uint8_t>(buffer->data(), buffer->data() + buffer->size()), table));
	auto metadata_schema = snapshot->schema()->WithMetadata(vgi_test::Metadata({"unexpected"}, {"value"}));
	CHECK_THROWS(
	    vgi::ValidateNativeConstraintSnapshot(vgi::SerializeToIpcBytes(snapshot->WithSchema(metadata_schema)), table));
}

TEST_CASE("Native CREATE resolves SQL spelling before emitting raw unique paths", "[constraint-contract]") {
	CreateTableInfo create;
	create.SetQualifiedName(QualifiedName(Identifier(), "main", "t"));
	create.columns.AddColumn(ColumnDefinition("Mixed.Name", LogicalType::INTEGER));
	create.constraints.push_back(make_uniq<UniqueConstraint>(vector<Identifier> {"mixed.name"}, false));
	auto schema = vgi::MakeSchema({vgi_test::Field("Mixed.Name", vgi::type::Int32())});
	auto bytes = vgi::EncodeNativeConstraints(create, schema);
	auto definition = vgi::DeserializeFromIpcBytes(bytes[0]);
	vgi::RecordBatchSingleRow row(definition, 0, "ConstraintDefinition", "test");
	CHECK(row["column_paths"].value_not_null<std::vector<std::vector<std::string>>>() ==
	      std::vector<std::vector<std::string>> {{"Mixed.Name"}});
}

TEST_CASE("Native FK discovery retains nested IPC storage while decoding raw paths", "[constraint-contract]") {
	CreateTableInfo create;
	create.SetQualifiedName(QualifiedName(Identifier(), "main", "t"));
	create.columns.AddColumn(ColumnDefinition("Child.Key", LogicalType::INTEGER));
	ForeignKeyInfo fk;
	fk.type = ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE;
	fk.schema = "main";
	fk.table = "parent";
	create.constraints.push_back(
	    make_uniq<ForeignKeyConstraint>(vector<Identifier> {"Parent.Key"}, vector<Identifier> {"child.key"}, fk));
	auto schema = vgi::MakeSchema({vgi_test::Field("Child.Key", vgi::type::Int32())});
	auto definitions = vgi::EncodeNativeConstraints(create, schema);
	auto record = vgi::DeserializeFromIpcBytes(ConstraintRecord(definitions[0]));
	auto target = vgi::SerializeToIpcBytes(
	    vgi::VgiBatch::FromColumns(vgi::generated::CatalogObjectRefSchema(), 1,
	                               {vgi::BuildStringScalar("constraint"), vgi::BuildStringListScalar({"main"}),
	                                vgi::BuildStringScalar("persisted-parent-key"), vgi::BuildStringScalar("parent")}));
	auto changed = vgi_test::ReplaceColumn(record, 4, vgi::BuildBinaryScalarRequired(target));
	vgi::VgiTableInfo table;
	table.schema_name = "main";
	table.name = "t";
	table.arrow_schema = schema;
	table.constraints = {vgi::SerializeToIpcBytes(changed)};
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
