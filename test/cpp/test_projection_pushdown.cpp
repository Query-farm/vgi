// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "core_functions_extension.hpp"
#include "vgi_extension.hpp"
#include "vgi_projection_pushdown.hpp"
#include "vgi_table_function_impl.hpp"
#include "vgi_arrow_ipc.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_column_expression.hpp"
#include "vgi_constraint_contract.hpp"
#include "vgi_rpc_types.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "../../src/generated/vgi_protocol_schemas.hpp"
#include <cstdlib>
#include "nanoarrow/nanoarrow.h"

using namespace duckdb;
namespace {
using Batch = vgi::VgiBatchPtr;
using Schema = vgi::VgiSchemaPtr;
using Bytes = std::vector<uint8_t>;
Schema Source() {
	return vgi::MakeSchema({vgi::type::Int32().Named("n").WithMetadata(vgi::VgiMetadata({{"raw", "source"}})),
	                        vgi::type::Int32().Named("other")},
	                       vgi::VgiMetadata({{"table", "metadata"}}));
}
Schema Results() {
	return vgi::MakeSchema({vgi::type::Int64().Named("wide"), vgi::type::Int64().Named("wide_other")});
}
Batch Bind(const Schema &schema, const std::vector<std::string> &profiles = {vgi::VGI_CONSTRAINT_SCALAR_READ},
           const std::string &repeatability = "UNKNOWN") {
	return vgi::VgiBatch::FromColumns(
	    vgi::generated::BindResultSchema(), 1,
	    {vgi::BuildBinaryScalarRequired(vgi::SerializeSchemaToIpcBytes(schema)), vgi::BuildBinaryScalarRequired({1, 2}),
	     vgi::BuildStringListScalar({}), vgi::BuildStringListScalar({}), vgi::BuildStringListScalar({}),
	     vgi::BuildStringListScalar(profiles), vgi::BuildEnumArray(repeatability, {"UNKNOWN", "STABLE_WITHIN_QUERY"})});
}
Batch Replace(const Batch &batch, const std::string &name, ArrowArray value) {
	ArrowArrayWrapper exported;
	batch->ExportTo(exported);
	auto column = exported.arrow_array.children[batch->schema()->GetFieldIndex(name)];
	column->release(column);
	ArrowArrayMove(&value, column);
	return vgi::VgiBatch::Adopt(batch->schema(), &exported.arrow_array);
}
Bytes Request(const std::string &repeatability = "UNKNOWN") {
	auto call = vgi::SerializeToIpcBytes(
	    Replace(vgi::BuildBindRequest("scan", {}, "TABLE"), "schema_path", vgi::BuildStringListScalar({"main"})));
	auto bind = vgi::SerializeToIpcBytes(Bind(Source(), {vgi::VGI_CONSTRAINT_SCALAR_READ}, repeatability));
	vgi::VgiBatchBuilder builder(vgi::generated::TableFunctionProjectionRequestSchema());
	vgi::append::Bytes(builder.column(0), call.data(), call.size());
	vgi::append::Bytes(builder.column(1), bind.data(), bind.size());
	for (auto sql : {"CAST(n AS BIGINT)", "CAST(other AS BIGINT)"}) {
		auto expression = vgi::BuildNativeColumnExpression(sql, "main", {}, true);
		vgi::append::Bytes(builder.column(2)->children[0], expression.data(), expression.size());
	}
	vgi::append::FinishElement(builder.column(2));
	auto result_schema = vgi::SerializeSchemaToIpcBytes(Results());
	vgi::append::Bytes(builder.column(3), result_schema.data(), result_schema.size());
	builder.FinishRow();
	return vgi::SerializeToIpcBytes(builder.Finish());
}
Batch Response(const std::vector<int64_t> &indices, const Batch &bind = nullptr) {
	auto result = bind ? vgi::BuildBinaryScalarRequired(vgi::SerializeToIpcBytes(bind))
	                   : vgi::BuildOptionalBinaryScalar(std::nullopt);
	return vgi::VgiBatch::FromColumns(vgi::generated::TableFunctionProjectionResultSchema(), 1,
	                                  {vgi::BuildInt64ListScalar(indices), result});
}
Schema Expanded(const std::vector<int64_t> &indices) {
	auto schema = Source();
	for (auto index : indices)
		schema = schema->AddField(schema->num_fields(), Results()->field(index));
	return schema;
}
LogicalGet *FindScan(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_GET &&
	    op.Cast<LogicalGet>().function.function == vgi::VgiTableFunctionScan)
		return &op.Cast<LogicalGet>();
	for (auto &child : op.children)
		if (auto found = FindScan(*child))
			return found;
	return nullptr;
}
} // namespace

TEST_CASE("Projection response retains base identity and validates partial result slots", "[projection-contract]") {
	auto request = Request();
	CHECK(vgi::ValidateProjectionAcceptance(request, Response({})).indices.empty());
	auto partial = vgi::ValidateProjectionAcceptance(request, Response({1}, Bind(Expanded({1}))));
	REQUIRE(partial.indices.size() == 1);
	CHECK(partial.indices[0] == 1);
	CHECK(partial.output_schema->Equals(*Expanded({1}), true));
	CHECK(vgi::ValidateProjectionAcceptance(request, Response({0, 1}, Bind(Expanded({0, 1})))).indices.size() == 2);
	CHECK_THROWS(vgi::ValidateProjectionAcceptance(request, Response({}, Bind(Source()))));
	CHECK_THROWS(vgi::ValidateProjectionAcceptance(request, Response({0})));
	for (auto indices : {std::vector<int64_t> {0, 0}, {1, 0}, {-1}, {2}})
		CHECK_THROWS(vgi::ValidateProjectionAcceptance(request, Response(indices, Bind(Expanded({0})))));
	CHECK_THROWS(vgi::ValidateProjectionAcceptance(request, Response({0}, Bind(Expanded({0}), {}))));
	auto secrets = Replace(Bind(Expanded({0})), "lookup_scopes", vgi::BuildStringListScalar({"scope"}));
	CHECK_THROWS(vgi::ValidateProjectionAcceptance(request, Response({0}, secrets)));
}

TEST_CASE("Projection acceptance preserves the source repeatability claim", "[projection-contract][repeatability]") {
	for (auto original : {"UNKNOWN", "STABLE_WITHIN_QUERY"}) {
		auto request = Request(original);
		CHECK_NOTHROW(vgi::ValidateProjectionAcceptance(
		    request, Response({0}, Bind(Expanded({0}), {vgi::VGI_CONSTRAINT_SCALAR_READ}, original))));
		auto changed = std::string(original) == "UNKNOWN" ? "STABLE_WITHIN_QUERY" : "UNKNOWN";
		CHECK_THROWS_WITH(vgi::ValidateProjectionAcceptance(
		                      request, Response({0}, Bind(Expanded({0}), {vgi::VGI_CONSTRAINT_SCALAR_READ}, changed))),
		                  Catch::Contains("changed repeatability"));
	}
}

TEST_CASE("Projection acceptance rejects type nullability metadata and envelope drift", "[projection-contract]") {
	auto request = Request();
	auto expected = Expanded({0});
	auto plain = vgi::MakeSchema({vgi::type::Int32().Named("n")});
	auto nonnull = vgi::MakeSchema({vgi::type::Int64().Named("wide", false)});
	auto wrong = vgi::MakeSchema({vgi::type::Int32().Named("wide")});
	for (auto bad : {expected->WithMetadata(vgi::VgiMetadata()), expected->SetField(0, plain->field(0)),
	                 expected->SetField(2, nonnull->field(0)), expected->SetField(2, wrong->field(0))}) {
		CHECK_THROWS(vgi::ValidateProjectionAcceptance(request, Response({0}, Bind(bad))));
	}
	auto bytes = vgi::SerializeToIpcBytes(Bind(expected));
	bytes.push_back(0);
	CHECK_THROWS(vgi::ValidateProjectionAcceptance(
	    request, Replace(Response({0}, Bind(expected)), "bind_result", vgi::BuildBinaryScalarRequired(bytes))));
	for (auto eos : {Bytes {255, 255, 255, 255, 0, 0, 0, 0}, Bytes {0, 0, 0, 0}}) {
		auto schema_bytes = vgi::SerializeSchemaToIpcBytes(expected);
		schema_bytes.insert(schema_bytes.end(), eos.begin(), eos.end());
		auto terminated = Replace(Bind(expected), "output_schema", vgi::BuildBinaryScalarRequired(schema_bytes));
		CHECK_NOTHROW(vgi::ValidateProjectionAcceptance(request, Response({0}, terminated)));
		schema_bytes.insert(schema_bytes.end(), eos.begin(), eos.end());
		auto repeated = Replace(Bind(expected), "output_schema", vgi::BuildBinaryScalarRequired(schema_bytes));
		CHECK_THROWS(vgi::ValidateProjectionAcceptance(request, Response({0}, repeated)));
	}
	for (auto trailing : {Bytes {1}, Bytes {0, 0, 0}, Bytes {255, 255, 255, 255, 0, 0, 0, 0, 1}}) {
		auto schema_bytes = vgi::SerializeSchemaToIpcBytes(expected);
		schema_bytes.insert(schema_bytes.end(), trailing.begin(), trailing.end());
		CHECK_THROWS(vgi::ValidateProjectionAcceptance(
		    request,
		    Response({0}, Replace(Bind(expected), "output_schema", vgi::BuildBinaryScalarRequired(schema_bytes)))));
	}
	vgi::VgiBatchBuilder null_builder(vgi::MakeSchema({vgi::type::List(vgi::type::Int64()).Named("accepted_indices")}));
	vgi::append::Null(null_builder.column(0));
	null_builder.FinishRow();
	ArrowArrayWrapper null_export;
	null_builder.Finish()->ExportTo(null_export);
	ArrowArray null_indices;
	ArrowArrayMove(null_export.arrow_array.children[0], &null_indices);
	CHECK_THROWS(vgi::ValidateProjectionAcceptance(request, Replace(Response({}), "accepted_indices", null_indices)));
}

TEST_CASE("Bind projection offers are bounded distinct semantic identifiers", "[projection-contract]") {
	CHECK(vgi::ParseBindResponse(Bind(Source(), {})).projection_expression_profiles.empty());
	CHECK(vgi::ParseBindResponse(Bind(Source(), {"other.scalar.v2"})).projection_expression_profiles.size() == 1);
	for (auto profiles : {std::vector<std::string> {"bad"},
	                      {"x.v0"},
	                      {"x.v1", "x.v1"},
	                      std::vector<std::string>(65, "x.v1"),
	                      {std::string(257, 'a') + ".v1"}})
		CHECK_THROWS(vgi::ParseBindResponse(Bind(Source(), profiles)));
	auto missing = Bind(Source())->SelectColumns({0, 1, 2, 3, 4, 6});
	CHECK_THROWS(vgi::ParseBindResponse(missing));
}

TEST_CASE("Accepted projections survive bind copies and table scan serialization", "[projection-native-copy]") {
	auto worker = std::getenv("VGI_PROJECTION_WORKER");
	REQUIRE(worker);
	auto common_env = std::getenv("VGI_PROJECTION_COMMON");
	const bool common = common_env && std::string(common_env) == "1";
	DuckDB db(nullptr);
	db.LoadStaticExtension<CoreFunctionsExtension>();
	db.LoadStaticExtension<VgiExtension>();
	Connection con(db);
	for (auto sql :
	     {"SET default_collation='binary'", "SET default_order='asc'", "SET default_null_order='nulls_last'"})
		REQUIRE_FALSE(con.Query(sql)->HasError());
	if (common) {
		// Match the SDK SQL probes: discover only the table under test instead
		// of importing unrelated example catalog constraint fixtures.
		REQUIRE_FALSE(con.Query("SET vgi_catalog_contents=false")->HasError());
		REQUIRE_FALSE(con.Query("SET vgi_eager_load_threshold=MAP {'table':0}")->HasError());
	}
	auto attach = "ATTACH '" + std::string(common ? "example" : "projection_reference") +
	              "' AS w (TYPE vgi, LOCATION '" + StringUtil::Replace(worker, "'", "''") + "')";
	auto attached = con.Query(attach);
	INFO(attached->GetError());
	REQUIRE_FALSE(attached->HasError());
	con.BeginTransaction();
	auto plan = con.ExtractPlan(common ? "SELECT CAST(n AS BIGINT) AS wide FROM w.main.projection_rows"
	                                   : "SELECT CAST(n AS BIGINT) AS wide FROM w.main.accepted WHERE other > 0");
	auto scan = FindScan(*plan);
	REQUIRE(scan);
	auto &data = scan->bind_data->Cast<vgi::VgiTableFunctionBindData>();
	INFO("profiles=" << data.bind_result.projection_expression_profiles.size() << " bind_bytes="
	                 << data.bind_result.bind_response_bytes.size() << " schema=" << data.schema_name);
	REQUIRE_FALSE(data.projection_response_bytes.empty());
	CHECK_FALSE(data.SupportStatementCache());
	auto cloned_bind = data.Copy();
	auto &copy = cloned_bind->Cast<vgi::VgiTableFunctionBindData>();
	CHECK(copy.projection_request_bytes == data.projection_request_bytes);
	CHECK(copy.projection_response_bytes == data.projection_response_bytes);
	CHECK(copy.bind_result.opaque_data == data.bind_result.opaque_data);
	auto cloned_plan = plan->Copy(*con.context);
	auto cloned_scan = FindScan(*cloned_plan);
	REQUIRE(cloned_scan);
	auto &roundtrip = cloned_scan->bind_data->Cast<vgi::VgiTableFunctionBindData>();
	CHECK(roundtrip.projection_response_bytes == data.projection_response_bytes);
	CHECK(roundtrip.bind_result.output_schema->Equals(*data.bind_result.output_schema, true));
	CHECK(roundtrip.bind_result.opaque_data == data.bind_result.opaque_data);
	CHECK_FALSE(roundtrip.supports_splits);
	CHECK_FALSE(cloned_scan->function.filter_pushdown);
	// A serialized proposal may not reuse provider state from a different bind.
	auto proposal = vgi::DeserializeVgiQueryRecord(data.projection_request_bytes);
	auto original_bytes = proposal->GetColumnByName("bind_result").GetView(0);
	auto original = vgi::DeserializeVgiQueryRecord(Bytes(original_bytes.begin(), original_bytes.end()));
	auto changed = Replace(original, "opaque_data", vgi::BuildBinaryScalarRequired({9, 9}));
	data.projection_request_bytes = vgi::SerializeToIpcBytes(
	    Replace(proposal, "bind_result", vgi::BuildBinaryScalarRequired(vgi::SerializeToIpcBytes(changed))));
	CHECK_THROWS_WITH(plan->Copy(*con.context), Catch::Contains("source bind state changed"));

	// Registered conversions remain local even when their executable happens
	// to equal the built-in cast. Registration itself may change semantics.
	auto &cast_functions = CastFunctionSet::Get(*con.context);
	GetCastFunctionInput cast_input(*con.context);
	auto registered_cast = cast_functions.GetCastFunction(LogicalType::INTEGER, LogicalType::BIGINT, cast_input);
	cast_functions.RegisterCastFunction(LogicalType::INTEGER, LogicalType::BIGINT, std::move(registered_cast));
	auto custom_plan = con.ExtractPlan(common ? "SELECT CAST(n AS BIGINT) FROM w.main.projection_rows"
	                                          : "SELECT CAST(n AS BIGINT) FROM w.main.accepted");
	auto custom_scan = FindScan(*custom_plan);
	REQUIRE(custom_scan);
	CHECK(custom_scan->bind_data->Cast<vgi::VgiTableFunctionBindData>().projection_response_bytes.empty());
}
