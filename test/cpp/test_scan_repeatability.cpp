// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catch.hpp"
#include "core_functions_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_bind_protocol.hpp"
#include "vgi_extension.hpp"
#include "vgi_rpc_types.hpp"
#include "vgi_table_function_impl.hpp"
#include "../../src/generated/vgi_protocol_schemas.hpp"
#include <cstdlib>

using namespace duckdb;
namespace {
using Batch = std::shared_ptr<arrow::RecordBatch>;
Batch Bind(const std::shared_ptr<arrow::Array> &repeatability) {
	auto schema = arrow::schema({arrow::field("n", arrow::int32())});
	return arrow::RecordBatch::Make(vgi::generated::BindResultSchema(), 1,
	                                {vgi::BuildBinaryScalarRequired(vgi::SerializeSchemaToIpcBytes(schema)),
	                                 vgi::BuildBinaryScalarRequired({1, 2, 3}), vgi::BuildStringListScalar({}),
	                                 vgi::BuildStringListScalar({}), vgi::BuildStringListScalar({}),
	                                 vgi::BuildStringListScalar({}), repeatability});
}
Batch Bind(const std::string &value) {
	return Bind(vgi::BuildEnumArray(value, {"UNKNOWN", "STABLE_WITHIN_QUERY"}));
}
LogicalGet *FindScan(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_GET &&
	    op.Cast<LogicalGet>().function.function == vgi::VgiTableFunctionScan)
		return &op.Cast<LogicalGet>();
	for (auto &child : op.children)
		if (auto scan = FindScan(*child))
			return scan;
	return nullptr;
}
} // namespace

TEST_CASE("Bind repeatability validates the required enum independently of dictionary order", "[repeatability]") {
	CHECK(vgi::ParseBindResponse(Bind("UNKNOWN")).repeatability == vgi::BindRepeatability::UNKNOWN);
	CHECK(vgi::ParseBindResponse(Bind("STABLE_WITHIN_QUERY")).repeatability ==
	      vgi::BindRepeatability::STABLE_WITHIN_QUERY);
	auto reversed = vgi::BuildEnumArray("STABLE_WITHIN_QUERY", {"STABLE_WITHIN_QUERY", "UNKNOWN"});
	CHECK(vgi::ParseBindResponse(Bind(reversed)).repeatability == vgi::BindRepeatability::STABLE_WITHIN_QUERY);
	CHECK_THROWS(vgi::ParseBindResponse(Bind("UNKNOWN")->RemoveColumn(6).ValueOrDie()));
	CHECK_THROWS(vgi::ParseBindResponse(Bind(vgi::BuildEnumArray("stable_within_query", {"stable_within_query"}))));
	CHECK_THROWS(vgi::ParseBindResponse(Bind(vgi::BuildEnumArray("ALWAYS", {"ALWAYS"}))));
	CHECK_THROWS(vgi::ParseBindResponse(Bind(arrow::MakeArrayOfNull(arrow::utf8(), 1).ValueOrDie())));
	CHECK_THROWS(vgi::ParseBindResponse(
	    Bind(arrow::MakeArrayOfNull(arrow::dictionary(arrow::int16(), arrow::utf8()), 1).ValueOrDie())));
	arrow::Int16Builder builder;
	vgi::ThrowOnArrowError(builder.Append(0));
	auto null_value = arrow::DictionaryArray::FromArrays(builder.Finish().ValueOrDie(),
	                                                     arrow::MakeArrayOfNull(arrow::utf8(), 1).ValueOrDie())
	                      .ValueOrDie();
	CHECK_THROWS(vgi::ParseBindResponse(Bind(null_value)));
	arrow::Int32Builder wrong_index;
	vgi::ThrowOnArrowError(wrong_index.Append(0));
	auto values = std::static_pointer_cast<arrow::DictionaryArray>(reversed)->dictionary();
	CHECK_THROWS(vgi::ParseBindResponse(
	    Bind(arrow::DictionaryArray::FromArrays(wrong_index.Finish().ValueOrDie(), values).ValueOrDie())));
}

TEST_CASE("Stable bind snapshots survive direct copies and disable statement reuse without a projection offer",
          "[repeatability]") {
	vgi::VgiTableFunctionBindData data;
	CHECK(data.SupportStatementCache());
	auto response = vgi::SerializeToIpcBytes(Bind("STABLE_WITHIN_QUERY"));
	data.bind_result = vgi::BuildBindResultFromInlinedBytes({4, 5}, response, "test", false, {});
	REQUIRE(data.bind_result.projection_expression_profiles.empty());
	CHECK(data.bind_result.repeatability == vgi::BindRepeatability::STABLE_WITHIN_QUERY);
	CHECK_FALSE(data.SupportStatementCache());
	auto copied = data.Copy();
	auto &copy = copied->Cast<vgi::VgiTableFunctionBindData>();
	CHECK(copy.bind_result.repeatability == data.bind_result.repeatability);
	CHECK(copy.bind_result.bind_request_bytes == data.bind_result.bind_request_bytes);
	CHECK(copy.bind_result.bind_response_bytes == data.bind_result.bind_response_bytes);
	CHECK(copy.bind_result.opaque_data == data.bind_result.opaque_data);
	CHECK_FALSE(copy.SupportStatementCache());
	data.bind_result.opaque_data[0] = 99;
	CHECK(copy.bind_result.opaque_data[0] == 1);
}

TEST_CASE("Stable table plan copies preserve the snapshot and reject changed rebind state", "[repeatability-native]") {
	auto worker = std::getenv("VGI_REPEATABILITY_WORKER");
	REQUIRE(worker);
	DuckDB db(nullptr);
	db.LoadStaticExtension<CoreFunctionsExtension>();
	db.LoadStaticExtension<VgiExtension>();
	Connection con(db);
	for (auto sql :
	     {"SET default_collation='binary'", "SET default_order='asc'", "SET default_null_order='nulls_last'"})
		REQUIRE_FALSE(con.Query(sql)->HasError());
	auto attach =
	    "ATTACH 'repeatability_reference' AS w (TYPE vgi, LOCATION '" + StringUtil::Replace(worker, "'", "''") + "')";
	auto attached = con.Query(attach);
	INFO(attached->GetError());
	REQUIRE_FALSE(attached->HasError());
	con.BeginTransaction();
	for (auto sql : {"SELECT n FROM w.main.stable", "SELECT CAST(n AS BIGINT) FROM w.main.stable"}) {
		auto plan = con.ExtractPlan(sql);
		auto scan = FindScan(*plan);
		REQUIRE(scan);
		auto &original = scan->bind_data->Cast<vgi::VgiTableFunctionBindData>();
		REQUIRE(original.bind_result.repeatability == vgi::BindRepeatability::STABLE_WITHIN_QUERY);
		auto copied = plan->Copy(*con.context);
		auto copied_scan = FindScan(*copied);
		REQUIRE(copied_scan);
		auto &restored = copied_scan->bind_data->Cast<vgi::VgiTableFunctionBindData>();
		CHECK(restored.bind_result.repeatability == original.bind_result.repeatability);
		CHECK(restored.bind_result.bind_request_bytes == original.bind_result.bind_request_bytes);
		CHECK(restored.bind_result.bind_response_bytes == original.bind_result.bind_response_bytes);
		CHECK(restored.bind_result.opaque_data == original.bind_result.opaque_data);
		CHECK_FALSE(restored.SupportStatementCache());
		// The serializer must compare complete provider state, not just schema
		// or the enum. Alter the saved result while the live provider stays fixed.
		original.bind_result.bind_response_bytes.push_back(0);
		CHECK_THROWS_WITH(plan->Copy(*con.context), Catch::Contains("Repeatable VGI source bind state changed"));
	}
}
