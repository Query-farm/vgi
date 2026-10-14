// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catch.hpp"
#include "duckdb.hpp"
#include "vgi_bind_protocol.hpp"
#include "vgi_catalog_metadata.hpp"
#include "vgi_query_rpc.hpp"
#include "vgi_query_function.hpp"
#include "vgi_rpc_types.hpp"

using namespace duckdb;

TEST_CASE("Prepared query binding cannot negotiate external secrets", "[query-rpc]") {
	DuckDB db(nullptr);
	Connection con(db);
	int calls = 0;
	auto transport = [&](const std::vector<uint8_t> &) {
		++calls;
		auto types = vgi::BuildStringListScalar({"s3"});
		return arrow::RecordBatch::Make(arrow::schema({arrow::field("lookup_secret_types", types->type())}),
		                                1, {types});
	};
	CHECK_THROWS_WITH(vgi::PerformBindProtocol(*con.context, vgi::VGI_QUERY_EXECUTOR, "TABLE", nullptr,
	                                          nullptr, {1}, {}, {}, {}, "test-worker", transport),
	                  Catch::Matchers::Contains("cannot request secret negotiation"));
	CHECK(calls == 1);

	vgi::VgiSecretRequirement secret;
	secret.secret_type = "s3";
	CHECK_THROWS_WITH(vgi::PerformBindProtocol(*con.context, vgi::VGI_QUERY_EXECUTOR, "TABLE", nullptr,
	                                          nullptr, {1}, {}, {}, {secret}, "test-worker", transport),
	                  Catch::Matchers::Contains("does not allow external secret resolution"));
	CHECK(calls == 1);
}

TEST_CASE("Prepared query output validates empty schemas and nested required values", "[query-rpc]") {
	auto schema = arrow::schema({arrow::field("value", arrow::int64())},
	                            arrow::key_value_metadata({"owner"}, {"prepared"}));
	auto empty = arrow::MakeArrayOfNull(arrow::int64(), 0).ValueOrDie();
	CHECK_NOTHROW(vgi::ValidateVgiQueryResultBatch(arrow::RecordBatch::Make(schema, 0, {empty}), schema));
	CHECK_THROWS(vgi::ValidateVgiQueryResultBatch(
	    arrow::RecordBatch::Make(schema->RemoveMetadata(), 0, {empty}), schema));

	auto nulls = arrow::MakeArrayOfNull(arrow::int64(), 1).ValueOrDie();
	auto child = arrow::field("required", arrow::int64(), false);
	auto nested = arrow::StructArray::Make({nulls}, {child}).ValueOrDie();
	auto nested_schema = arrow::schema({arrow::field("outer", nested->type())});
	CHECK_THROWS(vgi::ValidateVgiQueryResultBatch(
	    arrow::RecordBatch::Make(nested_schema, 1, {nested}), nested_schema));

	auto index = arrow::MakeArrayFromScalar(arrow::Int8Scalar(0), 1).ValueOrDie();
	auto dictionary = arrow::DictionaryArray::FromArrays(index, nulls).ValueOrDie();
	auto dictionary_schema = arrow::schema({arrow::field("required", dictionary->type(), false)});
	CHECK_THROWS(vgi::ValidateVgiQueryResultBatch(
	    arrow::RecordBatch::Make(dictionary_schema, 1, {dictionary}), dictionary_schema));
}
