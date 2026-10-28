// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catch.hpp"
#include "../support/vgi_contract_fixtures.hpp"
#include "duckdb.hpp"
#include "vgi_bind_protocol.hpp"
#include "vgi_catalog_metadata.hpp"
#include "vgi_query_rpc.hpp"
#include "vgi_query_function.hpp"
#include "vgi_rpc_types.hpp"

using namespace duckdb;
using namespace duckdb::vgi;
using namespace duckdb::vgi_test;

TEST_CASE("Prepared query binding cannot negotiate external secrets", "[query-rpc]") {
	DuckDB db(nullptr);
	Connection con(db);
	int calls = 0;
	auto transport = [&](const std::vector<uint8_t> &) {
		++calls;
		vgi::generated::BindResponse response;
		response.lookup_secret_types = {"s3"};
		response.lookup_scopes = {""};
		response.lookup_names = {""};
		return vgi::generated::ToBatch(response);
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
	auto schema = MakeSchema({type::Int64().Named("value")}, Metadata({"owner"}, {"prepared"}));
	CHECK_NOTHROW(ValidateVgiQueryResultBatch(VgiBatch::MakeEmpty(schema), schema));
	CHECK_THROWS(ValidateVgiQueryResultBatch(VgiBatch::MakeEmpty(schema->WithMetadata({})), schema));

	auto nested_schema = MakeSchema({type::Struct({type::Int64().Named("required", false)}).Named("outer")});
	VgiBatchBuilder nested(nested_schema);
	append::Null(nested.column(0)->children[0]);
	append::FinishElement(nested.column(0));
	nested.FinishRow();
	CHECK_THROWS(ValidateVgiQueryResultBatch(nested.Finish(), nested_schema));

	auto dictionary_schema = MakeSchema({type::Dictionary(type::Int8(), type::Int64()).Named("required", false)});
	VgiBatchBuilder dictionary(dictionary_schema);
	append::Null(dictionary.column(0)->dictionary);
	append::Int(dictionary.column(0), 0);
	dictionary.FinishRow();
	CHECK_THROWS(ValidateVgiQueryResultBatch(dictionary.Finish(), dictionary_schema));
}
