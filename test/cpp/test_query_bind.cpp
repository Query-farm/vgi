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

// Python's Arguments.deserialize_from_bytes reads row 0 of the arguments batch.
TEST_CASE("A bind request without arguments still carries one args row", "[query-rpc]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto bytes = BuildBindRequestBytes(*con.context, "f", "TABLE", nullptr, nullptr, {}, {}, {}, {}, false,
	                                   "test-worker");
	auto request = DecodeIpcRecord<generated::BindRequest>(bytes, "test-worker");
	REQUIRE(request.arguments.has_value());
	CHECK_FALSE(request.arguments_location.has_value());
	auto args = DeserializeFromIpcBytes(*request.arguments);
	REQUIRE(args->num_rows() == 1);
	CHECK_FALSE(args->GetColumnByName("args").IsNull(0));
}

// Protocol 3.4 argument references (docs/large-values-3.4.md §2, §4).
namespace {
struct CapturedBind {};
} // namespace

TEST_CASE("An externalized bind sends arguments_location and null arguments, decided once", "[query-rpc]") {
	DuckDB db(nullptr);
	Connection con(db);
	int externalize_calls = 0;
	std::vector<uint8_t> seen_arguments;
	ArgumentsExternalizerFn externalize = [&](const std::vector<uint8_t> &arguments_ipc) {
		++externalize_calls;
		seen_arguments = arguments_ipc;
		return std::optional<ArgumentsLocationRef>(ArgumentsLocationRef {{1, 2, 3, 4}, 4242, std::string(64, 'a')});
	};
	std::vector<std::vector<uint8_t>> sent;
	auto transport = [&](const std::vector<uint8_t> &request) -> VgiBatchPtr {
		sent.push_back(request);
		throw CapturedBind {};
	};
	CHECK_THROWS_AS(PerformBindProtocol(*con.context, "f", "TABLE", nullptr, nullptr, {}, {}, {}, {}, "test-worker",
	                                    transport, {}, {}, nullptr, nullptr, {}, std::nullopt, externalize),
	                CapturedBind);
	CHECK(externalize_calls == 1);
	// The externalizer saw exactly the bytes an inline bind would carry.
	auto inline_request = DecodeIpcRecord<generated::BindRequest>(
	    BuildBindRequestBytes(*con.context, "f", "TABLE", nullptr, nullptr, {}, {}, {}, {}, false, "test-worker"),
	    "test-worker");
	REQUIRE(inline_request.arguments.has_value());
	CHECK(seen_arguments == *inline_request.arguments);

	REQUIRE(sent.size() == 1);
	auto request = DecodeIpcRecord<generated::BindRequest>(sent[0], "test-worker");
	CHECK_FALSE(request.arguments.has_value());
	REQUIRE(request.arguments_location.has_value());
	CHECK(request.arguments_location->ref == std::vector<uint8_t> {1, 2, 3, 4});
	CHECK(request.arguments_location->size == 4242);
	CHECK(request.arguments_location->sha256 == std::string(64, 'a'));
	// arguments_location is BindRequest's last field (positional decoders rely on it).
	auto batch = DeserializeFromIpcBytes(sent[0]);
	const auto &schema = *batch->schema();
	CHECK(std::string(schema.field(schema.num_fields() - 1).name) == "arguments_location");
}

TEST_CASE("A bind the externalizer declines keeps its arguments inline", "[query-rpc]") {
	DuckDB db(nullptr);
	Connection con(db);
	ArgumentsExternalizerFn decline = [](const std::vector<uint8_t> &) { return std::optional<ArgumentsLocationRef>(); };
	std::vector<uint8_t> sent;
	auto transport = [&](const std::vector<uint8_t> &request) -> VgiBatchPtr {
		sent = request;
		throw CapturedBind {};
	};
	CHECK_THROWS_AS(PerformBindProtocol(*con.context, "f", "TABLE", nullptr, nullptr, {}, {}, {}, {}, "test-worker",
	                                    transport, {}, {}, nullptr, nullptr, {}, std::nullopt, decline),
	                CapturedBind);
	auto request = DecodeIpcRecord<generated::BindRequest>(sent, "test-worker");
	CHECK(request.arguments.has_value());
	CHECK_FALSE(request.arguments_location.has_value());
	// Byte-identical to the bind built without an externalizer.
	CHECK(sent == BuildBindRequestBytes(*con.context, "f", "TABLE", nullptr, nullptr, {}, {}, {}, {}, false,
	                                    "test-worker"));
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
