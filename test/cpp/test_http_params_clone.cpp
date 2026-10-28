// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "httpfs_extension.hpp"
#include "http/httpfs_client.hpp"

using namespace duckdb;

TEST_CASE("HTTP parameter clones keep configuration and isolate request policy", "[http_retry_budget][http_params_clone]") {
	HTTPUtil http;
	HTTPParams params(http);
	params.timeout = 17;
	params.retries = 4;
	params.http_proxy = "proxy.example";
	params.extra_headers["x-fixture"] = "value";
	auto clone = params.Clone();
	REQUIRE(clone);
	CHECK(&clone->http_util == &params.http_util);
	CHECK(clone->timeout == 17);
	CHECK(clone->retries == 4);
	CHECK(clone->http_proxy == "proxy.example");
	CHECK(clone->extra_headers["x-fixture"] == "value");
	clone->retries = 0;
	clone->extra_headers["x-fixture"] = "request-only";
	CHECK(params.retries == 4);
	CHECK(params.extra_headers["x-fixture"] == "value");
}

TEST_CASE("HTTPFS clones retain provider fields and isolate the request context", "[http-interrupt][http-params-clone]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<HttpfsExtension>();
	Connection con(db);
	Connection other(db);
	auto &context = *con.context;
	context.RunFunctionInTransaction([&] {
		auto owned = HTTPUtil::Get(*context.db).InitializeParameters(context, "http://localhost/clone");
		auto &provider = owned->Cast<HTTPFSParams>();
		provider.ca_cert_file = "fixture-ca.pem";
		provider.bearer_token = "fixture-token";
		provider.retries = 7;
		auto cloned = owned->Clone(); // virtual call through HTTPParams
		auto &copied = cloned->Cast<HTTPFSParams>();
		CHECK(copied.ca_cert_file == provider.ca_cert_file);
		CHECK(copied.bearer_token == provider.bearer_token);
		CHECK(copied.GetTransportReuseDomain() == provider.GetTransportReuseDomain());
		copied.interrupt_context = other.context;
		copied.retries = 0;
		context.Interrupt();
		CHECK(provider.IsInterrupted());
		CHECK_FALSE(copied.IsInterrupted());
		other.Interrupt();
		CHECK(copied.IsInterrupted());
		copied.interrupt_context.reset();
		CHECK_FALSE(copied.IsInterrupted());
		CHECK(provider.IsInterrupted());
		CHECK(provider.retries == 7);
		context.ClearInterrupt();
		other.context->ClearInterrupt();
	});
}
