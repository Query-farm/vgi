// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Unit tests for the structured-error helpers in vgi_exception.hpp: credential
// redaction (security-relevant: these values reach errors_as_json output and
// logs), the ErrorInfo builder, and type-preserving rethrow.

#include "catch.hpp"

#include "vgi_exception.hpp"

using namespace duckdb;
using namespace duckdb::vgi;

TEST_CASE("RedactCredentials strips URI userinfo passwords", "[error-info]") {
	REQUIRE(RedactCredentials("https://alice:hunter2@example.com/vgi") == "https://alice:***@example.com/vgi");
	REQUIRE(RedactCredentials("postgres://u:p%40ss@db:5432/x?sslmode=require") ==
	        "postgres://u:***@db:5432/x?sslmode=require");
	// A user without a password, and an '@' outside the authority, are left alone.
	REQUIRE(RedactCredentials("https://alice@example.com/a") == "https://alice@example.com/a");
	REQUIRE(RedactCredentials("https://example.com/users/@me") == "https://example.com/users/@me");
	REQUIRE(RedactCredentials("uv run --project ~/x vgi-fixture-worker") == "uv run --project ~/x vgi-fixture-worker");
}

TEST_CASE("RedactCredentials strips key=value secrets", "[error-info]") {
	REQUIRE(RedactCredentials("postgres:host=db user=u password=s3cret dbname=x") ==
	        "postgres:host=db user=u password=*** dbname=x");
	REQUIRE(RedactCredentials("Server=db;PWD=s3cret;Database=x") == "Server=db;PWD=***;Database=x");
	REQUIRE(RedactCredentials("https://h/p?a=1&token=abc&b=2") == "https://h/p?a=1&token=***&b=2");
	REQUIRE(RedactCredentials("secret=x") == "secret=***");
	// Only whole keys: "passwordless" and "api_token_count" are not secrets.
	REQUIRE(RedactCredentials("mode=passwordless") == "mode=passwordless");
	REQUIRE(RedactCredentials("mytoken=abc") == "mytoken=abc");
}

TEST_CASE("ErrorInfo skips empty values and names the transport", "[error-info]") {
	auto info = ErrorInfo(error_subtype::kProtocolViolation).Rpc("bind").Set(error_key::kTable, "").Worker("/bin/w", 42);
	REQUIRE(info.at(error_key::kErrorSubtype) == "PROTOCOL_VIOLATION");
	REQUIRE(info.at(error_key::kRpcMethod) == "bind");
	REQUIRE(info.count(error_key::kTable) == 0);
	REQUIRE(info.at(error_key::kWorkerPid) == "42");
	REQUIRE(info.at(error_key::kTransport) == "subprocess");

	auto http = ErrorInfo().Worker("https://user:pw@h.example/vgi");
	REQUIRE(http.at(error_key::kTransport) == "https");
	REQUIRE(http.at(error_key::kWorkerPath) == "https://user:***@h.example/vgi");
}

TEST_CASE("ErrorInfo::Merge keeps existing values", "[error-info]") {
	auto info = ErrorInfo(error_subtype::kSecretLookupFailed);
	info.Merge({{error_key::kErrorSubtype, "AUTH_FAILED"}, {error_key::kUrl, "u"}, {"stack_trace_pointers", "x"}});
	REQUIRE(info.at(error_key::kErrorSubtype) == "SECRET_LOOKUP_FAILED");
	REQUIRE(info.at(error_key::kUrl) == "u");
	REQUIRE(info.count("stack_trace_pointers") == 0);
}

TEST_CASE("RawMessageOf does not nest DuckDB's JSON envelope", "[error-info]") {
	try {
		throw IOException(ErrorInfo(error_subtype::kTransportFailure).Url("http://h"), "boom %d", 7);
	} catch (const std::exception &e) {
		REQUIRE(std::string(e.what()).find("{") == 0); // what() is the JSON form
		REQUIRE(RawMessageOf(e) == "boom 7");
		REQUIRE(ExtraInfoOf(e).at(error_key::kErrorSubtype) == "TRANSPORT_FAILURE");
	}
	REQUIRE(RawMessageOf(std::runtime_error("plain")) == "plain");
}

TEST_CASE("ThrowTypedException keeps the concrete exception class", "[error-info]") {
	// ErrorData::Throw() would raise the base Exception and miss these catches.
	bool caught = false;
	try {
		ThrowTypedException(ExceptionType::IO, ErrorInfo(error_subtype::kTransportFailure), "io");
	} catch (const IOException &e) {
		caught = true;
		REQUIRE(ExtraInfoOf(e).at(error_key::kErrorSubtype) == "TRANSPORT_FAILURE");
		REQUIRE(RawMessageOf(e) == "io");
	}
	REQUIRE(caught);
	REQUIRE_THROWS_AS(ThrowTypedException(ExceptionType::INVALID_INPUT, ErrorInfo(), "x"), InvalidInputException);
	REQUIRE_THROWS_AS(ThrowTypedException(ExceptionType::BINDER, ErrorInfo(), "x"), BinderException);
	REQUIRE_THROWS_AS(ThrowTypedException(ExceptionType::INTERRUPT, ErrorInfo(), "x"), InterruptException);
}
