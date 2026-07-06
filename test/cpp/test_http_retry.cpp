// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Layer-1 unit tests for the dependency-free HTTP retry helpers in
// vgi_http_retry.cpp: which statuses retry, Retry-After parsing (delta-seconds
// and HTTP-date), and the jittered exponential backoff. Pure functions — no
// network, no DuckDB, no real clock/RNG (both are injected). Mirrors the Python
// reference client's vgi_rpc/http/_retry.py semantics.

#include "catch.hpp"

#include "vgi_http_retry.hpp"

using duckdb::vgi::ComputeRetryDelaySeconds;
using duckdb::vgi::HttpRetryPolicy;
using duckdb::vgi::IsRetryableStatus;
using duckdb::vgi::ParseRetryAfter;

TEST_CASE("IsRetryableStatus: 429/503 always, 502/504 by policy", "[retry]") {
	auto full = HttpRetryPolicy::Default();
	auto tick = HttpRetryPolicy::ExchangeTick();

	// The proxy's backpressure codes are retryable under every policy.
	REQUIRE(IsRetryableStatus(429, full));
	REQUIRE(IsRetryableStatus(503, full));
	REQUIRE(IsRetryableStatus(429, tick));
	REQUIRE(IsRetryableStatus(503, tick));

	// 502/504 retry only for idempotent phases (Default), not exchange ticks —
	// a bad-gateway there may mean the tick reached the worker and advanced state.
	REQUIRE(IsRetryableStatus(502, full));
	REQUIRE(IsRetryableStatus(504, full));
	REQUIRE_FALSE(IsRetryableStatus(502, tick));
	REQUIRE_FALSE(IsRetryableStatus(504, tick));

	// Everything else is terminal.
	REQUIRE_FALSE(IsRetryableStatus(200, full));
	REQUIRE_FALSE(IsRetryableStatus(400, full));
	REQUIRE_FALSE(IsRetryableStatus(401, full));
	REQUIRE_FALSE(IsRetryableStatus(500, full)); // 500 is not transient-by-convention here
}

TEST_CASE("ParseRetryAfter: delta-seconds", "[retry]") {
	// now is irrelevant for the numeric form.
	REQUIRE(ParseRetryAfter("1", 0).value() == Approx(1.0));
	REQUIRE(ParseRetryAfter("  30 ", 0).value() == Approx(30.0));
	REQUIRE(ParseRetryAfter("0", 0).value() == Approx(0.0));
	REQUIRE(ParseRetryAfter("2.5", 0).value() == Approx(2.5)); // tolerate a fractional value
}

TEST_CASE("ParseRetryAfter: HTTP-date resolved against now", "[retry]") {
	// 2015-10-21 07:28:00 UTC == 1445412480 epoch seconds.
	const int64_t when = 1445412480;
	const std::string date = "Wed, 21 Oct 2015 07:28:00 GMT";

	// 10 seconds before the deadline → ~10s wait.
	REQUIRE(ParseRetryAfter(date, when - 10).value() == Approx(10.0));
	// Exactly at / past the deadline → clamped to 0 (never negative).
	REQUIRE(ParseRetryAfter(date, when).value() == Approx(0.0));
	REQUIRE(ParseRetryAfter(date, when + 100).value() == Approx(0.0));
}

TEST_CASE("ParseRetryAfter: empty / malformed → nullopt", "[retry]") {
	REQUIRE_FALSE(ParseRetryAfter("", 0).has_value());
	REQUIRE_FALSE(ParseRetryAfter("   ", 0).has_value());
	REQUIRE_FALSE(ParseRetryAfter("soon", 0).has_value());
	REQUIRE_FALSE(ParseRetryAfter("Wed, 99 Zzz 2015 07:28:00 GMT", 0).has_value());
}

TEST_CASE("ComputeRetryDelaySeconds: full jitter within the exponential window", "[retry]") {
	auto p = HttpRetryPolicy::Default(); // base 0.5, max 30
	// rand01=0 → 0; rand01→1 → approaches base*2^attempt.
	REQUIRE(ComputeRetryDelaySeconds(0, {}, p, 0.0) == Approx(0.0));
	REQUIRE(ComputeRetryDelaySeconds(0, {}, p, 0.5) == Approx(0.25));  // 0.5 * 0.5*2^0
	REQUIRE(ComputeRetryDelaySeconds(3, {}, p, 0.5) == Approx(2.0));   // 0.5 * 0.5*2^3 = 0.5*4
}

TEST_CASE("ComputeRetryDelaySeconds: Retry-After is a floor, clamped to max", "[retry]") {
	auto p = HttpRetryPolicy::Default(); // max 30

	// A tiny jittered delay is floored up to the server's Retry-After.
	REQUIRE(ComputeRetryDelaySeconds(0, 5.0, p, 0.0) == Approx(5.0));
	// But never below a larger computed backoff.
	REQUIRE(ComputeRetryDelaySeconds(6, 1.0, p, 0.9999) == Approx(std::min(0.9999 * 0.5 * 64.0, 30.0)));
	// Retry-After above the ceiling is clamped to backoff_max.
	REQUIRE(ComputeRetryDelaySeconds(0, 120.0, p, 0.0) == Approx(30.0));
}

TEST_CASE("ComputeRetryDelaySeconds: respect_retry_after=false ignores the header", "[retry]") {
	HttpRetryPolicy p;
	p.respect_retry_after = false;
	// The 20s Retry-After is ignored; only the (zero) jittered backoff remains.
	REQUIRE(ComputeRetryDelaySeconds(0, 20.0, p, 0.0) == Approx(0.0));
}
