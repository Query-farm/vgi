// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Dependency-free HTTP retry policy + backoff helpers for the VGI RPC transport.
//
// Mirrors the Python reference client (``vgi_rpc/http/_retry.py``) so both
// clients back off identically: retry transient statuses (429/503, and 502/504
// for idempotent phases), honor the ``Retry-After`` header, and use exponential
// backoff with full jitter. The VGI Cedar auth proxy emits ``503 + Retry-After``
// when it sheds load (backpressure); without this the extension would surface a
// hard query error instead of pausing briefly and succeeding.
//
// These functions are pure (no DuckDB, no clock, no RNG) so they unit-test in
// isolation — the caller injects "now" and a random in [0,1). See
// test/cpp/test_http_retry.cpp.

#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace duckdb {
namespace vgi {

// How the RPC transport retries a transient HTTP failure.
struct HttpRetryPolicy {
	// Maximum retries *after* the first attempt (0 disables retrying).
	int max_retries = 4;
	// Exponential backoff base, seconds: delay ~ base * 2^attempt (jittered).
	double backoff_base_s = 0.5;
	// Backoff / Retry-After ceiling, seconds.
	double backoff_max_s = 30.0;
	// Honor a ``Retry-After`` header (floor the computed delay to it).
	bool respect_retry_after = true;
	// Retry 502/504 too. Safe for idempotent phases (bind/init/unary) where the
	// request either did not execute or is replay-safe; disabled for stream
	// *exchange* ticks, where a 502/504 could mean the tick reached the worker
	// and advanced state (only 429/503 — the proxy's pre-work shed — is safe there).
	bool retry_bad_gateway = true;

	// Full-retry policy for idempotent phases (unary RPC, bind, stream init).
	static HttpRetryPolicy Default() {
		return HttpRetryPolicy{};
	}
	// Restricted policy for stream exchange ticks: only the proxy's backpressure
	// codes (429/503), which are shed *before* the request does any work.
	static HttpRetryPolicy ExchangeTick() {
		HttpRetryPolicy p;
		p.retry_bad_gateway = false;
		return p;
	}
};

// Is ``status_code`` retryable under ``policy``? 429/503 always; 502/504 only
// when ``policy.retry_bad_gateway``.
bool IsRetryableStatus(int status_code, const HttpRetryPolicy &policy);

// Parse a ``Retry-After`` header value into a non-negative delay in seconds.
//
// Accepts both forms from RFC 7231 §7.1.3: delta-seconds (e.g. ``"1"``) and an
// IMF-fixdate (e.g. ``"Wed, 21 Oct 2015 07:28:00 GMT"``), the latter resolved
// against ``now_unix`` (seconds since epoch, UTC) and clamped to >= 0. Returns
// ``std::nullopt`` when the value is empty or unparseable.
std::optional<double> ParseRetryAfter(const std::string &header_value, int64_t now_unix);

// Backoff delay in seconds for a 0-based ``attempt``.
//
// Exponential base*2^attempt with *full jitter* (delay in [0, exp)), clamped to
// ``backoff_max_s``; when a ``retry_after`` is present and honored, the result is
// floored to ``min(retry_after, backoff_max_s)``. ``rand01`` is the jitter draw
// in [0,1), injected so the computation is deterministic under test.
double ComputeRetryDelaySeconds(int attempt, std::optional<double> retry_after,
                                const HttpRetryPolicy &policy, double rand01);

} // namespace vgi
} // namespace duckdb
