// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
#include "vgi_http_retry.hpp"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdio>
#include <cstring>

namespace duckdb {
namespace vgi {

bool IsRetryableStatus(int status_code, const HttpRetryPolicy &policy) {
	switch (status_code) {
	case 429: // Too Many Requests
	case 503: // Service Unavailable (the proxy's backpressure shed)
		return true;
	case 502: // Bad Gateway
	case 504: // Gateway Timeout
		return policy.retry_bad_gateway;
	default:
		return false;
	}
}

namespace {

// Trim ASCII whitespace from both ends.
std::string Trim(const std::string &s) {
	size_t a = s.find_first_not_of(" \t\r\n");
	if (a == std::string::npos) {
		return "";
	}
	size_t b = s.find_last_not_of(" \t\r\n");
	return s.substr(a, b - a + 1);
}

// Days since 1970-01-01 for a proleptic-Gregorian y/m/d (Howard Hinnant's
// civil-from-days inverse). Portable — avoids the non-standard timegm().
int64_t DaysFromCivil(int64_t y, unsigned m, unsigned d) {
	y -= m <= 2;
	const int64_t era = (y >= 0 ? y : y - 399) / 400;
	const unsigned yoe = static_cast<unsigned>(y - era * 400);
	const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
	const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
	return era * 146097 + static_cast<int64_t>(doe) - 719468;
}

// Map a 3-letter English month abbreviation to 1..12, or 0 if unrecognized.
unsigned MonthFromAbbrev(const char *mon) {
	static const char *kMonths[12] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
	                                  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
	for (unsigned i = 0; i < 12; ++i) {
		if (std::strncmp(mon, kMonths[i], 3) == 0) {
			return i + 1;
		}
	}
	return 0;
}

// Parse an RFC 7231 IMF-fixdate ("Wed, 21 Oct 2015 07:28:00 GMT") to epoch
// seconds (UTC). Returns nullopt if it does not match that shape.
std::optional<int64_t> ParseImfFixdate(const std::string &s) {
	char wday[8] = {0};
	char mon[8] = {0};
	int day = 0, year = 0, hh = 0, mm = 0, ss = 0;
	// "%3s," tolerates the weekday; the trailing " GMT" is ignored.
	int n = std::sscanf(s.c_str(), "%3s %d %3s %d %d:%d:%d", wday, &day, mon, &year, &hh, &mm, &ss);
	if (n != 7) {
		// The comma after the weekday isn't consumed by %3s; retry with it split.
		n = std::sscanf(s.c_str(), "%*[^,], %d %3s %d %d:%d:%d", &day, mon, &year, &hh, &mm, &ss);
		if (n != 6) {
			return std::nullopt;
		}
	}
	unsigned month = MonthFromAbbrev(mon);
	if (month == 0 || day < 1 || day > 31 || hh < 0 || hh > 23 || mm < 0 || mm > 59 || ss < 0 || ss > 60) {
		return std::nullopt;
	}
	int64_t days = DaysFromCivil(year, month, static_cast<unsigned>(day));
	return days * 86400 + hh * 3600 + mm * 60 + ss;
}

} // namespace

std::optional<double> ParseRetryAfter(const std::string &header_value, int64_t now_unix) {
	std::string v = Trim(header_value);
	if (v.empty()) {
		return std::nullopt;
	}

	// delta-seconds: a run of digits (optionally with a fractional part, which
	// RFC 7231 doesn't require but some servers send).
	bool all_numeric = true;
	bool seen_dot = false;
	for (char c : v) {
		if (c == '.' && !seen_dot) {
			seen_dot = true;
			continue;
		}
		if (!std::isdigit(static_cast<unsigned char>(c))) {
			all_numeric = false;
			break;
		}
	}
	if (all_numeric) {
		try {
			double secs = std::stod(v);
			return secs < 0 ? 0.0 : secs;
		} catch (...) {
			return std::nullopt;
		}
	}

	// HTTP-date form.
	auto when = ParseImfFixdate(v);
	if (!when.has_value()) {
		return std::nullopt;
	}
	double delta = static_cast<double>(*when - now_unix);
	return delta < 0 ? 0.0 : delta;
}

double ComputeRetryDelaySeconds(int attempt, std::optional<double> retry_after,
                                const HttpRetryPolicy &policy, double rand01) {
	if (rand01 < 0.0) {
		rand01 = 0.0;
	} else if (rand01 >= 1.0) {
		rand01 = 0.9999999;
	}
	// Exponential window with full jitter: delay in [0, base * 2^attempt).
	double exp_delay = policy.backoff_base_s * std::pow(2.0, static_cast<double>(attempt));
	double delay = std::min(rand01 * exp_delay, policy.backoff_max_s);

	// Honor Retry-After as a floor (never wait less than the server asked),
	// still bounded by the ceiling.
	if (policy.respect_retry_after && retry_after.has_value()) {
		delay = std::max(delay, std::min(*retry_after, policy.backoff_max_s));
	}
	return delay < 0.0 ? 0.0 : delay;
}

} // namespace vgi
} // namespace duckdb
