// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// The pure half of the location policy: classifying a LOCATION as a transport
// and the transport token vocabulary. Kept apart from vgi_location_policy.cpp
// (settings, enforcement) so error reporting can name a transport without
// linking the storage extension.
#include "vgi_location_policy.hpp"

#include "vgi_transport.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {
namespace vgi {

namespace {

constexpr const char *kSettingName = "vgi_allowed_transports";

struct TokenBit {
	const char *name;
	uint32_t bit;
};

// Declaration order is the canonical rendering order.
constexpr TokenBit kTokens[] = {
    {"subprocess", POLICY_SUBPROCESS}, {"launch", POLICY_LAUNCH}, {"unix", POLICY_UNIX},
    {"oci", POLICY_OCI},               {"github", POLICY_GITHUB}, {"database", POLICY_DATABASE},
    {"http", POLICY_HTTP},             {"https", POLICY_HTTPS},   {"tcp", POLICY_TCP},
    {"httpi", POLICY_HTTPI},           {"iroh", POLICY_IROH},     {"worker", POLICY_WORKER},
};

} // namespace

const char *LocationEntryPointName(LocationEntryPoint entry) {
	switch (entry) {
	case LocationEntryPoint::ATTACH:
		return "attach";
	case LocationEntryPoint::VGI_PROTOCOLS:
		return "vgi_protocols";
	default:
		return "vgi_catalogs";
	}
}

const char *PolicyTransportName(uint32_t bit) {
	for (const auto &t : kTokens) {
		if (t.bit == bit) {
			return t.name;
		}
	}
	return "unknown";
}

uint32_t ParseAllowedTransports(const std::string &value) {
	uint32_t mask = 0;
	size_t start = 0;
	for (;;) {
		auto comma = value.find(',', start);
		auto token = StringUtil::Lower(value.substr(start, comma == std::string::npos ? std::string::npos : comma - start));
		StringUtil::Trim(token);
		if (token.empty()) {
			throw InvalidInputException("%s contains an empty entry ('%s'); use 'none' to refuse every transport",
			                            kSettingName, value);
		}
		if (token == "all") {
			mask |= POLICY_ALL_TRANSPORTS;
		} else if (token != "none") {
			bool found = false;
			for (const auto &t : kTokens) {
				if (token == t.name) {
					mask |= t.bit;
					found = true;
					break;
				}
			}
			if (!found) {
				throw InvalidInputException("%s: unknown transport '%s' (expected a comma-separated list of: all, "
				                            "none, subprocess, launch, unix, oci, github, database, http, https, tcp, "
				                            "httpi, iroh, worker)",
				                            kSettingName, token);
			}
		}
		if (comma == std::string::npos) {
			return mask;
		}
		start = comma + 1;
	}
}

std::string FormatAllowedTransports(uint32_t mask) {
	if (mask == POLICY_ALL_TRANSPORTS) {
		return "all";
	}
	if (mask == 0) {
		return "none";
	}
	std::string out;
	for (const auto &t : kTokens) {
		if (mask & t.bit) {
			if (!out.empty()) {
				out += ",";
			}
			out += t.name;
		}
	}
	return out;
}

uint32_t ClassifyLocationForPolicy(const std::string &location, LocationEntryPoint entry, std::string &refusal) {
	// Internal tokens are synthesized by ATTACH and never user-typed. Typed
	// directly they would skip the resolution that produced them.
	if (IsResolvedWorkerLocation(location) || IsContainerSharedLocation(location)) {
		refusal = "vgi: this LOCATION uses an internal VGI token prefix ('vgi-artifact:' / 'container-shared:') and "
		          "cannot be given directly; use the original database:// or oci:// LOCATION";
		return 0;
	}
	// Order and build guards mirror CreateFunctionConnection / InvokePooledUnaryRpc.
	if (IsHttpTransport(location)) {
		return StringUtil::StartsWith(StringUtil::Lower(location), "https://") ? POLICY_HTTPS : POLICY_HTTP;
	}
	if (IsHttpiTransport(location)) {
		return POLICY_HTTPI;
	}
#if defined(__EMSCRIPTEN__)
	if (IsWebWorkerTransport(location)) {
		return POLICY_WORKER;
	}
#endif
	// Native `worker:` has no branch of its own: it falls through to the bare
	// command path below, so it is classified as what it actually runs as.
	if (IsIrohTransport(location)) {
		return POLICY_IROH;
	}
	if (IsTcpTransport(location)) {
		return POLICY_TCP;
	}
	if (IsLaunchLocation(location)) {
		return POLICY_LAUNCH;
	}
	if (IsUnixLocation(location)) {
		return POLICY_UNIX;
	}
	if (IsGithubLocation(location) || IsGithubAutoLocation(location)) {
		return POLICY_GITHUB;
	}
	if (IsContainerLocation(location)) {
		return POLICY_OCI;
	}
	if (IsDatabaseLocation(location)) {
		if (entry != LocationEntryPoint::ATTACH) {
			// Only ATTACH resolves database:// into a verified artifact; anywhere
			// else the raw string would be handed to the shell.
			refusal = StringUtil::Format("vgi: %s() does not resolve database:// LOCATIONs; ATTACH the "
			                             "database:// LOCATION instead",
			                             LocationEntryPointName(entry));
			return 0;
		}
		return POLICY_DATABASE;
	}
	return POLICY_SUBPROCESS;
}

const char *TransportNameForLocation(const std::string &location) {
	if (location.empty()) {
		return "";
	}
	if (IsResolvedWorkerLocation(location)) {
		return PolicyTransportName(POLICY_DATABASE);
	}
	if (IsContainerSharedLocation(location)) {
		return PolicyTransportName(POLICY_OCI);
	}
	std::string refusal;
	auto bit = ClassifyLocationForPolicy(location, LocationEntryPoint::ATTACH, refusal);
	return bit ? PolicyTransportName(bit) : "";
}

} // namespace vgi
} // namespace duckdb
