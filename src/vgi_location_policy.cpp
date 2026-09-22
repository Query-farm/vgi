// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_location_policy.hpp"

#include "vgi_logging.hpp"
#include "vgi_transport.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/settings.hpp"

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

const char *EntryPointName(LocationEntryPoint entry) {
	return entry == LocationEntryPoint::ATTACH ? "attach" : "vgi_catalogs";
}

// SET calls the callback with the statement's raw scope (AUTOMATIC resolves to
// the option's GLOBAL default afterwards); RESET passes the resolved scope.
void AllowedTransportsSetCallback(ClientContext &context, SetScope scope, Value &parameter) {
	if (scope != SetScope::AUTOMATIC && scope != SetScope::GLOBAL) {
		throw InvalidInputException("%s can only be set globally (it applies to the whole database)", kSettingName);
	}
	auto *policy = FindVgiLocationPolicy(DatabaseInstance::GetDatabase(context));
	if (!policy) {
		throw InternalException("vgi: %s set before the VGI storage extension was registered", kSettingName);
	}
	if (parameter.IsNull()) {
		throw InvalidInputException("%s must not be NULL (use 'none' to refuse every transport)", kSettingName);
	}
	auto requested = ParseAllowedTransports(parameter.ToString());
	policy->Narrow(requested);
	// Store the canonical form so current_setting() shows what is enforced.
	parameter = Value(FormatAllowedTransports(requested));
}

} // namespace

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

void VgiLocationPolicy::Narrow(uint32_t requested) {
	auto current = allowed_.load(std::memory_order_acquire);
	for (;;) {
		auto added = requested & ~current;
		if (added) {
			throw InvalidInputException("Cannot widen %s while the database is running (currently '%s'; '%s' would "
			                            "add '%s'). The restriction lasts for the life of the database instance.",
			                            kSettingName, FormatAllowedTransports(current),
			                            FormatAllowedTransports(requested), FormatAllowedTransports(added));
		}
		if (allowed_.compare_exchange_weak(current, requested, std::memory_order_acq_rel,
		                                   std::memory_order_acquire)) {
			return;
		}
	}
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
			refusal = "vgi: vgi_catalogs() does not resolve database:// LOCATIONs; ATTACH the database:// "
			          "LOCATION instead";
			return 0;
		}
		return POLICY_DATABASE;
	}
	return POLICY_SUBPROCESS;
}

void CheckLocationPolicy(ClientContext &context, const std::string &location, LocationEntryPoint entry) {
	std::string refusal;
	auto transport = ClassifyLocationForPolicy(location, entry, refusal);
	if (!transport) {
		VGI_LOG(context, "location_policy.refused", {{"entry", EntryPointName(entry)}, {"reason", "internal"}});
		throw PermissionException(refusal);
	}
	auto &db = DatabaseInstance::GetDatabase(context);
	auto *policy = FindVgiLocationPolicy(db);
	if (!policy) {
		throw InternalException("vgi: LOCATION policy is unavailable (VGI storage extension not registered)");
	}
	auto allowed = policy->Allowed();
	if (!(allowed & transport)) {
		VGI_LOG(context, "location_policy.refused",
		        {{"entry", EntryPointName(entry)},
		         {"transport", PolicyTransportName(transport)},
		         {"reason", "not_allowed"}});
		throw PermissionException("vgi: LOCATION transport '%s' is not permitted by %s (allowed: %s)",
		                          PolicyTransportName(transport), kSettingName, FormatAllowedTransports(allowed));
	}
	if ((transport & POLICY_LOCAL_TRANSPORTS) && !Settings::Get<EnableExternalAccessSetting>(DBConfig::GetConfig(db))) {
		VGI_LOG(context, "location_policy.refused",
		        {{"entry", EntryPointName(entry)},
		         {"transport", PolicyTransportName(transport)},
		         {"reason", "external_access_disabled"}});
		throw PermissionException("vgi: LOCATION transport '%s' is local (it can start a process or connect to "
		                          "local IPC) and is not permitted while enable_external_access is false",
		                          PolicyTransportName(transport));
	}
}

void RegisterLocationPolicySetting(DBConfig &config, VgiLocationPolicy &policy) {
	config.AddExtensionOption(kSettingName,
	                          "Comma-separated worker LOCATION transports ATTACH / vgi_catalogs() may use: all "
	                          "(default), none, or any of subprocess, launch, unix, oci, github, database, http, "
	                          "https, tcp, httpi, iroh, worker. Narrow-only: once restricted it cannot be widened or "
	                          "reset for the life of the database instance.",
	                          LogicalType::VARCHAR, Value("all"), AllowedTransportsSetCallback, SetScope::GLOBAL);
	// A value given at database open (e.g. duckdb.connect(config=...)) is copied
	// into the option WITHOUT invoking the callback, so seed from what is stored.
	// A bad value throws here: failing the load is the fail-closed outcome.
	Value current;
	if (config.TryGetCurrentSetting(kSettingName, current) && !current.IsNull()) {
		policy.Seed(ParseAllowedTransports(current.ToString()));
	}
}

} // namespace vgi
} // namespace duckdb
