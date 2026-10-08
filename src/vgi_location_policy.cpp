// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_location_policy.hpp"
#include "vgi_settings.hpp"

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

void CheckLocationPolicy(ClientContext &context, const std::string &location, LocationEntryPoint entry) {
	std::string refusal;
	auto transport = ClassifyLocationForPolicy(location, entry, refusal);
	if (!transport) {
		VGI_LOG(context, "location_policy.refused", {{"entry", LocationEntryPointName(entry)}, {"reason", "internal"}});
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
		        {{"entry", LocationEntryPointName(entry)},
		         {"transport", PolicyTransportName(transport)},
		         {"reason", "not_allowed"}});
		throw PermissionException("vgi: LOCATION transport '%s' is not permitted by %s (allowed: %s)",
		                          PolicyTransportName(transport), kSettingName, FormatAllowedTransports(allowed));
	}
	if ((transport & POLICY_LOCAL_TRANSPORTS) && !Settings::Get<EnableExternalAccessSetting>(DBConfig::GetConfig(db))) {
		VGI_LOG(context, "location_policy.refused",
		        {{"entry", LocationEntryPointName(entry)},
		         {"transport", PolicyTransportName(transport)},
		         {"reason", "external_access_disabled"}});
		throw PermissionException("vgi: LOCATION transport '%s' is local (it can start a process or connect to "
		                          "local IPC) and is not permitted while enable_external_access is false",
		                          PolicyTransportName(transport));
	}
}

void ApplyAllowedTransportsSetting(ClientContext &context, SetScope scope, Value &parameter) {
	AllowedTransportsSetCallback(context, scope, parameter);
}

void RegisterLocationPolicySetting(DBConfig &config, VgiLocationPolicy &policy) {
	RegisterVgiSetting(config, kSettingName);
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
