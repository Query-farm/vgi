// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// LOCATION transport policy: which worker transports a user may reach through
// ATTACH / vgi_catalogs(). Lets an operator hand untrusted SQL to users without
// letting them make VGI start local processes. See docs/location_policy.md.
//
// Two inputs, both one-way for the life of the database instance:
//   * the `vgi_allowed_transports` setting (narrow-only: a SET may only remove
//     transports, never add them back; RESET is refused once narrowed), and
//   * DuckDB's own `enable_external_access` — when false, every LOCAL transport
//     (one that spawns a process or touches local IPC) is refused too.
//
// Enforcement runs only where a LOCATION enters VGI. Catalogs attached before a
// narrowing keep working (the same model as extensions loaded before
// enable_external_access=false).

#include <atomic>
#include <cstdint>
#include <string>

namespace duckdb {

class ClientContext;
class DatabaseInstance;
class DBConfig;

namespace vgi {

// One bit per policy transport. Names (see PolicyTransportName) are the tokens
// accepted by `vgi_allowed_transports`.
enum PolicyTransport : uint32_t {
	POLICY_SUBPROCESS = 1u << 0, // bare command (incl. the haybarn `vgi:` form)
	POLICY_LAUNCH = 1u << 1,     // launch:<argv>
	POLICY_UNIX = 1u << 2,       // unix:///path (Windows: named pipe)
	POLICY_OCI = 1u << 3,        // oci:// and docker://
	POLICY_GITHUB = 1u << 4,     // github:// and github-auto://
	POLICY_DATABASE = 1u << 5,   // database://
	POLICY_HTTP = 1u << 6,       // http://
	POLICY_HTTPS = 1u << 7,      // https://
	POLICY_TCP = 1u << 8,        // tcp://
	POLICY_HTTPI = 1u << 9,      // httpi://
	POLICY_IROH = 1u << 10,      // iroh://
	POLICY_WORKER = 1u << 11,    // worker: (DuckDB-WASM only)
};

// Transports that spawn a local process or connect to local IPC. Refused while
// enable_external_access=false.
constexpr uint32_t POLICY_LOCAL_TRANSPORTS =
    POLICY_SUBPROCESS | POLICY_LAUNCH | POLICY_UNIX | POLICY_OCI | POLICY_GITHUB | POLICY_DATABASE;
constexpr uint32_t POLICY_ALL_TRANSPORTS = POLICY_LOCAL_TRANSPORTS | POLICY_HTTP | POLICY_HTTPS | POLICY_TCP |
                                           POLICY_HTTPI | POLICY_IROH | POLICY_WORKER;

// Where a LOCATION enters VGI. The two entry points resolve differently, so the
// same string can dispatch to a different transport at each.
enum class LocationEntryPoint { ATTACH, VGI_CATALOGS };

// Classify `location` as the transport the dispatch code will ACTUALLY use for
// it at `entry` on this build. Built from the same predicates, in the same
// build configurations, as CreateFunctionConnection / InvokePooledUnaryRpc /
// SpawnWorker; anything that no network branch claims falls through to
// POLICY_SUBPROCESS, exactly as dispatch does (so native `worker:x` and
// `IROH://x` are subprocess). Returns 0 and sets `refusal` for locations that
// are refused regardless of policy (internal tokens, unresolved database://).
uint32_t ClassifyLocationForPolicy(const std::string &location, LocationEntryPoint entry, std::string &refusal);

// Parse a `vgi_allowed_transports` value (comma-separated, case-insensitive
// tokens, or `all` / `none`). Throws InvalidInputException on an unknown or
// empty token, so a typo can never loosen the policy.
uint32_t ParseAllowedTransports(const std::string &value);
// Canonical rendering: `all`, `none`, or the tokens in declaration order.
std::string FormatAllowedTransports(uint32_t mask);
const char *PolicyTransportName(uint32_t bit);

// Per-database effective allowlist. Owned by the VGI storage extension; the
// setting is only its input channel, so session shadowing or any path that sets
// the option without its callback cannot change what is enforced.
class VgiLocationPolicy {
public:
	uint32_t Allowed() const {
		return allowed_.load(std::memory_order_acquire);
	}
	// Narrow to `requested`. Throws InvalidInputException if `requested` would
	// add any transport. Compare-and-swap, so two concurrent narrowings can never
	// combine into a widening.
	void Narrow(uint32_t requested);
	// Initial value at extension load (startup config reaches extension options
	// without their callback).
	void Seed(uint32_t mask) {
		allowed_.store(mask, std::memory_order_release);
	}

private:
	std::atomic<uint32_t> allowed_ {POLICY_ALL_TRANSPORTS};
};

// Defined in vgi_extension.cpp next to the storage extension that owns it.
// nullptr if VGI is not registered on this instance.
VgiLocationPolicy *FindVgiLocationPolicy(DatabaseInstance &db);

// Throw PermissionException unless `location` is permitted at `entry`. Call
// before ANY I/O on the location (container inspection, package resolution,
// connecting, spawning).
void CheckLocationPolicy(ClientContext &context, const std::string &location, LocationEntryPoint entry);

// Register `vgi_allowed_transports` and seed `policy` from its current value.
void RegisterLocationPolicySetting(DBConfig &config, VgiLocationPolicy &policy);

} // namespace vgi
} // namespace duckdb
