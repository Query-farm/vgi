// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace duckdb {

class ClientContext;

namespace vgi {

// All the data needed to build one `attach` telemetry event. Populated at the
// emit site in VgiCatalogAttach (success path only) and handed to
// VgiSendAttachEvent, which derives host_kind / transport-classification /
// scrubbing and fires the event. Empty optional-ish strings serialize to JSON
// null; the caller leaves them empty when a value is absent.
struct VgiAttachInfo {
	std::string extension_version;

	int64_t attach_duration_ms = 0;

	// --- catalog (from CatalogAttachResult; absent fields left empty/false) ---
	std::string catalog_name;
	bool has_catalog_version = false;
	int64_t catalog_version = 0;
	std::string resolved_impl_version; // empty -> null
	std::string resolved_data_version; // empty -> null
	std::string secret_service_url;    // empty -> null (scrubbed before passing)
	int64_t companion_count = 0;
	std::vector<std::string> companion_types; // db_type per companion catalog

	// --- transport (raw location; classified + scrubbed inside) ---
	// For the container STRUCT ATTACH form (no user string), the caller passes a
	// synthesized "oci://<image>" so classification + location stay correct.
	std::string raw_location;
	bool is_container = false;
	std::string container_runtime;    // docker|podman|nerdctl|container
	std::string container_connection; // stdio|tcp|http
	bool container_shared = false;

	// --- auth ---
	std::string auth_mode; // none|oauth|bearer|oauth_refresh_token
	bool auth_interactive = false;
	std::string oauth_issuer; // empty -> null

	// --- options ---
	bool opt_pool = true;
	bool has_pool_max = false;
	int64_t opt_pool_max = 0;
	bool has_pool_timeout = false;
	int64_t opt_pool_timeout = 0;
	bool opt_secrets = true;
	bool opt_cache = true;
	bool opt_attach_companions = true;
	bool opt_attach_companion_secrets = false;
};

// Build and fire the per-ATTACH telemetry event (fire-and-forget, async, honors
// the opt-out env var). Never throws — telemetry must not perturb ATTACH.
void VgiSendAttachEvent(ClientContext &context, const VgiAttachInfo &info);

// Build the full attach-event JSON payload string (network-free — the unit-test
// seam). `host_kind` is passed in so this needs no ClientContext.
std::string VgiBuildAttachEventJson(const std::string &host_kind, const VgiAttachInfo &info);

// --- Pure helpers (exposed for unit testing) --------------------------------

// Strip credentials from a worker location before it is sent: userinfo
// (user:pass@), auth-bearing query params (token/sig/signature/X-Amz-*), and
// token-shaped flags in a `launch:` argv (--bearer-token, --password, etc.).
std::string VgiScrubLocation(const std::string &location);

struct VgiTransportClass {
	std::string type;   // subprocess|http|tcp|unix|launch|container|github
	std::string scheme; // subprocess|http|https|tcp|unix|launch|oci|docker|github|github-auto
};

// Classify a (raw, pre-rewrite) worker location into (type, scheme). Handles the
// github schemes that DetectTransport does not.
VgiTransportClass VgiClassifyTransport(const std::string &raw_location);

// Map a DuckDB `duckdb_api` setting value to a coarse host_kind bucket.
std::string VgiMapHostKind(const std::string &duckdb_api);

// Resolve host_kind for this client (reads the `duckdb_api` setting; compiles to
// "wasm" under Emscripten).
std::string VgiDetectHostKind(ClientContext &context);

} // namespace vgi
} // namespace duckdb
