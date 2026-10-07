// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// Client side of attach tickets (vgi.attach_tickets.v1) and sealed grants
// (vgi_rpc.Identity.v1 issue_grant). Spec: vgi-python
// docs/protocol/vgi-attach-tickets.md.
//
// A ticket says WHAT to attach (the catalog and the options the user attached
// with, secret ones included, sealed by the worker). A grant says WHO attaches.
// vgi_export_session() mints both for each attached catalog while the user is
// logged in; a runner later attaches with
//     ATTACH '...' (TYPE vgi, LOCATION '...', bearer_token '<grant>', attach_ticket '<ticket>')
// and the worker restores the original attach before any catalog code runs.

#include <map>
#include <memory>
#include <string>

#include "duckdb/common/types/value.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

class ClientContext;

namespace vgi {

struct CatalogRpcContext;

inline constexpr const char *ATTACH_TICKETS_PROTOCOL_NAME = "vgi.attach_tickets.v1";
inline constexpr const char *ATTACH_TICKETS_PROTOCOL_VERSION = "1.0.0";
inline constexpr const char *IDENTITY_PROTOCOL_NAME = "vgi_rpc.Identity.v1";
// The ATTACH option a user types, and the reserved catalog_attach option the
// worker redeems (spec §6).
inline constexpr const char *ATTACH_TICKET_OPTION = "attach_ticket";
inline constexpr const char *ATTACH_TICKET_WIRE_OPTION = "vgi_attach_ticket";
// issue_grant purpose for a grant held by an unattended runner.
inline constexpr const char *UNATTENDED_GRANT_PURPOSE = "vgi.unattended";

// What a catalog was attached with, retained in memory (never logged) so it
// can be sealed into a ticket later. Held by VgiAttachParameters.
struct VgiRetainedAttach {
	// LOCATION as typed at ATTACH (before any internal rewrite).
	std::string location;
	// The worker's catalog name (the ATTACH path), not the local alias.
	std::string catalog_name;
	// Catalog attach options as validated at ATTACH (declared names, declared
	// types), secret ones included. Extension options (LOCATION, bearer_token,
	// pool, ...) are not catalog options and are not here.
	std::map<std::string, Value> options;
	// As typed at ATTACH; "" for none (not the worker's resolved versions).
	std::string data_version_spec;
	std::string implementation_version;
	// Attached by redeeming an attach_ticket: the original options were never
	// seen by this session, so there is nothing to re-seal.
	bool via_ticket = false;
};

struct VgiIssuedGrant {
	std::string token;
	double expires_at = 0; // Unix seconds
	std::string grant_id;
};

struct VgiAttachTicket {
	std::string ticket;
	double expires_at = 0; // Unix seconds; +inf for no expiry
};

// vgi_rpc.Identity.v1 issue_grant(purpose, scopes = [], ttl_seconds) as the
// catalog's caller. `identity_version` is the version reflection reported ("" for none).
VgiIssuedGrant InvokeIssueGrant(const CatalogRpcContext &ctx, ClientContext &context,
                                const std::string &identity_version, const std::string &purpose, int64_t ttl_seconds);

// vgi.attach_tickets.v1 seal_attach(request) for `retained`.
VgiAttachTicket InvokeSealAttach(const CatalogRpcContext &ctx, ClientContext &context,
                                 const VgiRetainedAttach &retained, int64_t ttl_seconds);

// Register vgi_export_session(aliases := NULL, ttl_seconds := NULL).
void RegisterVgiExportSessionFunction(ExtensionLoader &loader);

} // namespace vgi
} // namespace duckdb
