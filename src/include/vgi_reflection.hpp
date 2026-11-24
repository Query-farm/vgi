// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// Client side of vgi_rpc.Reflection.v1: which protocols a worker hosts, so
// optional features (attach tickets, report services, identity) are discovered
// rather than assumed, e.g. HostsProtocol(context, catalog, "vgi.attach_tickets.v1").
//
// Fetched lazily once per attached catalog with its own connection and auth,
// cached on its VgiAttachParameters, and refetched after the worker connection
// is re-established (WorkerConnectionGeneration) or vgi_clear_cache(). A worker
// that predates reflection is treated (and cached) as hosting exactly `vgi.v3`.

#include <memory>
#include <optional>
#include <stdexcept>
#include <string>

#include "vgi_hosted_protocols.hpp"

namespace duckdb {

class ClientContext;
class VgiCatalog;

namespace vgi {

struct CatalogRpcContext;
struct VgiAttachParameters;

// True when `e` says the worker does not host vgi_rpc.Reflection.v1: a
// protocol_not_supported or method_not_implemented error kind, an
// UNIMPLEMENTED code, or a bare HTTP 404 from the protocol's route.
bool IsReflectionNotHostedError(const std::exception &e);

// One uncached list_protocols call through `ctx`. A worker that does not host
// reflection yields PreReflectionListing() when `tolerate_pre_reflection`, and
// rethrows the worker's error otherwise. Other failures always propagate.
// protocol_versions, when non-null, receives the reply's
// vgi_rpc.protocol_versions metadata: nullopt when absent (a server released
// before negotiation), "" when it negotiates but gates nothing.
VgiProtocolListing InvokeListProtocols(const CatalogRpcContext &ctx, ClientContext &context,
                                       bool tolerate_pre_reflection = true,
                                       std::optional<std::string> *protocol_versions = nullptr);

// The catalog's cached listing, fetched on first use. Never null; throws only
// when the worker cannot be reached (a pre-reflection worker is a listing).
std::shared_ptr<const VgiProtocolListing> GetHostedProtocols(ClientContext &context,
                                                             const std::shared_ptr<VgiAttachParameters> &params);
std::shared_ptr<const VgiProtocolListing> GetHostedProtocols(ClientContext &context, const VgiCatalog &catalog);

// Whether the catalog's worker hosts `protocol_name`: nullopt when it does
// not, otherwise its declared version ("" when it declares none, or when the
// worker predates reflection and the protocol is the implied `vgi.v3`).
std::optional<std::string> HostsProtocol(ClientContext &context, const VgiCatalog &catalog,
                                         const std::string &protocol_name);
std::optional<std::string> HostsProtocol(ClientContext &context, const std::shared_ptr<VgiAttachParameters> &params,
                                         const std::string &protocol_name);

} // namespace vgi
} // namespace duckdb
