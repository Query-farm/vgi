// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// Client side of vgi_rpc.Reflection.v1: which protocols a worker hosts.
//
// The extension's own optional features (attach tickets, report services,
// identity) are separate vgi-rpc protocols, discovered rather than assumed:
//
//     auto v = vgi::HostsProtocol(context, catalog, "vgi.attach_tickets.v1");
//     if (v) {
//         ... the worker hosts it, at version *v ("" if it declares none) ...
//     }
//
// The answer is fetched lazily, once per attached catalog, through the
// catalog's own connection and auth, and cached on its VgiAttachParameters:
// DETACH drops it, and it is refetched after the extension re-establishes the
// worker connection (see WorkerConnectionGeneration) or vgi_clear_cache().
//
// A worker that predates reflection is not an error: it is treated (and
// cached) as hosting exactly `vgi.v2`.

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
VgiProtocolListing InvokeListProtocols(const CatalogRpcContext &ctx, ClientContext &context,
                                       bool tolerate_pre_reflection = true);

// The catalog's cached listing, fetched on first use. Never null; throws only
// when the worker cannot be reached (a pre-reflection worker is a listing).
std::shared_ptr<const VgiProtocolListing> GetHostedProtocols(ClientContext &context,
                                                             const std::shared_ptr<VgiAttachParameters> &params);
std::shared_ptr<const VgiProtocolListing> GetHostedProtocols(ClientContext &context, const VgiCatalog &catalog);

// Whether the catalog's worker hosts `protocol_name`: nullopt when it does
// not, otherwise its declared version ("" when it declares none, or when the
// worker predates reflection and the protocol is the implied `vgi.v2`).
std::optional<std::string> HostsProtocol(ClientContext &context, const VgiCatalog &catalog,
                                         const std::string &protocol_name);
std::optional<std::string> HostsProtocol(ClientContext &context, const std::shared_ptr<VgiAttachParameters> &params,
                                         const std::string &protocol_name);

} // namespace vgi
} // namespace duckdb
