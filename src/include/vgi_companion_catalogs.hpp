// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once
// =============================================================================
// Companion catalog helpers (lakehouse federation). The companion registry
// lives on the file-local VgiStorageExtension in vgi_extension.cpp; this header
// exposes only the release side so VgiCatalog::OnDetach (in vgi_catalog.cpp)
// can decrement refcounts and detach companions that reach zero, without
// depending on the VgiStorageExtension definition. See docs/companion_catalogs.md.
// =============================================================================
#include <string>
#include <vector>

namespace duckdb {

class ClientContext;

namespace vgi {

// Release the companion catalogs a detaching VGI catalog referenced: decrement
// each alias's registry refcount and DETACH those that hit zero. Safe to call
// with an empty list. Called from VgiCatalog::OnDetach.
void ReleaseCompanionCatalogs(ClientContext &context, const std::vector<std::string> &aliases);

} // namespace vgi
} // namespace duckdb
