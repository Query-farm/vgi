// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once
// Companion catalogs (lakehouse federation). The registry lives on the
// file-local VgiStorageExtension in vgi_extension.cpp; only the release side is
// exposed, for VgiCatalog::OnDetach. See docs/companion_catalogs.md.
#include <string>
#include <vector>

namespace duckdb {

class ClientContext;

namespace vgi {

// Decrement each alias's registry refcount and DETACH those that hit zero.
// Safe to call with an empty list.
void ReleaseCompanionCatalogs(ClientContext &context, const std::vector<std::string> &aliases);

} // namespace vgi
} // namespace duckdb
