// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

namespace duckdb {
class ExtensionLoader;

namespace vgi {

//! Register vgi_catalog_contents(catalog, if_none_match := ...): issue one
//! catalog_contents RPC against an attached VGI catalog and report the answer
//! (version, etag, not_modified, schema names); no rows when the worker does not
//! advertise it. A diagnostic and conformance probe; it does not touch the
//! catalog caches.
void RegisterVgiCatalogContentsFunction(ExtensionLoader &loader);

} // namespace vgi
} // namespace duckdb
