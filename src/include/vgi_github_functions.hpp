// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

namespace duckdb {

class ExtensionLoader;

namespace vgi {

//! Register the vgi_github_cache() table function (list cached release workers).
void RegisterVgiGithubCacheFunction(ExtensionLoader &loader);

//! Register the vgi_github_cache_flush() table function (clear the release cache).
void RegisterVgiGithubCacheFlushFunction(ExtensionLoader &loader);

} // namespace vgi
} // namespace duckdb
