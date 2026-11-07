// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

namespace duckdb {

class ExtensionLoader;

namespace vgi {

//! Register the vgi_github_cache() table function (list cached release workers).
void RegisterVgiGithubCacheFunction(ExtensionLoader &loader);

//! Register the vgi_github_cache_flush() table function (clear the release cache).
void RegisterVgiGithubCacheFlushFunction(ExtensionLoader &loader);

//! Register vgi_github_latest() (cached github-auto:// latest-release tag resolutions).
void RegisterVgiGithubLatestFunction(ExtensionLoader &loader);

//! Register vgi_github_resolve(location) (which release a github LOCATION runs).
void RegisterVgiGithubResolveFunction(ExtensionLoader &loader);

} // namespace vgi
} // namespace duckdb
