// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

namespace duckdb {

class ExtensionLoader;

namespace vgi {

//! Register the vgi_worker_subprocess_pool() table function
void RegisterVgiWorkerPoolFunction(ExtensionLoader &loader);

//! Register the vgi_worker_pool_stats() table function (hit/miss statistics)
void RegisterVgiWorkerPoolStatsFunction(ExtensionLoader &loader);

//! Register the vgi_worker_pool_flush() scalar function
void RegisterVgiWorkerPoolFlushFunction(ExtensionLoader &loader);

} // namespace vgi
} // namespace duckdb
