// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once

namespace duckdb {
struct DBConfig;
namespace vgi {
//! Push `geom && <constant>` (optional) into VGI scans for exact spatial predicates.
void RegisterVgiSpatialExtentOptimizer(DBConfig &config);
} // namespace vgi
} // namespace duckdb
