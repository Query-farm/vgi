// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <cstdint>
#include <map>
#include <string>

namespace duckdb {

// Per-object-kind counts unpacked from the SchemaInfo / threshold-setting maps.
// Keys are VgiCatalogSet::CacheKindName() values.
struct VgiObjectCounts {
	int64_t table = 1;
	int64_t view = 1;
	int64_t index = 1;
	int64_t scalar_function = 1;
	int64_t aggregate_function = 1;
	int64_t table_function = 1;
	int64_t macro = 1;
};

// Missing keys take `default_value`: 1 ("small, eager-load") for estimated
// counts, the threshold default for thresholds. Unknown keys are ignored.
VgiObjectCounts ObjectCountsFromMap(const std::map<std::string, int64_t> &m, int64_t default_value);

// String lookup; VgiCatalogSet::ResolveEagerLoadParamsLocked calls it once per set
// and caches the result. Unknown kinds return 1.
int64_t ObjectCountFor(const VgiObjectCounts &counts, const std::string &kind);

} // namespace duckdb
