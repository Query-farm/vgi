// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <cstdint>
#include <map>
#include <string>

namespace duckdb {

// Per-object-kind counts unpacked from the SchemaInfo / threshold-setting maps
// at construction time. Keys are matched against VgiCatalogSet::CacheKindName().
// The hot path (VgiCatalogSet::GetEntry) reads scalar fields off this struct
// directly — no map lookups, no string compares.
struct VgiObjectCounts {
	int64_t table = 1;
	int64_t view = 1;
	int64_t index = 1;
	int64_t scalar_function = 1;
	int64_t aggregate_function = 1;
	int64_t table_function = 1;
	int64_t macro = 1;
};

// Build a VgiObjectCounts from a wire-side map. Missing keys take ``default_value``
// so callers can pick the policy: 1 (treat absent as "small, eager-load") for
// estimated counts, or the threshold default (e.g. 1000) for thresholds.
VgiObjectCounts ObjectCountsFromMap(const std::map<std::string, int64_t> &m, int64_t default_value);

// Reverse lookup — used only at construction sites, never on the hot path.
int64_t ObjectCountFor(const VgiObjectCounts &counts, const std::string &kind);

} // namespace duckdb
