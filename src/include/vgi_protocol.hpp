// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace duckdb {
namespace vgi {

// Provider guarantee for repeated scans of the exact same bind state in one
// statement. Independent binds and prepared executions need not share a snapshot.
enum class BindRepeatability : uint8_t { UNKNOWN, STABLE_WITHIN_QUERY };

struct BindResult {
	VgiSchemaPtr output_schema;
	std::vector<uint8_t> opaque_data;            // From BindResponse
	std::vector<uint8_t> bind_request_bytes;     // Cached serialized BindRequest (needed for init)
	std::vector<uint8_t> output_schema_bytes;    // Cached serialized output schema (needed for init)
	// Sanitized cache-safety signal. True when secrets came from function
	// metadata or the worker requested them during the two-phase bind. Secret
	// names, scopes, and values deliberately do not escape the bind machinery.
	bool secret_dependent = false;
	// When secret_dependent: ComputeSecretCacheFingerprint over the exact secrets
	// this bind sent to the worker — the ones init replays via bind_request_bytes.
	// The result cache keys on it, so a changed or dropped secret misses. Empty
	// when not secret_dependent, or when hashing failed (cache must then refuse).
	std::string secret_scope;
	std::vector<std::string> projection_expression_profiles;
	std::vector<uint8_t> bind_response_bytes;
	BindRepeatability repeatability = BindRepeatability::UNKNOWN;
};

struct InitResult {
	std::vector<uint8_t> execution_id;
	int64_t max_workers = 1;
	std::vector<uint8_t> opaque_data;
};

// ORDER BY + LIMIT hint from DuckDB's RowGroupPruner optimizer, sent to the
// worker as nullable InitRequest fields.
struct OrderByHint {
	std::string column_name;
	std::string direction;    // "ASC" or "DESC"
	std::string null_order;   // "NULLS_FIRST" or "NULLS_LAST"
	int64_t row_limit = -1;   // Combined limit+offset, -1 = no limit
};

// TABLESAMPLE SYSTEM hint from DuckDB's SamplingPushdown optimizer.
// Only SYSTEM_SAMPLE with percentage is pushed down; Bernoulli and Reservoir
// are always handled by DuckDB's physical operators.
struct TableSampleHint {
	double sample_percentage;  // 0.0 to 100.0
	int64_t seed;              // -1 = no seed specified
};

} // namespace vgi
} // namespace duckdb
