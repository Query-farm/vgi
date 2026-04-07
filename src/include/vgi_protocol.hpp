#pragma once

#include <arrow/api.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace duckdb {
namespace vgi {

// ============================================================================
// vgi_rpc-based Function Protocol Types
// ============================================================================

// Result from bind RPC call
struct BindResult {
	std::shared_ptr<arrow::Schema> output_schema;
	std::vector<uint8_t> opaque_data;            // From BindResponse
	std::vector<uint8_t> bind_request_bytes;     // Cached serialized BindRequest (needed for init)
	std::vector<uint8_t> output_schema_bytes;    // Cached serialized output schema (needed for init)
};

// Result from init RPC call
struct InitResult {
	std::vector<uint8_t> execution_id;
	int64_t max_workers = 1;
	std::vector<uint8_t> opaque_data;
};

// ORDER BY + LIMIT hint from DuckDB's RowGroupPruner optimizer.
// Passed to worker as nullable fields on the InitRequest.
// Workers may use this to optimize server-side ordering and early termination.
struct OrderByHint {
	std::string column_name;  // Column name to order by
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
