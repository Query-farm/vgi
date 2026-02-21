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

} // namespace vgi
} // namespace duckdb
