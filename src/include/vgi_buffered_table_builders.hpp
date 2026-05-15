#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <arrow/api.h>

namespace duckdb {
namespace vgi {

// ============================================================================
// Buffered-table RPC inner-request builders + outer-response decoder
// ============================================================================
// Shared between FunctionConnection (subprocess) and HttpFunctionConnection.
// Each Build*Inner function constructs the inner RecordBatch matching the
// worker's expected schema, serializes it to IPC bytes, and wraps the bytes
// in the outer RPC envelope (BuildBufferedTable*Params from the generated
// header). Callers serialize the resulting outer batch with WriteRpcRequest
// (subprocess) or HttpInvokeUnary (HTTP).

// `batch_index` is forwarded only when the function declared
// Meta.requires_input_batch_index = True; pass nullopt otherwise. The inner
// schema field is nullable and matches BufferedTableProcessRequest in
// vgi-python/vgi/protocol.py.
std::shared_ptr<arrow::RecordBatch>
BuildBufferedTableProcessInner(const std::string &function_name,
                                const std::vector<uint8_t> &execution_id, int64_t state_id,
                                const std::vector<uint8_t> &input_batch_bytes,
                                const std::vector<uint8_t> &attach_opaque_data,
                                std::optional<int64_t> batch_index = std::nullopt);

std::shared_ptr<arrow::RecordBatch>
BuildBufferedTableCombineInner(const std::string &function_name,
                                const std::vector<uint8_t> &execution_id,
                                const std::vector<int64_t> &state_ids,
                                const std::vector<uint8_t> &attach_opaque_data);

// BuildBufferedTableFinalizeInner removed: Source phase now uses
// PerformInit(phase=BUFFERED_TABLE_FINALIZE) instead of a unary RPC.

std::shared_ptr<arrow::RecordBatch>
BuildBufferedTableDestructorInner(const std::string &function_name,
                                   const std::vector<uint8_t> &execution_id,
                                   const std::vector<uint8_t> &attach_opaque_data);

} // namespace vgi
} // namespace duckdb
