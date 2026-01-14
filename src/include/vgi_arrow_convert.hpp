#pragma once

#include "duckdb.hpp"

#include <arrow/api.h>

namespace duckdb {
namespace vgi {

// ============================================================================
// Arrow to DuckDB Type Conversion
// ============================================================================

// Convert an Arrow DataType to a DuckDB LogicalType
LogicalType ArrowTypeToDuckDB(const std::shared_ptr<arrow::DataType> &arrow_type);

// Convert an Arrow Schema to DuckDB types and names
void ArrowSchemaToDuckDB(const std::shared_ptr<arrow::Schema> &schema, vector<LogicalType> &types,
                         vector<string> &names);

// ============================================================================
// Arrow to DuckDB Data Conversion
// ============================================================================

// Convert an Arrow Array to a DuckDB Vector
// This handles all supported Arrow types and properly converts nulls.
// Parameters:
//   array: The Arrow array to convert
//   offset: Starting offset within the Arrow array
//   count: Number of elements to convert
//   target: The DuckDB vector to write to
//   target_offset: Starting offset within the target vector
void ArrowArrayToVector(const std::shared_ptr<arrow::Array> &array, idx_t offset, idx_t count, Vector &target,
                        idx_t target_offset);

// Convert an entire Arrow RecordBatch to a DuckDB DataChunk
// Parameters:
//   batch: The Arrow RecordBatch to convert
//   offset: Starting row offset within the batch
//   count: Number of rows to convert
//   output: The DuckDB DataChunk to write to
//   output_offset: Starting row offset within the output chunk
void ArrowBatchToDataChunk(const std::shared_ptr<arrow::RecordBatch> &batch, idx_t offset, idx_t count,
                           DataChunk &output, idx_t output_offset);

} // namespace vgi
} // namespace duckdb
