// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// Validation of Arrow batches received from a worker.
//
// Arrow's IPC reader verifies only the message framing: the flatbuffer
// metadata, and that each buffer's offset is aligned and inside the message
// body. It does not check buffer *contents* -- offsets that run past their data,
// buffers too short for the array's length, invalid UTF-8, dictionary indices
// out of range. DuckDB's Arrow conversion trusts those values, so a malformed
// batch from a worker reads out of bounds: wrong results, a crash, or process
// memory returned as query data. Every batch that enters from a worker is
// therefore validated at the level `vgi_validate_worker_batches` selects.

#include <memory>
#include <string>
#include <vector>

namespace arrow {
class RecordBatch;
class DataType;
class Schema;
}

namespace duckdb {

class ClientContext;

namespace vgi {

enum class WorkerBatchValidation : unsigned char {
	NONE,       // no checks
	STRUCTURAL, // RecordBatch::Validate(): buffer sizes, lengths, offsets bounds (per column)
	FULL,       // RecordBatch::ValidateFull(): also every offset, UTF-8, dictionary index (per value)
};

constexpr const char *kWorkerBatchValidationSetting = "vgi_validate_worker_batches";

// Parse a setting value (`full`, `structural`, `none`; case-insensitive).
// Throws InvalidInputException on anything else.
WorkerBatchValidation ParseWorkerBatchValidation(const std::string &value);

// Canonical setting spelling of `level`.
const char *WorkerBatchValidationName(WorkerBatchValidation level);

// The level for a query: the setting's value, or FULL when there is no
// context (or the setting is unavailable).
WorkerBatchValidation GetWorkerBatchValidation(ClientContext *context);

// Validate `batch` at `level`; throw IOException naming `worker` if it is
// malformed. A null batch is ignored.
void ValidateWorkerBatch(const arrow::RecordBatch *batch, WorkerBatchValidation level, const std::string &worker);

// Throw IOException if `batch`'s column types do not match `expected` (one entry
// per wire column; type structure only -- nullability and field metadata are
// ignored, matching what ArrowToDuckDB actually reads). A column-count mismatch
// is an error too. This catches a worker whose scan-time batch type disagrees
// with the type it declared at bind: ArrowToDuckDB reads each buffer using the
// BIND-TIME type (arrow_conversion.cpp), so a mismatch is a type-confusion
// misread, not a clean cast. Cheap (O(columns), no data scan).
void ValidateWireBatchTypes(const arrow::RecordBatch &batch,
                            const std::vector<std::shared_ptr<arrow::DataType>> &expected,
                            const std::string &worker, const std::string &function);

// Validate a worker OUTPUT batch against a declared output schema projected by
// `projection_ids` (worker-original column indices; empty = the full schema,
// all columns in order). Builds the expected per-wire-column types and defers
// to ValidateWireBatchTypes. A no-op when `context`'s level is NONE or
// `declared` is null. This is the shared form for every path whose wire batch
// is the worker's (possibly projection-narrowed) output: the producer table
// scan and the exchange table-in-out / buffered / lateral operators.
void ValidateProjectedWireBatch(ClientContext *context, const arrow::RecordBatch &batch,
                                const std::shared_ptr<arrow::Schema> &declared,
                                const std::vector<int32_t> &projection_ids, const std::string &worker,
                                const std::string &function);

// Convenience: validate at the level configured for `context`.
inline void ValidateWorkerBatch(ClientContext *context, const arrow::RecordBatch *batch, const std::string &worker) {
	ValidateWorkerBatch(batch, GetWorkerBatchValidation(context), worker);
}

} // namespace vgi
} // namespace duckdb
