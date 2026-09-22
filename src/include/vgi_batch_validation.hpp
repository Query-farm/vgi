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

#include <string>

namespace arrow {
class RecordBatch;
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

// Convenience: validate at the level configured for `context`.
inline void ValidateWorkerBatch(ClientContext *context, const arrow::RecordBatch *batch, const std::string &worker) {
	ValidateWorkerBatch(batch, GetWorkerBatchValidation(context), worker);
}

} // namespace vgi
} // namespace duckdb
