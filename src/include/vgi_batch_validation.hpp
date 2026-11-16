// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"

// Validation of Arrow batches received from a worker. IPC decoding checks only
// framing, not buffer contents (offsets past data, short buffers, invalid UTF-8,
// dictionary indices out of range). DuckDB's Arrow conversion trusts those, so a
// malformed batch reads out of bounds. Every worker batch is therefore validated
// at the level `vgi_validate_worker_batches` selects.

#include <memory>
#include <string>
#include <vector>

namespace duckdb {

class ClientContext;

namespace vgi {

enum class WorkerBatchValidation : unsigned char {
	NONE,       // no checks
	STRUCTURAL, // nanoarrow DEFAULT: buffer sizes vs lengths and offsets (per column)
	FULL,       // nanoarrow FULL plus UTF-8: every offset, union id, dictionary index, string (per value)
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
void ValidateWorkerBatch(const VgiBatch *batch, WorkerBatchValidation level, const std::string &worker);

// Throw IOException if `batch`'s column count or type structure (ignoring
// names, nullability, metadata) differs from `expected` (one per wire column).
// DuckDB's arrow_conversion.cpp reads buffers using the bind-time type, so a
// scan-time mismatch would be a type-confusion misread. O(columns).
void ValidateWireBatchTypes(const VgiBatch &batch,
                            const std::vector<const ArrowSchema *> &expected,
                            const std::string &worker, const std::string &function);

// ValidateWireBatchTypes against `declared` narrowed by `projection_ids`
// (worker-original column indices; empty = all columns). No-op when the level
// is NONE or `declared` is null. Used by every path whose wire batch is the
// worker's output (table scan, table-in-out, buffered, lateral).
void ValidateProjectedWireBatch(ClientContext *context, const VgiBatch &batch,
                                const VgiSchemaPtr &declared,
                                const std::vector<int32_t> &projection_ids, const std::string &worker,
                                const std::string &function);

inline void ValidateWorkerBatch(ClientContext *context, const VgiBatch *batch, const std::string &worker) {
	ValidateWorkerBatch(batch, GetWorkerBatchValidation(context), worker);
}

} // namespace vgi
} // namespace duckdb
