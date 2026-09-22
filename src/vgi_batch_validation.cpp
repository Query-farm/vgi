// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_batch_validation.hpp"

#include "vgi_exception.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"

#include <arrow/record_batch.h>
#include <arrow/status.h>

namespace duckdb {
namespace vgi {

WorkerBatchValidation ParseWorkerBatchValidation(const std::string &value) {
	auto lower = StringUtil::Lower(value);
	StringUtil::Trim(lower);
	if (lower == "full") {
		return WorkerBatchValidation::FULL;
	}
	if (lower == "structural") {
		return WorkerBatchValidation::STRUCTURAL;
	}
	if (lower == "none") {
		return WorkerBatchValidation::NONE;
	}
	throw InvalidInputException("%s must be 'full', 'structural' or 'none' (got '%s')", kWorkerBatchValidationSetting,
	                            value);
}

const char *WorkerBatchValidationName(WorkerBatchValidation level) {
	switch (level) {
	case WorkerBatchValidation::FULL:
		return "full";
	case WorkerBatchValidation::STRUCTURAL:
		return "structural";
	default:
		return "none";
	}
}

WorkerBatchValidation GetWorkerBatchValidation(ClientContext *context) {
	if (!context) {
		return WorkerBatchValidation::FULL;
	}
	Value value;
	if (!context->TryGetCurrentSetting(kWorkerBatchValidationSetting, value) || value.IsNull()) {
		return WorkerBatchValidation::FULL;
	}
	try {
		return ParseWorkerBatchValidation(value.ToString());
	} catch (...) {
		return WorkerBatchValidation::FULL; // the setting callback rejects bad values; fail safe regardless
	}
}

void ValidateWorkerBatch(const arrow::RecordBatch *batch, WorkerBatchValidation level, const std::string &worker) {
	if (!batch || level == WorkerBatchValidation::NONE) {
		return;
	}
	auto status = level == WorkerBatchValidation::FULL ? batch->ValidateFull() : batch->Validate();
	if (!status.ok()) {
		ThrowVgiIOException("vgi: worker sent a malformed Arrow batch (%s validation): %s", worker, -1, "",
		                    WorkerBatchValidationName(level), status.ToString());
	}
}

} // namespace vgi
} // namespace duckdb
