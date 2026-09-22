// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_batch_validation.hpp"

#include "vgi_exception.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"

#include <arrow/record_batch.h>
#include <arrow/type.h>
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

void ValidateWireBatchTypes(const arrow::RecordBatch &batch,
                            const std::vector<std::shared_ptr<arrow::DataType>> &expected, const std::string &worker,
                            const std::string &function) {
	const auto &schema = *batch.schema();
	if (static_cast<size_t>(schema.num_fields()) != expected.size()) {
		ThrowVgiIOException("vgi: worker function '%s' sent a batch with %lld column(s) but its declared bind output "
		                    "schema has %llu; the scan-time output disagrees with the bind schema",
		                    worker, -1, "", function.c_str(), (long long)schema.num_fields(),
		                    (unsigned long long)expected.size());
	}
	for (size_t i = 0; i < expected.size(); i++) {
		if (!expected[i] || !schema.field(static_cast<int>(i))->type()) {
			continue;
		}
		if (!schema.field(static_cast<int>(i))->type()->Equals(*expected[i])) {
			ThrowVgiIOException("vgi: worker function '%s' sent column %llu as type %s but declared %s at bind; "
			                    "reading it as the declared type would misread memory",
			                    worker, -1, "", function.c_str(), (unsigned long long)i,
			                    schema.field(static_cast<int>(i))->type()->ToString().c_str(),
			                    expected[i]->ToString().c_str());
		}
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
