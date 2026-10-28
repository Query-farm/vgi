// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_batch_validation.hpp"

#include "vgi_exception.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"

#include "nanoarrow/nanoarrow.h"
#include "utf8proc_wrapper.hpp"

#include <cstring>

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

namespace {

// Type structure equality for the type-confusion guard: formats, children and
// dictionaries (plus the map-keys-sorted / dictionary-ordered flags, which are
// part of the type). Field names, nullability and metadata are ignored -- a
// worker may legitimately rename a nested child or tighten nullability, and
// neither changes how ArrowToDuckDB reads the buffers.
bool SameTypeStructure(const ArrowSchema &a, const ArrowSchema &b) {
	const char *fa = a.format ? a.format : "";
	const char *fb = b.format ? b.format : "";
	if (std::strcmp(fa, fb) != 0 || a.n_children != b.n_children) {
		return false;
	}
	constexpr int64_t kTypeFlags = ARROW_FLAG_DICTIONARY_ORDERED | ARROW_FLAG_MAP_KEYS_SORTED;
	if ((a.flags & kTypeFlags) != (b.flags & kTypeFlags)) {
		return false;
	}
	for (int64_t i = 0; i < a.n_children; i++) {
		if (!SameTypeStructure(*a.children[i], *b.children[i])) {
			return false;
		}
	}
	if ((a.dictionary == nullptr) != (b.dictionary == nullptr)) {
		return false;
	}
	return !a.dictionary || SameTypeStructure(*a.dictionary, *b.dictionary);
}

[[noreturn]] void ThrowMalformed(WorkerBatchValidation level, const std::string &worker, const std::string &detail) {
	throw IOException(ErrorInfo(error_subtype::kMalformedBatch)
	                      .Worker(worker)
	                      .Set(error_key::kValidationLevel, WorkerBatchValidationName(level)),
	                  BuildMessageWithContext("vgi: worker sent a malformed Arrow batch (%s validation): %s", worker),
	                  WorkerBatchValidationName(level), detail);
}

// The detail text in Arrow C++'s Status::ToString() spelling, which the
// malformed-batch tests (and anything matching on the message) key on.
std::string NanoarrowDetail(const ArrowError &error, ArrowErrorCode code) {
	std::string message = error.message[0] ? std::string(error.message) : std::string(std::strerror(code));
	if (message.find("dictionary index") != std::string::npos) {
		return "Invalid: Dictionary indices invalid: " + message;
	}
	return "Invalid: " + message;
}

// Arrow's ValidateFull also checked that every utf8 / large_utf8 value is valid
// UTF-8; nanoarrow's FULL level does not. Walk every string node (nested
// children and dictionary values included), over the node's whole length, as
// Arrow did. Returns false and fills `detail` on the first invalid value.
bool ValidateUtf8(const ArrowArrayView &view, std::string &detail) {
	if (view.storage_type == NANOARROW_TYPE_STRING || view.storage_type == NANOARROW_TYPE_LARGE_STRING) {
		const int64_t length = view.length;
		if (length > 0) {
			const bool large = view.storage_type == NANOARROW_TYPE_LARGE_STRING;
			const char *data = view.buffer_views[2].data.as_char;
			auto offset_at = [&](int64_t i) -> int64_t {
				return large ? view.buffer_views[1].data.as_int64[view.offset + i]
				             : static_cast<int64_t>(view.buffer_views[1].data.as_int32[view.offset + i]);
			};
			const int64_t first = offset_at(0);
			const int64_t last = offset_at(length);
			// Fast path: a span that is entirely valid ASCII cannot hold an invalid
			// value (ASCII has no multi-byte sequences to split across values).
			bool all_ascii = false;
			if (last > first && data) {
				all_ascii = Utf8Proc::Analyze(data + first, static_cast<size_t>(last - first)) == UnicodeType::ASCII;
			} else {
				all_ascii = true;
			}
			if (!all_ascii) {
				for (int64_t i = 0; i < length; i++) {
					if (ArrowArrayViewIsNull(&view, i)) {
						continue;
					}
					const int64_t begin = offset_at(i);
					const int64_t end = offset_at(i + 1);
					if (end > begin &&
					    Utf8Proc::Analyze(data + begin, static_cast<size_t>(end - begin)) == UnicodeType::INVALID) {
						detail = "Invalid: Invalid UTF8 sequence at string index " + std::to_string(i);
						return false;
					}
				}
			}
		}
	} else if (view.storage_type == NANOARROW_TYPE_STRING_VIEW) {
		for (int64_t i = 0; i < view.length; i++) {
			if (ArrowArrayViewIsNull(&view, i)) {
				continue;
			}
			auto s = ArrowArrayViewGetStringUnsafe(&view, i);
			if (s.size_bytes > 0 &&
			    Utf8Proc::Analyze(s.data, static_cast<size_t>(s.size_bytes)) == UnicodeType::INVALID) {
				detail = "Invalid: Invalid UTF8 sequence at string index " + std::to_string(i);
				return false;
			}
		}
	}
	for (int64_t c = 0; c < view.n_children; c++) {
		if (!ValidateUtf8(*view.children[c], detail)) {
			return false;
		}
	}
	if (view.dictionary && !ValidateUtf8(*view.dictionary, detail)) {
		return false;
	}
	return true;
}

} // namespace

void ValidateWireBatchTypes(const VgiBatch &batch, const std::vector<const ArrowSchema *> &expected,
                            const std::string &worker, const std::string &function) {
	const auto &schema = *batch.schema();
	if (static_cast<size_t>(schema.num_fields()) != expected.size()) {
		throw IOException(ErrorInfo(error_subtype::kSchemaMismatch)
		                      .Function(function)
		                      .Worker(worker)
		                      .ExpectedActual(std::to_string(expected.size()) + " columns",
		                                      std::to_string(schema.num_fields()) + " columns"),
		                  BuildMessageWithContext("vgi: worker function '%s' sent a batch with %lld column(s) but its "
		                                          "declared bind output schema has %llu; the scan-time output "
		                                          "disagrees with the bind schema",
		                                          worker),
		                  function.c_str(), (long long)schema.num_fields(), (unsigned long long)expected.size());
	}
	for (size_t i = 0; i < expected.size(); i++) {
		if (!expected[i]) {
			continue;
		}
		const auto &field = schema.field(static_cast<int>(i));
		if (!SameTypeStructure(field, *expected[i])) {
			const auto actual_type = TypeToString(field);
			const auto expected_type = TypeToString(*expected[i]);
			throw IOException(ErrorInfo(error_subtype::kSchemaMismatch)
			                      .Function(function)
			                      .Worker(worker)
			                      .Set(error_key::kColumnIndex, static_cast<int64_t>(i))
			                      .Set(error_key::kColumnName, schema.field_name(static_cast<int>(i)))
			                      .ExpectedActual(expected_type, actual_type),
			                  BuildMessageWithContext("vgi: worker function '%s' sent column %llu as type %s but "
			                                          "declared %s at bind; reading it as the declared type would "
			                                          "misread memory",
			                                          worker),
			                  function.c_str(), (unsigned long long)i, actual_type.c_str(), expected_type.c_str());
		}
	}
}

void ValidateProjectedWireBatch(ClientContext *context, const VgiBatch &batch, const VgiSchemaPtr &declared,
                                const std::vector<int32_t> &projection_ids, const std::string &worker,
                                const std::string &function) {
	if (!declared || GetWorkerBatchValidation(context) == WorkerBatchValidation::NONE) {
		return;
	}
	std::vector<const ArrowSchema *> expected;
	if (projection_ids.empty()) {
		expected.reserve(declared->num_fields());
		for (int i = 0; i < declared->num_fields(); i++) {
			expected.push_back(&declared->field(i));
		}
	} else {
		expected.reserve(projection_ids.size());
		for (int32_t original : projection_ids) {
			expected.push_back(original >= 0 && original < declared->num_fields() ? &declared->field(original)
			                                                                      : nullptr);
		}
	}
	ValidateWireBatchTypes(batch, expected, worker, function);
}

void ValidateWorkerBatch(const VgiBatch *batch, WorkerBatchValidation level, const std::string &worker) {
	if (!batch || level == WorkerBatchValidation::NONE) {
		return;
	}
	// Buffer-size checks need the real body sizes, which only the IPC decoder had.
	if (!batch->structural_error().empty()) {
		ThrowMalformed(level, worker, "Invalid: " + batch->structural_error());
	}
	// structural -> nanoarrow DEFAULT (buffer sizes vs. lengths and offsets, per
	// column); full -> FULL (every offset, union id, dictionary index) plus the
	// UTF-8 pass Arrow's ValidateFull did.
	ArrowArrayView view;
	ArrowError error;
	ArrowErrorInit(&error);
	ArrowErrorCode code = ArrowArrayViewInitFromSchema(&view, &batch->schema()->c_schema(), &error);
	if (code != NANOARROW_OK) {
		ArrowArrayViewReset(&view);
		ThrowMalformed(level, worker, NanoarrowDetail(error, code));
	}
	code = ArrowArrayViewSetArray(&view, &batch->c_array(), &error);
	if (code == NANOARROW_OK && level == WorkerBatchValidation::FULL) {
		code = ArrowArrayViewValidate(&view, NANOARROW_VALIDATION_LEVEL_FULL, &error);
	}
	std::string detail;
	bool ok = code == NANOARROW_OK;
	if (ok) {
		if (level == WorkerBatchValidation::FULL && !ValidateUtf8(view, detail)) {
			ok = false;
		}
	} else {
		detail = NanoarrowDetail(error, code);
	}
	ArrowArrayViewReset(&view);
	if (!ok) {
		ThrowMalformed(level, worker, detail);
	}
}

} // namespace vgi
} // namespace duckdb
