// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {
namespace vgi {

struct VgiAttachParameters;

// ============================================================================
// VgiCopyFromFunctionInfo - per-format carrier for a registered COPY FROM
// format. Attached to the copy_from_function's TableFunctionInfo so
// VgiCopyFromBind can recover the worker + format context.
//
// CRITICAL: this carrier MUST be fully self-contained and outlive DETACH.
// The system-catalog CopyFunction entry persists past DETACH, so a post-DETACH
// `COPY ... (FORMAT x)` re-enters this carrier. It therefore holds NO Catalog&
// / VgiCatalog* — only value/shared_ptr state (attach_params is a shared_ptr
// and self-contained: worker path + auth, no back-pointer into VgiCatalog).
// ============================================================================
class VgiCopyFromFunctionInfo final : public TableFunctionInfo {
public:
	VgiCopyFromFunctionInfo(std::string catalog_name, std::shared_ptr<VgiAttachParameters> attach_params,
	                        std::vector<uint8_t> attach_opaque_data, std::string format_name, std::string handler,
	                        std::shared_ptr<arrow::Schema> options_schema, std::vector<std::string> setting_names)
	    : catalog_name(std::move(catalog_name)), attach_params(std::move(attach_params)),
	      attach_opaque_data(std::move(attach_opaque_data)), format_name(std::move(format_name)),
	      handler(std::move(handler)), options_schema(std::move(options_schema)),
	      setting_names(std::move(setting_names)) {
	}
	~VgiCopyFromFunctionInfo() override = default;

	std::string catalog_name; // for error messages only (catalog may be detached)
	std::shared_ptr<VgiAttachParameters> attach_params;
	std::vector<uint8_t> attach_opaque_data;
	std::string format_name;
	std::string handler;
	std::shared_ptr<arrow::Schema> options_schema; // null when the format has no options
	std::vector<std::string> setting_names;
};

// Build the TableFunction used as CopyFunction::copy_from_function. Reuses the
// existing VGI table-function init/scan callbacks (producer mode) and copies
// over the diagnostic hooks (cardinality / progress / to_string) so EXPLAIN and
// progress work for COPY scans.
TableFunction MakeVgiCopyFromTableFunction();

// copy_from_bind_t entry for VGI-registered COPY formats. Validates/coerces the
// COPY options against the format's option schema, builds a
// VgiTableFunctionBindData targeting the worker handler with the COPY context
// attached, binds it, and hard-validates the worker's output schema against the
// COPY target schema.
unique_ptr<FunctionData> VgiCopyFromBind(ClientContext &context, CopyFromFunctionBindInput &info,
                                         vector<string> &expected_names, vector<LogicalType> &expected_types);

} // namespace vgi
} // namespace duckdb
