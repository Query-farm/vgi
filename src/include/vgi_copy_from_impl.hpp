// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"

#include <memory>
#include <string>
#include <vector>


#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {
namespace vgi {

struct VgiAttachParameters;

// Per-format worker context for a registered COPY FROM format, carried on the
// copy_from_function's TableFunctionInfo for VgiCopyFromBind.
//
// MUST be self-contained: the system-catalog CopyFunction entry outlives
// DETACH, so a post-DETACH `COPY ... (FORMAT x)` re-enters this carrier. Hold
// no Catalog& / VgiCatalog* — only value / shared_ptr state.
class VgiCopyFromFunctionInfo final : public TableFunctionInfo {
public:
	VgiCopyFromFunctionInfo(std::string catalog_name, std::shared_ptr<VgiAttachParameters> attach_params,
	                        std::vector<uint8_t> attach_opaque_data, std::string format_name, std::string handler,
	                        VgiSchemaPtr options_schema, std::vector<std::string> setting_names)
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
	VgiSchemaPtr options_schema; // null when the format has no options
	std::vector<std::string> setting_names;
};

// CopyFunction::copy_from_function: the producer-mode VGI table-function
// callbacks plus the EXPLAIN / progress hooks.
TableFunction MakeVgiCopyFromTableFunction();

// Coerces COPY options to the format's option schema, binds the worker handler
// with the COPY context, and requires the worker's output types to equal the
// COPY target's exactly.
unique_ptr<FunctionData> VgiCopyFromBind(ClientContext &context, CopyFromFunctionBindInput &info,
                                         vector<Identifier> &expected_names, vector<LogicalType> &expected_types);

} // namespace vgi
} // namespace duckdb
