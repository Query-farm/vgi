// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "duckdb/function/copy_function.hpp"

namespace duckdb {
namespace vgi {

struct VgiAttachParameters;

// ============================================================================
// VgiCopyToFunctionInfo - per-format carrier for a registered COPY TO format.
// Attached to CopyFunction::function_info (which copy_to_bind receives via
// CopyFunctionBindInput.function_info — unlike COPY FROM, no table-function
// indirection is needed).
//
// CRITICAL: self-contained and must outlive DETACH (the system-catalog
// CopyFunction entry persists). Holds NO Catalog& / VgiCatalog* — only
// value/shared_ptr state (attach_params is a shared_ptr: worker path + auth).
// ============================================================================
class VgiCopyToFunctionInfo final : public CopyFunctionInfo {
public:
	VgiCopyToFunctionInfo(std::string catalog_name, std::shared_ptr<VgiAttachParameters> attach_params,
	                      std::vector<uint8_t> attach_opaque_data, std::string format_name, std::string handler,
	                      std::shared_ptr<arrow::Schema> options_schema, std::vector<std::string> setting_names)
	    : catalog_name(std::move(catalog_name)), attach_params(std::move(attach_params)),
	      attach_opaque_data(std::move(attach_opaque_data)), format_name(std::move(format_name)),
	      handler(std::move(handler)), options_schema(std::move(options_schema)),
	      setting_names(std::move(setting_names)) {
	}
	~VgiCopyToFunctionInfo() override = default;

	std::string catalog_name; // attach alias, for error messages (may be detached)
	std::shared_ptr<VgiAttachParameters> attach_params;
	std::vector<uint8_t> attach_opaque_data;
	std::string format_name;
	std::string handler;
	std::shared_ptr<arrow::Schema> options_schema; // null when the format has no options
	std::vector<std::string> setting_names;
};

// Install the copy_to_* callbacks (bind / init_global / init_local / sink /
// combine / finalize / execution_mode) onto a CopyFunction. The per-format
// worker context must already be set on cf.function_info as a
// VgiCopyToFunctionInfo. Reuses the buffered-table Sink+Combine RPCs
// (table_buffering_process / table_buffering_combine); there is no Source phase.
//
// When `ordered` is true the format requires source order, so a single-threaded
// sink (REGULAR_COPY_TO_FILE) execution mode is installed instead of the default
// parallel sharded write (PARALLEL_COPY_TO_FILE).
void InstallVgiCopyToCallbacks(CopyFunction &cf, bool ordered);

} // namespace vgi
} // namespace duckdb
