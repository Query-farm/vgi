// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"

#include <memory>
#include <string>
#include <vector>


#include "duckdb/function/copy_function.hpp"

namespace duckdb {
namespace vgi {

struct VgiAttachParameters;

// Per-format worker context for a registered COPY TO format, carried on
// CopyFunction::function_info. Same DETACH rule as VgiCopyFromFunctionInfo:
// self-contained, no Catalog& / VgiCatalog*.
class VgiCopyToFunctionInfo final : public CopyFunctionInfo {
public:
	VgiCopyToFunctionInfo(std::string catalog_name, std::shared_ptr<VgiAttachParameters> attach_params,
	                      std::vector<uint8_t> attach_opaque_data, std::string format_name, std::string handler,
	                      VgiSchemaPtr options_schema, std::vector<std::string> setting_names)
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
	VgiSchemaPtr options_schema; // null when the format has no options
	std::vector<std::string> setting_names;
};

// Install the copy_to_* callbacks; cf.function_info must already hold a
// VgiCopyToFunctionInfo. Uses the buffered-table RPCs (table_buffering_process /
// _combine), with no Source phase. `ordered` selects a single-threaded sink
// (REGULAR_COPY_TO_FILE) whenever DuckDB preserves insertion order.
void InstallVgiCopyToCallbacks(CopyFunction &cf, bool ordered);

} // namespace vgi
} // namespace duckdb
