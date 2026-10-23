// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once
#include "vgi_catalog_metadata.hpp"

namespace duckdb {
namespace vgi {
std::shared_ptr<arrow::RecordBatch> DecodeMacroInfoRecord(const uint8_t *data, size_t size);
void RequireNativeMacroContext(ClientContext &context);
LogicalType NativeMacroParameterType(ClientContext &context, const VgiMacroParameter &parameter);
Value NativeMacroDefaultValue(ClientContext &context, const VgiMacroParameter &parameter);
} // namespace vgi
} // namespace duckdb
