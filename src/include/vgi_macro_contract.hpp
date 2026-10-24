// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once
#include "vgi_catalog_metadata.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {
class QueryNode;
class ParsedExpression;
namespace vgi {
void QualifyNativeMacroQuery(QueryNode &node, const case_insensitive_set_t &worker_functions,
                             const string &catalog_name, const string &schema_name);
void QualifyNativeMacroExpression(ParsedExpression &expression, const case_insensitive_set_t &worker_functions,
                                  const string &catalog_name, const string &schema_name);
std::shared_ptr<arrow::RecordBatch> DecodeMacroInfoRecord(const uint8_t *data, size_t size);
void RequireNativeMacroContext(ClientContext &context);
LogicalType NativeMacroParameterType(ClientContext &context, const VgiMacroParameter &parameter);
Value NativeMacroDefaultValue(ClientContext &context, const VgiMacroParameter &parameter);
} // namespace vgi
} // namespace duckdb
