// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_catalog_metadata.hpp"
#include "vgi_query_rpc.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace arrow {
class Field;
}

namespace duckdb {
class ParsedExpression;
namespace vgi {
inline constexpr const char *VGI_COLUMN_DEFAULT = "vgi.column.default.v1";

// The initial native adapter supports the default explicit context and a closed
// deterministic scalar grammar. Generated-column semantics remain unsupported.
VgiQueryEvaluationContext NativeColumnEvaluationContext(ClientContext &context);
std::vector<uint8_t> BuildNativeColumnExpression(const std::string &sql, const std::string &schema_name,
                                                 const VgiQueryEvaluationContext &context, bool using_expression);
unique_ptr<ParsedExpression> ReadNativeColumnDefault(const std::shared_ptr<arrow::Field> &field,
                                                     const std::string &schema_name);
unique_ptr<ParsedExpression> BindNativeColumnDefault(ClientContext &context, const std::shared_ptr<arrow::Field> &field,
                                                     const std::string &schema_name, const LogicalType &target_type);
// Remove only an already validated top-level default annotation for Arrow type
// import. The authoritative table schema is retained unchanged by the caller.
std::shared_ptr<arrow::Schema> NativeColumnTypeSchema(const std::shared_ptr<arrow::Schema> &schema,
                                                      const std::string &schema_name);
std::string EncodeNativeColumnDefaultMetadata(const std::vector<uint8_t> &expression);
} // namespace vgi
} // namespace duckdb
