// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_catalog_metadata.hpp"
#include "vgi_query_rpc.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {
class ParsedExpression;
class ColumnList;
class TypeExpression;
namespace vgi {
inline constexpr const char *VGI_COLUMN_DEFAULT = "vgi.column.default.v1";
inline constexpr const char *VGI_COLUMN_GENERATED = "vgi.column.generated.v1";
inline constexpr const char *VGI_COLUMN_GENERATION = "vgi.column.generation.v1";

// The initial native adapter supports the default explicit context and a closed
// deterministic scalar grammar. Generated expressions use its bounded row subset.
// Keep engine-generated literals and validated built-in casts independent of caller search paths.
void QualifyNativeBuiltinType(TypeExpression &type);
unique_ptr<ParsedExpression> NativeLiteralExpression(const Value &value);
VgiQueryEvaluationContext NativeColumnEvaluationContext(ClientContext &context);
std::vector<uint8_t> BuildNativeColumnExpression(const std::string &sql, const std::string &schema_name,
                                                 const VgiQueryEvaluationContext &context, bool using_expression);
std::vector<uint8_t> BuildNativeGeneratedExpression(const std::string &sql, const std::string &schema_name,
                                                    const VgiQueryEvaluationContext &context);
unique_ptr<ParsedExpression> ReadNativeColumnGenerated(const ArrowSchema &field, const std::string &schema_name);
void ValidateNativeGeneratedColumns(ClientContext &context, const ColumnList &columns);
unique_ptr<ParsedExpression> ReadNativeColumnDefault(const ArrowSchema &field, const std::string &schema_name);
unique_ptr<ParsedExpression> BindNativeColumnDefault(ClientContext &context, const ArrowSchema &field,
                                                     const std::string &schema_name, const LogicalType &target_type);
// Remove only already validated top-level default/generated annotations for Arrow type
// import. The authoritative table schema is retained unchanged by the caller.
VgiSchemaPtr NativeColumnTypeSchema(const VgiSchemaPtr &schema, const std::string &schema_name);
std::string EncodeNativeColumnDefaultMetadata(const std::vector<uint8_t> &expression);
} // namespace vgi
} // namespace duckdb
