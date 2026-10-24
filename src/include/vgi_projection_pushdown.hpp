// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once
#include "duckdb/common/common.hpp"
#include <memory>
#include <vector>
namespace arrow {
class RecordBatch;
class Schema;
} // namespace arrow
namespace duckdb {
class DBConfig;
class ClientContext;
class TableFunction;
namespace vgi {
struct VgiTableFunctionBindData;
struct ProjectionAcceptance {
	std::vector<idx_t> indices;
	std::shared_ptr<arrow::RecordBatch> bind_result;
	std::shared_ptr<arrow::Schema> output_schema;
};
ProjectionAcceptance ValidateProjectionAcceptance(const std::vector<uint8_t> &request,
                                                  const std::shared_ptr<arrow::RecordBatch> &response);
void RestoreProjectionAcceptance(ClientContext &context, VgiTableFunctionBindData &data, TableFunction &function,
                                 const std::vector<uint8_t> &request, const std::vector<uint8_t> &response);
void RegisterVgiProjectionOptimizer(DBConfig &config);
} // namespace vgi
} // namespace duckdb
