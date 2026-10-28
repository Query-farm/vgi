// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once
#include "duckdb/common/common.hpp"
#include "vgi_arrow.hpp"
#include <vector>
namespace duckdb {
class DBConfig;
class ClientContext;
class BaseTableFunction;
namespace vgi {
struct VgiTableFunctionBindData;
struct ProjectionAcceptance {
	std::vector<idx_t> indices;
	VgiBatchPtr bind_result;
	VgiSchemaPtr output_schema;
};
ProjectionAcceptance ValidateProjectionAcceptance(const std::vector<uint8_t> &request,
                                                  const VgiBatchPtr &response);
void RestoreProjectionAcceptance(ClientContext &context, VgiTableFunctionBindData &data, BaseTableFunction &function,
                                 const std::vector<uint8_t> &request, const std::vector<uint8_t> &response);
void RegisterVgiProjectionOptimizer(DBConfig &config);
} // namespace vgi
} // namespace duckdb
