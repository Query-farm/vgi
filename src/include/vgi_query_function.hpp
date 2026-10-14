// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once

#include <memory>

namespace arrow {
class RecordBatch;
class Schema;
}

namespace duckdb {
class ExtensionLoader;
namespace vgi {
void RegisterVgiQueryFunction(ExtensionLoader &loader);
void ValidateVgiQueryResultBatch(const std::shared_ptr<arrow::RecordBatch> &batch,
                                 const std::shared_ptr<arrow::Schema> &expected);
} // namespace vgi
} // namespace duckdb
