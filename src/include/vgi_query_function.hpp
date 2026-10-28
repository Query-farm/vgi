// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once

#include <memory>

#include "vgi_arrow.hpp"

namespace duckdb {
class ExtensionLoader;
namespace vgi {
void RegisterVgiQueryFunction(ExtensionLoader &loader);
void ValidateVgiQueryResultBatch(const VgiBatchPtr &batch,
                                 const VgiSchemaPtr &expected);
} // namespace vgi
} // namespace duckdb
