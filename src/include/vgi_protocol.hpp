#pragma once

#include <arrow/api.h>

#include <memory>
#include <string>

namespace duckdb {
namespace vgi {

// Create the VGI Invocation RecordBatch for a catalog method call
// The Invocation message is the first message sent to a VGI worker
std::shared_ptr<arrow::RecordBatch> CreateCatalogInvocation(const std::string &method_name);

// Create an empty arguments batch (for catalog methods with no parameters)
std::shared_ptr<arrow::RecordBatch> CreateEmptyArgsBatch();

} // namespace vgi
} // namespace duckdb
