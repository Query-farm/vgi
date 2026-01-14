#pragma once

#include <arrow/api.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace duckdb {
namespace vgi {

// Create the VGI Invocation RecordBatch for a catalog method call
// The Invocation message is the first message sent to a VGI worker
std::shared_ptr<arrow::RecordBatch> CreateCatalogInvocation(const std::string &method_name);

// Create an empty arguments batch (for catalog methods with no parameters)
std::shared_ptr<arrow::RecordBatch> CreateEmptyArgsBatch();

// Create arguments batch with attach_id (for catalog methods that need it)
std::shared_ptr<arrow::RecordBatch> CreateCatalogArgsWithAttachId(const std::vector<uint8_t> &attach_id);

// Create arguments batch for catalog_attach method
std::shared_ptr<arrow::RecordBatch> CreateCatalogAttachArgs(const std::string &catalog_name);

// Create arguments batch for schema_get method
std::shared_ptr<arrow::RecordBatch> CreateSchemaGetArgs(const std::vector<uint8_t> &attach_id,
                                                        const std::string &schema_name);

// Create arguments batch for table_get method
std::shared_ptr<arrow::RecordBatch> CreateTableGetArgs(const std::vector<uint8_t> &attach_id,
                                                       const std::string &schema_name, const std::string &table_name);

// Create arguments batch for schema_contents method
std::shared_ptr<arrow::RecordBatch> CreateSchemaContentsArgs(const std::vector<uint8_t> &attach_id,
                                                              const std::string &schema_name);

// ============================================================================
// Function Invocation Protocol
// ============================================================================

// Create arguments batch for function_get method (get function metadata)
std::shared_ptr<arrow::RecordBatch> CreateFunctionGetArgs(const std::vector<uint8_t> &attach_id,
                                                          const std::string &schema_name,
                                                          const std::string &function_name);

// Create the VGI Invocation RecordBatch for a function invocation
// Unlike catalog invocations, this uses function_type="function"
std::shared_ptr<arrow::RecordBatch> CreateFunctionInvocation(const std::string &function_name);

// Create arguments batch for function_invoke method
// positional_args: JSON-encoded array of positional arguments
// named_args: map of named arguments (string keys, JSON-encoded values)
// projection_ids: optional list of column indices for projection pushdown
std::shared_ptr<arrow::RecordBatch> CreateFunctionInvokeArgs(const std::vector<uint8_t> &attach_id,
                                                              const std::string &schema_name,
                                                              const std::string &function_name,
                                                              const std::string &positional_args_json,
                                                              const std::vector<std::pair<std::string, std::string>> &named_args,
                                                              const std::vector<int32_t> &projection_ids = {});

} // namespace vgi
} // namespace duckdb
