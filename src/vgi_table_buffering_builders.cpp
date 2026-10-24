// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_table_buffering_builders.hpp"

#include "generated/vgi_request_builders.hpp"
#include "vgi_arrow_ipc.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_rpc_types.hpp"

namespace duckdb {
namespace vgi {


// ----------------------------------------------------------------------------
// Table sink+source inner builders (new buffered API).
// ----------------------------------------------------------------------------

std::shared_ptr<arrow::RecordBatch>
BuildTableBufferingProcessInner(const std::string &function_name,
                                  const std::string &schema_name,
                                  const std::vector<uint8_t> &execution_id,
                                  const std::vector<uint8_t> &input_batch_bytes,
                                  const std::vector<uint8_t> &attach_opaque_data,
                                  std::optional<int64_t> batch_index) {
	// No state_id field — the worker chooses it and ships it on the response.
	// transaction_id is reserved and not populated yet.
	generated::TableBufferingProcessRequest request;
	request.function_name = function_name;
	request.execution_id = execution_id;
	request.input_batch = input_batch_bytes;
	request.attach_opaque_data = BytesOrNull(attach_opaque_data);
	request.batch_index = batch_index;
	request.schema_path = OptionalSingleSchemaPath(schema_name);
	return generated::BuildTableBufferingProcessParams(SerializeRecord(request));
}

std::shared_ptr<arrow::RecordBatch>
BuildTableBufferingCombineInner(const std::string &function_name,
                                  const std::string &schema_name,
                                  const std::vector<uint8_t> &execution_id,
                                  const std::vector<std::vector<uint8_t>> &state_ids,
                                  const std::vector<uint8_t> &attach_opaque_data) {
	generated::TableBufferingCombineRequest request;
	request.function_name = function_name;
	request.execution_id = execution_id;
	request.state_ids = state_ids;
	request.attach_opaque_data = BytesOrNull(attach_opaque_data);
	request.schema_path = OptionalSingleSchemaPath(schema_name);
	return generated::BuildTableBufferingCombineParams(SerializeRecord(request));
}

std::shared_ptr<arrow::RecordBatch>
BuildTableBufferingDestructorInner(const std::string &function_name,
                                     const std::string &schema_name,
                                     const std::vector<uint8_t> &execution_id,
                                     const std::vector<uint8_t> &attach_opaque_data) {
	generated::TableBufferingDestructorRequest request;
	request.function_name = function_name;
	request.execution_id = execution_id;
	request.attach_opaque_data = BytesOrNull(attach_opaque_data);
	request.schema_path = OptionalSingleSchemaPath(schema_name);
	return generated::BuildTableBufferingDestructorParams(SerializeRecord(request));
}

} // namespace vgi
} // namespace duckdb
