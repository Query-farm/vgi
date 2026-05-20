// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_table_buffering_builders.hpp"

#include "generated/vgi_request_builders.hpp"
#include "vgi_arrow_ipc.hpp"
#include "vgi_arrow_utils.hpp"

namespace duckdb {
namespace vgi {


// ----------------------------------------------------------------------------
// Table sink+source inner builders (new buffered API).
// ----------------------------------------------------------------------------

std::shared_ptr<arrow::RecordBatch>
BuildTableBufferingProcessInner(const std::string &function_name,
                                  const std::vector<uint8_t> &execution_id,
                                  const std::vector<uint8_t> &input_batch_bytes,
                                  const std::vector<uint8_t> &attach_opaque_data,
                                  std::optional<int64_t> batch_index) {
	// Inner schema matches TableBufferingProcessRequest in protocol.py.
	// No state_id field — the worker chooses it and ships it on the response.
	auto inner_schema = arrow::schema({
	    arrow::field("function_name", arrow::utf8(), false),
	    arrow::field("execution_id", arrow::binary(), false),
	    arrow::field("input_batch", arrow::binary(), false),
	    arrow::field("attach_opaque_data", arrow::binary(), true),
	    arrow::field("transaction_id", arrow::binary(), true),
	    arrow::field("batch_index", arrow::int64(), true),
	});
	arrow::Int64Builder bi_builder;
	if (batch_index.has_value()) {
		(void)bi_builder.Append(*batch_index);
	} else {
		(void)bi_builder.AppendNull();
	}
	std::shared_ptr<arrow::Array> bi_array;
	(void)bi_builder.Finish(&bi_array);
	arrow::BinaryBuilder txn_builder;
	(void)txn_builder.AppendNull();  // transaction_id reserved; not populated yet
	std::shared_ptr<arrow::Array> txn_array;
	(void)txn_builder.Finish(&txn_array);
	auto inner = arrow::RecordBatch::Make(inner_schema, 1, {
	    vgi::MakeSingleStringArray(function_name),
	    vgi::MakeSingleBinaryArray(execution_id),
	    vgi::MakeSingleBinaryArray(input_batch_bytes),
	    vgi::MakeSingleBinaryArrayOrNull(attach_opaque_data),
	    txn_array,
	    bi_array,
	});
	auto inner_bytes = vgi::SerializeToIpcBytes(inner);
	return vgi::generated::BuildTableBufferingProcessParams(inner_bytes);
}

std::shared_ptr<arrow::RecordBatch>
BuildTableBufferingCombineInner(const std::string &function_name,
                                  const std::vector<uint8_t> &execution_id,
                                  const std::vector<std::vector<uint8_t>> &state_ids,
                                  const std::vector<uint8_t> &attach_opaque_data) {
	auto inner_schema = arrow::schema({
	    arrow::field("function_name", arrow::utf8(), false),
	    arrow::field("execution_id", arrow::binary(), false),
	    arrow::field("state_ids", arrow::list(arrow::binary()), false),
	    arrow::field("attach_opaque_data", arrow::binary(), true),
	    arrow::field("transaction_id", arrow::binary(), true),
	});
	arrow::ListBuilder list_builder(arrow::default_memory_pool(), std::make_shared<arrow::BinaryBuilder>());
	auto *value_builder = static_cast<arrow::BinaryBuilder *>(list_builder.value_builder());
	(void)list_builder.Append();
	for (const auto &sid : state_ids) {
		(void)value_builder->Append(sid.data(), static_cast<int32_t>(sid.size()));
	}
	std::shared_ptr<arrow::Array> state_ids_array;
	(void)list_builder.Finish(&state_ids_array);
	arrow::BinaryBuilder txn_builder;
	(void)txn_builder.AppendNull();
	std::shared_ptr<arrow::Array> txn_array;
	(void)txn_builder.Finish(&txn_array);
	auto inner = arrow::RecordBatch::Make(inner_schema, 1, {
	    vgi::MakeSingleStringArray(function_name),
	    vgi::MakeSingleBinaryArray(execution_id),
	    state_ids_array,
	    vgi::MakeSingleBinaryArrayOrNull(attach_opaque_data),
	    txn_array,
	});
	auto inner_bytes = vgi::SerializeToIpcBytes(inner);
	return vgi::generated::BuildTableBufferingCombineParams(inner_bytes);
}

std::shared_ptr<arrow::RecordBatch>
BuildTableBufferingDestructorInner(const std::string &function_name,
                                     const std::vector<uint8_t> &execution_id,
                                     const std::vector<uint8_t> &attach_opaque_data) {
	auto inner_schema = arrow::schema({
	    arrow::field("function_name", arrow::utf8(), false),
	    arrow::field("execution_id", arrow::binary(), false),
	    arrow::field("attach_opaque_data", arrow::binary(), true),
	    arrow::field("transaction_id", arrow::binary(), true),
	});
	arrow::BinaryBuilder txn_builder;
	(void)txn_builder.AppendNull();
	std::shared_ptr<arrow::Array> txn_array;
	(void)txn_builder.Finish(&txn_array);
	auto inner = arrow::RecordBatch::Make(inner_schema, 1, {
	    vgi::MakeSingleStringArray(function_name),
	    vgi::MakeSingleBinaryArray(execution_id),
	    vgi::MakeSingleBinaryArrayOrNull(attach_opaque_data),
	    txn_array,
	});
	auto inner_bytes = vgi::SerializeToIpcBytes(inner);
	return vgi::generated::BuildTableBufferingDestructorParams(inner_bytes);
}

} // namespace vgi
} // namespace duckdb
