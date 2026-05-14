#include "vgi_buffered_table_builders.hpp"

#include "generated/vgi_request_builders.hpp"
#include "vgi_arrow_ipc.hpp"
#include "vgi_arrow_utils.hpp"

namespace duckdb {
namespace vgi {

std::shared_ptr<arrow::RecordBatch>
BuildBufferedTableProcessInner(const std::string &function_name,
                                const std::vector<uint8_t> &execution_id, int64_t state_id,
                                const std::vector<uint8_t> &input_batch_bytes,
                                const std::vector<uint8_t> &attach_opaque_data,
                                std::optional<int64_t> batch_index) {
	// `batch_index` is a nullable int64 — present iff the function declared
	// Meta.requires_input_batch_index = True; absent (NULL) otherwise. The
	// schema field is always present so the worker's deserializer can rely
	// on the column shape regardless of whether any value is set.
	auto inner_schema = arrow::schema({
	    arrow::field("function_name", arrow::utf8(), false),
	    arrow::field("execution_id", arrow::binary(), false),
	    arrow::field("state_id", arrow::int64(), false),
	    arrow::field("input_batch", arrow::binary(), false),
	    arrow::field("attach_opaque_data", arrow::binary(), true),
	    arrow::field("batch_index", arrow::int64(), true),
	});
	arrow::Int64Builder sid_builder;
	(void)sid_builder.Append(state_id);
	std::shared_ptr<arrow::Array> sid_array;
	(void)sid_builder.Finish(&sid_array);
	arrow::Int64Builder bi_builder;
	if (batch_index.has_value()) {
		(void)bi_builder.Append(*batch_index);
	} else {
		(void)bi_builder.AppendNull();
	}
	std::shared_ptr<arrow::Array> bi_array;
	(void)bi_builder.Finish(&bi_array);
	auto inner = arrow::RecordBatch::Make(inner_schema, 1, {
	    vgi::MakeSingleStringArray(function_name),
	    vgi::MakeSingleBinaryArray(execution_id),
	    sid_array,
	    vgi::MakeSingleBinaryArray(input_batch_bytes),
	    vgi::MakeSingleBinaryArrayOrNull(attach_opaque_data),
	    bi_array,
	});
	auto inner_bytes = vgi::SerializeToIpcBytes(inner);
	return vgi::generated::BuildBufferedTableProcessParams(inner_bytes);
}

std::shared_ptr<arrow::RecordBatch>
BuildBufferedTableCombineInner(const std::string &function_name,
                                const std::vector<uint8_t> &execution_id,
                                const std::vector<int64_t> &state_ids,
                                const std::vector<uint8_t> &attach_opaque_data) {
	auto inner_schema = arrow::schema({
	    arrow::field("function_name", arrow::utf8(), false),
	    arrow::field("execution_id", arrow::binary(), false),
	    arrow::field("state_ids", arrow::list(arrow::int64()), false),
	    arrow::field("attach_opaque_data", arrow::binary(), true),
	});
	arrow::ListBuilder list_builder(arrow::default_memory_pool(), std::make_shared<arrow::Int64Builder>());
	auto *value_builder = static_cast<arrow::Int64Builder *>(list_builder.value_builder());
	(void)list_builder.Append();
	for (auto v : state_ids) {
		(void)value_builder->Append(v);
	}
	std::shared_ptr<arrow::Array> state_ids_array;
	(void)list_builder.Finish(&state_ids_array);
	auto inner = arrow::RecordBatch::Make(inner_schema, 1, {
	    vgi::MakeSingleStringArray(function_name),
	    vgi::MakeSingleBinaryArray(execution_id),
	    state_ids_array,
	    vgi::MakeSingleBinaryArrayOrNull(attach_opaque_data),
	});
	auto inner_bytes = vgi::SerializeToIpcBytes(inner);
	return vgi::generated::BuildBufferedTableCombineParams(inner_bytes);
}

std::shared_ptr<arrow::RecordBatch>
BuildBufferedTableFinalizeInner(const std::string &function_name,
                                 const std::vector<uint8_t> &execution_id,
                                 int64_t finalize_state_id,
                                 const std::vector<uint8_t> &attach_opaque_data) {
	auto inner_schema = arrow::schema({
	    arrow::field("function_name", arrow::utf8(), false),
	    arrow::field("execution_id", arrow::binary(), false),
	    arrow::field("finalize_state_id", arrow::int64(), false),
	    arrow::field("attach_opaque_data", arrow::binary(), true),
	});
	arrow::Int64Builder sid_builder;
	(void)sid_builder.Append(finalize_state_id);
	std::shared_ptr<arrow::Array> sid_array;
	(void)sid_builder.Finish(&sid_array);
	auto inner = arrow::RecordBatch::Make(inner_schema, 1, {
	    vgi::MakeSingleStringArray(function_name),
	    vgi::MakeSingleBinaryArray(execution_id),
	    sid_array,
	    vgi::MakeSingleBinaryArrayOrNull(attach_opaque_data),
	});
	auto inner_bytes = vgi::SerializeToIpcBytes(inner);
	return vgi::generated::BuildBufferedTableFinalizeParams(inner_bytes);
}

} // namespace vgi
} // namespace duckdb
