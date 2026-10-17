// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_aggregate_streaming_impl.hpp"
#include "vgi_nanoarrow_ipc.hpp"

#include <cstring>

#include <arrow/builder.h>
#include <arrow/ipc/api.h>

#include "duckdb/common/exception.hpp"

#include "vgi_aggregate_function_impl.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_exception.hpp"
#include "vgi_logging.hpp"
#include "vgi_rpc_types.hpp" // SerializeToIpcBytes / DeserializeFromIpcBytes
#include "generated/vgi_request_builders.hpp"

namespace duckdb {
namespace vgi {

namespace {

// Use vgi::ThrowOnArrowError (vgi_arrow_utils.hpp) for status checks. We
// don't redeclare it here — that creates an ambiguous overload set when
// arrow::Result<T>::status() is passed.

std::vector<uint8_t> SerializeSchemaBytes(const std::shared_ptr<arrow::Schema> &schema) {
	auto buf = ::duckdb::vgi::nanoipc::SerializeSchema(*schema);
	ThrowOnArrowError(buf.status());
	auto &b = buf.ValueUnsafe();
	return std::vector<uint8_t>(b->data(), b->data() + b->size());
}

// Build aggregate_streaming_open request batch matching
// vgi.protocol.AggregateStreamingOpenRequest field-for-field.
//
// arguments: bytes (Arguments.serialize_to_bytes() — empty for the v1
// streaming operator until we wire const-arg passing through the operator).
std::shared_ptr<arrow::RecordBatch> BuildStreamingOpenRequest(
    const std::string &function_name,
    const std::string &schema_name,
    const std::vector<uint8_t> &arguments,
    const std::shared_ptr<arrow::Schema> &input_schema,
    int64_t partition_key_count,
    int64_t order_key_count,
    const std::shared_ptr<arrow::Schema> &output_schema,
    const std::vector<uint8_t> &attach_opaque_data) {
	generated::AggregateStreamingOpenRequest request;
	request.function_name = function_name;
	request.arguments = arguments;
	request.input_schema = SerializeSchemaBytes(input_schema);
	request.partition_key_count = partition_key_count;
	request.order_key_count = order_key_count;
	request.output_schema = SerializeSchemaBytes(output_schema);
	// settings / secrets are not wired through the streaming operator yet.
	request.attach_opaque_data = BytesOrNull(attach_opaque_data);
	request.schema_path = OptionalSingleSchemaPath(schema_name);
	return generated::BuildAggregateStreamingOpenParams(SerializeRecord(request));
}

std::shared_ptr<arrow::RecordBatch> BuildStreamingChunkRequest(
    const std::string &function_name,
    const std::string &schema_name,
    const std::vector<uint8_t> &execution_id,
    const std::shared_ptr<arrow::RecordBatch> &input_batch,
    const std::vector<uint8_t> &attach_opaque_data) {
	generated::AggregateStreamingChunkRequest request;
	request.function_name = function_name;
	request.execution_id = execution_id;
	request.input_batch = SerializeToIpcBytes(input_batch);
	request.attach_opaque_data = BytesOrNull(attach_opaque_data);
	request.schema_path = OptionalSingleSchemaPath(schema_name);
	return generated::BuildAggregateStreamingChunkParams(SerializeRecord(request));
}

std::shared_ptr<arrow::RecordBatch> BuildStreamingCloseRequest(
    const std::string &function_name,
    const std::string &schema_name,
    const std::vector<uint8_t> &execution_id,
    const std::vector<uint8_t> &attach_opaque_data) {
	generated::AggregateStreamingCloseRequest request;
	request.function_name = function_name;
	request.execution_id = execution_id;
	request.attach_opaque_data = BytesOrNull(attach_opaque_data);
	request.schema_path = OptionalSingleSchemaPath(schema_name);
	return generated::BuildAggregateStreamingCloseParams(SerializeRecord(request));
}

} // anonymous namespace

// ============================================================================
// Public entry points
// ============================================================================

VgiStreamingSession VgiAggregateStreamingOpen(
    ClientContext &context,
    const VgiAggregateBindData &bind_data,
    const std::shared_ptr<arrow::Schema> &input_schema,
    int64_t partition_key_count,
    int64_t order_key_count) {

	// Build an empty Arguments envelope: a 1-row RecordBatch with a single
	// ``args: struct<>`` column whose only row is an empty struct. The
	// Python ``Arguments.serialize_to_bytes()`` produces this exact shape
	// for the no-const-args case (see vgi/arguments.py — it uses
	// ``pa.array([{}], type=pa.struct([]))``), and the deserializer reads
	// ``batch.column("args")[0]`` so the batch must be length-1.
	//
	// (The existing ``BuildAggregateBindRequest`` produces a 0-row batch in
	// this branch — that path doesn't trip in practice because real bind
	// requests always carry at least one positional/named arg, so the
	// 0-row branch is dead code. The streaming protocol *can* exercise
	// it, hence the explicit 1-row construction here.)
	std::vector<uint8_t> arguments_bytes;
	{
		auto empty_struct_type = arrow::struct_({});
		// Length-1 StructArray with no child fields.
		std::vector<std::shared_ptr<arrow::Array>> child_arrays;
		auto struct_array = std::make_shared<arrow::StructArray>(empty_struct_type, /*length=*/1,
		                                                          child_arrays);
		auto args_schema = arrow::schema({arrow::field("args", empty_struct_type)});
		auto args_batch = arrow::RecordBatch::Make(args_schema, 1, {struct_array});
		arguments_bytes = SerializeToIpcBytes(args_batch);
	}

	auto request = BuildStreamingOpenRequest(
	    bind_data.function_name, bind_data.schema_name, arguments_bytes, input_schema,
	    partition_key_count, order_key_count, bind_data.resolved_output_schema,
	    bind_data.attach_opaque_data);

	auto rpc_result = InvokeAggregateRpc(context, bind_data, "aggregate_streaming_open", request);
	auto open = DecodeAggregateResult<generated::AggregateStreamingOpenResult>(rpc_result, bind_data,
	                                                                           "aggregate_streaming_open");

	VgiStreamingSession session;
	session.execution_id = std::move(open.execution_id);
	session.function_name = bind_data.function_name;
	session.schema_name = bind_data.schema_name;
	session.attach_opaque_data = bind_data.attach_opaque_data;
	return session;
}

std::shared_ptr<arrow::RecordBatch> VgiAggregateStreamingChunk(
    ClientContext &context,
    const VgiAggregateBindData &bind_data,
    const VgiStreamingSession &session,
    const std::shared_ptr<arrow::RecordBatch> &input_batch) {

	auto request = BuildStreamingChunkRequest(
	    session.function_name, session.schema_name, session.execution_id, input_batch,
	    session.attach_opaque_data);

	auto rpc_result = InvokeAggregateRpc(context, bind_data, "aggregate_streaming_chunk", request);
	auto chunk = DecodeAggregateResult<generated::AggregateStreamingChunkResult>(rpc_result, bind_data,
	                                                                             "aggregate_streaming_chunk");
	auto result_batch = DeserializeFromIpcBytes(chunk.result_batch, GetWorkerBatchValidation(&context));

	if (!result_batch) {
		throw IOException(
		    AggregateViolation(bind_data, "aggregate_streaming_chunk"),
		    "VGI aggregate_streaming_chunk returned no result batch for %lld input rows",
		    static_cast<long long>(input_batch->num_rows()));
	}
	if (result_batch->num_rows() != input_batch->num_rows()) {
		throw IOException(
		    AggregateViolation(bind_data, "aggregate_streaming_chunk")
		        .ExpectedActual(std::to_string(input_batch->num_rows()) + " rows",
		                        std::to_string(result_batch->num_rows()) + " rows"),
		    "VGI aggregate_streaming_chunk returned %lld rows for %lld input rows",
		    static_cast<long long>(result_batch->num_rows()),
		    static_cast<long long>(input_batch->num_rows()));
	}
	return result_batch;
}

void VgiAggregateStreamingClose(
    ClientContext &context,
    const VgiAggregateBindData &bind_data,
    const VgiStreamingSession &session,
    bool enable_logging) {

	auto request = BuildStreamingCloseRequest(
	    session.function_name, session.schema_name, session.execution_id,
	    session.attach_opaque_data);

	InvokeAggregateRpc(context, bind_data, "aggregate_streaming_close", request, enable_logging);
}

} // namespace vgi
} // namespace duckdb
