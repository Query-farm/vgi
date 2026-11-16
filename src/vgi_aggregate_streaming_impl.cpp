// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_aggregate_streaming_impl.hpp"

#include <cstring>

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

// The streaming operator passes no constant arguments yet: `arguments` is
// always the empty envelope built by VgiAggregateStreamingOpen.
VgiBatchPtr BuildStreamingOpenRequest(
    const std::string &function_name,
    const std::string &schema_name,
    const std::vector<uint8_t> &arguments,
    const VgiSchemaPtr &input_schema,
    int64_t partition_key_count,
    int64_t order_key_count,
    const VgiSchemaPtr &output_schema,
    const std::vector<uint8_t> &attach_opaque_data) {
	generated::AggregateStreamingOpenRequest request;
	request.function_name = function_name;
	request.arguments = arguments;
	request.input_schema = SerializeSchemaToIpcBytes(input_schema);
	request.partition_key_count = partition_key_count;
	request.order_key_count = order_key_count;
	request.output_schema = SerializeSchemaToIpcBytes(output_schema);
	// settings / secrets are not wired through the streaming operator yet.
	request.attach_opaque_data = BytesOrNull(attach_opaque_data);
	request.schema_path = OptionalSingleSchemaPath(schema_name);
	return generated::BuildAggregateStreamingOpenParams(SerializeRecord(request));
}

VgiBatchPtr BuildStreamingChunkRequest(
    const std::string &function_name,
    const std::string &schema_name,
    const std::vector<uint8_t> &execution_id,
    const VgiBatchPtr &input_batch,
    const std::vector<uint8_t> &attach_opaque_data) {
	generated::AggregateStreamingChunkRequest request;
	request.function_name = function_name;
	request.execution_id = execution_id;
	request.input_batch = SerializeToIpcBytes(input_batch);
	request.attach_opaque_data = BytesOrNull(attach_opaque_data);
	request.schema_path = OptionalSingleSchemaPath(schema_name);
	return generated::BuildAggregateStreamingChunkParams(SerializeRecord(request));
}

VgiBatchPtr BuildStreamingCloseRequest(
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

VgiStreamingSession VgiAggregateStreamingOpen(
    ClientContext &context,
    const VgiAggregateBindData &bind_data,
    const VgiSchemaPtr &input_schema,
    int64_t partition_key_count,
    int64_t order_key_count) {

	// Empty Arguments envelope: ONE valid, empty `args: struct<>` row. Python's
	// Arguments.deserialize_from_bytes reads batch.column("args")[0], so a
	// zero-row batch would fail there.
	std::vector<uint8_t> arguments_bytes;
	{
		VgiBatchBuilder builder(MakeSchema({type::Struct({}).Named("args")}));
		append::FinishElement(builder.column(0));
		builder.FinishRow();
		arguments_bytes = SerializeToIpcBytes(builder.Finish());
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

VgiBatchPtr VgiAggregateStreamingChunk(
    ClientContext &context,
    const VgiAggregateBindData &bind_data,
    const VgiStreamingSession &session,
    const VgiBatchPtr &input_batch) {

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
