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

// Build aggregate_streaming_open request batch matching
// vgi.protocol.AggregateStreamingOpenRequest field-for-field.
//
// arguments: bytes (Arguments.serialize_to_bytes() — empty for the v1
// streaming operator until we wire const-arg passing through the operator).
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

// ============================================================================
// Public entry points
// ============================================================================

VgiStreamingSession VgiAggregateStreamingOpen(
    ClientContext &context,
    const VgiAggregateBindData &bind_data,
    const VgiSchemaPtr &input_schema,
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
		// Length-1 struct<> column (one valid, empty row) with no child fields.
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
