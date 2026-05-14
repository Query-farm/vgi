#include "vgi_aggregate_streaming_impl.hpp"

#include <cstring>

#include <arrow/builder.h>
#include <arrow/ipc/api.h>

#include "duckdb/common/exception.hpp"

#include "vgi_aggregate_function_impl.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_logging.hpp"

namespace duckdb {
namespace vgi {

namespace {

// Use vgi::ThrowOnArrowError (vgi_arrow_utils.hpp) for status checks. We
// don't redeclare it here — that creates an ambiguous overload set when
// arrow::Result<T>::status() is passed.

std::vector<uint8_t> SerializeSchemaBytes(const std::shared_ptr<arrow::Schema> &schema) {
	auto buf = arrow::ipc::SerializeSchema(*schema);
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
    const std::vector<uint8_t> &arguments,
    const std::shared_ptr<arrow::Schema> &input_schema,
    int64_t partition_key_count,
    int64_t order_key_count,
    const std::shared_ptr<arrow::Schema> &output_schema,
    const std::vector<uint8_t> &attach_opaque_data) {

	auto input_schema_bytes = SerializeSchemaBytes(input_schema);
	auto output_schema_bytes = SerializeSchemaBytes(output_schema);

	auto schema = arrow::schema({
	    arrow::field("function_name", arrow::utf8(), false),
	    arrow::field("arguments", arrow::binary(), false),
	    arrow::field("input_schema", arrow::binary(), false),
	    arrow::field("partition_key_count", arrow::int64(), false),
	    arrow::field("order_key_count", arrow::int64(), false),
	    arrow::field("output_schema", arrow::binary(), false),
	    arrow::field("settings", arrow::binary(), true),
	    arrow::field("secrets", arrow::binary(), true),
	    arrow::field("attach_opaque_data", arrow::binary(), true),
	});

	arrow::StringBuilder fn_b;
	arrow::BinaryBuilder args_b, input_schema_b, output_schema_b, settings_b, secrets_b, aid_b;
	arrow::Int64Builder pkc_b, okc_b;

	ThrowOnArrowError(fn_b.Append(function_name));
	ThrowOnArrowError(args_b.Append(arguments.data(), arguments.size()));
	ThrowOnArrowError(input_schema_b.Append(input_schema_bytes.data(), input_schema_bytes.size()));
	ThrowOnArrowError(pkc_b.Append(partition_key_count));
	ThrowOnArrowError(okc_b.Append(order_key_count));
	ThrowOnArrowError(output_schema_b.Append(output_schema_bytes.data(), output_schema_bytes.size()));
	ThrowOnArrowError(settings_b.AppendNull());
	ThrowOnArrowError(secrets_b.AppendNull());
	if (attach_opaque_data.empty()) {
		ThrowOnArrowError(aid_b.AppendNull());
	} else {
		ThrowOnArrowError(aid_b.Append(attach_opaque_data.data(), attach_opaque_data.size()));
	}

	std::shared_ptr<arrow::Array> fn_a, args_a, is_a, pkc_a, okc_a, os_a, st_a, sc_a, aid_a;
	ThrowOnArrowError(fn_b.Finish(&fn_a));
	ThrowOnArrowError(args_b.Finish(&args_a));
	ThrowOnArrowError(input_schema_b.Finish(&is_a));
	ThrowOnArrowError(pkc_b.Finish(&pkc_a));
	ThrowOnArrowError(okc_b.Finish(&okc_a));
	ThrowOnArrowError(output_schema_b.Finish(&os_a));
	ThrowOnArrowError(settings_b.Finish(&st_a));
	ThrowOnArrowError(secrets_b.Finish(&sc_a));
	ThrowOnArrowError(aid_b.Finish(&aid_a));

	return WrapAsRpcParams(arrow::RecordBatch::Make(
	    schema, 1, {fn_a, args_a, is_a, pkc_a, okc_a, os_a, st_a, sc_a, aid_a}));
}

std::shared_ptr<arrow::RecordBatch> BuildStreamingChunkRequest(
    const std::string &function_name,
    const std::vector<uint8_t> &execution_id,
    const std::shared_ptr<arrow::RecordBatch> &input_batch,
    const std::vector<uint8_t> &attach_opaque_data) {

	auto batch_bytes = SerializeToIpcBytes(input_batch);

	auto schema = arrow::schema({
	    arrow::field("function_name", arrow::utf8(), false),
	    arrow::field("execution_id", arrow::binary(), false),
	    arrow::field("input_batch", arrow::binary(), false),
	    arrow::field("attach_opaque_data", arrow::binary(), true),
	});

	arrow::StringBuilder fn_b;
	arrow::BinaryBuilder eid_b, batch_b, aid_b;

	ThrowOnArrowError(fn_b.Append(function_name));
	ThrowOnArrowError(eid_b.Append(execution_id.data(), execution_id.size()));
	ThrowOnArrowError(batch_b.Append(batch_bytes.data(), batch_bytes.size()));
	if (attach_opaque_data.empty()) {
		ThrowOnArrowError(aid_b.AppendNull());
	} else {
		ThrowOnArrowError(aid_b.Append(attach_opaque_data.data(), attach_opaque_data.size()));
	}

	std::shared_ptr<arrow::Array> fn_a, eid_a, batch_a, aid_a;
	ThrowOnArrowError(fn_b.Finish(&fn_a));
	ThrowOnArrowError(eid_b.Finish(&eid_a));
	ThrowOnArrowError(batch_b.Finish(&batch_a));
	ThrowOnArrowError(aid_b.Finish(&aid_a));

	return WrapAsRpcParams(arrow::RecordBatch::Make(schema, 1, {fn_a, eid_a, batch_a, aid_a}));
}

std::shared_ptr<arrow::RecordBatch> BuildStreamingCloseRequest(
    const std::string &function_name,
    const std::vector<uint8_t> &execution_id,
    const std::vector<uint8_t> &attach_opaque_data) {

	auto schema = arrow::schema({
	    arrow::field("function_name", arrow::utf8(), false),
	    arrow::field("execution_id", arrow::binary(), false),
	    arrow::field("attach_opaque_data", arrow::binary(), true),
	});

	arrow::StringBuilder fn_b;
	arrow::BinaryBuilder eid_b, aid_b;

	ThrowOnArrowError(fn_b.Append(function_name));
	ThrowOnArrowError(eid_b.Append(execution_id.data(), execution_id.size()));
	if (attach_opaque_data.empty()) {
		ThrowOnArrowError(aid_b.AppendNull());
	} else {
		ThrowOnArrowError(aid_b.Append(attach_opaque_data.data(), attach_opaque_data.size()));
	}

	std::shared_ptr<arrow::Array> fn_a, eid_a, aid_a;
	ThrowOnArrowError(fn_b.Finish(&fn_a));
	ThrowOnArrowError(eid_b.Finish(&eid_a));
	ThrowOnArrowError(aid_b.Finish(&aid_a));

	return WrapAsRpcParams(arrow::RecordBatch::Make(schema, 1, {fn_a, eid_a, aid_a}));
}

// Standard envelope unwrap: outer {result: binary} wrapping the
// method-specific response RecordBatch.
std::shared_ptr<arrow::RecordBatch> UnwrapResponse(
    const std::shared_ptr<arrow::RecordBatch> &response_batch,
    const std::string &method) {
	if (!response_batch || response_batch->num_rows() == 0) {
		throw IOException("VGI %s returned empty response", method);
	}
	auto col = response_batch->GetColumnByName("result");
	if (!col) {
		throw IOException("VGI %s response missing 'result' column", method);
	}
	auto bin = std::dynamic_pointer_cast<arrow::BinaryArray>(col);
	if (!bin || bin->IsNull(0)) {
		throw IOException("VGI %s response has null result", method);
	}
	auto view = bin->GetView(0);
	return DeserializeFromIpcBytes(reinterpret_cast<const uint8_t *>(view.data()), view.size());
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
	    bind_data.function_name, arguments_bytes, input_schema, partition_key_count,
	    order_key_count, bind_data.resolved_output_schema, bind_data.attach_opaque_data);

	auto rpc_result = InvokeAggregateRpc(context, bind_data, "aggregate_streaming_open", request);
	auto inner = UnwrapResponse(rpc_result.response_batch, "aggregate_streaming_open");

	auto eid_col = inner->GetColumnByName("execution_id");
	if (!eid_col) {
		throw IOException("VGI aggregate_streaming_open response missing 'execution_id'");
	}
	auto eid_bin = std::dynamic_pointer_cast<arrow::BinaryArray>(eid_col);
	if (!eid_bin || eid_bin->IsNull(0)) {
		throw IOException("VGI aggregate_streaming_open returned null execution_id");
	}
	auto eid_view = eid_bin->GetView(0);

	VgiStreamingSession session;
	session.execution_id.assign(eid_view.data(), eid_view.data() + eid_view.size());
	session.function_name = bind_data.function_name;
	session.attach_opaque_data = bind_data.attach_opaque_data;
	return session;
}

std::shared_ptr<arrow::RecordBatch> VgiAggregateStreamingChunk(
    ClientContext &context,
    const VgiAggregateBindData &bind_data,
    const VgiStreamingSession &session,
    const std::shared_ptr<arrow::RecordBatch> &input_batch) {

	auto request = BuildStreamingChunkRequest(
	    session.function_name, session.execution_id, input_batch, session.attach_opaque_data);

	auto rpc_result = InvokeAggregateRpc(context, bind_data, "aggregate_streaming_chunk", request);
	auto inner = UnwrapResponse(rpc_result.response_batch, "aggregate_streaming_chunk");

	auto rb_col = inner->GetColumnByName("result_batch");
	if (!rb_col) {
		throw IOException("VGI aggregate_streaming_chunk response missing 'result_batch'");
	}
	auto rb_bin = std::dynamic_pointer_cast<arrow::BinaryArray>(rb_col);
	if (!rb_bin || rb_bin->IsNull(0)) {
		throw IOException("VGI aggregate_streaming_chunk response has null result_batch");
	}
	auto rb_view = rb_bin->GetView(0);
	auto result_batch = DeserializeFromIpcBytes(
	    reinterpret_cast<const uint8_t *>(rb_view.data()), rb_view.size());

	if (!result_batch) {
		throw IOException(
		    "VGI aggregate_streaming_chunk returned no result batch for %lld input rows",
		    static_cast<long long>(input_batch->num_rows()));
	}
	if (result_batch->num_rows() != input_batch->num_rows()) {
		throw IOException(
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
	    session.function_name, session.execution_id, session.attach_opaque_data);

	InvokeAggregateRpc(context, bind_data, "aggregate_streaming_close", request, enable_logging);
}

} // namespace vgi
} // namespace duckdb
