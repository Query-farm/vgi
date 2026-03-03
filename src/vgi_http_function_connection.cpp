#include "vgi_http_function_connection.hpp"

#include "duckdb.hpp"

#include "vgi_exception.hpp"
#include "vgi_http_client.hpp"
#include "vgi_logging.hpp"
#include "vgi_rpc_client.hpp"
#include "vgi_rpc_types.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// Constructor
// ============================================================================

HttpFunctionConnection::HttpFunctionConnection(
    const std::string &worker_path, const std::string &function_name,
    const ArrowArguments &arguments, const std::vector<uint8_t> &attach_id,
    ClientContext &context, const std::string &function_type,
    const std::vector<uint8_t> &global_execution_id,
    bool worker_debug, const std::map<std::string, std::string> &settings)
    : base_url_(worker_path), function_name_(function_name), function_type_(function_type),
      arguments_type_(arguments.type), arguments_array_(arguments.array), attach_id_(attach_id),
      global_execution_id_(global_execution_id), context_(context), worker_debug_(worker_debug), settings_(settings) {
	// Strip trailing slash from base URL
	if (!base_url_.empty() && base_url_.back() == '/') {
		base_url_.pop_back();
	}
}

// ============================================================================
// Identity helpers
// ============================================================================

std::string HttpFunctionConnection::GetExecutionIdHex() const {
	if (execution_id_.empty()) {
		return "";
	}
	return BytesToHex(execution_id_);
}

std::string HttpFunctionConnection::GetAttachIdHex() const {
	if (attach_id_.empty()) {
		return "";
	}
	return BytesToHex(attach_id_);
}

// ============================================================================
// Helpers
// ============================================================================

std::vector<uint8_t> HttpFunctionConnection::SerializeBatchWithState(
    const std::shared_ptr<arrow::RecordBatch> &batch,
    const std::shared_ptr<arrow::Schema> &schema) {
	auto sink_result = arrow::io::BufferOutputStream::Create();
	if (!sink_result.ok()) {
		throw IOException("Failed to create buffer: %s", sink_result.status().ToString());
	}
	auto sink = sink_result.ValueUnsafe();

	auto writer_result = arrow::ipc::MakeStreamWriter(sink, schema);
	if (!writer_result.ok()) {
		throw IOException("Failed to create IPC writer: %s", writer_result.status().ToString());
	}
	auto writer = writer_result.ValueUnsafe();

	// Build metadata with stream_state if we have one
	std::shared_ptr<arrow::KeyValueMetadata> metadata;
	if (!stream_state_token_.empty()) {
		metadata = arrow::KeyValueMetadata::Make(
		    {RPC_STREAM_STATE_KEY}, {stream_state_token_});
	}

	auto status = writer->WriteRecordBatch(*batch, metadata);
	if (!status.ok()) {
		throw IOException("Failed to write batch: %s", status.ToString());
	}

	status = writer->Close();
	if (!status.ok()) {
		throw IOException("Failed to close IPC writer: %s", status.ToString());
	}

	auto finish_result = sink->Finish();
	if (!finish_result.ok()) {
		throw IOException("Failed to finish buffer: %s", finish_result.status().ToString());
	}
	auto buffer = finish_result.ValueUnsafe();
	return std::vector<uint8_t>(buffer->data(), buffer->data() + buffer->size());
}

std::shared_ptr<arrow::RecordBatch> HttpFunctionConnection::ExtractStreamState(
    const std::shared_ptr<arrow::RecordBatch> &batch,
    const std::shared_ptr<arrow::KeyValueMetadata> &metadata) {
	if (!metadata) {
		return batch;
	}

	int idx = metadata->FindKey(RPC_STREAM_STATE_KEY);
	if (idx >= 0) {
		stream_state_token_ = metadata->value(idx);
	}

	return batch;
}

void HttpFunctionConnection::BufferDataBatches(const std::string &response_body, size_t offset) {
	buffered_batches_.clear();
	buffered_batch_index_ = 0;
	stream_state_token_.clear();

	if (offset >= response_body.size()) {
		return;
	}

	// Copy into an owning buffer — Arrow IPC zero-copy reads reference this memory
	auto substr = response_body.substr(offset);
	auto buffer = arrow::Buffer::FromString(std::move(substr));
	auto input = std::make_shared<arrow::io::BufferReader>(buffer);
	auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(input);
	if (!reader_result.ok()) {
		// Empty data stream is valid (no output yet)
		return;
	}
	auto reader = reader_result.ValueUnsafe();

	while (true) {
		auto read_result = reader->ReadNext();
		if (!read_result.ok() || !read_result.ValueUnsafe().batch) {
			break;
		}
		auto &bwm = read_result.ValueUnsafe();

		// Check for log/error batches
		auto batch_type = ClassifyBatch(bwm.batch, bwm.custom_metadata);
		if (batch_type == RpcBatchType::ERROR) {
			HandleBatchLogMessage(bwm.batch, bwm.custom_metadata, &context_, base_url_);
			throw IOException("VGI HTTP error from server [url: %s]", base_url_);
		}
		if (batch_type == RpcBatchType::LOG) {
			HandleBatchLogMessage(bwm.batch, bwm.custom_metadata, &context_, base_url_);
			continue;
		}

		// Check for stream_state in the last batch
		if (bwm.custom_metadata) {
			int state_idx = bwm.custom_metadata->FindKey(RPC_STREAM_STATE_KEY);
			if (state_idx >= 0) {
				stream_state_token_ = bwm.custom_metadata->value(state_idx);
			}
		}

		buffered_batches_.push_back(bwm.batch);
	}
}

// ============================================================================
// Phase 1: Bind
// ============================================================================

void HttpFunctionConnection::SetInputSchema(const std::shared_ptr<arrow::Schema> &input_schema) {
	if (bind_done_) {
		throw IOException("HttpFunctionConnection::SetInputSchema called after bind [url: %s]", base_url_);
	}
	input_schema_ = input_schema;
}

BindResult HttpFunctionConnection::PerformBindFull() {
	if (bind_done_) {
		return bind_result_;
	}

	VGI_LOG(context_, "http_function_connection.bind",
	        {{"url", base_url_},
	         {"function_name", function_name_},
	         {"function_type", function_type_}});

	// Build BindRequest (same logic as FunctionConnection::PerformBindFull)
	std::vector<uint8_t> arguments_bytes;
	if (arguments_array_) {
		auto args_schema = arrow::schema({arrow::field("args", arguments_array_->type())});
		auto args_batch = arrow::RecordBatch::Make(args_schema, arguments_array_->length(), {arguments_array_});
		arguments_bytes = SerializeToIpcBytes(args_batch);
	} else {
		auto empty_struct_type = arrow::struct_({});
		auto empty_struct_result = arrow::MakeEmptyArray(empty_struct_type);
		if (!empty_struct_result.ok()) {
			throw IOException("Failed to create empty struct array: %s [url: %s]",
			                  empty_struct_result.status().ToString(), base_url_);
		}
		auto args_schema = arrow::schema({arrow::field("args", empty_struct_type)});
		auto args_batch = arrow::RecordBatch::Make(args_schema, 0, {empty_struct_result.ValueUnsafe()});
		arguments_bytes = SerializeToIpcBytes(args_batch);
	}

	std::vector<uint8_t> settings_bytes;
	if (!settings_.empty()) {
		std::vector<std::shared_ptr<arrow::Field>> fields;
		std::vector<std::shared_ptr<arrow::Array>> arrays;
		for (const auto &[key, value] : settings_) {
			fields.push_back(arrow::field(key, arrow::utf8()));
			arrow::StringBuilder builder;
			auto status = builder.Append(value);
			if (!status.ok()) {
				throw IOException("Failed to build settings: %s [url: %s]", status.ToString(), base_url_);
			}
			auto result = builder.Finish();
			if (!result.ok()) {
				throw IOException("Failed to finish settings: %s [url: %s]", result.status().ToString(), base_url_);
			}
			arrays.push_back(result.ValueUnsafe());
		}
		auto settings_schema = arrow::schema(fields);
		auto settings_batch = arrow::RecordBatch::Make(settings_schema, 1, arrays);
		settings_bytes = SerializeToIpcBytes(settings_batch);
	}

	std::vector<uint8_t> input_schema_bytes;
	if (input_schema_) {
		input_schema_bytes = SerializeSchemaToIpcBytes(input_schema_);
	}

	auto bind_request = BuildBindRequest(function_name_, arguments_bytes, function_type_,
	                                     input_schema_bytes, settings_bytes, attach_id_);
	auto bind_request_bytes = SerializeToIpcBytes(bind_request);
	auto params = BuildBindRpcParams(bind_request_bytes);

	// Send bind RPC via HTTP
	auto response = HttpInvokeUnary(context_, base_url_, "bind", params);

	// Parse BindResponse
	if (!response.batch || response.batch->num_rows() == 0) {
		throw IOException("Empty bind response from HTTP server [url: %s]", base_url_);
	}

	auto result_col = response.batch->GetColumnByName("result");
	if (!result_col) {
		throw IOException("Bind response missing 'result' column [url: %s]", base_url_);
	}

	auto binary_array = std::dynamic_pointer_cast<arrow::BinaryArray>(result_col);
	if (!binary_array || binary_array->IsNull(0)) {
		throw IOException("Bind response 'result' column is null [url: %s]", base_url_);
	}

	auto view = binary_array->GetView(0);
	auto bind_response_batch = DeserializeFromIpcBytes(
	    reinterpret_cast<const uint8_t *>(view.data()), view.size());
	auto bind_response = ParseBindResponse(bind_response_batch, base_url_);

	auto output_schema_bytes = SerializeSchemaToIpcBytes(bind_response.output_schema);

	bind_result_ = BindResult {
	    bind_response.output_schema,
	    bind_response.opaque_data,
	    bind_request_bytes,
	    output_schema_bytes
	};
	bind_done_ = true;

	VGI_LOG(context_, "http_function_connection.bind_result",
	        {{"url", base_url_},
	         {"function_name", function_name_},
	         {"num_output_columns", std::to_string(bind_response.output_schema->num_fields())}});

	return bind_result_;
}

// ============================================================================
// Phase 2: Init
// ============================================================================

InitResult HttpFunctionConnection::PerformInit(const std::vector<int32_t> &projection_ids,
                                                std::shared_ptr<arrow::Buffer> pushdown_filters,
                                                const std::string &phase) {
	if (!bind_done_) {
		throw IOException("HttpFunctionConnection::PerformInit called before PerformBind [url: %s]", base_url_);
	}
	if (init_done_) {
		throw IOException("HttpFunctionConnection::PerformInit called twice [url: %s]", base_url_);
	}

	// Convert projection_ids to int64_t
	std::vector<int64_t> projection_ids_64;
	projection_ids_64.reserve(projection_ids.size());
	for (auto id : projection_ids) {
		projection_ids_64.push_back(static_cast<int64_t>(id));
	}

	std::vector<uint8_t> pushdown_filters_bytes;
	if (pushdown_filters) {
		pushdown_filters_bytes.assign(pushdown_filters->data(),
		                              pushdown_filters->data() + pushdown_filters->size());
	}

	auto &execution_id = global_execution_id_;

	auto init_request = BuildInitRequest(
	    bind_result_.bind_request_bytes,
	    bind_result_.output_schema_bytes,
	    bind_result_.opaque_data,
	    projection_ids_64,
	    pushdown_filters_bytes,
	    phase,
	    execution_id);
	auto init_request_bytes = SerializeToIpcBytes(init_request);
	auto rpc_params = BuildInitRpcParams(init_request_bytes);

	// Serialize the init RPC request to Arrow IPC
	auto body = SerializeRpcRequest("init", rpc_params);

	// POST to {base_url}/init/init
	std::string init_url = base_url_ + "/init/init";

	VGI_LOG(context_, "http_function_connection.init",
	        {{"url", init_url},
	         {"function_name", function_name_},
	         {"phase", phase.empty() ? "default" : phase}});

	auto response_body = HttpPostArrowIpc(context_, init_url, body);

	// Parse response: header IPC stream + data IPC stream
	auto header_result = ReadStreamHeaderFromBuffer(
	    reinterpret_cast<const uint8_t *>(response_body.data()),
	    response_body.size(), &context_, init_url);

	auto init_response = ParseGlobalInitResponse(header_result.header.header_batch, base_url_);
	execution_id_ = init_response.execution_id;

	// Determine mode based on input_schema presence
	if (!input_schema_) {
		is_producer_mode_ = true;
	} else {
		is_producer_mode_ = false;
	}

	// Buffer data batches from the init response
	BufferDataBatches(response_body, header_result.data_offset);

	init_done_ = true;

	VGI_LOG(context_, "http_function_connection.init_result",
	        {{"url", base_url_},
	         {"function_name", function_name_},
	         {"execution_id", BytesToHex(execution_id_)},
	         {"max_workers", std::to_string(init_response.max_workers)},
	         {"is_producer_mode", is_producer_mode_ ? "true" : "false"},
	         {"buffered_batches", std::to_string(buffered_batches_.size())},
	         {"has_state_token", stream_state_token_.empty() ? "false" : "true"}});

	return InitResult {init_response.execution_id, init_response.max_workers, init_response.opaque_data};
}

void HttpFunctionConnection::PerformFinalizeInit() {
	if (!init_done_) {
		throw IOException("HttpFunctionConnection::PerformFinalizeInit called before PerformInit [url: %s]", base_url_);
	}

	VGI_LOG(context_, "http_function_connection.finalize_init",
	        {{"url", base_url_},
	         {"function_name", function_name_},
	         {"execution_id", GetExecutionIdHex()}});

	// Reset state for the FINALIZE phase
	init_done_ = false;
	data_finished_ = false;
	buffered_batches_.clear();
	buffered_batch_index_ = 0;
	stream_state_token_.clear();
	pending_input_.reset();
	input_writer_opened_ = false;
	input_writer_closed_ = false;

	// Clear input_schema_ so PerformInit enters producer mode
	auto saved_input_schema = input_schema_;
	auto saved_global_exec_id = global_execution_id_;
	input_schema_.reset();
	global_execution_id_ = execution_id_;

	struct RestoreGuard {
		std::shared_ptr<arrow::Schema> &input_schema;
		std::vector<uint8_t> &global_exec_id;
		std::shared_ptr<arrow::Schema> saved_schema;
		std::vector<uint8_t> saved_id;
		~RestoreGuard() {
			input_schema = std::move(saved_schema);
			global_exec_id = std::move(saved_id);
		}
	} guard{input_schema_, global_execution_id_, std::move(saved_input_schema), std::move(saved_global_exec_id)};

	PerformInit({}, nullptr, "FINALIZE");
}

// ============================================================================
// Phase 3: Data exchange
// ============================================================================

void HttpFunctionConnection::OpenInputWriter() {
	// No-op for HTTP — exchange doesn't need pre-opened writers
	input_writer_opened_ = true;
}

void HttpFunctionConnection::WriteInputBatch(const std::shared_ptr<arrow::RecordBatch> &batch) {
	if (!input_writer_opened_) {
		throw IOException("HttpFunctionConnection::WriteInputBatch called before OpenInputWriter [url: %s]", base_url_);
	}
	if (input_writer_closed_) {
		throw IOException("HttpFunctionConnection::WriteInputBatch called after CloseInputWriter [url: %s]", base_url_);
	}
	pending_input_ = batch;
}

void HttpFunctionConnection::CloseInputWriter() {
	input_writer_closed_ = true;
}

std::shared_ptr<arrow::RecordBatch> HttpFunctionConnection::ReadDataBatch() {
	if (!init_done_) {
		throw IOException("HttpFunctionConnection::ReadDataBatch called before PerformInit [url: %s]", base_url_);
	}
	if (data_finished_) {
		return nullptr;
	}

	// ========================================================================
	// Producer mode (table functions): consume buffered batches, then continue
	// ========================================================================
	if (is_producer_mode_) {
		// Return next buffered batch if available
		if (buffered_batch_index_ < buffered_batches_.size()) {
			return buffered_batches_[buffered_batch_index_++];
		}

		// No more buffered batches and no state token → EOS
		if (stream_state_token_.empty()) {
			data_finished_ = true;
			return nullptr;
		}

		// Continuation: POST exchange with state token to get more data
		auto tick_schema = arrow::schema({});
		auto tick_batch = arrow::RecordBatch::Make(tick_schema, 0, std::vector<std::shared_ptr<arrow::Array>>{});
		auto body = SerializeBatchWithState(tick_batch, tick_schema);

		std::string exchange_url = base_url_ + "/init/exchange";

		VGI_LOG(context_, "http_function_connection.producer_exchange",
		        {{"url", exchange_url}, {"function_name", function_name_}});

		auto response_body = HttpPostArrowIpc(context_, exchange_url, body);

		// Parse response — buffer new data batches
		BufferDataBatches(response_body);

		if (buffered_batch_index_ < buffered_batches_.size()) {
			return buffered_batches_[buffered_batch_index_++];
		}

		// Empty response with no state token → EOS
		if (stream_state_token_.empty()) {
			data_finished_ = true;
		}
		return nullptr;
	}

	// ========================================================================
	// Exchange mode (scalar/table-in-out): send input, get output
	// ========================================================================

	// Take the pending input batch
	auto input_batch = std::move(pending_input_);
	if (!input_batch) {
		// No input available — if writer is closed, we're done
		if (input_writer_closed_) {
			data_finished_ = true;
			return nullptr;
		}
		throw IOException("HttpFunctionConnection::ReadDataBatch called with no pending input [url: %s]", base_url_);
	}

	// Serialize the input batch with stream_state token
	auto body = SerializeBatchWithState(input_batch, input_schema_);

	std::string exchange_url = base_url_ + "/init/exchange";

	auto response_body = HttpPostArrowIpc(context_, exchange_url, body);

	// Parse response — copy into owning buffer since Arrow IPC reads zero-copy reference it
	auto buffer = arrow::Buffer::FromString(std::move(response_body));
	auto input_stream = std::make_shared<arrow::io::BufferReader>(buffer);
	auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(input_stream);
	if (!reader_result.ok()) {
		// Empty response in exchange mode means no output for this input
		return arrow::RecordBatch::Make(
		    bind_result_.output_schema, 0, std::vector<std::shared_ptr<arrow::Array>>{});
	}
	auto reader = reader_result.ValueUnsafe();

	std::shared_ptr<arrow::RecordBatch> output_batch;
	std::string new_state_token;

	while (true) {
		auto read_result = reader->ReadNext();
		if (!read_result.ok() || !read_result.ValueUnsafe().batch) {
			break;
		}
		auto &bwm = read_result.ValueUnsafe();

		// Dispatch log/error batches
		auto batch_type = ClassifyBatch(bwm.batch, bwm.custom_metadata);
		if (batch_type == RpcBatchType::ERROR) {
			HandleBatchLogMessage(bwm.batch, bwm.custom_metadata, &context_, base_url_);
			throw IOException("VGI HTTP error from server [url: %s]", base_url_);
		}
		if (batch_type == RpcBatchType::LOG) {
			HandleBatchLogMessage(bwm.batch, bwm.custom_metadata, &context_, base_url_);
			continue;
		}

		// Check for stream_state
		if (bwm.custom_metadata) {
			int state_idx = bwm.custom_metadata->FindKey(RPC_STREAM_STATE_KEY);
			if (state_idx >= 0) {
				new_state_token = bwm.custom_metadata->value(state_idx);
			}
		}

		if (!output_batch) {
			output_batch = bwm.batch;
		}
	}

	// Update state token
	if (!new_state_token.empty()) {
		stream_state_token_ = new_state_token;
	} else {
		// No new state token means server is done
		stream_state_token_.clear();
		data_finished_ = true;
	}

	return output_batch;
}

} // namespace vgi
} // namespace duckdb
