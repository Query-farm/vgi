#include "vgi_http_function_connection.hpp"

#include "duckdb.hpp"

#include "vgi_bind_protocol.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_exception.hpp"
#include "vgi_http_client.hpp"
#include "vgi_logging.hpp"
#include "vgi_rpc_client.hpp"
#include "vgi_rpc_types.hpp"

namespace duckdb {
namespace vgi {

// Helper: extract stream_state token value from batch metadata, or return empty string.
static std::string ExtractStreamStateValue(const std::shared_ptr<arrow::KeyValueMetadata> &metadata) {
	if (!metadata) {
		return "";
	}
	int idx = metadata->FindKey(RPC_STREAM_STATE_KEY);
	if (idx >= 0) {
		return metadata->value(idx);
	}
	return "";
}

// ============================================================================
// Constructor
// ============================================================================

HttpFunctionConnection::HttpFunctionConnection(
    const std::string &worker_path, const std::string &function_name,
    const ArrowArguments &arguments, const std::vector<uint8_t> &attach_id,
    const std::vector<uint8_t> &transaction_id,
    ClientContext &context, const std::string &function_type,
    const std::vector<uint8_t> &global_execution_id,
    bool worker_debug, const std::map<std::string, Value> &settings,
    const std::vector<VgiSecretRequirement> &required_secrets)
    : base_url_(worker_path), function_name_(function_name), function_type_(function_type),
      arguments_type_(arguments.type), arguments_array_(arguments.array), attach_id_(attach_id),
      transaction_id_(transaction_id), global_execution_id_(global_execution_id), context_(context),
      worker_debug_(worker_debug), settings_(settings), required_secrets_(required_secrets) {
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

	// Build metadata with stream_state and optional dynamic filters
	std::vector<std::string> meta_keys;
	std::vector<std::string> meta_values;
	if (!stream_state_token_.empty()) {
		meta_keys.push_back(RPC_STREAM_STATE_KEY);
		meta_values.push_back(stream_state_token_);
	}
	if (tick_filter_state_) {
		lock_guard<mutex> l(tick_filter_state_->lock);
		if (tick_filter_state_->has_filters) {
			meta_keys.push_back("vgi_pushdown_filters");
			meta_values.push_back(tick_filter_state_->encoded_filters);
		}
	}
	std::shared_ptr<arrow::KeyValueMetadata> metadata;
	if (!meta_keys.empty()) {
		metadata = arrow::KeyValueMetadata::Make(meta_keys, meta_values);
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
			HandleBatchLogMessage(bwm.batch, bwm.custom_metadata, &context_, base_url_,
			                     -1, GetExecutionIdHex(), GetAttachIdHex());
			throw IOException("VGI HTTP error from server [url: %s]", base_url_);
		}
		if (batch_type == RpcBatchType::LOG) {
			HandleBatchLogMessage(bwm.batch, bwm.custom_metadata, &context_, base_url_,
			                     -1, GetExecutionIdHex(), GetAttachIdHex());
			continue;
		}

		// Resolve external location pointer batches.
		// Stream_state is inside the resolved data, not on the pointer batch itself.
		if (batch_type == RpcBatchType::EXTERNAL_LOCATION) {
			auto location_url = bwm.custom_metadata->value(bwm.custom_metadata->FindKey(RPC_LOCATION_KEY));
			auto resolved = ResolveExternalLocation(context_, location_url,
			                                         base_url_, GetExecutionIdHex(), GetAttachIdHex(),
			                                         bwm.custom_metadata);
			buffered_batches_.push_back(resolved.batch);
			ExtractStreamState(resolved.batch, resolved.metadata);
			continue;
		}

		// Extract stream_state from regular data batches
		ExtractStreamState(bwm.batch, bwm.custom_metadata);
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

void HttpFunctionConnection::UpdateInputSchemaForExecution(const std::shared_ptr<arrow::Schema> &input_schema) {
	if (input_writer_opened_) {
		throw IOException("HttpFunctionConnection::UpdateInputSchemaForExecution called after OpenInputWriter [url: %s]",
		                   base_url_);
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

	// Transport: send bind via HTTP
	auto transport_fn = [&](const std::vector<uint8_t> &request_bytes) -> std::shared_ptr<arrow::RecordBatch> {
		auto rpc_params = BuildBindRpcParams(request_bytes);
		auto resp = HttpInvokeUnary(context_, base_url_, "bind", rpc_params);
		if (!resp.batch || resp.batch->num_rows() == 0) {
			throw IOException("Empty bind response from HTTP server [url: %s]", base_url_);
		}
		auto rcol = resp.batch->GetColumnByName("result");
		if (!rcol) {
			throw IOException("Bind response missing 'result' column [url: %s]", base_url_);
		}
		auto bin_arr = std::dynamic_pointer_cast<arrow::BinaryArray>(rcol);
		if (!bin_arr || bin_arr->IsNull(0)) {
			throw IOException("Bind response 'result' column is null [url: %s]", base_url_);
		}
		auto v = bin_arr->GetView(0);
		return DeserializeFromIpcBytes(reinterpret_cast<const uint8_t *>(v.data()), v.size());
	};

	bind_result_ = PerformBindProtocol(context_, function_name_, function_type_,
	                                    arguments_array_, input_schema_, attach_id_,
	                                    transaction_id_, settings_, required_secrets_,
	                                    base_url_, transport_fn);
	bind_done_ = true;

	VGI_LOG(context_, "http_function_connection.bind_result",
	        {{"url", base_url_},
	         {"function_name", function_name_},
	         {"num_output_columns", std::to_string(bind_result_.output_schema->num_fields())}});

	return bind_result_;
}

// ============================================================================
// Phase 2: Init
// ============================================================================

InitResult HttpFunctionConnection::PerformInit(const std::vector<int32_t> &projection_ids,
                                                std::shared_ptr<arrow::Buffer> pushdown_filters,
                                                std::vector<std::shared_ptr<arrow::Buffer>> join_keys,
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

	auto &execution_id = global_execution_id_;

	// Build InitRequest — pass arrow::Buffer directly to avoid copying
	auto init_request = BuildInitRequest(
	    bind_result_.bind_request_bytes,
	    bind_result_.output_schema_bytes,
	    bind_result_.opaque_data,
	    projection_ids_64,
	    pushdown_filters,
	    join_keys,
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

	// Resolve external location pointer batch in stream header if needed
	if (header_result.header.header_batch && header_result.header.metadata) {
		int loc_idx = header_result.header.metadata->FindKey(RPC_LOCATION_KEY);
		if (loc_idx >= 0) {
			auto location_url = header_result.header.metadata->value(loc_idx);
			auto resolved = ResolveExternalLocation(context_, location_url, base_url_,
			                                         "", "", header_result.header.metadata);
			header_result.header.header_batch = resolved.batch;
			header_result.header.metadata = resolved.metadata;
		}
	}

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

	PerformInit({}, nullptr, {}, "FINALIZE");
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

	// Check if batch exceeds max_request_bytes and upload URL support is available
	if (!capabilities_.discovered) {
		capabilities_ = HttpDiscoverCapabilities(context_, base_url_);
	}
	if (capabilities_.max_request_bytes > 0 &&
	    static_cast<int64_t>(body.size()) > capabilities_.max_request_bytes &&
	    capabilities_.upload_url_support) {
		// Upload via server-vended URL, send pointer batch instead
		auto urls = HttpRequestUploadUrls(context_, base_url_, 1);
		if (!urls.empty()) {
			HttpPutBytes(context_, urls[0].upload_url, body, false);
			body = SerializePointerBatch(input_schema_, urls[0].download_url, stream_state_token_);
		}
	}

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
			HandleBatchLogMessage(bwm.batch, bwm.custom_metadata, &context_, base_url_,
			                     -1, GetExecutionIdHex(), GetAttachIdHex());
			throw IOException("VGI HTTP error from server [url: %s]", base_url_);
		}
		if (batch_type == RpcBatchType::LOG) {
			HandleBatchLogMessage(bwm.batch, bwm.custom_metadata, &context_, base_url_,
			                     -1, GetExecutionIdHex(), GetAttachIdHex());
			continue;
		}

		// Resolve external location pointer batches.
		// Stream_state is inside the resolved data, not on the pointer batch.
		if (batch_type == RpcBatchType::EXTERNAL_LOCATION) {
			auto location_url = bwm.custom_metadata->value(bwm.custom_metadata->FindKey(RPC_LOCATION_KEY));
			auto resolved = ResolveExternalLocation(context_, location_url,
			                                         base_url_, GetExecutionIdHex(), GetAttachIdHex(),
			                                         bwm.custom_metadata);
			if (!output_batch) {
				output_batch = resolved.batch;
			}
			new_state_token = ExtractStreamStateValue(resolved.metadata);
			continue;
		}

		// Data batch — extract stream_state
		new_state_token = ExtractStreamStateValue(bwm.custom_metadata);
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
