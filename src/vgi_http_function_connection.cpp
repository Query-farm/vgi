#include "vgi_http_function_connection.hpp"

#include "duckdb.hpp"

#include "vgi_arrow_utils.hpp"
#include "vgi_bind_protocol.hpp"
#include "vgi_catalog_api.hpp"
#include "generated/vgi_protocol_constants.hpp"
#include "vgi_exception.hpp"
#include "vgi_http_client.hpp"
#include "vgi_logging.hpp"
#include "vgi_rpc_client.hpp"
#include "vgi_rpc_types.hpp"
#include "generated/vgi_request_builders.hpp"

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
    const std::vector<VgiSecretRequirement> &required_secrets,
    const std::shared_ptr<VgiAttachParameters> &attach_params)
    : conn_id_hex_(VgiGenerateConnId()), base_url_(worker_path),
      function_name_(function_name), function_type_(function_type),
      arguments_type_(arguments.type), arguments_array_(arguments.array), attach_id_(attach_id),
      transaction_id_(transaction_id), global_execution_id_(global_execution_id), context_(context),
      worker_debug_(worker_debug), settings_(settings), required_secrets_(required_secrets),
      attach_params_(attach_params) {
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

std::string HttpFunctionConnection::GetTransactionIdHex() const {
	if (transaction_id_.empty()) {
		return "";
	}
	return BytesToHex(transaction_id_);
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
		// Arrow's IPC writer rejects any batch whose schema differs from the
		// writer's schema (down to per-field nullable flags and metadata).
		// The bare status string ("Tried to write record batch with
		// different schema") is useless for debugging. Diff both schemas
		// inline so an operator can see exactly which field changed.
		std::string writer_schema_str = schema ? schema->ToString(/*show_metadata=*/true) : "<null>";
		std::string batch_schema_str = batch && batch->schema()
		    ? batch->schema()->ToString(/*show_metadata=*/true) : "<null>";
		throw IOException(
		    "Failed to write batch: %s\n"
		    "  writer schema (advertised at bind):\n%s\n"
		    "  batch schema (received from caller):\n%s",
		    status.ToString(), writer_schema_str, batch_schema_str);
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

	// Per-response counters surfaced via VGI_LOG below.  Useful to confirm
	// the server's batch-bundling (``max_stream_response_bytes``) is taking
	// effect end-to-end on the C++ client.
	int64_t spike_data_batches = 0;
	int64_t spike_log_batches = 0;
	int64_t spike_external_batches = 0;
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
			                     -1, GetExecutionIdHex(), GetAttachIdHex(), "", GetConnIdHex());
			throw IOException("VGI HTTP error from server [url: %s]", base_url_);
		}
		if (batch_type == RpcBatchType::LOG) {
			HandleBatchLogMessage(bwm.batch, bwm.custom_metadata, &context_, base_url_,
			                     -1, GetExecutionIdHex(), GetAttachIdHex(), "", GetConnIdHex());
			++spike_log_batches;
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
			++spike_external_batches;
			continue;
		}

		// Extract stream_state from regular data batches
		ExtractStreamState(bwm.batch, bwm.custom_metadata);
		buffered_batches_.push_back(bwm.batch);
		++spike_data_batches;
	}

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("data_batches", std::to_string(spike_data_batches));
		fields.emplace_back("log_batches", std::to_string(spike_log_batches));
		fields.emplace_back("external_batches", std::to_string(spike_external_batches));
		fields.emplace_back("response_bytes", std::to_string(response_body.size() - offset));
		VGI_LOG(context_, "http_function_connection.buffer_data_batches", fields);
	}
}

// ============================================================================
// Phase 1: Bind
// ============================================================================

void HttpFunctionConnection::SetInputSchema(const std::shared_ptr<arrow::Schema> &input_schema) {
	// HTTP has no "worker spawned" lifecycle marker (every RPC is its own
	// POST), so we don't gate this. The schema is captured into the bind
	// RPC payload at the time PerformBindRpc runs; calling SetInputSchema
	// after that would silently desync caller and server, but it's a
	// programmer error, not protected against here.
	input_schema_ = input_schema;
}

void HttpFunctionConnection::UpdateInputSchemaForExecution(const std::shared_ptr<arrow::Schema> &input_schema) {
	if (input_writer_opened_) {
		throw IOException("HttpFunctionConnection::UpdateInputSchemaForExecution called after OpenInputWriter [url: %s]",
		                   base_url_);
	}
	input_schema_ = input_schema;
}

BindResult HttpFunctionConnection::PerformBindRpc() {
	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("url", base_url_);
		fields.emplace_back("function_name", function_name_);
		fields.emplace_back("function_type", function_type_);
		VGI_LOG(context_, "http_function_connection.bind", fields);
	}

	// Transport: send bind via HTTP
	auto transport_fn = [&](const std::vector<uint8_t> &request_bytes) -> std::shared_ptr<arrow::RecordBatch> {
		auto rpc_params = ::duckdb::vgi::generated::BuildBindParams(request_bytes);
		auto auth = attach_params_ ? attach_params_->auth() : nullptr;
		auto cached_params = attach_params_
		    ? attach_params_->GetOrInitHttpParams(context_, base_url_) : nullptr;
		auto resp = HttpInvokeUnary(context_, base_url_, "bind", rpc_params, auth,
		                             /*cookie_jar=*/nullptr, cached_params);
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

	auto bind_result = PerformBindProtocol(context_, function_name_, function_type_,
	                                        arguments_array_, input_schema_, attach_id_,
	                                        transaction_id_, settings_, required_secrets_,
	                                        base_url_, transport_fn);

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		fields.emplace_back("num_output_columns", std::to_string(bind_result.output_schema->num_fields()));
		VGI_LOG(context_, "http_function_connection.bind_result", fields);
	}

	return bind_result;
}

// ============================================================================
// Phase 2: Init
// ============================================================================

InitResult HttpFunctionConnection::PerformInit(const BindResult &bind_result,
                                                const std::vector<int32_t> &projection_ids,
                                                std::shared_ptr<arrow::Buffer> pushdown_filters,
                                                std::vector<std::shared_ptr<arrow::Buffer>> join_keys,
                                                const std::string &phase,
                                                const std::optional<OrderByHint> &order_by,
                                                const std::optional<TableSampleHint> &table_sample) {
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

	// Extract order hint fields (empty strings / -1 when no hint)
	std::string ob_col, ob_dir, ob_null;
	int64_t ob_limit = -1;
	if (order_by.has_value()) {
		ob_col = order_by->column_name;
		ob_dir = order_by->direction;
		ob_null = order_by->null_order;
		ob_limit = order_by->row_limit;
	}

	// Extract table sample hint fields (-1.0 / -1 when no hint)
	double ts_percentage = -1.0;
	int64_t ts_seed = -1;
	if (table_sample.has_value()) {
		ts_percentage = table_sample->sample_percentage;
		ts_seed = table_sample->seed;
	}

	// Build InitRequest — pass arrow::Buffer directly to avoid copying
	auto init_request = BuildInitRequest(
	    bind_result.bind_request_bytes,
	    bind_result.output_schema_bytes,
	    bind_result.opaque_data,
	    projection_ids_64,
	    pushdown_filters,
	    join_keys,
	    phase,
	    execution_id,
	    {},  // init_opaque_data
	    ob_col, ob_dir, ob_null, ob_limit,
	    ts_percentage, ts_seed);
	auto init_request_bytes = SerializeToIpcBytes(init_request);
	auto rpc_params = ::duckdb::vgi::generated::BuildInitParams(init_request_bytes);

	// Serialize the init RPC request to Arrow IPC
	auto body = SerializeRpcRequest("init", rpc_params);

	// POST to {base_url}/init/init
	std::string init_url = base_url_ + "/init/init";

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("url", init_url);
		fields.emplace_back("function_name", function_name_);
		fields.emplace_back("phase", phase.empty() ? "default" : phase);
		VGI_LOG(context_, "http_function_connection.init", fields);
	}

	auto auth = attach_params_ ? attach_params_->auth() : nullptr;
	auto cached_params_init = attach_params_
	    ? attach_params_->GetOrInitHttpParams(context_, init_url) : nullptr;
	auto response_body = HttpPostArrowIpc(context_, init_url, body, auth,
	                                        /*cookie_jar=*/nullptr, cached_params_init);

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

	// Capture the output schema for empty-response fallback in ReadDataBatch.
	cached_output_schema_ = bind_result.output_schema;

	// Determine mode based on input_schema presence
	if (!input_schema_) {
		is_producer_mode_ = true;
	} else {
		is_producer_mode_ = false;
	}

	// Buffer data batches from the init response
	BufferDataBatches(response_body, header_result.data_offset);

	init_done_ = true;

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		fields.emplace_back("max_workers", std::to_string(init_response.max_workers));
		fields.emplace_back("is_producer_mode", is_producer_mode_ ? "true" : "false");
		fields.emplace_back("buffered_batches", std::to_string(buffered_batches_.size()));
		fields.emplace_back("has_state_token", stream_state_token_.empty() ? "false" : "true");
		VGI_LOG(context_, "http_function_connection.init_result", fields);
	}

	return InitResult {init_response.execution_id, init_response.max_workers, init_response.opaque_data};
}

void HttpFunctionConnection::PerformFinalizeInit(const BindResult &bind_result) {
	if (!init_done_) {
		throw IOException("HttpFunctionConnection::PerformFinalizeInit called before PerformInit [url: %s]", base_url_);
	}

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		VGI_LOG(context_, "http_function_connection.finalize_init", fields);
	}

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

	PerformInit(bind_result, {}, nullptr, {}, "FINALIZE");
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
	// Reconcile to the writer's declared (worker-facing) schema before
	// buffering. DuckDB's ArrowConverter::ToArrowSchema can't preserve every
	// Arrow attribute on its types (notably nullability flags and
	// TIMESTAMP_TZ unit/tz), so a batch produced by DataChunkToArrow may
	// not match the schema the IPC stream was opened with even when the
	// data would round-trip cleanly. ReconcileBatchToSchema handles the
	// metadata reshape (no copy) and any genuine type cast
	// (arrow::compute::Cast) recursively into nested types. Fast-path
	// returns the batch unchanged when schemas already match. This mirrors
	// FunctionConnection::WriteInputBatch (subprocess transport) which
	// gained the same call in commit ca8ad96.
	auto reconciled = batch;
	if (input_schema_) {
		reconciled = ReconcileBatchToSchema(batch, input_schema_);
	}
	pending_input_ = reconciled;
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

		{
			auto fields = BuildConnLogFields(*this);
			fields.emplace_back("function_name", function_name_);
			VGI_LOG(context_, "http_function_connection.producer_exchange", fields);
		}

		auto p_auth = attach_params_ ? attach_params_->auth() : nullptr;
		auto p_cached_params = attach_params_
		    ? attach_params_->GetOrInitHttpParams(context_, exchange_url) : nullptr;
		auto response_body = HttpPostArrowIpc(context_, exchange_url, body, p_auth,
		                                        /*cookie_jar=*/nullptr, p_cached_params);

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

	// Check if batch exceeds max_request_bytes and upload URL support is available.
	// Re-probe when we have never discovered yet, or when the cached snapshot's
	// Cache-Control max-age has elapsed (cache_expires_at == time_point{} means
	// "no expiry advertised" — treat as valid for the connection's lifetime).
	if (!capabilities_.discovered ||
	    (capabilities_.cache_expires_at != std::chrono::steady_clock::time_point{} &&
	     std::chrono::steady_clock::now() >= capabilities_.cache_expires_at)) {
		capabilities_ = HttpDiscoverCapabilities(context_, base_url_);
	}
	if (capabilities_.max_request_bytes > 0 &&
	    static_cast<int64_t>(body.size()) > capabilities_.max_request_bytes &&
	    capabilities_.upload_url_support) {
		// Upload via server-vended URL, send pointer batch instead
		auto e_auth = attach_params_ ? attach_params_->auth() : nullptr;
		auto urls = HttpRequestUploadUrls(context_, base_url_, 1, e_auth);
		if (!urls.empty()) {
			HttpPutBytes(context_, urls[0].upload_url, body, false);
			body = SerializePointerBatch(input_schema_, urls[0].download_url, stream_state_token_);
		}
	}

	std::string exchange_url = base_url_ + "/init/exchange";

	auto x_auth = attach_params_ ? attach_params_->auth() : nullptr;
	auto x_cached_params = attach_params_
	    ? attach_params_->GetOrInitHttpParams(context_, exchange_url) : nullptr;
	auto response_body = HttpPostArrowIpc(context_, exchange_url, body, x_auth,
	                                        /*cookie_jar=*/nullptr, x_cached_params);

	// Parse response — copy into owning buffer since Arrow IPC reads zero-copy reference it
	auto buffer = arrow::Buffer::FromString(std::move(response_body));
	auto input_stream = std::make_shared<arrow::io::BufferReader>(buffer);
	auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(input_stream);
	if (!reader_result.ok()) {
		// Empty response in exchange mode means no output for this input
		return arrow::RecordBatch::Make(
		    cached_output_schema_, 0, std::vector<std::shared_ptr<arrow::Array>>{});
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
			                     -1, GetExecutionIdHex(), GetAttachIdHex(), "", GetConnIdHex());
			throw IOException("VGI HTTP error from server [url: %s]", base_url_);
		}
		if (batch_type == RpcBatchType::LOG) {
			HandleBatchLogMessage(bwm.batch, bwm.custom_metadata, &context_, base_url_,
			                     -1, GetExecutionIdHex(), GetAttachIdHex(), "", GetConnIdHex());
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

void HttpFunctionConnection::CancelStream(const std::vector<uint8_t> &state_token) {
	// Need a state-token to address the server-side stream. Prefer the
	// caller-supplied token (captured at the call site, immune to
	// concurrent mutation); fall back to the live member for paths that
	// don't propagate it through.
	std::string token;
	if (!state_token.empty()) {
		token.assign(state_token.begin(), state_token.end());
	} else if (!stream_state_token_.empty()) {
		token = stream_state_token_;
	} else {
		// No stream established — nothing to cancel.
		return;
	}

	auto schema = arrow::schema({});
	auto cancel_batch = arrow::RecordBatch::Make(schema, 0, std::vector<std::shared_ptr<arrow::Array>>{});

	auto sink_result = arrow::io::BufferOutputStream::Create();
	if (!sink_result.ok()) {
		return;
	}
	auto sink = sink_result.ValueUnsafe();
	auto writer_result = arrow::ipc::MakeStreamWriter(sink, schema);
	if (!writer_result.ok()) {
		return;
	}
	auto writer = writer_result.ValueUnsafe();

	auto metadata = arrow::KeyValueMetadata::Make(
	    {RPC_STREAM_STATE_KEY, std::string(generated::VGI_RPC_CANCEL_KEY)},
	    {token, "1"});
	auto write_status = writer->WriteRecordBatch(*cancel_batch, metadata);
	if (!write_status.ok()) {
		return;
	}
	auto close_status = writer->Close();
	if (!close_status.ok()) {
		return;
	}
	auto finish_result = sink->Finish();
	if (!finish_result.ok()) {
		return;
	}
	auto buffer = finish_result.ValueUnsafe();
	std::vector<uint8_t> body(buffer->data(), buffer->data() + buffer->size());

	std::string exchange_url = base_url_ + "/init/exchange";
	auto c_auth = attach_params_ ? attach_params_->auth() : nullptr;
	auto c_cached_params = attach_params_
	    ? attach_params_->GetOrInitHttpParams(context_, exchange_url) : nullptr;
	// Best-effort: dispatcher catches any thrown exception.
	(void)HttpPostArrowIpc(context_, exchange_url, body, c_auth,
	                        /*cookie_jar=*/nullptr, c_cached_params);
}

} // namespace vgi
} // namespace duckdb
