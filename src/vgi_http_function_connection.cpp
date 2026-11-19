// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_http_function_connection.hpp"
#include "vgi_query_rpc.hpp"

#include "duckdb.hpp"
#include "duckdb/common/types/blob.hpp"
#ifdef __EMSCRIPTEN__
#include <pthread.h>
#endif

#include "vgi_arrow_ipc.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_bind_protocol.hpp"
#include "vgi_table_buffering_builders.hpp"
#include "vgi_catalog_metadata.hpp"
#include "generated/vgi_protocol_constants.hpp"
#include "vgi_exception.hpp"
#include "vgi_http_client.hpp"
#include "vgi_logging.hpp"
#include "vgi_rpc_client.hpp"
#include "vgi_rpc_types.hpp"
#include "vgi_protocol.hpp" // BindResult
#include "vgi_transport.hpp"
#include "generated/vgi_request_builders.hpp"

namespace duckdb {
namespace vgi {

// `function_kind` token for a connection's function_type_ ("TABLE",
// "SCALAR_FUNCTION", "TABLE_BUFFERING", ...).
static std::string HttpFunctionKindToken(const std::string &function_type) {
	std::string kind = function_type;
	const std::string suffix = "_FUNCTION";
	if (kind.size() > suffix.size() && kind.compare(kind.size() - suffix.size(), suffix.size(), suffix) == 0) {
		kind.resize(kind.size() - suffix.size());
	}
	if (kind == "TABLE_BUFFERING") {
		return "TABLE_IN_OUT";
	}
	return kind;
}

// extra_info for an error on this connection: the transport-neutral subtype
// (nullptr for none), `url` / `worker_path` and the function being run.
static ErrorInfo HttpFunctionInfo(const char *subtype, const std::string &base_url, const std::string &function_name,
                                  const std::string &function_type) {
	ErrorInfo info;
	if (IsHttpTransport(base_url)) {
		info = BuildHttpExtraInfo(subtype ? subtype : "", base_url);
		if (!subtype) {
			info.erase(error_key::kErrorSubtype);
		}
	} else if (subtype) {
		info = ErrorInfo(subtype);
	}
	info.Worker(base_url).Url(base_url).Function(function_name, HttpFunctionKindToken(function_type));
	return info;
}

// Error context for a request whose worker error comes back in the HTTP body:
// the worker (base URL), this execution, and the function. `url` is the
// request route, reported as its own field.
static RpcErrorContext HttpCallErrorContext(const std::string &base_url, const std::string &url,
                                            const std::string &method, const std::string &function_name,
                                            const std::string &function_type, const std::string &execution_id_hex,
                                            const std::string &attach_opaque_data_digest,
                                            const std::string &conn_id_hex) {
	RpcErrorContext ctx;
	ctx.worker_path = base_url;
	ctx.execution_id_hex = execution_id_hex;
	ctx.attach_opaque_data_digest = attach_opaque_data_digest;
	ctx.conn_id_hex = conn_id_hex;
	auto info = HttpFunctionInfo(nullptr, base_url, function_name, function_type);
	info.erase(error_key::kErrorSubtype);
	info.Url(url).Rpc(method);
	ctx.fields.insert(info.begin(), info.end());
	return ctx;
}

// ``vgi_batch_index`` from custom_metadata: INVALID_INDEX when absent,
// IOException when unparseable. Cap and monotonicity are checked downstream
// (VgiTableFunctionImpl's InstallBatch).
static idx_t ParseVgiBatchIndex(const VgiMetadataPtr &custom_metadata,
                                const std::string &base_url, const ErrorInfo &error_context) {
	if (!custom_metadata) {
		return DConstants::INVALID_INDEX;
	}
	int64_t idx = custom_metadata->FindKey("vgi_batch_index");
	if (idx < 0) {
		return DConstants::INVALID_INDEX;
	}
	const std::string &value = custom_metadata->value(idx);
	try {
		size_t pos = 0;
		uint64_t parsed = std::stoull(value, &pos);
		if (pos != value.size()) {
			throw IOException(ErrorInfo(error_context).Set(error_key::kMetadataKey, "vgi_batch_index"),
			                  "VGI worker emitted invalid vgi_batch_index '%s' "
			                  "(trailing characters; expected decimal uint64) [url: %s]",
			                  value, base_url);
		}
		return static_cast<idx_t>(parsed);
	} catch (const std::invalid_argument &) {
		throw IOException(ErrorInfo(error_context).Set(error_key::kMetadataKey, "vgi_batch_index"),
		                  "VGI worker emitted invalid vgi_batch_index '%s' "
		                  "(expected decimal uint64) [url: %s]",
		                  value, base_url);
	} catch (const std::out_of_range &) {
		throw IOException(ErrorInfo(error_context).Set(error_key::kMetadataKey, "vgi_batch_index"),
		                  "VGI worker emitted vgi_batch_index '%s' that exceeds uint64 range "
		                  "[url: %s]",
		                  value, base_url);
	}
}

// Base64-decode the `key` metadata value (e.g. vgi_partition_values#b64): empty
// when absent, IOException on invalid base64. Payload decoding happens
// downstream on the consumer thread.
static std::string ParseVgiB64Bytes(
    const VgiMetadataPtr &custom_metadata,
    const char *key, const std::string &base_url, const ErrorInfo &error_context) {
	if (!custom_metadata) {
		return "";
	}
	int64_t idx = custom_metadata->FindKey(key);
	if (idx < 0) {
		return "";
	}
	const std::string &b64_value = custom_metadata->value(idx);
	try {
		string_t b64_str(b64_value.data(), static_cast<uint32_t>(b64_value.size()));
		idx_t decoded_size = Blob::FromBase64Size(b64_str);
		std::string out;
		out.resize(decoded_size);
		Blob::FromBase64(b64_str, data_ptr_cast(out.data()), decoded_size);
		return out;
	} catch (const std::exception &e) {
		throw IOException(ErrorInfo(error_context).Set(error_key::kMetadataKey, key).Merge(ExtraInfoOf(e)),
		                  "VGI worker emitted invalid base64 payload in %s: %s [url: %s]", key, RawMessageOf(e),
		                  base_url);
	}
}

static std::string ExtractStreamStateValue(const VgiMetadataPtr &metadata) {
	if (!metadata) {
		return "";
	}
	int64_t idx = metadata->FindKey(RPC_STREAM_STATE_KEY);
	if (idx >= 0) {
		return metadata->value(idx);
	}
	return "";
}

HttpFunctionConnection::HttpFunctionConnection(
    const std::string &worker_path, const std::string &function_name,
    const ArrowArguments &arguments, const std::vector<uint8_t> &attach_opaque_data,
    const std::vector<uint8_t> &transaction_opaque_data,
    ClientContext &context, const std::string &function_type,
    const std::vector<uint8_t> &global_execution_id,
    bool worker_debug, const std::map<std::string, Value> &settings,
    const std::vector<VgiSecretRequirement> &required_secrets,
    const std::shared_ptr<VgiAttachParameters> &attach_params)
    : conn_id_hex_(VgiGenerateConnId()), base_url_(worker_path),
      function_name_(function_name), function_type_(function_type),
      arguments_batch_(arguments.batch), attach_opaque_data_(attach_opaque_data),
      transaction_opaque_data_(transaction_opaque_data), global_execution_id_(global_execution_id), context_(context),
      worker_debug_(worker_debug), settings_(settings), required_secrets_(required_secrets),
      attach_params_(attach_params) {
	if (!base_url_.empty() && base_url_.back() == '/') {
		base_url_.pop_back();
	}
}

std::string HttpFunctionConnection::GetExecutionIdHex() const {
	if (execution_id_.empty()) {
		return "";
	}
	return BytesToHex(execution_id_);
}

std::string HttpFunctionConnection::GetAttachOpaqueDataDigest() const {
	if (attach_opaque_data_.empty()) {
		return "";
	}
	return OpaqueDigest(attach_opaque_data_);
}

std::string HttpFunctionConnection::GetTransactionOpaqueDataDigest() const {
	if (transaction_opaque_data_.empty()) {
		return "";
	}
	return OpaqueDigest(transaction_opaque_data_);
}

ServerCapabilities HttpFunctionConnection::FreshCapabilities() {
	auto caps = CurrentCapabilities();
	if (!caps.discovered || (caps.cache_expires_at != std::chrono::steady_clock::time_point{} &&
	                         std::chrono::steady_clock::now() >= caps.cache_expires_at)) {
		caps = HttpDiscoverCapabilities(context_, base_url_, attach_params_ ? attach_params_->iroh() : nullptr);
	}
	return caps;
}

std::vector<uint8_t> HttpFunctionConnection::SerializeBatchWithState(
    const VgiBatchPtr &batch,
    const VgiSchemaPtr &schema) {
	// The batch must match the declared (worker-facing) schema, metadata
	// excluded; otherwise its own schema would be serialized silently.
	// WriteInputBatch's ReconcileBatchToSchema normally guarantees a match.
	if (schema && batch->schema().get() != schema.get() &&
	    !batch->schema()->Equals(*schema, /*check_metadata=*/false)) {
		std::string writer_schema_str = schema->ToString();
		std::string batch_schema_str = batch->schema()
		    ? batch->schema()->ToString() : "<null>";
		throw IOException(
		    "Failed to write batch: schema mismatch\n"
		    "  writer schema (advertised at bind):\n%s\n"
		    "  batch schema (received from caller):\n%s",
		    writer_schema_str, batch_schema_str);
	}

	std::vector<std::pair<std::string, std::string>> meta_pairs;
	if (!stream_state_token_.empty()) {
		meta_pairs.emplace_back(RPC_STREAM_STATE_KEY, stream_state_token_);
	}
	if (!call_state_token_.empty()) {
		meta_pairs.emplace_back(RPC_CALL_STATE_KEY, call_state_token_);
	}
	if (tick_filter_state_) {
		// Only a delta this stream has not been sent yet; an unchanged filter
		// rides no tick at all (see UpdateDynamicFilterState).
		auto delta = tick_filter_cursor_.Take(*tick_filter_state_);
		if (!delta.empty()) {
			meta_pairs.emplace_back("vgi_pushdown_filters", std::move(delta));
		}
	}
	// Conditional-revalidation validators, sent once (if PerformInit has not
	// already sent them); the worker can then answer not_modified.
	if (!cond_sent_ && (!cond_if_none_match_.empty() || !cond_if_modified_since_.empty())) {
		if (!cond_if_none_match_.empty()) {
			meta_pairs.emplace_back(VGI_CACHE_IF_NONE_MATCH_KEY, cond_if_none_match_);
		}
		if (!cond_if_modified_since_.empty()) {
			meta_pairs.emplace_back(VGI_CACHE_IF_MODIFIED_SINCE_KEY, cond_if_modified_since_);
		}
		cond_sent_ = true;
	}
	if (pending_tick_) {
		meta_pairs.emplace_back(VGI_STATUS_KEY, VGI_STATUS_CONTINUE);
		pending_tick_ = false;
	}
	VgiMetadataPtr metadata;
	if (!meta_pairs.empty()) {
		metadata = VgiMetadata::Make(std::move(meta_pairs));
	}

	// Schema + batch-with-metadata + EOS in a single allocation.
	return SerializeToIpcBytes(batch, metadata);
}

VgiBatchPtr HttpFunctionConnection::ExtractStreamState(
    const VgiBatchPtr &batch,
    const VgiMetadataPtr &metadata) {
	if (!metadata) {
		return batch;
	}

	int64_t idx = metadata->FindKey(RPC_STREAM_STATE_KEY);
	if (idx >= 0) {
		stream_state_token_ = metadata->value(idx);
	}
	// Only /init carries this; it is kept for the whole stream.
	int64_t call_idx = metadata->FindKey(RPC_CALL_STATE_KEY);
	if (call_idx >= 0) {
		call_state_token_ = metadata->value(call_idx);
	}

	return batch;
}

void HttpFunctionConnection::BufferDataBatches(VgiBufferPtr owned, size_t offset) {
	buffered_batches_.clear();
	buffered_batch_indexes_.clear();
	buffered_partition_values_bytes_.clear();
	buffered_parent_row_bytes_.clear();
	buffered_cache_controls_.clear();
	buffered_batch_index_ = 0;
	stream_state_token_.clear();

	if (!owned || offset >= static_cast<size_t>(owned->size())) {
		if (function_name_ == VGI_QUERY_EXECUTOR) {
			throw IOException("vgi_query response is missing its Arrow output stream");
		}
		return;
	}
	const size_t data_bytes = static_cast<size_t>(owned->size()) - offset;

	// Zero-copy: the reader's slices reference the owning buffer.
	auto buffer = owned->Slice(static_cast<int64_t>(offset), static_cast<int64_t>(data_bytes));
	owned.reset();
	std::unique_ptr<VgiIpcReader> reader;
	try {
		reader = VgiIpcReader::Open(std::make_shared<VgiBufferReader>(std::move(buffer)));
	} catch (const std::exception &) {
		if (function_name_ == VGI_QUERY_EXECUTOR) {
			throw IOException("vgi_query response has an invalid Arrow output stream");
		}
		// An empty data stream is valid (no output yet).
		return;
	}
	if (function_name_ == VGI_QUERY_EXECUTOR &&
	    !reader->schema()->Equals(*cached_output_schema_, true)) {
		throw IOException("vgi_query output stream schema differs from the prepared schema");
	}

	// Per-response counters for the VGI_LOG below (shows server batch bundling).
	int64_t spike_data_batches = 0;
	int64_t spike_log_batches = 0;
	int64_t spike_external_batches = 0;
	const auto violation =
	    HttpFunctionInfo(error_subtype::kProtocolViolation, base_url_, function_name_, function_type_);
	const auto log_error_context = HttpFunctionInfo(nullptr, base_url_, function_name_, function_type_);
	while (true) {
		// A bundled response can hold many batches, each possibly an external
		// location costing its own GET.
		if (context_.IsInterrupted()) {
			throw InterruptException();
		}
		// A stream that ends early or carries a malformed message ends the
		// response (lenient HTTP bodies), except for a prepared query.
		VgiBatchPtr batch;
		try {
			batch = reader->ReadNext();
		} catch (const std::exception &e) {
			if (function_name_ == VGI_QUERY_EXECUTOR) {
				throw IOException("vgi_query response contains an invalid Arrow batch: %s", RawMessageOf(e));
			}
			break;
		}
		if (!batch) {
			break;
		}
		const VgiMetadataPtr &batch_metadata = batch->metadata();

		auto batch_type = ClassifyBatch(batch, batch_metadata);
		if (batch_type == RpcBatchType::ERROR) {
			HandleBatchLogMessage(batch, batch_metadata, &context_, base_url_,
			                     -1, GetExecutionIdHex(), GetAttachOpaqueDataDigest(), "", GetConnIdHex(),
			                     &log_error_context);
			throw IOException(violation, "VGI HTTP error from server [url: %s]", base_url_);
		}
		if (batch_type == RpcBatchType::LOG) {
			HandleBatchLogMessage(batch, batch_metadata, &context_, base_url_,
			                     -1, GetExecutionIdHex(), GetAttachOpaqueDataDigest(), "", GetConnIdHex(),
			                     &log_error_context);
			++spike_log_batches;
			continue;
		}

		// External location pointer: stream_state and the per-batch metadata ride
		// the resolved batch, not the pointer.
		if (batch_type == RpcBatchType::EXTERNAL_LOCATION) {
			auto location_url = batch_metadata->value(batch_metadata->FindKey(RPC_LOCATION_KEY));
			auto resolved = ResolveExternalLocation(context_, location_url,
			                                         base_url_, GetExecutionIdHex(), GetAttachOpaqueDataDigest(),
			                                         batch_metadata, &context_, GetConnIdHex());
			buffered_batches_.push_back(resolved.batch);
			buffered_batch_indexes_.push_back(ParseVgiBatchIndex(resolved.metadata, base_url_, violation));
			buffered_partition_values_bytes_.push_back(
			    ParseVgiB64Bytes(resolved.metadata, "vgi_partition_values#b64", base_url_, violation));
			buffered_parent_row_bytes_.push_back(
			    ParseVgiB64Bytes(resolved.metadata, "vgi_rpc.parent_row#b64", base_url_, violation));
			buffered_cache_controls_.push_back(ParseVgiCacheControl(resolved.metadata));
			ExtractStreamState(resolved.batch, resolved.metadata);
			++spike_external_batches;
			continue;
		}

		// Validate what DuckDB will read (externalized batches above are
		// validated by the reader that fetched them).
		ValidateWorkerBatch(&context_, batch.get(), base_url_);

		ExtractStreamState(batch, batch_metadata);
		buffered_batches_.push_back(batch);
		buffered_batch_indexes_.push_back(ParseVgiBatchIndex(batch_metadata, base_url_, violation));
		buffered_partition_values_bytes_.push_back(
		    ParseVgiB64Bytes(batch_metadata, "vgi_partition_values#b64", base_url_, violation));
		buffered_parent_row_bytes_.push_back(
		    ParseVgiB64Bytes(batch_metadata, "vgi_rpc.parent_row#b64", base_url_, violation));
		buffered_cache_controls_.push_back(ParseVgiCacheControl(batch_metadata));
		++spike_data_batches;
	}

	// Gated: fires once per response on the producer hot path.
	if (VgiInfoLogActive(context_)) {
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("data_batches", std::to_string(spike_data_batches));
		fields.emplace_back("log_batches", std::to_string(spike_log_batches));
		fields.emplace_back("external_batches", std::to_string(spike_external_batches));
		fields.emplace_back("response_bytes", std::to_string(data_bytes));
		VGI_LOG(context_, "http_function_connection.buffer_data_batches", fields);
	}
}

// Bind

void HttpFunctionConnection::SetInputSchema(const VgiSchemaPtr &input_schema) {
	// Ungated (HTTP has no spawn step), but it only takes effect if called
	// before PerformBindRpc, which serializes it into the bind request.
	input_schema_ = input_schema;
}

void HttpFunctionConnection::UpdateInputSchemaForExecution(const VgiSchemaPtr &input_schema) {
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

	auto transport_fn = [&](const std::vector<uint8_t> &request_bytes) -> VgiBatchPtr {
		auto rpc_params = ::duckdb::vgi::generated::BuildBindParams(request_bytes);
		auto auth = attach_params_ ? attach_params_->auth() : nullptr;
		auto cached_params = attach_params_ && IsHttpTransport(base_url_)
		    ? attach_params_->GetOrInitHttpParams(context_, base_url_) : nullptr;
		// In/out capability snapshot: picks this request's Content-Encoding and
		// is refreshed from the response (see CurrentCapabilities).
		ServerCapabilities b_caps = CurrentCapabilities();
		const auto call_error_fields = HttpFunctionInfo(nullptr, base_url_, function_name_, function_type_);
		auto resp = HttpInvokeUnary(context_, base_url_, "bind", rpc_params, auth,
		                             /*cookie_jar=*/nullptr, cached_params,
		                             "", "", "", "", VGI_MAIN_PROTOCOL, &http_client_, &b_caps,
		                             attach_params_ ? attach_params_->iroh() : nullptr,
		                             &call_error_fields);
		PublishHarvestedCapabilities(b_caps);
		return UnwrapResultEnvelope(resp.batch, "bind",
		                            HttpFunctionInfo(nullptr, base_url_, function_name_, function_type_));
	};

	auto bind_result = PerformBindProtocol(context_, function_name_, function_type_,
	                                        arguments_batch_, input_schema_, attach_opaque_data_,
	                                        transaction_opaque_data_, settings_, required_secrets_,
	                                        base_url_, transport_fn, at_unit_, at_value_,
	                                        copy_from_ ? &*copy_from_ : nullptr,
	                                        copy_to_ ? &*copy_to_ : nullptr, schema_name_, argument_names_);

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		fields.emplace_back("num_output_columns", std::to_string(bind_result.output_schema->num_fields()));
		VGI_LOG(context_, "http_function_connection.bind_result", fields);
	}

	return bind_result;
}

// Init

InitResult HttpFunctionConnection::PerformInit(const BindResult &bind_result,
                                                const std::vector<int32_t> &projection_ids,
                                                VgiBufferPtr pushdown_filters,
                                                std::vector<VgiBufferPtr> join_keys,
                                                const std::string &phase,
                                                const std::optional<OrderByHint> &order_by,
                                                const std::optional<TableSampleHint> &table_sample,
                                                const std::vector<uint8_t> &init_opaque_data,
                                                const std::optional<std::vector<uint8_t>> &finalize_state_id,
                                           const std::vector<std::string> &split_tokens) {
#ifdef __EMSCRIPTEN__
#endif
	if (init_done_) {
		throw IOException("HttpFunctionConnection::PerformInit called twice [url: %s]", base_url_);
	}
	// A new stream: its worker state has applied no dynamic-filter delta yet.
	tick_filter_cursor_.Reset();

	std::vector<int64_t> projection_ids_64;
	projection_ids_64.reserve(projection_ids.size());
	for (auto id : projection_ids) {
		projection_ids_64.push_back(static_cast<int64_t>(id));
	}

	auto &execution_id = global_execution_id_;

	// Empty strings / -1 when there is no hint.
	std::string ob_col, ob_dir, ob_null;
	int64_t ob_limit = -1;
	if (order_by.has_value()) {
		ob_col = order_by->column_name;
		ob_dir = order_by->direction;
		ob_null = order_by->null_order;
		ob_limit = order_by->row_limit;
	}

	// -1.0 / -1 when there is no hint.
	double ts_percentage = -1.0;
	int64_t ts_seed = -1;
	if (table_sample.has_value()) {
		ts_percentage = table_sample->sample_percentage;
		ts_seed = table_sample->seed;
	}

	auto init_request = BuildInitRequest(
	    bind_result.bind_request_bytes,
	    bind_result.output_schema_bytes,
	    bind_result.opaque_data,
	    projection_ids_64,
	    pushdown_filters,
	    join_keys,
	    phase,
	    execution_id,
	    init_opaque_data,
	    ob_col, ob_dir, ob_null, ob_limit,
	    ts_percentage, ts_seed,
	    finalize_state_id,
	    substream_id_, split_tokens);
	auto init_request_bytes = SerializeToIpcBytes(init_request);
#ifdef __EMSCRIPTEN__
#endif
	auto rpc_params = ::duckdb::vgi::generated::BuildInitParams(init_request_bytes);
#ifdef __EMSCRIPTEN__
#endif

	// Over HTTP the worker's first producer turn runs inside this /init request,
	// so conditional-revalidation validators must ride the init request itself
	// (a first tick would be too late). cond_sent_ ensures single delivery.
	std::vector<std::pair<std::string, std::string>> extra_metadata;
	if (!cond_sent_ && (!cond_if_none_match_.empty() || !cond_if_modified_since_.empty())) {
		if (!cond_if_none_match_.empty()) {
			extra_metadata.emplace_back(VGI_CACHE_IF_NONE_MATCH_KEY, cond_if_none_match_);
		}
		if (!cond_if_modified_since_.empty()) {
			extra_metadata.emplace_back(VGI_CACHE_IF_MODIFIED_SINCE_KEY, cond_if_modified_since_);
		}
		cond_sent_ = true;
	}
	auto body = SerializeRpcRequest("init", rpc_params, VGI_MAIN_PROTOCOL, extra_metadata);
#ifdef __EMSCRIPTEN__
#endif

	// {base}/{protocol}/init/init: the server rejects a request whose URL protocol
	// segment and body routing key disagree.
	std::string init_url = base_url_ + "/" + std::string(VGI_MAIN_PROTOCOL.name) + "/init/init";

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("url", init_url);
		fields.emplace_back("function_name", function_name_);
		fields.emplace_back("phase", phase.empty() ? "default" : phase);
#ifdef __EMSCRIPTEN__
#endif
		VGI_LOG(context_, "http_function_connection.init", fields);
#ifdef __EMSCRIPTEN__
#endif
	}

	auto auth = attach_params_ ? attach_params_->auth() : nullptr;
#ifdef __EMSCRIPTEN__
#endif
	auto cached_params_init = attach_params_ && IsHttpTransport(init_url)
	    ? attach_params_->GetOrInitHttpParams(context_, init_url) : nullptr;
#ifdef __EMSCRIPTEN__
#endif
	// See CurrentCapabilities. A large init (e.g. a big table-function
	// parameter carried in the bind request) goes up via an upload URL.
	ServerCapabilities harvested = FreshCapabilities();
	MaybeExternalizeRequestBody(context_, base_url_, body, harvested, auth,
	                            attach_params_ ? attach_params_->iroh() : nullptr);
	const auto call_error_context = HttpCallErrorContext(base_url_, init_url, "init", function_name_, function_type_,
	                                                       GetExecutionIdHex(), GetAttachOpaqueDataDigest(), GetConnIdHex());
	auto response_body = HttpPostArrowIpc(context_, init_url, body, auth,
	                                        /*cookie_jar=*/nullptr, cached_params_init, &http_client_,
	                                        &harvested, attach_params_ ? attach_params_->iroh() : nullptr,
	                                        &context_, &call_error_context);
	PublishHarvestedCapabilities(harvested);
#ifdef __EMSCRIPTEN__
#endif

	// The single owning buffer: the header parse and data buffering below use
	// zero-copy slices of it, so a large first producer turn is not copied.
	auto owned_body = VgiBuffer::FromString(std::move(response_body));

	// Response: header IPC stream, then data IPC stream.
	auto header_result = ReadStreamHeaderFromBuffer(owned_body, &context_, init_url);

	if (header_result.header.header_batch && header_result.header.metadata) {
		int64_t loc_idx = header_result.header.metadata->FindKey(RPC_LOCATION_KEY);
		if (loc_idx >= 0) {
			auto location_url = header_result.header.metadata->value(loc_idx);
			auto resolved = ResolveExternalLocation(context_, location_url, base_url_, GetExecutionIdHex(),
			                                         GetAttachOpaqueDataDigest(), header_result.header.metadata,
			                                         &context_, GetConnIdHex());
			header_result.header.header_batch = resolved.batch;
			header_result.header.metadata = resolved.metadata;
		}
	}

	auto init_response = ParseGlobalInitResponse(header_result.header.header_batch, base_url_);
	execution_id_ = init_response.execution_id;

	cached_output_schema_ = bind_result.output_schema;

	// Exchange mode iff there is an input schema, except that the FINALIZE
	// phases always run as producers.
	bool producer_phase_override = (phase == "FINALIZE" || phase == "TABLE_BUFFERING_FINALIZE");
	if (!input_schema_ || producer_phase_override) {
		is_producer_mode_ = true;
	} else {
		is_producer_mode_ = false;
	}

	BufferDataBatches(std::move(owned_body), header_result.data_offset);

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

void HttpFunctionConnection::ResetForNextSplit() {
	if (!init_done_) {
		throw IOException("HttpFunctionConnection::ResetForNextSplit called before PerformInit [url: %s]", base_url_);
	}

	init_done_ = false;
	data_finished_ = false;

	// Everything buffered belongs to the split that just ended.
	buffered_batches_.clear();
	buffered_batch_index_ = 0;
	stream_state_token_.clear();

	call_state_token_.clear(); // see ResetForNextSplit in the header

	pending_input_.reset();
	input_writer_opened_ = false;
	input_writer_closed_ = false;

	// http_client_ is deliberately kept.
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

	init_done_ = false;
	data_finished_ = false;
	buffered_batches_.clear();
	buffered_batch_index_ = 0;
	stream_state_token_.clear();
	// The FINALIZE /init mints a fresh call token.
	call_state_token_.clear();
	pending_input_.reset();
	input_writer_opened_ = false;
	input_writer_closed_ = false;

	// Run PerformInit without an input schema, under the INPUT phase's
	// execution id; both are restored afterwards.
	auto saved_input_schema = input_schema_;
	auto saved_global_exec_id = global_execution_id_;
	input_schema_.reset();
	global_execution_id_ = execution_id_;

	struct RestoreGuard {
		VgiSchemaPtr &input_schema;
		std::vector<uint8_t> &global_exec_id;
		VgiSchemaPtr saved_schema;
		std::vector<uint8_t> saved_id;
		~RestoreGuard() {
			input_schema = std::move(saved_schema);
			global_exec_id = std::move(saved_id);
		}
	} guard{input_schema_, global_execution_id_, std::move(saved_input_schema), std::move(saved_global_exec_id)};

	PerformInit(bind_result, {}, nullptr, {}, "FINALIZE");
}

// Data exchange

void HttpFunctionConnection::OpenInputWriter() {
	// Nothing to open: each exchange is its own POST.
	input_writer_opened_ = true;
}

void HttpFunctionConnection::WriteInputBatch(const VgiBatchPtr &batch) {
	if (!input_writer_opened_) {
		throw IOException("HttpFunctionConnection::WriteInputBatch called before OpenInputWriter [url: %s]", base_url_);
	}
	if (input_writer_closed_) {
		throw IOException("HttpFunctionConnection::WriteInputBatch called after CloseInputWriter [url: %s]", base_url_);
	}
	// Reconcile to the declared (worker-facing) schema, as
	// FunctionConnection::WriteInputBatch does: DuckDB's Arrow export cannot
	// preserve every attribute (nullability flags, TIMESTAMP_TZ unit/tz), so a
	// DataChunkToArrow batch may differ from it even when the data round-trips.
	auto reconciled = batch;
	if (input_schema_) {
		reconciled = ReconcileBatchToSchema(batch, input_schema_);
	}
	pending_input_ = reconciled;
}

void HttpFunctionConnection::WriteContinuationTick() {
	if (!input_writer_opened_ || input_writer_closed_) {
		throw IOException("HttpFunctionConnection::WriteContinuationTick called without an open input writer [url: %s]",
		                  base_url_);
	}
	try {
		pending_input_ = VgiBatch::MakeEmpty(input_schema_);
	} catch (const std::exception &e) {
		throw IOException("Failed to build continuation tick: %s [url: %s]", RawMessageOf(e), base_url_);
	}
	pending_tick_ = true;
}

void HttpFunctionConnection::CloseInputWriter() {
	input_writer_closed_ = true;
}

// Buffered table function RPCs: requests from vgi_table_buffering_builders.hpp,
// sent with HttpInvokeUnary (which dispatches log batches with this
// connection's ids, like the subprocess path).

namespace {

// The inner response batch; null when `result` is null (e.g.
// table_buffering_destructor).
VgiBatchPtr DecodeHttpOuterResponse(const UnaryResponseResult &response,
                                                              const std::string &method_name,
                                                              const std::string &base_url) {
	return UnwrapResultEnvelope(response.batch, method_name,
	                            HttpFunctionInfo(nullptr, base_url, std::string(), std::string()),
	                            /*null_result_ok=*/true);
}

} // namespace


std::vector<uint8_t>
HttpFunctionConnection::RpcTableBufferingProcess(const std::string &function_name,
                                                    const std::vector<uint8_t> &execution_id,
                                                    const VgiBatchPtr &input_batch,
                                                    std::optional<int64_t> batch_index) {
	auto batch_bytes = vgi::SerializeToIpcBytes(input_batch);
	auto rpc_params = vgi::BuildTableBufferingProcessInner(function_name, schema_name_, execution_id,
	                                                          batch_bytes, attach_opaque_data_, batch_index);
	auto auth = attach_params_ ? attach_params_->auth() : nullptr;
	auto cached_params = attach_params_ && IsHttpTransport(base_url_)
	    ? attach_params_->GetOrInitHttpParams(context_, base_url_) : nullptr;
	ServerCapabilities tb_caps = CurrentCapabilities();
	const auto call_error_fields = HttpFunctionInfo(nullptr, base_url_, function_name_, function_type_);
	auto resp = HttpInvokeUnary(context_, base_url_, "table_buffering_process", rpc_params, auth,
	                             /*cookie_jar=*/nullptr, cached_params,
	                             GetExecutionIdHex(), GetAttachOpaqueDataDigest(), "", GetConnIdHex(),
	                             VGI_MAIN_PROTOCOL, &http_client_, &tb_caps,
	                             attach_params_ ? attach_params_->iroh() : nullptr,
	                             &call_error_fields);
	PublishHarvestedCapabilities(tb_caps);
	auto inner = DecodeHttpOuterResponse(resp, "table_buffering_process", base_url_);
	if (!inner || inner->num_rows() == 0) {
		throw IOException(HttpFunctionInfo(error_subtype::kProtocolViolation, base_url_, function_name_, function_type_)
		                      .Rpc("table_buffering_process"),
		                  "table_buffering_process response missing data [url: %s]", base_url_);
	}
	return vgi::generated::Decode<vgi::generated::TableBufferingProcessResult>(inner, 0, base_url_).state_id;
}

std::vector<std::vector<uint8_t>>
HttpFunctionConnection::RpcTableBufferingCombine(const std::string &function_name,
                                                    const std::vector<uint8_t> &execution_id,
                                                    const std::vector<std::vector<uint8_t>> &state_ids) {
	auto rpc_params = vgi::BuildTableBufferingCombineInner(function_name, schema_name_, execution_id, state_ids,
	                                                          attach_opaque_data_);
	auto auth = attach_params_ ? attach_params_->auth() : nullptr;
	auto cached_params = attach_params_ && IsHttpTransport(base_url_)
	    ? attach_params_->GetOrInitHttpParams(context_, base_url_) : nullptr;
	ServerCapabilities tb_caps = CurrentCapabilities();
	const auto call_error_fields = HttpFunctionInfo(nullptr, base_url_, function_name_, function_type_);
	auto resp = HttpInvokeUnary(context_, base_url_, "table_buffering_combine", rpc_params, auth,
	                             /*cookie_jar=*/nullptr, cached_params,
	                             GetExecutionIdHex(), GetAttachOpaqueDataDigest(), "", GetConnIdHex(),
	                             VGI_MAIN_PROTOCOL, &http_client_, &tb_caps,
	                             attach_params_ ? attach_params_->iroh() : nullptr,
	                             &call_error_fields);
	PublishHarvestedCapabilities(tb_caps);
	auto inner = DecodeHttpOuterResponse(resp, "table_buffering_combine", base_url_);
	if (!inner || inner->num_rows() == 0) {
		throw IOException(HttpFunctionInfo(error_subtype::kProtocolViolation, base_url_, function_name_, function_type_)
		                      .Rpc("table_buffering_combine"),
		                  "table_buffering_combine response missing data [url: %s]", base_url_);
	}
	return vgi::generated::Decode<vgi::generated::TableBufferingCombineResult>(inner, 0, base_url_).finalize_state_ids;
}

void HttpFunctionConnection::RpcTableBufferingDestructor(const std::string &function_name,
                                                           const std::vector<uint8_t> &execution_id) {
	auto rpc_params = vgi::BuildTableBufferingDestructorInner(function_name, schema_name_, execution_id,
	                                                             attach_opaque_data_);
	auto auth = attach_params_ ? attach_params_->auth() : nullptr;
	auto cached_params = attach_params_ && IsHttpTransport(base_url_)
	    ? attach_params_->GetOrInitHttpParams(context_, base_url_) : nullptr;
	ServerCapabilities tb_caps = CurrentCapabilities();
	const auto call_error_fields = HttpFunctionInfo(nullptr, base_url_, function_name_, function_type_);
	auto resp = HttpInvokeUnary(context_, base_url_, "table_buffering_destructor", rpc_params, auth,
	                             /*cookie_jar=*/nullptr, cached_params,
	                             GetExecutionIdHex(), GetAttachOpaqueDataDigest(), "", GetConnIdHex(),
	                             VGI_MAIN_PROTOCOL, &http_client_, &tb_caps,
	                             attach_params_ ? attach_params_->iroh() : nullptr,
	                             &call_error_fields);
	PublishHarvestedCapabilities(tb_caps);
	auto inner = DecodeHttpOuterResponse(resp, "table_buffering_destructor", base_url_);
	(void)inner;
}

VgiBatchPtr HttpFunctionConnection::ReadDataBatch() {
	if (!init_done_) {
		throw IOException("HttpFunctionConnection::ReadDataBatch called before PerformInit [url: %s]", base_url_);
	}
	if (data_finished_) {
		return nullptr;
	}

	// Producer mode: drain buffered batches, then POST a continuation tick.
	if (is_producer_mode_) {
		if (buffered_batch_index_ < buffered_batches_.size()) {
			last_batch_index_ = buffered_batch_indexes_[buffered_batch_index_];
			last_partition_values_bytes_ = buffered_partition_values_bytes_[buffered_batch_index_];
			last_parent_row_bytes_ = buffered_parent_row_bytes_[buffered_batch_index_];
			last_cache_control_ = buffered_cache_controls_[buffered_batch_index_];
			return buffered_batches_[buffered_batch_index_++];
		}

		// No state token → EOS.
		if (stream_state_token_.empty()) {
			data_finished_ = true;
			return nullptr;
		}

		// The request itself is cancellable, but a fast worker answers before an
		// interrupt lands mid-transfer, so check between requests too.
		if (context_.IsInterrupted()) {
			throw InterruptException();
		}
		// Immutable and shared: built once, reused by every continuation tick.
		static const VgiSchemaPtr tick_schema = VgiSchema::Empty();
		static const VgiBatchPtr tick_batch = VgiBatch::MakeEmpty(tick_schema);

		auto body = SerializeBatchWithState(tick_batch, tick_schema);

		std::string exchange_url = base_url_ + "/" + std::string(VGI_MAIN_PROTOCOL.name) + "/init/exchange";

		// Gated: fires once per continuation on the producer hot path.
		if (VgiInfoLogActive(context_)) {
			auto fields = BuildConnLogFields(*this);
			fields.emplace_back("function_name", function_name_);
			VGI_LOG(context_, "http_function_connection.producer_exchange", fields);
		}

		auto p_auth = attach_params_ ? attach_params_->auth() : nullptr;
		auto p_cached_params = attach_params_ && IsHttpTransport(exchange_url)
		    ? attach_params_->GetOrInitHttpParams(context_, exchange_url) : nullptr;
		ServerCapabilities p_harvested = CurrentCapabilities();
		const auto call_error_context = HttpCallErrorContext(base_url_, exchange_url, "exchange", function_name_, function_type_,
		                                                       GetExecutionIdHex(), GetAttachOpaqueDataDigest(), GetConnIdHex());
		auto response_body = HttpPostArrowIpc(context_, exchange_url, body, p_auth,
		                                        /*cookie_jar=*/nullptr, p_cached_params, &http_client_,
		                                        &p_harvested, attach_params_ ? attach_params_->iroh() : nullptr,
		                                        &context_, &call_error_context);
		PublishHarvestedCapabilities(p_harvested);

		BufferDataBatches(VgiBuffer::FromString(std::move(response_body)));

		if (buffered_batch_index_ < buffered_batches_.size()) {
			last_batch_index_ = buffered_batch_indexes_[buffered_batch_index_];
			last_partition_values_bytes_ = buffered_partition_values_bytes_[buffered_batch_index_];
			last_parent_row_bytes_ = buffered_parent_row_bytes_[buffered_batch_index_];
			last_cache_control_ = buffered_cache_controls_[buffered_batch_index_];
			return buffered_batches_[buffered_batch_index_++];
		}

		if (stream_state_token_.empty()) {
			data_finished_ = true;
		}
		return nullptr;
	}

	// Exchange mode (scalar / table-in-out): one POST per input batch.
	auto input_batch = std::move(pending_input_);
	if (!input_batch) {
		if (input_writer_closed_) {
			data_finished_ = true;
			return nullptr;
		}
		throw IOException("HttpFunctionConnection::ReadDataBatch called with no pending input [url: %s]", base_url_);
	}

	auto body = SerializeBatchWithState(input_batch, input_schema_);

	// A body over max_request_bytes goes through a server-vended upload URL.
	// Capabilities are normally warm from /init.
	capabilities_ = FreshCapabilities();
	MaybeExternalizeRequestBody(context_, base_url_, body, capabilities_,
	                            attach_params_ ? attach_params_->auth() : nullptr,
	                            attach_params_ ? attach_params_->iroh() : nullptr);

	std::string exchange_url = base_url_ + "/" + std::string(VGI_MAIN_PROTOCOL.name) + "/init/exchange";

	auto x_auth = attach_params_ ? attach_params_->auth() : nullptr;
	auto x_cached_params = attach_params_ && IsHttpTransport(exchange_url)
	    ? attach_params_->GetOrInitHttpParams(context_, exchange_url) : nullptr;
	ServerCapabilities x_harvested = CurrentCapabilities();
	const auto call_error_context = HttpCallErrorContext(base_url_, exchange_url, "exchange", function_name_, function_type_,
	                                                       GetExecutionIdHex(), GetAttachOpaqueDataDigest(), GetConnIdHex());
	auto response_body = HttpPostArrowIpc(context_, exchange_url, body, x_auth,
	                                        /*cookie_jar=*/nullptr, x_cached_params, &http_client_,
	                                        &x_harvested, attach_params_ ? attach_params_->iroh() : nullptr,
	                                        &context_, &call_error_context);
	PublishHarvestedCapabilities(x_harvested);

	// Owning buffer; decoded batches reference it zero-copy.
	auto buffer = VgiBuffer::FromString(std::move(response_body));
	std::unique_ptr<VgiIpcReader> reader;
	const auto violation =
	    HttpFunctionInfo(error_subtype::kProtocolViolation, base_url_, function_name_, function_type_);
	// Reset per-reply values before the loop and the empty-response return, so
	// none outlives its reply. Not inside the loop: a data batch may be followed
	// by a 0-row batch carrying only the state token.
	const bool was_more_pending = last_more_output_;
	last_batch_index_ = DConstants::INVALID_INDEX;
	last_partition_values_bytes_.clear();
	last_parent_row_bytes_.clear();
	last_cache_control_ = VgiCacheControl{};
	last_more_output_ = false;
	try {
		reader = VgiIpcReader::Open(std::make_shared<VgiBufferReader>(std::move(buffer)));
	} catch (const std::exception &) {
		// While output is pending this exchange was a continuation tick, which the
		// worker must answer with a reply; an empty body would otherwise end the
		// output silently (or, with a stale "more", tick forever).
		if (was_more_pending) {
			throw IOException(violation, "VGI HTTP worker function '%s' sent an empty response to a continuation tick",
			                  function_name_);
		}
		// Empty response: no output for this input.
		return VgiBatch::MakeEmpty(cached_output_schema_);
	}

	const auto log_error_context = HttpFunctionInfo(nullptr, base_url_, function_name_, function_type_);
	VgiBatchPtr output_batch;
	std::string new_state_token;

	while (true) {
		// A stream that ends early or carries a malformed message ends the
		// response (lenient HTTP bodies).
		VgiBatchPtr batch;
		try {
			batch = reader->ReadNext();
		} catch (const std::exception &) {
			break;
		}
		if (!batch) {
			break;
		}
		// By value: an externalized batch is replaced by the resolved one below.
		VgiMetadataPtr batch_metadata = batch->metadata();

		auto batch_type = ClassifyBatch(batch, batch_metadata);
		if (batch_type == RpcBatchType::ERROR) {
			HandleBatchLogMessage(batch, batch_metadata, &context_, base_url_,
			                     -1, GetExecutionIdHex(), GetAttachOpaqueDataDigest(), "", GetConnIdHex(),
			                     &log_error_context);
			throw IOException(violation, "VGI HTTP error from server [url: %s]", base_url_);
		}
		if (batch_type == RpcBatchType::LOG) {
			HandleBatchLogMessage(batch, batch_metadata, &context_, base_url_,
			                     -1, GetExecutionIdHex(), GetAttachOpaqueDataDigest(), "", GetConnIdHex(),
			                     &log_error_context);
			continue;
		}

		// Splice in an externalized batch and parse it like an inline one; its
		// metadata rides the resolved batch, and the fetch already validated it.
		if (batch_type == RpcBatchType::EXTERNAL_LOCATION) {
			auto location_url = batch_metadata->value(batch_metadata->FindKey(RPC_LOCATION_KEY));
			auto resolved = ResolveExternalLocation(context_, location_url,
			                                         base_url_, GetExecutionIdHex(), GetAttachOpaqueDataDigest(),
			                                         batch_metadata, &context_, GetConnIdHex());
			batch = std::move(resolved.batch);
			batch_metadata = std::move(resolved.metadata);
		} else {
			ValidateWorkerBatch(&context_, batch.get(), base_url_);
		}

		new_state_token = ExtractStreamStateValue(batch_metadata);
		// Only overwrite on a present value, so a trailing state-token-only
		// batch does not clobber the data batch's values.
		{
			const idx_t parsed = ParseVgiBatchIndex(batch_metadata, base_url_, violation);
			if (parsed != DConstants::INVALID_INDEX) {
				last_batch_index_ = parsed;
			}
			std::string pv = ParseVgiB64Bytes(batch_metadata, "vgi_partition_values#b64", base_url_, violation);
			if (!pv.empty()) {
				last_partition_values_bytes_ = std::move(pv);
			}
			std::string pr = ParseVgiB64Bytes(batch_metadata, "vgi_rpc.parent_row#b64", base_url_, violation);
			if (!pr.empty()) {
				last_parent_row_bytes_ = std::move(pr);
			}
			auto cc = ParseVgiCacheControl(batch_metadata);
			if (cc.present) {
				last_cache_control_ = cc;
			}
			if (HasMoreOutputStatus(batch_metadata)) {
				last_more_output_ = true;
			}
		}
		if (!output_batch) {
			output_batch = batch;
		}
	}

	if (!new_state_token.empty()) {
		stream_state_token_ = new_state_token;
	} else {
		// No new state token: the server is done.
		stream_state_token_.clear();
		data_finished_ = true;
	}

	return output_batch;
}

void HttpFunctionConnection::CancelStream(const std::vector<uint8_t> &state_token, ClientContext &live_context) {
	// Prefer the token captured by the caller (immune to concurrent mutation)
	// over the live member.
	std::string token;
	if (!state_token.empty()) {
		token.assign(state_token.begin(), state_token.end());
	} else if (!stream_state_token_.empty()) {
		token = stream_state_token_;
	} else {
		return;
	}

	VgiMetadata metadata {{RPC_STREAM_STATE_KEY, token}, {std::string(generated::VGI_RPC_CANCEL_KEY), "1"}};
	if (!call_state_token_.empty()) {
		metadata.Set(RPC_CALL_STATE_KEY, call_state_token_);
	}
	std::vector<uint8_t> body;
	try {
		auto cancel_batch = VgiBatch::MakeEmpty(VgiSchema::Empty());
		body = SerializeBatch(*cancel_batch, &metadata)->ToVector();
	} catch (const std::exception &) {
		// Best-effort: a cancel that cannot be encoded is dropped.
		return;
	}

	std::string exchange_url = base_url_ + "/" + std::string(VGI_MAIN_PROTOCOL.name) + "/init/exchange";
	auto c_auth = attach_params_ ? attach_params_->auth() : nullptr;
	// live_context, never context_: see IFunctionConnection::CancelStream.
	auto c_cached_params = attach_params_ && IsHttpTransport(exchange_url)
	    ? attach_params_->GetOrInitHttpParams(live_context, exchange_url) : nullptr;
	// Best-effort: dispatcher catches any thrown exception.
	(void)HttpPostArrowIpc(live_context, exchange_url, body, c_auth,
	                        /*cookie_jar=*/nullptr, c_cached_params, nullptr, nullptr,
	                        attach_params_ ? attach_params_->iroh() : nullptr,
	                        &live_context);
}

} // namespace vgi
} // namespace duckdb
