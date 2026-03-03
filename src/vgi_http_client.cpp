#include "vgi_http_client.hpp"

#include "duckdb.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/enums/http_status_code.hpp"

#include "vgi_logging.hpp"
#include "vgi_rpc_client.hpp"

namespace duckdb {
namespace vgi {

// Helper: strip trailing slash from a URL
static std::string NormalizeBaseUrl(const std::string &url) {
	if (!url.empty() && url.back() == '/') {
		return url.substr(0, url.size() - 1);
	}
	return url;
}

std::string HttpPostArrowIpc(ClientContext &context,
                              const std::string &url,
                              const std::vector<uint8_t> &body) {
	auto &db = *context.db;
	auto &http_util = HTTPUtil::Get(db);
	auto params = http_util.InitializeParameters(context, url);

	// Use the configurable HTTP timeout setting (default 5 minutes)
	Value timeout_val;
	if (context.TryGetCurrentSetting("vgi_http_timeout_seconds", timeout_val)) {
		params->timeout = static_cast<uint64_t>(timeout_val.GetValue<int64_t>());
	} else {
		params->timeout = 300; // fallback: 5 minutes
	}

	HTTPHeaders headers;
	headers.Insert("Content-Type", ARROW_IPC_CONTENT_TYPE);

	PostRequestInfo post(url, headers, *params,
	                     reinterpret_cast<const_data_ptr_t>(body.data()),
	                     static_cast<idx_t>(body.size()));

	auto response = http_util.Request(post);

	// Check for HTTP-level errors
	if (response->status == HTTPStatusCode::Unauthorized_401) {
		throw IOException("VGI HTTP authentication required (HTTP 401) [url: %s]. "
		                  "Response: %s", url, response->body);
	}

	if (!response->Success()) {
		// Try to parse Arrow IPC error batch from the response body
		auto &error_body = post.buffer_out.empty() ? response->body : post.buffer_out;
		if (!error_body.empty()) {
			try {
				auto error_result = ReadUnaryResponseFromBuffer(
				    reinterpret_cast<const uint8_t *>(error_body.data()),
				    error_body.size(), nullptr, url);
				// If ReadUnaryResponseFromBuffer threw an IOException from an EXCEPTION batch,
				// we won't reach here. If we do, throw a generic error.
			} catch (const IOException &e) {
				// Re-throw the parsed VGI error (this is the normal path for server EXCEPTION batches)
				throw;
			} catch (...) {
				// Not a valid Arrow IPC stream — fall through to generic error
			}
		}
		throw IOException("VGI HTTP request failed (HTTP %d): %s [url: %s]",
		                  static_cast<int>(response->status),
		                  error_body.empty() ? response->GetError() : error_body, url);
	}

	// For POST requests, response body is in the PostRequestInfo buffer_out
	return std::move(post.buffer_out);
}

UnaryResponseResult HttpInvokeUnary(ClientContext &context,
                                     const std::string &worker_path,
                                     const std::string &method_name,
                                     const std::shared_ptr<arrow::RecordBatch> &params) {
	std::string base_url = NormalizeBaseUrl(worker_path);
	std::string url = base_url + "/" + method_name;

	VGI_LOG(context, "http.invoke_unary",
	        {{"url", url}, {"method", method_name}});

	// Serialize the RPC request to Arrow IPC bytes
	std::vector<uint8_t> body;
	if (params) {
		body = SerializeRpcRequest(method_name, params);
	} else {
		body = SerializeEmptyRpcRequest(method_name);
	}

	// POST to {worker_path}/{method_name} using standard HTTP timeout
	auto response_body = HttpPostArrowIpc(context, url, body);

	// Parse the Arrow IPC response
	auto result = ReadUnaryResponseFromBuffer(
	    reinterpret_cast<const uint8_t *>(response_body.data()),
	    response_body.size(), &context, url);

	VGI_LOG(context, "http.invoke_unary_result",
	        {{"url", url},
	         {"method", method_name},
	         {"has_batch", result.batch ? "true" : "false"}});

	return result;
}

} // namespace vgi
} // namespace duckdb
