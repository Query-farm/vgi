// © Copyright 2025, 2026 Query Farm LLC - https://query.farm

#include "vgi_iroh_native.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"

#include <openssl/rand.h>

#if VGI_ENABLE_IROH
#include <vgi_iroh.h>
#endif

#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <limits>
#include <map>
#include <mutex>

namespace duckdb {
namespace vgi {

#if VGI_ENABLE_IROH

namespace {

const char *IrohStageName(uint32_t stage) {
	switch (stage) {
	case VGI_IROH_STAGE_PARSE: return "parse";
	case VGI_IROH_STAGE_BIND: return "bind";
	case VGI_IROH_STAGE_RESOLVE: return "resolve";
	case VGI_IROH_STAGE_CONNECT: return "connect";
	case VGI_IROH_STAGE_ALPN: return "alpn";
	case VGI_IROH_STAGE_OPEN_STREAM: return "open_stream";
	case VGI_IROH_STAGE_WRITE: return "write";
	case VGI_IROH_STAGE_READ: return "read";
	case VGI_IROH_STAGE_CANCEL: return "cancel";
	case VGI_IROH_STAGE_CLOSE: return "close";
	case VGI_IROH_STAGE_INTERNAL: return "internal";
	default: return "unknown";
	}
}

const char *IrohCategoryName(uint32_t category) {
	switch (category) {
	case VGI_IROH_CATEGORY_INVALID_INPUT: return "invalid_input";
	case VGI_IROH_CATEGORY_UNSUPPORTED: return "unsupported";
	case VGI_IROH_CATEGORY_UNAVAILABLE: return "unavailable";
	case VGI_IROH_CATEGORY_TIMEOUT: return "timeout";
	case VGI_IROH_CATEGORY_PROTOCOL: return "protocol";
	case VGI_IROH_CATEGORY_CONNECTION_RESET: return "connection_reset";
	case VGI_IROH_CATEGORY_CANCELLED: return "cancelled";
	case VGI_IROH_CATEGORY_AUTHENTICATION: return "authentication";
	case VGI_IROH_CATEGORY_RESOURCE_EXHAUSTED: return "resource_exhausted";
	case VGI_IROH_CATEGORY_INTERNAL: return "internal";
	default: return "unknown";
	}
}

const char *IrohDispatchName(uint32_t dispatch) {
	switch (dispatch) {
	case VGI_IROH_DISPATCH_NOT_SENT: return "not_sent";
	case VGI_IROH_DISPATCH_UNKNOWN: return "unknown";
	case VGI_IROH_DISPATCH_SENT: return "sent";
	default: return "unknown";
	}
}

std::string FormatIrohError(const char *operation, const vgi_iroh_error &error) {
	return StringUtil::Format("VGI Iroh %s failed (stage=%s category=%s dispatch=%s): %s", operation,
	                          IrohStageName(error.stage), IrohCategoryName(error.category),
	                          IrohDispatchName(error.dispatch_certainty),
	                          error.message[0] ? error.message : "unspecified transport error");
}

uint8_t CheckDuckDBInterrupted(void *userdata) {
	auto context = static_cast<ClientContext *>(userdata);
	return context && context->interrupted.load() ? 1 : 0;
}

[[noreturn]] void ThrowIroh(const char *operation, const vgi_iroh_error &error) {
	// The Rust ABI deliberately returns a sanitized bounded message. Include
	// structured stage/category/certainty for diagnostics without ever logging
	// keys, relay credentials, or request bodies.
	throw IOException(FormatIrohError(operation, error));
}

void CheckIroh(vgi_iroh_result result, const char *operation, const vgi_iroh_error &error) {
	if (result != VGI_IROH_OK) {
		ThrowIroh(operation, error);
	}
}

std::string CopyEndpointId(const vgi_iroh_endpoint *endpoint) {
	vgi_iroh_error error {};
	size_t required = 0;
	CheckIroh(vgi_iroh_endpoint_id(endpoint, nullptr, 0, &required, &error), "endpoint id", error);
	std::string result(required, '\0');
	if (required) {
		CheckIroh(vgi_iroh_endpoint_id(endpoint, result.data(), result.size(), &required, &error),
		          "endpoint id", error);
	}
	if (required != 65 || result.back() != '\0') {
		throw IOException("VGI Iroh endpoint returned an invalid endpoint ID");
	}
	result.resize(required - 1);
	return result;
}

std::string CopyRemoteId(const vgi_iroh_stream *stream) {
	vgi_iroh_error error {};
	size_t required = 0;
	CheckIroh(vgi_iroh_stream_remote_id(stream, nullptr, 0, &required, &error), "remote id", error);
	std::string result(required, '\0');
	if (required) {
		CheckIroh(vgi_iroh_stream_remote_id(stream, result.data(), result.size(), &required, &error),
		          "remote id", error);
	}
	if (required != 65 || result.back() != '\0') {
		throw IOException("VGI Iroh stream returned an invalid remote endpoint ID");
	}
	result.resize(required - 1);
	return result;
}

std::string CopyHttpRemoteId(const vgi_iroh_http_response *response) {
	vgi_iroh_error error {};
	size_t required = 0;
	CheckIroh(vgi_iroh_http_response_remote_id(response, nullptr, 0, &required, &error),
	          "HTTP remote id", error);
	std::string result(required, '\0');
	if (required) {
		CheckIroh(vgi_iroh_http_response_remote_id(response, result.data(), result.size(), &required, &error),
		          "HTTP remote id", error);
	}
	if (required != 65 || result.back() != '\0') {
		throw IOException("VGI Iroh HTTP response returned an invalid remote endpoint ID");
	}
	result.resize(required - 1);
	return result;
}

std::string EphemeralRegistryKey(const IrohClientConfig &config) {
	std::string result = config.no_relay ? "disabled" : (config.relay_urls.empty() ? "default" : "custom");
	for (const auto &relay : config.relay_urls) {
		result.push_back('\n');
		result.append(relay);
	}
	result.append("\nconnect_timeout_seconds=");
	result.append(std::to_string(config.connect_timeout_seconds));
	result.append("\nio_timeout_seconds=");
	result.append(std::to_string(config.io_timeout_seconds));
	return result;
}

struct StreamHandle {
	explicit StreamHandle(vgi_iroh_stream *value) : value(value) {
	}
	~StreamHandle() {
		if (value) {
			vgi_iroh_stream_cancel(value);
			vgi_iroh_stream_free(value);
		}
	}

	vgi_iroh_stream *value;
	std::mutex write_mutex;
};

struct WipingString {
	explicit WipingString(std::string value) : value(std::move(value)) {
	}
	~WipingString() {
		volatile char *bytes = value.empty() ? nullptr : value.data();
		for (size_t i = 0; i < value.size(); ++i) bytes[i] = 0;
	}
	std::string value;
};

const IrohSecretKey &ProcessEphemeralSecret() {
	// Operational endpoint settings may differ between ATTACHes, but an omitted
	// key still denotes one process identity. Generate that identity once with a
	// CSPRNG, then feed it to every operational endpoint instead of asking each
	// Rust endpoint to generate a different key.
	static const auto *secret = []() {
		std::array<unsigned char, 32> raw {};
		if (RAND_bytes(raw.data(), static_cast<int>(raw.size())) != 1) {
			throw IOException("VGI Iroh could not generate a process-stable ephemeral identity");
		}
		static constexpr char kHex[] = "0123456789abcdef";
		std::string encoded(raw.size() * 2, '\0');
		for (size_t i = 0; i < raw.size(); ++i) {
			encoded[i * 2] = kHex[raw[i] >> 4];
			encoded[i * 2 + 1] = kHex[raw[i] & 0x0f];
		}
		volatile unsigned char *bytes = raw.data();
		for (size_t i = 0; i < raw.size(); ++i) {
			bytes[i] = 0;
		}
		return new IrohSecretKey(std::move(encoded));
	}();
	return *secret;
}

class IrohInputStream final : public arrow::io::InputStream {
public:
	IrohInputStream(std::shared_ptr<StreamHandle> stream, ClientContext *context, uint64_t io_timeout_seconds)
	    : stream_(std::move(stream)), context_(context), io_timeout_(std::chrono::seconds(io_timeout_seconds)) {
	}

	arrow::Status Close() override {
		closed_ = true;
		return arrow::Status::OK();
	}
	bool closed() const override {
		return closed_;
	}
	arrow::Result<int64_t> Tell() const override {
		return position_;
	}
	arrow::Result<int64_t> Read(int64_t nbytes, void *out) override {
		if (closed_) {
			return arrow::Status::Invalid("Iroh input stream is closed");
		}
		if (nbytes < 0) {
			return arrow::Status::Invalid("negative Iroh read size");
		}
		uint8_t *target = static_cast<uint8_t *>(out);
		int64_t total = 0;
		auto last_progress = std::chrono::steady_clock::now();
		while (total < nbytes) {
			if (context_ && context_->interrupted) {
				vgi_iroh_stream_cancel(stream_->value);
				return arrow::Status::Cancelled("VGI Iroh request cancelled");
			}
			size_t got = 0;
			uint8_t timed_out = 0;
			vgi_iroh_error error {};
			const size_t want = static_cast<size_t>(std::min<int64_t>(nbytes - total, 1LL << 30));
			auto result = vgi_iroh_stream_read_timeout(stream_->value, target + total, want,
			                                                   250, &got, &timed_out, &error);
			if (result != VGI_IROH_OK) {
				return arrow::Status::IOError(FormatIrohError("read", error));
			}
			if (timed_out) {
				if (std::chrono::steady_clock::now() - last_progress >= io_timeout_) {
					vgi_iroh_stream_cancel(stream_->value);
					return arrow::Status::IOError("VGI Iroh read exceeded its idle timeout");
				}
				continue;
			}
			if (got == 0) {
				break;
			}
			total += static_cast<int64_t>(got);
			last_progress = std::chrono::steady_clock::now();
		}
		position_ += total;
		return total;
	}
	arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
		ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes));
		ARROW_ASSIGN_OR_RAISE(auto count, Read(nbytes, buffer->mutable_data()));
		ARROW_RETURN_NOT_OK(buffer->Resize(count, false));
		return std::shared_ptr<arrow::Buffer>(std::move(buffer));
	}

private:
	std::shared_ptr<StreamHandle> stream_;
	ClientContext *context_;
	std::chrono::steady_clock::duration io_timeout_;
	int64_t position_ = 0;
	bool closed_ = false;
};

class IrohOutputStream final : public arrow::io::OutputStream {
public:
	IrohOutputStream(std::shared_ptr<StreamHandle> stream, ClientContext *context)
	    : stream_(std::move(stream)), context_(context) {
	}

	arrow::Status Close() override {
		closed_ = true;
		return arrow::Status::OK();
	}
	bool closed() const override {
		return closed_;
	}
	arrow::Result<int64_t> Tell() const override {
		return position_;
	}
	arrow::Status Write(const void *data, int64_t nbytes) override {
		if (closed_) {
			return arrow::Status::Invalid("Iroh output stream is closed");
		}
		if (nbytes < 0) {
			return arrow::Status::Invalid("negative Iroh write size");
		}
		std::lock_guard<std::mutex> lock(stream_->write_mutex);
		vgi_iroh_error error {};
		auto result = vgi_iroh_stream_write_cancellable(
		    stream_->value, static_cast<const uint8_t *>(data), static_cast<size_t>(nbytes),
		    CheckDuckDBInterrupted, context_, &error);
		if (result != VGI_IROH_OK) {
			if (error.category == VGI_IROH_CATEGORY_CANCELLED) {
				return arrow::Status::Cancelled("VGI Iroh write cancelled");
			}
			return arrow::Status::IOError(FormatIrohError("write", error));
		}
		position_ += nbytes;
		return arrow::Status::OK();
	}
	arrow::Status Flush() override {
		return arrow::Status::OK();
	}

private:
	std::shared_ptr<StreamHandle> stream_;
	ClientContext *context_;
	int64_t position_ = 0;
	bool closed_ = false;
};

} // namespace

class IrohNativeEndpoint {
public:
	explicit IrohNativeEndpoint(const IrohClientConfig &config) {
		WipingString secret(config.secret_key ? config.secret_key->CopyEncoded()
		                                     : ProcessEphemeralSecret().CopyEncoded());
		std::vector<const char *> relay_ptrs;
		relay_ptrs.reserve(config.relay_urls.size());
		for (const auto &relay : config.relay_urls) {
			relay_ptrs.push_back(relay.c_str());
		}
		vgi_iroh_endpoint_config native {};
		native.abi_version = VGI_IROH_ABI_VERSION;
		native.secret_key = secret.value.empty() ? nullptr : secret.value.c_str();
		native.relay_mode = config.no_relay ? VGI_IROH_RELAY_DISABLED
		                                   : (relay_ptrs.empty() ? VGI_IROH_RELAY_DEFAULT
		                                                         : VGI_IROH_RELAY_CUSTOM);
		native.relay_urls = relay_ptrs.empty() ? nullptr : relay_ptrs.data();
		native.relay_url_count = relay_ptrs.size();
		native.connect_timeout_ms = config.connect_timeout_seconds * 1000;
		native.io_timeout_ms = config.io_timeout_seconds * 1000;
		vgi_iroh_error error {};
		vgi_iroh_endpoint *created = nullptr;
		CheckIroh(vgi_iroh_endpoint_create(&native, &created, &error), "endpoint creation", error);
		std::unique_ptr<vgi_iroh_endpoint, decltype(&vgi_iroh_endpoint_free)> guard(
		    created, &vgi_iroh_endpoint_free);
		local_id_ = CopyEndpointId(created);
		endpoint_ = guard.release();
	}

	~IrohNativeEndpoint() {
		if (endpoint_) {
			vgi_iroh_endpoint_cancel(endpoint_);
			vgi_iroh_endpoint_free(endpoint_);
		}
	}

	vgi_iroh_endpoint *get() const {
		return endpoint_;
	}
	const std::string &local_id() const {
		return local_id_;
	}

private:
	vgi_iroh_endpoint *endpoint_ = nullptr;
	std::string local_id_;
};

std::shared_ptr<IrohNativeEndpoint>
GetIrohNativeEndpoint(const std::shared_ptr<IrohClientConfig> &config) {
	if (!config) {
		throw InternalException("missing VGI Iroh client configuration");
	}
	if (config->identity_source != IrohIdentitySource::EPHEMERAL) {
		// A configured secret deterministically yields the same public identity;
		// keeping it per ATTACH avoids putting secret material in a registry key.
		struct Entry {
			std::weak_ptr<IrohClientConfig> config;
			std::shared_ptr<IrohNativeEndpoint> endpoint;
		};
		// These registries deliberately outlive C++ static destruction. Destroying
		// a process-stable Iroh endpoint after Tokio's thread-local runtime state
		// has already gone away can panic during executable shutdown. The OS owns
		// final process cleanup; expired configured entries are still reclaimed
		// during normal operation below.
		static auto mutex = new std::mutex();
		static auto endpoints = new std::map<const IrohClientConfig *, Entry>();
		std::lock_guard<std::mutex> lock(*mutex);
		for (auto it = endpoints->begin(); it != endpoints->end();) {
			it = it->second.config.expired() ? endpoints->erase(it) : std::next(it);
		}
		auto &entry = (*endpoints)[config.get()];
		if (!entry.endpoint) {
			entry.config = config;
			entry.endpoint = std::make_shared<IrohNativeEndpoint>(*config);
		}
		return entry.endpoint;
	}

	// No secret is involved, so relay configuration is safe as the registry
	// key. Hold strongly to guarantee a process-lifetime ephemeral identity.
	static auto mutex = new std::mutex();
	static auto endpoints = new std::map<std::string, std::shared_ptr<IrohNativeEndpoint>>();
	std::lock_guard<std::mutex> lock(*mutex);
	auto &endpoint = (*endpoints)[EphemeralRegistryKey(*config)];
	if (!endpoint) {
		endpoint = std::make_shared<IrohNativeEndpoint>(*config);
	}
	return endpoint;
}

IrohNativeDuplex OpenIrohArrowMuxStream(const std::shared_ptr<IrohClientConfig> &config,
	                                    ClientContext *context) {
	if (!config || config->protocol != IrohProtocol::ARROW_MUX) {
		throw InternalException("VGI raw Iroh stream requested with a non-arrow-mux configuration");
	}
	auto endpoint = GetIrohNativeEndpoint(config);
	vgi_iroh_remote remote {};
	remote.endpoint_id = config->endpoint_id.c_str();
	remote.relay_url = config->remote_relay_url.empty() ? nullptr : config->remote_relay_url.c_str();
	std::vector<const char *> direct_addresses;
	direct_addresses.reserve(config->direct_addresses.size());
	for (const auto &address : config->direct_addresses) {
		direct_addresses.push_back(address.c_str());
	}
	remote.direct_addresses = direct_addresses.empty() ? nullptr : direct_addresses.data();
	remote.direct_address_count = direct_addresses.size();
	vgi_iroh_stream *raw = nullptr;
	vgi_iroh_error error {};
	auto open_result = vgi_iroh_stream_open_cancellable(endpoint->get(), &remote,
	                                                   CheckDuckDBInterrupted, context,
	                                                   &raw, &error);
	if (open_result != VGI_IROH_OK && error.category == VGI_IROH_CATEGORY_CANCELLED) {
		throw InterruptException();
	}
	CheckIroh(open_result, "stream open", error);
	auto stream = std::make_shared<StreamHandle>(raw);
	IrohNativeDuplex result;
	result.input = std::make_shared<IrohInputStream>(stream, context, config->io_timeout_seconds);
	result.output = std::make_shared<IrohOutputStream>(stream, context);
	result.cancel = [stream]() { vgi_iroh_stream_cancel(stream->value); };
	result.local_endpoint_id = endpoint->local_id();
	result.remote_endpoint_id = CopyRemoteId(raw);
	if (result.remote_endpoint_id != config->endpoint_id) {
		vgi_iroh_stream_cancel(raw);
		throw IOException("VGI Iroh authenticated peer identity did not match the requested EndpointId");
	}
	return result;
}

IrohNativeHttpResponse PerformIrohHttpRequest(
    const std::shared_ptr<IrohClientConfig> &config, ClientContext *context,
    const std::string &method, const std::string &path,
    const std::vector<std::pair<std::string, std::string>> &headers,
    const uint8_t *body, size_t body_size, uint64_t max_body_bytes) {
	if (!config || config->protocol != IrohProtocol::HTTP) {
		throw InternalException("VGI Iroh HTTP request requested with a non-HTTP configuration");
	}
	auto endpoint = GetIrohNativeEndpoint(config);
	vgi_iroh_remote remote {};
	remote.endpoint_id = config->endpoint_id.c_str();
	remote.relay_url = config->remote_relay_url.empty() ? nullptr : config->remote_relay_url.c_str();
	std::vector<const char *> direct_addresses;
	direct_addresses.reserve(config->direct_addresses.size());
	for (const auto &address : config->direct_addresses) {
		direct_addresses.push_back(address.c_str());
	}
	remote.direct_addresses = direct_addresses.empty() ? nullptr : direct_addresses.data();
	remote.direct_address_count = direct_addresses.size();
	std::vector<vgi_iroh_header> native_headers;
	native_headers.reserve(headers.size());
	for (const auto &header : headers) {
		native_headers.push_back({reinterpret_cast<const uint8_t *>(header.first.data()), header.first.size(),
		                          reinterpret_cast<const uint8_t *>(header.second.data()), header.second.size()});
	}
	vgi_iroh_http_request request {};
	request.method = method.c_str();
	request.path = path.c_str();
	request.headers = native_headers.empty() ? nullptr : native_headers.data();
	request.header_count = native_headers.size();
	request.body = body;
	request.body_len = body_size;
	vgi_iroh_http_response *raw = nullptr;
	vgi_iroh_error error {};
	auto request_result = vgi_iroh_http_request_start_cancellable(
	    endpoint->get(), &remote, &request, CheckDuckDBInterrupted, context, &raw, &error);
	if (request_result != VGI_IROH_OK && error.category == VGI_IROH_CATEGORY_CANCELLED) {
		throw InterruptException();
	}
	CheckIroh(request_result, "HTTP request", error);
	std::unique_ptr<vgi_iroh_http_response, decltype(&vgi_iroh_http_response_free)> response(
	    raw, &vgi_iroh_http_response_free);
	if (CopyHttpRemoteId(raw) != config->endpoint_id) {
		vgi_iroh_http_response_cancel(raw);
		throw IOException("VGI Iroh HTTP authenticated peer identity did not match the requested EndpointId");
	}

	IrohNativeHttpResponse result;
	result.status = vgi_iroh_http_response_status(raw);
	const size_t header_count = vgi_iroh_http_response_header_count(raw);
	result.headers.reserve(header_count);
	for (size_t i = 0; i < header_count; ++i) {
		size_t name_size = 0, value_size = 0;
		CheckIroh(vgi_iroh_http_response_header(raw, i, nullptr, 0, &name_size, nullptr, 0,
		                                          &value_size, &error), "HTTP response header", error);
		std::string name(name_size, '\0'), value(value_size, '\0');
		CheckIroh(vgi_iroh_http_response_header(raw, i,
		                                          reinterpret_cast<uint8_t *>(name.data()), name.size(),
		                                          &name_size,
		                                          reinterpret_cast<uint8_t *>(value.data()), value.size(),
		                                          &value_size, &error), "HTTP response header", error);
		result.headers.emplace_back(std::move(name), std::move(value));
	}

	uint8_t chunk[64 * 1024];
	auto last_progress = std::chrono::steady_clock::now();
	while (true) {
		if (context && context->interrupted) {
			vgi_iroh_http_response_cancel(raw);
			throw InterruptException();
		}
		size_t count = 0;
		uint8_t timed_out = 0;
		CheckIroh(vgi_iroh_http_response_read_timeout(raw, chunk, sizeof(chunk), 250, &count,
		                                                  &timed_out, &error),
		          "HTTP response read", error);
		if (timed_out) {
			if (std::chrono::steady_clock::now() - last_progress >=
			    std::chrono::seconds(config->io_timeout_seconds)) {
				vgi_iroh_http_response_cancel(raw);
				throw IOException("VGI Iroh HTTP response exceeded its idle timeout");
			}
			continue;
		}
		if (count == 0) {
			break;
		}
		if (max_body_bytes && (count > max_body_bytes || result.body.size() > max_body_bytes - count)) {
			vgi_iroh_http_response_cancel(raw);
			throw IOException("VGI Iroh HTTP response exceeded the configured encoded response limit");
		}
		result.body.insert(result.body.end(), chunk, chunk + count);
		last_progress = std::chrono::steady_clock::now();
	}
	return result;
}

#else

class IrohNativeEndpoint {};

[[noreturn]] static void ThrowIrohDisabled() {
	throw IOException("Native Iroh support was not compiled into this VGI extension");
}

std::shared_ptr<IrohNativeEndpoint>
GetIrohNativeEndpoint(const std::shared_ptr<IrohClientConfig> &) {
	ThrowIrohDisabled();
}

IrohNativeDuplex OpenIrohArrowMuxStream(const std::shared_ptr<IrohClientConfig> &, ClientContext *) {
	ThrowIrohDisabled();
}

IrohNativeHttpResponse PerformIrohHttpRequest(
    const std::shared_ptr<IrohClientConfig> &, ClientContext *, const std::string &,
    const std::string &, const std::vector<std::pair<std::string, std::string>> &,
    const uint8_t *, size_t, uint64_t) {
	ThrowIrohDisabled();
}

#endif

} // namespace vgi
} // namespace duckdb
