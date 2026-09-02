// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_iroh_config.hpp"

#include <arrow/api.h>
#include <arrow/io/api.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace duckdb {

class ClientContext;

namespace vgi {

// Native, extension-owned Iroh endpoint. The implementation is deliberately
// hidden behind this C++ façade so no Iroh/Rust types enter the DuckDB ABI.
class IrohNativeEndpoint;

// One bidirectional QUIC stream speaking vgi-rpc/arrow-mux/1. Input and output
// wrappers share ownership and never close the opposite half implicitly.
struct IrohNativeDuplex {
	std::shared_ptr<arrow::io::InputStream> input;
	std::shared_ptr<arrow::io::OutputStream> output;
	// Context-free, thread-safe cancellation. The dispatcher may invoke this
	// after the originating query's ClientContext has already been destroyed.
	std::function<void()> cancel;
	std::string local_endpoint_id;
	std::string remote_endpoint_id;
};

struct IrohNativeHttpResponse {
	uint16_t status = 0;
	// Ordered and duplicate-preserving, as required for Set-Cookie.
	std::vector<std::pair<std::string, std::string>> headers;
	std::vector<uint8_t> body;
};

// Returns a process-reused endpoint for an immutable ATTACH configuration.
// Ephemeral identities remain stable for the process lifetime; configured
// secret keys deterministically produce their requested identity.
std::shared_ptr<IrohNativeEndpoint>
GetIrohNativeEndpoint(const std::shared_ptr<IrohClientConfig> &config);

IrohNativeDuplex OpenIrohArrowMuxStream(const std::shared_ptr<IrohClientConfig> &config,
	                                    ClientContext *context);

// Issue one iroh-http/2 request. max_body_bytes is enforced while streaming,
// before body growth; zero means no application limit (the Rust layer still
// enforces its configured I/O deadline). No redirect or automatic retry occurs.
IrohNativeHttpResponse PerformIrohHttpRequest(
    const std::shared_ptr<IrohClientConfig> &config, ClientContext *context,
    const std::string &method, const std::string &path,
    const std::vector<std::pair<std::string, std::string>> &headers,
    const uint8_t *body, size_t body_size, uint64_t max_body_bytes);

} // namespace vgi
} // namespace duckdb
