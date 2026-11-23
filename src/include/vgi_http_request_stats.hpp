// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once

// Process-wide counters of the request bodies this extension POSTs to each
// HTTP worker, read from SQL with vgi_http_request_stats(). Protocol 3.4's
// large-value tests use them to prove that every request of a query stays
// under the worker's turn budget once arguments travel by reference
// (docs/large-values-3.4.md §4, "max-request-bytes counter").

#include <cstdint>
#include <string>
#include <vector>

namespace duckdb {
class ExtensionLoader;

namespace vgi {

struct HttpRequestStats {
	std::string worker_url;
	// POSTs to the worker (unary calls, stream init/exchange/tick/cancel, upload
	// grants), and the largest and total body bytes sent: the uncompressed Arrow
	// IPC body as posted, i.e. after any whole-request upload replaced it with a
	// pointer. VGI-Max-Request-Bytes applies to exactly this size.
	uint64_t requests = 0;
	uint64_t max_request_bytes = 0;
	uint64_t total_request_bytes = 0;
	// Whole-request uploads (a body over VGI-Max-Request-Bytes PUT through an
	// upload URL and replaced by a pointer).
	uint64_t request_uploads = 0;
	uint64_t request_upload_bytes = 0;
	// Argument references: arguments uploaded once and sent as
	// BindRequest.arguments_location, and binds that reused an upload from the
	// query's memo instead of uploading again.
	uint64_t arguments_ref_uploads = 0;
	uint64_t arguments_ref_upload_bytes = 0;
	uint64_t arguments_ref_reuses = 0;
};

// Normalized worker base URL (no trailing slash), as the HTTP client uses it.
void RecordHttpRequestBody(const std::string &base_url, uint64_t body_bytes);
void RecordHttpRequestUpload(const std::string &base_url, uint64_t body_bytes);
void RecordArgumentsRefUpload(const std::string &base_url, uint64_t argument_bytes);
void RecordArgumentsRefReuse(const std::string &base_url);

// A snapshot, one entry per worker URL, sorted by URL. `reset` clears the
// counters after taking it.
std::vector<HttpRequestStats> SnapshotHttpRequestStats(bool reset);

// vgi_http_request_stats([reset := false]).
void RegisterVgiHttpRequestStatsFunction(ExtensionLoader &loader);

} // namespace vgi
} // namespace duckdb
