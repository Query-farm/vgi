// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace duckdb {
class ClientContext;
namespace vgi {

// Binary SAB envelope shared with vgi-rpc-iroh-browser/js/adapter-worker.ts.
// All integers are little-endian. Request/response bodies are framed into
// bounded chunks even though HttpFunctionConnection currently buffers them.
namespace httpi {
constexpr uint8_t kMagic[4] = {'V', 'G', 'I', 'H'};
constexpr uint8_t kVersion = 1;
constexpr uint8_t kRequest = 1;
constexpr uint8_t kResponse = 2;
constexpr uint16_t kRawRepresentation = 1;
constexpr uint8_t kBodyChunk = 1;
constexpr uint8_t kBodyEnd = 2;
constexpr uint8_t kBodyTerminal = 3;
constexpr size_t kChunkBytes = 64 * 1024;
constexpr size_t kMaxHeaders = 1024;
constexpr size_t kMaxHeaderBytes = 1024 * 1024;
constexpr size_t kMaxTerminalDetailBytes = 512;
constexpr size_t kMaxBufferedBodyBytes = 1024ULL * 1024ULL * 1024ULL;

enum class Stage : uint8_t {
	NONE = 0,
	PARSE = 1,
	RESOLVE = 2,
	CONNECT = 3,
	REQUEST = 4,
	RESPONSE_HEAD = 5,
	RESPONSE_BODY = 6,
};

enum class Category : uint8_t {
	NONE = 0,
	INVALID_REQUEST = 1,
	UNAUTHORIZED_TARGET = 2,
	UNAVAILABLE = 3,
	TIMEOUT = 4,
	CANCELLED = 5,
	PROTOCOL = 6,
	TRANSPORT = 7,
	INTERNAL = 8,
};

enum class DispatchCertainty : uint8_t {
	NONE = 0,
	NOT_DISPATCHED = 1,
	DISPATCHED = 2,
	AMBIGUOUS = 3,
};

struct Header {
	std::string name;
	std::string value;
};

struct Response {
	int status = 0;
	std::vector<Header> headers;
	std::string body;
	bool raw_representation = false;
};

inline const char *StageName(Stage stage) {
	switch (stage) {
	case Stage::PARSE:
		return "parse";
	case Stage::RESOLVE:
		return "resolve";
	case Stage::CONNECT:
		return "connect";
	case Stage::REQUEST:
		return "request";
	case Stage::RESPONSE_HEAD:
		return "response_head";
	case Stage::RESPONSE_BODY:
		return "response_body";
	default:
		return "none";
	}
}

inline const char *CategoryName(Category category) {
	switch (category) {
	case Category::INVALID_REQUEST:
		return "invalid_request";
	case Category::UNAUTHORIZED_TARGET:
		return "unauthorized_target";
	case Category::UNAVAILABLE:
		return "unavailable";
	case Category::TIMEOUT:
		return "timeout";
	case Category::CANCELLED:
		return "cancelled";
	case Category::PROTOCOL:
		return "protocol";
	case Category::TRANSPORT:
		return "transport";
	case Category::INTERNAL:
		return "internal";
	default:
		return "none";
	}
}

inline const char *CertaintyName(DispatchCertainty certainty) {
	switch (certainty) {
	case DispatchCertainty::NOT_DISPATCHED:
		return "not_dispatched";
	case DispatchCertainty::DISPATCHED:
		return "dispatched";
	case DispatchCertainty::AMBIGUOUS:
		return "ambiguous";
	default:
		return "none";
	}
}

#if defined(__EMSCRIPTEN__)
Response BrowserPost(ClientContext &context, const std::string &url, const std::vector<Header> &headers,
                     const uint8_t *body, size_t body_size, uint64_t timeout_seconds);
#endif

} // namespace httpi
} // namespace vgi
} // namespace duckdb
