// © Copyright 2026 Query Farm LLC - https://query.farm
#include "vgi_webworker_function_connection.hpp"
#include "vgi_query_rpc.hpp"

// Only the wasm extension and the native SAB unit test (-DVGI_SAB_NATIVE_TEST,
// which provides the ring stubs) link the ring functions.
#if defined(__EMSCRIPTEN__) || defined(VGI_SAB_NATIVE_TEST)

#include "duckdb.hpp"
#include "duckdb/common/types/blob.hpp"

#include "vgi_arrow_utils.hpp"
#include "vgi_bind_protocol.hpp"
#include "vgi_cache_control.hpp"
#include "vgi_catalog_metadata.hpp"
#include "generated/vgi_protocol_constants.hpp"
#include "generated/vgi_request_builders.hpp"
#include "vgi_exception.hpp"
#include "vgi_logging.hpp"
#include "vgi_httpi.hpp"
#include "vgi_rpc_client.hpp"
#include "vgi_rpc_types.hpp"
#include "vgi_sab_abi.hpp"
#include "vgi_sab_stream.hpp"
#include "vgi_schema_registry.hpp"
#include "vgi_table_buffering_builders.hpp"
#include "vgi_transport.hpp"
#include "vgi_worker_pool.hpp" // complete PooledWorker for ReleaseForPooling's deleter

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iterator>
#include <map>
#include <mutex>
#include <optional>

namespace duckdb {
namespace vgi {

// SAB entry points onto ReadWorkerUnaryStream / ReadWorkerStreamHeader.
// SabInputStream::Read polls the ClientContext for cancellation. The ring is a
// lenient transport: a stream ending without the Arrow EOS marker ends the read,
// and errors while draining after the data batch are ignored.
namespace {

// `function_kind` token for a connection's function_type_ ("TABLE",
// "SCALAR_FUNCTION", "TABLE_BUFFERING", ...).
std::string WebWorkerFunctionKindToken(const std::string &function_type) {
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

ErrorInfo WebWorkerFunctionInfo(const std::string &function_name, const std::string &function_type,
                                const char *subtype = nullptr) {
	ErrorInfo info = subtype ? ErrorInfo(subtype) : ErrorInfo();
	info.Function(function_name, WebWorkerFunctionKindToken(function_type));
	return info;
}

// ThrowVgiIOException plus the caller's extra_info; the message is built
// exactly as ThrowVgiIOException builds it.
template <typename... ARGS>
[[noreturn]] void ThrowWebWorkerIOException(ErrorInfo info, const std::string &msg, const std::string &worker_path,
                                            pid_t worker_pid, const std::string &execution_id_hex, ARGS... params) {
	info.Worker(worker_path, worker_pid, execution_id_hex);
	throw IOException(info, BuildMessageWithContext(msg, worker_path), params...);
}

WorkerStreamOptions SabStreamOptions(ClientContext *context, const std::string &worker_path) {
	WorkerStreamOptions opts;
	opts.context = context;
	opts.worker = worker_path;
	opts.lenient = true;
	return opts;
}

// Read a unary response from the slot's response ring: dispatch log/error
// until the first data batch, then drain to EOS.
UnaryResponseResult SabReadUnaryResponse(int region_offset, int slot, ClientContext *context,
                                         const std::string &worker_path, const std::string &execution_id_hex = "",
                                         const std::string &attach_opaque_data_digest = "",
                                         const std::string &transaction_opaque_data_digest = "",
                                         const std::string &conn_id_hex = "") {
	auto opts = SabStreamOptions(context, worker_path);
	opts.execution_id_hex = execution_id_hex;
	opts.attach_opaque_data_digest = attach_opaque_data_digest;
	opts.transaction_opaque_data_digest = transaction_opaque_data_digest;
	opts.conn_id_hex = conn_id_hex;
	return ReadWorkerUnaryStream(std::make_shared<SabInputStream>(region_offset, slot, context), opts);
}

// Read the stream header; the data IPC stream then begins on the same ring (a
// fresh SabInputStream/reader picks it up), exactly as on the subprocess fd.
StreamHeaderResult SabReadStreamHeader(int region_offset, int slot, ClientContext *context,
                                       const std::string &worker_path) {
	return ReadWorkerStreamHeader(std::make_shared<SabInputStream>(region_offset, slot, context),
	                              SabStreamOptions(context, worker_path));
}

// Inner batch of a buffered-table unary response; null when `result` is null.
VgiBatchPtr DecodeOuterResponse(const UnaryResponseResult &response, const std::string &method_name,
                                const std::string &worker_path) {
	return UnwrapResultEnvelope(response.batch, method_name, ErrorInfo().Worker(worker_path), /*null_result_ok=*/true);
}

// The zero-row, zero-column tick batch (immutable; shared by every producer stream).
const VgiBatchPtr &EmptyTickBatch() {
	static const VgiBatchPtr batch = VgiBatch::MakeEmpty(VgiSchema::Empty());
	return batch;
}

} // namespace

#if defined(__EMSCRIPTEN__)
namespace {
// Defined below; WebWorkerInvokeUnary needs it first.
int EnsureVgiSabRegion(const std::string &canonical_target);
} // namespace

// Publish the region offset onto the calling pthread's JS realm. The offset the
// JS ring stubs read through (`vgiSab.base`) is per-realm, and DuckDB can run a
// connection's ring I/O on a different pthread than the one that bound it; with
// a stale offset its reads hit the wrong memory and block forever. SabInputStream
// / SabOutputStream call this before every Read/Write (a cheap JS field store).
void VgiSabEnsureChannelOnRealm(int region_offset) {
	vgi_wasm_set_channel(region_offset);
}
#else
void VgiSabEnsureChannelOnRealm(int) {
}
#endif

// See the header. On the native test the ensure-worker stub is a no-op and the
// in-process ring backend serves.
UnaryResponseResult WebWorkerInvokeUnary(ClientContext &context, const std::string &worker_path,
                                         const std::string &method_name,
                                         const VgiBatchPtr &params,
                                         const VgiProtocolId &protocol) {
	const std::string location = CanonicalizeBrowserWorkerTarget(worker_path);
	int region_offset = 0;
#if defined(__EMSCRIPTEN__)
	region_offset = EnsureVgiSabRegion(location);
	vgi_wasm_set_channel(region_offset);
#endif
	if (vgi_wasm_ensure_worker(location.c_str(), region_offset) != 0) {
		ThrowVgiIOException("Failed to spawn/ready the VGI Web Worker", location, -1, "");
	}
	int slot = vgi_wasm_slot_open(location.c_str(), region_offset);
	if (slot < 0) {
		ThrowVgiIOException("Failed to open SAB slot (channel exhausted)", location, -1, "");
	}
	struct SlotReleaser {
		int region_offset;
		int slot;
		~SlotReleaser() {
			if (slot >= 0) {
				vgi_wasm_slot_release(region_offset, slot);
			}
		}
	} releaser {region_offset, slot};

	std::vector<uint8_t> request = params ? SerializeRpcRequest(method_name, params, protocol)
	                                      : SerializeEmptyRpcRequest(method_name, protocol);
	auto out = std::make_shared<SabOutputStream>(region_offset, slot);
	try {
		out->Write(request.data(), static_cast<int64_t>(request.size()));
	} catch (const std::exception &e) {
		ThrowVgiIOException("Failed to write unary request: %s", location, -1, "", RawMessageOf(e));
	}
	// EOS on c2w: the worker serves exactly this one request, then returns.
	vgi_wasm_slot_write_eos(region_offset, slot);
	return SabReadUnaryResponse(region_offset, slot, &context, location);
}

// WebWorkerFunctionConnection

WebWorkerFunctionConnection::WebWorkerFunctionConnection(
    const std::string &location, const std::string &function_name, const ArrowArguments &arguments,
    const std::vector<uint8_t> &attach_opaque_data, const std::vector<uint8_t> &transaction_opaque_data,
    ClientContext &context, const std::string &function_type, const std::vector<uint8_t> &global_execution_id,
    const std::map<std::string, Value> &settings, const std::vector<VgiSecretRequirement> &required_secrets)
    : conn_id_hex_(VgiGenerateConnId()), location_(CanonicalizeBrowserWorkerTarget(location)),
      function_name_(function_name), function_type_(function_type), arguments_batch_(arguments.batch),
      attach_opaque_data_(attach_opaque_data),
      transaction_opaque_data_(transaction_opaque_data), global_execution_id_(global_execution_id), context_(context),
      settings_(settings), required_secrets_(required_secrets) {
}

WebWorkerFunctionConnection::~WebWorkerFunctionConnection() {
	if (slot_ >= 0) {
		// Ring EOF on c2w ends the worker's serve loop for this slot, so it belongs
		// here, not at the end of a stream: a connection outlives its streams (each
		// split re-inits it). Idempotent.
		vgi_wasm_slot_write_eos(region_offset_, slot_);
		vgi_wasm_slot_release(region_offset_, slot_);
		slot_ = -1;
	}
}

#if defined(__EMSCRIPTEN__)
namespace {
// Each canonical target owns an independent ABI-v1 region (byte-compatible with
// v1, so a legacy worker module serves one region unchanged). Beyond the region
// limit, the least recently used region is recycled, and only while all its slots
// are free, so a live connection is never rebound underneath an operation.
struct SabRegionEntry {
	int offset;
	uint64_t last_used;
};

#ifndef VGI_SAB_MAX_TARGET_REGIONS
#define VGI_SAB_MAX_TARGET_REGIONS 32
#endif
static_assert(VGI_SAB_MAX_TARGET_REGIONS > 0, "VGI_SAB_MAX_TARGET_REGIONS must be positive");
constexpr size_t kMaximumSabTargetRegions = VGI_SAB_MAX_TARGET_REGIONS;

bool SabRegionIsIdle(int offset) {
	auto *h = reinterpret_cast<int32_t *>(static_cast<intptr_t>(offset));
	const int n_slots = h[sab::HDR_N_SLOTS];
	const int stride = h[sab::HDR_SLOT_STRIDE];
	const int slots_off = h[sab::HDR_SLOTS_OFF];
	for (int slot = 0; slot < n_slots; ++slot) {
		auto *control = reinterpret_cast<int32_t *>(reinterpret_cast<uint8_t *>(h) + slots_off + slot * stride);
		if (__atomic_load_n(&control[sab::SLOT_STATE], __ATOMIC_ACQUIRE) != sab::kSlotFree) {
			return false;
		}
	}
	return true;
}

void InitializeSabRegion(int offset) {
	const int n_slots = 4;
	const int ring_cap = sab::kDefaultRingCap;
	const int stride = sab::SlotStride(ring_cap);
	const int bytes = sab::kHeaderBytes + n_slots * stride;
	auto *mem = reinterpret_cast<void *>(static_cast<intptr_t>(offset));
	std::memset(mem, 0, static_cast<size_t>(bytes));
	auto *h = reinterpret_cast<int32_t *>(mem);
	h[sab::HDR_MAGIC] = static_cast<int32_t>(sab::kMagic);
	h[sab::HDR_VERSION] = sab::kVersion;
	h[sab::HDR_N_SLOTS] = n_slots;
	h[sab::HDR_RING_CAP] = ring_cap;
	h[sab::HDR_SLOT_STRIDE] = stride;
	h[sab::HDR_SLOTS_OFF] = sab::kHeaderBytes;
	h[sab::HDR_FEATURES] = static_cast<int32_t>(sab::kSupportedFeatures);
}

int AllocateSabRegion() {
	const int stride = sab::SlotStride(sab::kDefaultRingCap);
	const int bytes = sab::kHeaderBytes + 4 * stride;
	void *mem = std::malloc(static_cast<size_t>(bytes));
	if (mem == nullptr) {
		throw IOException("VGI worker: failed to allocate SAB target region (%d bytes)", bytes);
	}
	const int offset = static_cast<int>(reinterpret_cast<intptr_t>(mem));
	InitializeSabRegion(offset);
	return offset;
}

int EnsureVgiSabRegion(const std::string &canonical_target) {
	static std::map<std::string, SabRegionEntry> g_regions;
	static uint64_t g_touch = 0;
	static std::mutex g_mutex;
	std::lock_guard<std::mutex> lk(g_mutex);
	auto found = g_regions.find(canonical_target);
	if (found != g_regions.end()) {
		found->second.last_used = ++g_touch;
		return found->second.offset;
	}
	int offset = 0;
	if (g_regions.size() < kMaximumSabTargetRegions) {
		offset = AllocateSabRegion();
	} else {
		auto victim = g_regions.end();
		for (auto it = g_regions.begin(); it != g_regions.end(); ++it) {
			if (SabRegionIsIdle(it->second.offset) &&
			    (victim == g_regions.end() || it->second.last_used < victim->second.last_used)) {
				victim = it;
			}
		}
		if (victim == g_regions.end()) {
			throw IOException("VGI worker: target region limit reached (%d active targets)",
			                  static_cast<int>(kMaximumSabTargetRegions));
		}
		offset = victim->second.offset;
		g_regions.erase(victim);
		InitializeSabRegion(offset);
	}
	g_regions.emplace(canonical_target, SabRegionEntry {offset, ++g_touch});
	return offset;
}
} // namespace
#endif

#if defined(__EMSCRIPTEN__)
namespace httpi {
namespace {

void AppendU16(std::vector<uint8_t> &out, uint16_t value) {
	out.push_back(static_cast<uint8_t>(value));
	out.push_back(static_cast<uint8_t>(value >> 8));
}

void AppendU32(std::vector<uint8_t> &out, uint32_t value) {
	for (int shift = 0; shift < 32; shift += 8) {
		out.push_back(static_cast<uint8_t>(value >> shift));
	}
}

void AppendBytes(std::vector<uint8_t> &out, const std::string &value) {
	out.insert(out.end(), value.begin(), value.end());
}

void WriteAll(SabOutputStream &out, const uint8_t *data, size_t size, const std::string &url) {
	try {
		out.Write(data, static_cast<int64_t>(size));
	} catch (const std::exception &e) {
		throw IOException("VGI HTTPI request write failed: %s [url: %s]", RawMessageOf(e), url);
	}
}

void ReadExact(SabInputStream &in, void *data, size_t size, const std::string &url) {
	int64_t got = 0;
	try {
		got = in.Read(static_cast<int64_t>(size), data);
	} catch (const std::exception &e) {
		throw IOException("VGI HTTPI response read failed: %s [url: %s]", RawMessageOf(e), url);
	}
	if (static_cast<size_t>(got) != size) {
		throw IOException("VGI HTTPI response ended early (wanted %llu bytes, got %llu) [url: %s]",
		                  static_cast<unsigned long long>(size), static_cast<unsigned long long>(got), url);
	}
}

uint16_t ReadU16(SabInputStream &in, const std::string &url) {
	uint8_t bytes[2];
	ReadExact(in, bytes, sizeof(bytes), url);
	return static_cast<uint16_t>(bytes[0]) | (static_cast<uint16_t>(bytes[1]) << 8);
}

uint32_t ReadU32(SabInputStream &in, const std::string &url) {
	uint8_t bytes[4];
	ReadExact(in, bytes, sizeof(bytes), url);
	return static_cast<uint32_t>(bytes[0]) | (static_cast<uint32_t>(bytes[1]) << 8) |
	       (static_cast<uint32_t>(bytes[2]) << 16) | (static_cast<uint32_t>(bytes[3]) << 24);
}

std::string ReadString(SabInputStream &in, uint32_t size, const std::string &url) {
	std::string result(size, '\0');
	if (size > 0) {
		ReadExact(in, result.data(), size, url);
	}
	return result;
}

bool ValidHeaderName(const std::string &name) {
	if (name.empty())
		return false;
	for (unsigned char c : name) {
		const bool alnum = std::isalnum(c);
		const bool token = c == '!' || c == '#' || c == '$' || c == '%' || c == '&' || c == '\'' || c == '*' ||
		                   c == '+' || c == '-' || c == '.' || c == '^' || c == '_' || c == '`' || c == '|' || c == '~';
		if (!alnum && !token)
			return false;
	}
	return true;
}

bool ValidHeaderValue(const std::string &value) {
	return std::none_of(value.begin(), value.end(),
	                    [](unsigned char c) { return (c < 0x20 && c != '\t') || c == 0x7f; });
}

bool HeaderNameIs(const std::string &actual, const char *expected) {
	const auto expected_size = std::strlen(expected);
	if (actual.size() != expected_size)
		return false;
	for (size_t i = 0; i < expected_size; ++i) {
		if (std::tolower(static_cast<unsigned char>(actual[i])) !=
		    std::tolower(static_cast<unsigned char>(expected[i])))
			return false;
	}
	return true;
}

uint64_t ParseResponseMaximum(const std::string &value, const std::string &url) {
	static constexpr uint64_t kMinimum = 65536;
	static constexpr uint64_t kMaximum = 9007199254740991ULL;
	if (value.empty() || value.front() == '0') {
		throw IOException("VGI-Max-Response-Bytes must match [1-9][0-9]* [url: %s]", url);
	}
	uint64_t parsed = 0;
	for (const auto byte : value) {
		if (byte < '0' || byte > '9') {
			throw IOException("VGI-Max-Response-Bytes must match [1-9][0-9]* [url: %s]", url);
		}
		const auto digit = static_cast<uint64_t>(byte - '0');
		if (parsed > (kMaximum - digit) / 10) {
			throw IOException("VGI-Max-Response-Bytes is outside 65536..=2^53-1 [url: %s]", url);
		}
		parsed = parsed * 10 + digit;
	}
	if (parsed < kMinimum || parsed > kMaximum) {
		throw IOException("VGI-Max-Response-Bytes is outside 65536..=2^53-1 [url: %s]", url);
	}
	return parsed;
}

bool KnownCompressedRepresentation(const Header &header) {
	if (!HeaderNameIs(header.name, "Content-Encoding") &&
	    !HeaderNameIs(header.name, "X-VGI-Content-Encoding")) {
		return false;
	}
	std::string value;
	value.reserve(header.value.size());
	for (const auto byte : header.value) {
		if (byte != ' ' && byte != '\t')
			value.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(byte))));
	}
	return value == "gzip" || value == "zstd";
}

} // namespace

Response BrowserRequest(ClientContext &context, const std::string &url, const std::string &method,
                        const std::vector<Header> &headers, const uint8_t *body, size_t body_size,
                        uint64_t timeout_seconds, size_t max_buffered_response_bytes,
                        size_t max_decoded_response_bytes) {
	auto parsed = ParseHttpiUrl(url);
	const std::string target = "httpi://" + parsed.endpoint_id;
	const std::string path = parsed.path.empty() ? "/" : parsed.path;
	if ((method != "POST" && method != "OPTIONS") || method.size() > UINT16_MAX ||
	    headers.size() > kMaxHeaders || path.size() > UINT32_MAX || body_size > INT64_MAX ||
	    max_buffered_response_bytes == 0 || max_buffered_response_bytes > kMaxBufferedBodyBytes ||
	    max_decoded_response_bytes == 0) {
		throw IOException("VGI HTTPI request exceeds envelope limits [url: %s]", url);
	}
	size_t header_bytes = 0;
	for (const auto &header : headers) {
		header_bytes += header.name.size() + header.value.size();
		if (header.name.size() > UINT32_MAX || header.value.size() > UINT32_MAX || header_bytes > kMaxHeaderBytes) {
			throw IOException("VGI HTTPI request headers exceed envelope limits [url: %s]", url);
		}
	}

	const int region_offset = EnsureVgiSabRegion(target);
	vgi_wasm_set_channel(region_offset);
	if (vgi_wasm_ensure_worker(target.c_str(), region_offset) != 0) {
		throw IOException("VGI HTTPI adapter rejected target before dispatch [url: %s]", url);
	}
	int slot = vgi_wasm_slot_open(target.c_str(), region_offset);
	if (slot < 0) {
		throw IOException("VGI HTTPI adapter has no available SAB slot [url: %s]", url);
	}
	struct SlotReleaser {
		int region_offset;
		int slot;
		~SlotReleaser() {
			if (slot >= 0)
				vgi_wasm_slot_release(region_offset, slot);
		}
	} releaser {region_offset, slot};

	std::vector<uint8_t> request;
	request.reserve(32 + path.size() + header_bytes);
	request.insert(request.end(), std::begin(kMagic), std::end(kMagic));
	request.push_back(kVersion);
	request.push_back(kRequest);
	AppendU16(request, 0);
	AppendU16(request, static_cast<uint16_t>(method.size()));
	AppendU16(request, 0);
	AppendU32(request, static_cast<uint32_t>(path.size()));
	AppendU32(request, static_cast<uint32_t>(headers.size()));
	AppendBytes(request, method);
	AppendBytes(request, path);
	for (const auto &header : headers) {
		AppendU32(request, static_cast<uint32_t>(header.name.size()));
		AppendU32(request, static_cast<uint32_t>(header.value.size()));
		AppendBytes(request, header.name);
		AppendBytes(request, header.value);
	}
	SabOutputStream output(region_offset, slot);
	WriteAll(output, request.data(), request.size(), url);
	for (size_t offset = 0; offset < body_size;) {
		const size_t length = std::min(kChunkBytes, body_size - offset);
		uint8_t frame[8] = {kBodyChunk,
		                    0,
		                    0,
		                    0,
		                    static_cast<uint8_t>(length),
		                    static_cast<uint8_t>(length >> 8),
		                    static_cast<uint8_t>(length >> 16),
		                    static_cast<uint8_t>(length >> 24)};
		WriteAll(output, frame, sizeof(frame), url);
		WriteAll(output, body + offset, length, url);
		offset += length;
	}
	const uint8_t end_frame[8] = {kBodyEnd, 0, 0, 0, 0, 0, 0, 0};
	WriteAll(output, end_frame, sizeof(end_frame), url);
	vgi_wasm_slot_write_eos(region_offset, slot);

	std::optional<std::chrono::steady_clock::time_point> deadline;
	if (timeout_seconds > 0) {
		deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_seconds);
	}
	SabInputStream input(region_offset, slot, &context, deadline);
	uint8_t prefix[8];
	ReadExact(input, prefix, sizeof(prefix), url);
	if (!std::equal(std::begin(kMagic), std::end(kMagic), prefix) || prefix[4] != kVersion || prefix[5] != kResponse) {
		throw IOException("VGI HTTPI adapter returned an invalid response envelope [url: %s]", url);
	}
	const uint16_t flags = static_cast<uint16_t>(prefix[6]) | (static_cast<uint16_t>(prefix[7]) << 8);
	Response response;
	response.raw_representation = true;
	response.status = ReadU16(input, url);
	(void)ReadU16(input, url);
	const uint32_t header_count = ReadU32(input, url);
	if (header_count > kMaxHeaders) {
		throw IOException("VGI HTTPI response has too many headers [url: %s]", url);
	}
	const auto head_kind = ClassifyResponseHead(flags, static_cast<uint16_t>(response.status), header_count);
	if (head_kind == ResponseHeadKind::INVALID) {
		throw IOException("VGI HTTPI adapter returned an invalid response head [url: %s]", url);
	}
	const bool terminal_only = head_kind == ResponseHeadKind::TERMINAL_ONLY;
	header_bytes = 0;
	response.headers.reserve(header_count);
	for (uint32_t i = 0; i < header_count; ++i) {
		const uint32_t name_size = ReadU32(input, url);
		const uint32_t value_size = ReadU32(input, url);
		header_bytes += static_cast<size_t>(name_size) + value_size;
		if (header_bytes > kMaxHeaderBytes) {
			throw IOException("VGI HTTPI response headers exceed envelope limits [url: %s]", url);
		}
		auto name = ReadString(input, name_size, url);
		auto value = ReadString(input, value_size, url);
		if (!ValidHeaderName(name) || !ValidHeaderValue(value)) {
			throw IOException("VGI HTTPI adapter returned an invalid response header [url: %s]", url);
		}
		response.headers.push_back({std::move(name), std::move(value)});
	}
	size_t response_body_limit = max_buffered_response_bytes;
	bool compressed_representation = false;
	std::optional<uint64_t> advertised_maximum;
	for (const auto &header : response.headers) {
		compressed_representation = compressed_representation || KnownCompressedRepresentation(header);
		if (HeaderNameIs(header.name, "VGI-Max-Response-Bytes")) {
			if (advertised_maximum.has_value()) {
				throw IOException("VGI-Max-Response-Bytes must occur exactly once [url: %s]", url);
			}
			advertised_maximum = ParseResponseMaximum(header.value, url);
		}
	}
	if (!compressed_representation) {
		response_body_limit = std::min(response_body_limit, max_decoded_response_bytes);
		if (advertised_maximum) {
			response_body_limit = static_cast<size_t>(
			    std::min<uint64_t>(response_body_limit, *advertised_maximum));
		}
	}
	for (;;) {
		uint8_t frame[8];
		ReadExact(input, frame, sizeof(frame), url);
		const uint32_t length = static_cast<uint32_t>(frame[4]) | (static_cast<uint32_t>(frame[5]) << 8) |
		                        (static_cast<uint32_t>(frame[6]) << 16) | (static_cast<uint32_t>(frame[7]) << 24);
		if (frame[0] == kBodyChunk) {
			if (terminal_only) {
				throw IOException("VGI HTTPI terminal-only response contained body bytes [url: %s]", url);
			}
			if (length > kChunkBytes) {
				throw IOException("VGI HTTPI response chunk exceeds envelope limit [url: %s]", url);
			}
			if (length > response_body_limit || response.body.size() > response_body_limit - length) {
				throw IOException("VGI HTTPI response representation exceeds buffered transport limit (%llu) [url: %s]",
				                  static_cast<unsigned long long>(response_body_limit), url);
			}
			auto chunk = ReadString(input, length, url);
			response.body.append(chunk);
			continue;
		}
		if (frame[0] == kBodyEnd && length == 0) {
			if (terminal_only) {
				throw IOException("VGI HTTPI terminal-only response ended without evidence [url: %s]", url);
			}
			break;
		}
		if (frame[0] == kBodyTerminal && length <= kMaxTerminalDetailBytes) {
			auto detail = ReadString(input, length, url);
			auto stage = static_cast<Stage>(frame[1]);
			auto category = static_cast<Category>(frame[2]);
			auto certainty = static_cast<DispatchCertainty>(frame[3]);
			if (frame[1] < static_cast<uint8_t>(Stage::PARSE) ||
			    frame[1] > static_cast<uint8_t>(Stage::RESPONSE_BODY) ||
			    frame[2] < static_cast<uint8_t>(Category::INVALID_REQUEST) ||
			    frame[2] > static_cast<uint8_t>(Category::INTERNAL) ||
			    frame[3] < static_cast<uint8_t>(DispatchCertainty::NOT_DISPATCHED) ||
			    frame[3] > static_cast<uint8_t>(DispatchCertainty::AMBIGUOUS)) {
				throw IOException("VGI HTTPI adapter returned invalid terminal evidence [url: %s]", url);
			}
			throw IOException("VGI HTTPI transport failed stage=%s category=%s dispatch=%s detail=%s; "
			                  "ambiguous requests are never replayed [url: %s]",
			                  StageName(stage), CategoryName(category), CertaintyName(certainty), detail, url);
		}
		throw IOException("VGI HTTPI adapter returned an invalid body frame [url: %s]", url);
	}
	return response;
}

Response BrowserPost(ClientContext &context, const std::string &url, const std::vector<Header> &headers,
                     const uint8_t *body, size_t body_size, uint64_t timeout_seconds,
                     size_t max_buffered_response_bytes, size_t max_decoded_response_bytes) {
	return BrowserRequest(context, url, "POST", headers, body, body_size, timeout_seconds,
	                      max_buffered_response_bytes, max_decoded_response_bytes);
}

} // namespace httpi
#endif

void WebWorkerFunctionConnection::EnsureWorkerSpawned() {
	// Lazy slot open (the analog of the subprocess lazy fork/exec).
	if (slot_ < 0) {
#if defined(__EMSCRIPTEN__)
		// Publish the target's region on this pthread before the slot stubs run.
		// (Native test: static ring.)
		region_offset_ = EnsureVgiSabRegion(location_);
		vgi_wasm_set_channel(region_offset_);
#endif
		// Idempotent; spawns once per location and blocks until the worker serves
		// the region. A no-op on the native test.
		if (vgi_wasm_ensure_worker(location_.c_str(), region_offset_) != 0) {
			ThrowWebWorkerIOException(WebWorkerFunctionInfo(function_name_, function_type_, error_subtype::kConnectFailed), "Failed to spawn/ready the VGI Web Worker", location_, -1, "");
		}
		slot_ = vgi_wasm_slot_open(location_.c_str(), region_offset_);
		if (slot_ < 0) {
			ThrowWebWorkerIOException(WebWorkerFunctionInfo(function_name_, function_type_, error_subtype::kLimitExceeded), "Failed to open SAB slot (channel exhausted)", location_, -1, "");
		}
	}
}

BindResult WebWorkerFunctionConnection::PerformBindRpc() {
	EnsureWorkerSpawned();

	int64_t num_args = arguments_batch_ ? arguments_batch_->num_rows() : 0;
	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		fields.emplace_back("function_type", function_type_);
		fields.emplace_back("num_args", std::to_string(num_args));
		VGI_LOG(context_, "function_connection.bind", fields);
	}

	auto transport_fn = [&](const std::vector<uint8_t> &request_bytes) -> VgiBatchPtr {
		auto rpc_params = generated::BuildBindParams(request_bytes);
		ValidateRequestSchema(rpc_params, "bind", location_);

		auto request = SerializeRpcRequest("bind", rpc_params);
		auto out = std::make_shared<SabOutputStream>(region_offset_, slot_);
		try {
			out->Write(request.data(), static_cast<int64_t>(request.size()));
		} catch (const std::exception &e) {
			ThrowWebWorkerIOException(WebWorkerFunctionInfo(function_name_, function_type_, nullptr), "Failed to write bind request: %s", location_, -1, "", RawMessageOf(e));
		}

		auto response = SabReadUnaryResponse(region_offset_, slot_, &context_, location_);
		auto bind_batch = UnwrapResultEnvelope(response.batch, "bind",
		                                       WebWorkerFunctionInfo(function_name_, function_type_, nullptr).Worker(location_));
		ValidateResponseSchema(bind_batch, "bind", location_);
		return bind_batch;
	};

	auto bind_result = PerformBindProtocol(
	    context_, function_name_, function_type_, arguments_batch_, input_schema_, attach_opaque_data_,
	    transaction_opaque_data_, settings_, required_secrets_, location_, transport_fn, at_unit_, at_value_,
	    copy_from_ ? &*copy_from_ : nullptr, copy_to_ ? &*copy_to_ : nullptr, schema_name_, argument_names_);

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		fields.emplace_back("num_output_columns", std::to_string(bind_result.output_schema->num_fields()));
		fields.emplace_back("has_opaque_data", bind_result.opaque_data.empty() ? "false" : "true");
		VGI_LOG(context_, "function_connection.bind_result", fields);
	}
	return bind_result;
}

InitResult WebWorkerFunctionConnection::PerformInit(
    const BindResult &bind_result, const std::vector<int32_t> &projection_ids,
    VgiBufferPtr pushdown_filters, std::vector<VgiBufferPtr> join_keys,
    const std::string &phase, const std::optional<OrderByHint> &order_by,
    const std::optional<TableSampleHint> &table_sample, const std::vector<uint8_t> &init_opaque_data,
    const std::optional<std::vector<uint8_t>> &finalize_state_id, const std::vector<std::string> &split_tokens) {
	if (slot_ < 0) {
		ThrowVgiIOException("WebWorkerFunctionConnection::PerformInit called before EnsureWorkerSpawned", location_, -1,
		                    GetExecutionIdHex());
	}
	if (init_done_) {
		ThrowVgiIOException("WebWorkerFunctionConnection::PerformInit called twice", location_, -1,
		                    GetExecutionIdHex());
	}
	// A new stream: its worker has applied no dynamic-filter delta yet.
	tick_filter_cursor_.Reset();

	std::vector<int64_t> projection_ids_64;
	projection_ids_64.reserve(projection_ids.size());
	for (auto id : projection_ids) {
		projection_ids_64.push_back(static_cast<int64_t>(id));
	}

	auto &execution_id = global_execution_id_;

	std::string ob_col, ob_dir, ob_null;
	int64_t ob_limit = -1;
	if (order_by.has_value()) {
		ob_col = order_by->column_name;
		ob_dir = order_by->direction;
		ob_null = order_by->null_order;
		ob_limit = order_by->row_limit;
	}

	double ts_percentage = -1.0;
	int64_t ts_seed = -1;
	if (table_sample.has_value()) {
		ts_percentage = table_sample->sample_percentage;
		ts_seed = table_sample->seed;
	}

	auto init_request = BuildInitRequest(bind_result.bind_request_bytes, bind_result.output_schema_bytes,
	                                      bind_result.opaque_data, projection_ids_64, pushdown_filters, join_keys, phase,
	                                      execution_id, init_opaque_data, ob_col, ob_dir, ob_null, ob_limit,
	                                      ts_percentage, ts_seed, finalize_state_id, substream_id_, split_tokens);
	auto init_request_bytes = SerializeToIpcBytes(init_request);

	auto rpc_params = generated::BuildInitParams(init_request_bytes);
	ValidateRequestSchema(rpc_params, "init", location_);

	{
		auto request = SerializeRpcRequest("init", rpc_params);
		auto out = std::make_shared<SabOutputStream>(region_offset_, slot_);
		try {
			out->Write(request.data(), static_cast<int64_t>(request.size()));
		} catch (const std::exception &e) {
			ThrowWebWorkerIOException(WebWorkerFunctionInfo(function_name_, function_type_, nullptr), "Failed to write init request: %s", location_, -1, GetExecutionIdHex(),
			                    RawMessageOf(e));
		}
	}

	auto header = SabReadStreamHeader(region_offset_, slot_, &context_, location_);
	auto init_response = ParseGlobalInitResponse(header.header_batch, location_);
	execution_id_ = init_response.execution_id;

	// A worker may not write its output schema until it has input, so:
	//   - Producer mode: send the first tick before opening the data reader.
	//   - Exchange mode: open the data reader at the first ReadDataBatch, after
	//     the caller has written an input batch.
	// The FINALIZE phases always run in producer mode.
	bool producer_phase_override = (phase == "FINALIZE" || phase == "TABLE_BUFFERING_FINALIZE");
	if (!input_schema_ || producer_phase_override) {
		is_producer_mode_ = true;
		const auto &tick_batch = EmptyTickBatch();
		tick_schema_ = tick_batch->schema();

		auto sink = std::make_shared<SabOutputStream>(region_offset_, slot_);
		try {
			input_writer_ = VgiIpcWriter::Open(sink, tick_schema_);
		} catch (const std::exception &e) {
			ThrowWebWorkerIOException(WebWorkerFunctionInfo(function_name_, function_type_, nullptr), "Failed to create tick writer: %s", location_, -1, GetExecutionIdHex(),
			                    RawMessageOf(e));
		}
		input_writer_opened_ = true;

		// Conditional-revalidation validators ride the first tick.
		std::optional<VgiMetadata> first_tick_metadata;
		if (!cond_if_none_match_.empty() || !cond_if_modified_since_.empty()) {
			first_tick_metadata.emplace();
			if (!cond_if_none_match_.empty()) {
				first_tick_metadata->Append(VGI_CACHE_IF_NONE_MATCH_KEY, cond_if_none_match_);
			}
			if (!cond_if_modified_since_.empty()) {
				first_tick_metadata->Append(VGI_CACHE_IF_MODIFIED_SINCE_KEY, cond_if_modified_since_);
			}
		}
		try {
			input_writer_->Write(*tick_batch, first_tick_metadata ? &*first_tick_metadata : nullptr);
		} catch (const std::exception &e) {
			ThrowWebWorkerIOException(WebWorkerFunctionInfo(function_name_, function_type_, nullptr), "Failed to write initial tick batch: %s", location_, -1, GetExecutionIdHex(),
			                    RawMessageOf(e));
		}

		data_stream_ = std::make_shared<SabInputStream>(region_offset_, slot_, &context_);
		try {
			data_reader_ = VgiIpcReader::Open(data_stream_);
		} catch (const std::exception &e) {
			ThrowWebWorkerIOException(WebWorkerFunctionInfo(function_name_, function_type_, nullptr), "Failed to open data stream: %s", location_, -1, GetExecutionIdHex(),
			                    RawMessageOf(e));
		}
		if (function_name_ == VGI_QUERY_EXECUTOR &&
		    !data_reader_->schema()->Equals(*bind_result.output_schema, true)) {
			throw IOException("vgi_query output stream schema differs from the prepared schema");
		}
	} else {
		is_producer_mode_ = false;

		auto sink = std::make_shared<SabOutputStream>(region_offset_, slot_);
		try {
			input_writer_ = VgiIpcWriter::Open(sink, input_schema_);
		} catch (const std::exception &e) {
			ThrowWebWorkerIOException(WebWorkerFunctionInfo(function_name_, function_type_, nullptr), "Failed to create input writer: %s", location_, -1, GetExecutionIdHex(),
			                    RawMessageOf(e));
		}
		input_writer_opened_ = true;
	}

	init_done_ = true;

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		fields.emplace_back("max_workers", std::to_string(init_response.max_workers));
		fields.emplace_back("is_producer_mode", is_producer_mode_ ? "true" : "false");
		fields.emplace_back("phase", phase.empty() ? "default" : phase);
		VGI_LOG(context_, "function_connection.init_result", fields);
	}

	return InitResult{init_response.execution_id, init_response.max_workers, init_response.opaque_data};
}

void WebWorkerFunctionConnection::PerformFinalizeInit(const BindResult &bind_result) {
	if (!init_done_) {
		ThrowVgiIOException("WebWorkerFunctionConnection::PerformFinalizeInit called before PerformInit", location_, -1,
		                    GetExecutionIdHex());
	}

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		VGI_LOG(context_, "function_connection.finalize_init", fields);
	}

	if (input_writer_ && !input_writer_closed_) {
		try {
			input_writer_->Close();
		} catch (const std::exception &e) {
			ThrowWebWorkerIOException(WebWorkerFunctionInfo(function_name_, function_type_, nullptr), "Failed to close input writer for finalize: %s", location_, -1, GetExecutionIdHex(),
			                    RawMessageOf(e));
		}
	}
	input_writer_.reset();
	input_writer_opened_ = false;
	input_writer_closed_ = false;

	// With 0 input rows the reader was never opened and the INPUT phase's output
	// stream is still on the w2c ring: drain it, or SabReadStreamHeader would read
	// it as the FINALIZE header.
	if (!data_reader_ && slot_ >= 0) {
		data_stream_ = std::make_shared<SabInputStream>(region_offset_, slot_, &context_);
		try {
			data_reader_ = VgiIpcReader::Open(data_stream_);
		} catch (const std::exception &) {
			// Nothing readable to drain.
		}
	}
	while (data_reader_) {
		try {
			if (!data_reader_->ReadNext()) {
				break;
			}
		} catch (const std::exception &) {
			break;
		}
	}
	data_reader_.reset();
	data_stream_.reset();

	init_done_ = false;
	data_finished_ = false;

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

void WebWorkerFunctionConnection::ResetForNextSplit() {
	if (!init_done_) {
		ThrowVgiIOException("WebWorkerFunctionConnection::ResetForNextSplit called before PerformInit", location_, -1,
		                    GetExecutionIdHex());
	}

	// Same sequence as PerformFinalizeInit; runs at every split boundary.
	// First close the input writer: its IPC end-of-stream returns the worker's
	// tick reader to its dispatch loop. Otherwise the next init request would
	// land inside an unterminated tick stream.
	if (input_writer_ && !input_writer_closed_) {
		try {
			input_writer_->Close();
		} catch (const std::exception &e) {
			// No poison flag (unlike the subprocess connection): never pooled.
			ThrowWebWorkerIOException(WebWorkerFunctionInfo(function_name_, function_type_, nullptr), "Failed to close input writer between splits: %s", location_, -1, GetExecutionIdHex(),
			                    RawMessageOf(e));
		}
	}
	input_writer_.reset();
	input_writer_opened_ = false;
	input_writer_closed_ = false;

	// Drain any unconsumed output (e.g. an empty split never opened the reader),
	// or the next split's SabReadStreamHeader would read it (see
	// PerformFinalizeInit).
	if (!data_reader_ && slot_ >= 0) {
		data_stream_ = std::make_shared<SabInputStream>(region_offset_, slot_, &context_);
		try {
			data_reader_ = VgiIpcReader::Open(data_stream_);
		} catch (const std::exception &) {
			// Nothing readable to drain.
		}
	}
	while (data_reader_) {
		try {
			if (!data_reader_->ReadNext()) {
				break;
			}
		} catch (const std::exception &) {
			break;
		}
	}
	data_reader_.reset();
	data_stream_.reset();

	// Per-stream state.
	init_done_ = false;
	data_finished_ = false;
	last_batch_index_ = DConstants::INVALID_INDEX;
	last_partition_values_bytes_.clear();
	last_cache_control_ = {};
	last_more_output_ = false;

	// Deliberately kept: the slot, tick_filter_state_ (shared gstate, so dynamic
	// filter pushdown carries into the next split) and substream_id_ (one reader
	// keeps one identity across its splits).
}

VgiBatchPtr WebWorkerFunctionConnection::ReadDataBatch() {
	if (!init_done_) {
		ThrowVgiIOException("WebWorkerFunctionConnection::ReadDataBatch called before PerformInit", location_, -1,
		                    GetExecutionIdHex());
	}
	if (data_finished_) {
		return nullptr;
	}

	// Exchange mode opens the reader lazily.
	if (!data_reader_) {
		if (slot_ < 0) {
			ThrowVgiIOException("WebWorkerFunctionConnection::ReadDataBatch slot is not open", location_, -1,
			                    GetExecutionIdHex());
		}
		data_stream_ = std::make_shared<SabInputStream>(region_offset_, slot_, &context_);
		try {
			data_reader_ = VgiIpcReader::Open(data_stream_);
		} catch (const std::exception &e) {
			ThrowWebWorkerIOException(WebWorkerFunctionInfo(function_name_, function_type_, nullptr), "Failed to open data stream: %s", location_, -1, GetExecutionIdHex(),
			                    RawMessageOf(e));
		}
	}

	// Producer one-ahead pipeline: PerformInit sent the first tick; the next one
	// goes out once a batch is committed to, so the worker produces the next batch
	// while DuckDB consumes this one.
	auto send_next_tick = [this]() {
		if (!(is_producer_mode_ && input_writer_ && !input_writer_closed_)) {
			return;
		}
		std::optional<VgiMetadata> tick_metadata;
		if (tick_filter_state_) {
			// Only a delta this stream has not been sent yet; an unchanged filter
			// rides no tick at all (see UpdateDynamicFilterState).
			auto delta = tick_filter_cursor_.Take(*tick_filter_state_);
			if (!delta.empty()) {
				tick_metadata.emplace();
				tick_metadata->Append("vgi_pushdown_filters", std::move(delta));
			}
		}
		try {
			input_writer_->Write(*EmptyTickBatch(), tick_metadata ? &*tick_metadata : nullptr);
		} catch (const std::exception &) {
			// The worker may already have finished (e.g. finalize with no output):
			// mark the writer closed; the next read returns EOS.
			input_writer_closed_ = true;
		}
	};

	// At producer EOS the worker drains its tick input, so close the tick stream:
	// the Arrow end-of-stream marker ends that drain. Close the stream only, never
	// the ring (vgi_wasm_slot_write_eos ends the worker's serve loop, and the
	// connection is re-inited per split); ring EOF is the destructor's job.
	auto finish_producer_input = [this]() {
		if (is_producer_mode_ && input_writer_ && !input_writer_closed_) {
			try {
				input_writer_->Close();
			} catch (const std::exception &) {
				// Best-effort: the worker may already have gone; the stream is over.
			}
			input_writer_closed_ = true;
		}
	};
	// Store+notify-only teardown for a throw mid-stream; must never wait (see
	// the log-batch catch below).
	auto abandon_slot = [this]() {
		data_finished_ = true;
		input_writer_closed_ = true; // abandon the Arrow writer; ring EOS below drives teardown
		if (slot_ >= 0) {
			vgi_wasm_slot_write_eos(region_offset_, slot_);
			vgi_wasm_slot_release(region_offset_, slot_);
			slot_ = -1;
		}
	};

	while (true) {
		VgiBatchPtr batch;
		try {
			batch = data_reader_->ReadNext();
		} catch (const InvalidInputException &) {
			// Malformed / truncated IPC. With no worker process to probe, treat it
			// as end-of-stream, like the tolerant HTTP path.
			finish_producer_input();
			data_finished_ = true;
			return nullptr;
		} catch (const std::exception &e) {
			ThrowWebWorkerIOException(WebWorkerFunctionInfo(function_name_, function_type_, nullptr), "Failed to read data batch: %s", location_, -1, GetExecutionIdHex(), RawMessageOf(e));
		}

		if (!batch) {
			finish_producer_input();
			data_finished_ = true;
			return nullptr;
		}

		const VgiMetadataPtr &batch_metadata = batch->metadata();

		// Skip log batches. HandleBatchLogMessage throws on a worker error, with the
		// tick stream still open, so the worker's post-turn drain would block
		// forever: abandon the slot (ring EOF) before rethrowing.
		try {
			const auto log_error_context = WebWorkerFunctionInfo(function_name_, function_type_);
			if (HandleBatchLogMessage(batch, batch_metadata, &context_, location_, -1,
			                          GetExecutionIdHex(), GetAttachOpaqueDataDigest(), "", GetConnIdHex(),
			                          &log_error_context)) {
				continue;
			}
		} catch (...) {
			// No blocking / Atomics.wait here: under DuckDB-WASM the scan can run on
			// the Asyncify-instrumented thread, and a wait during exception unwind
			// corrupts the Asyncify coroutine and deadlocks the whole engine. So only
			// store+notify: write_eos (the worker's next c2w read ends its serve loop
			// and it closes w2c itself, so no client drain) and release. No
			// input_writer_->Close() either (it can wait on a full ring). Slot reuse
			// safety comes from the next slot_open's worker-done handshake. Freeing
			// now rather than in the destructor avoids "channel exhausted" for a
			// following scan.
			abandon_slot();
			throw;
		}

		// Known limitation: externalized (location pointer) batches are not
		// supported on worker:; SAB workers stream data inline.
		if (batch_metadata &&
		    ClassifyBatch(batch, batch_metadata) == RpcBatchType::EXTERNAL_LOCATION) {
			throw IOException(WebWorkerFunctionInfo(function_name_, function_type_, error_subtype::kUnsupported)
			                      .Worker(location_, -1, GetExecutionIdHex()),
			                  "VGI worker: external-location batches are not supported over the "
			                  "worker: (SAB) transport [worker: %s]",
			                  location_);
		}

		// A malformed batch fails the scan with the same non-blocking teardown.
		try {
			ValidateWorkerBatch(&context_, batch.get(), location_);
		} catch (...) {
			abandon_slot();
			throw;
		}

		// vgi_partition_values#b64: base64-decoded here; the consumer
		// (InstallBatch) decodes and validates the IPC.
		last_partition_values_bytes_.clear();
		if (batch_metadata) {
			int64_t pv_idx = batch_metadata->FindKey("vgi_partition_values#b64");
			if (pv_idx >= 0) {
				const std::string &b64_value = batch_metadata->value(pv_idx);
				try {
					string_t b64_str(b64_value.data(), static_cast<uint32_t>(b64_value.size()));
					idx_t decoded_size = Blob::FromBase64Size(b64_str);
					last_partition_values_bytes_.resize(decoded_size);
					Blob::FromBase64(b64_str, data_ptr_cast(last_partition_values_bytes_.data()), decoded_size);
				} catch (const std::exception &e) {
					throw IOException(WebWorkerFunctionInfo(function_name_, function_type_,
					                                        error_subtype::kProtocolViolation)
					                      .Worker(location_, -1, GetExecutionIdHex())
					                      .Set(error_key::kMetadataKey, "vgi_partition_values#b64")
					                      .Merge(ExtraInfoOf(e)),
					                  "VGI worker emitted invalid base64 payload in "
					                  "vgi_partition_values#b64: %s [worker: %s]",
					                  RawMessageOf(e), location_);
				}
			}
		}

		last_batch_index_ = DConstants::INVALID_INDEX;
		if (batch_metadata) {
			int64_t bi_idx = batch_metadata->FindKey("vgi_batch_index");
			if (bi_idx >= 0) {
				const std::string &value = batch_metadata->value(bi_idx);
				try {
					size_t pos = 0;
					uint64_t parsed = std::stoull(value, &pos);
					if (pos != value.size()) {
						throw IOException(WebWorkerFunctionInfo(function_name_, function_type_, error_subtype::kProtocolViolation)
						                      .Worker(location_, -1, GetExecutionIdHex())
						                      .Set(error_key::kMetadataKey, "vgi_batch_index"),
						                  "VGI worker emitted invalid vgi_batch_index '%s' "
						                  "(trailing characters; expected decimal uint64) [worker: %s]",
						                  value, location_);
					}
					last_batch_index_ = static_cast<idx_t>(parsed);
				} catch (const std::invalid_argument &) {
					throw IOException(WebWorkerFunctionInfo(function_name_, function_type_, error_subtype::kProtocolViolation)
					                      .Worker(location_, -1, GetExecutionIdHex())
					                      .Set(error_key::kMetadataKey, "vgi_batch_index"),
					                  "VGI worker emitted invalid vgi_batch_index '%s' "
					                  "(expected decimal uint64) [worker: %s]",
					                  value, location_);
				} catch (const std::out_of_range &) {
					throw IOException(WebWorkerFunctionInfo(function_name_, function_type_, error_subtype::kProtocolViolation)
					                      .Worker(location_, -1, GetExecutionIdHex())
					                      .Set(error_key::kMetadataKey, "vgi_batch_index"),
					                  "VGI worker emitted vgi_batch_index '%s' that exceeds "
					                  "uint64 range [worker: %s]",
					                  value, location_);
				}
			}
		}

		last_cache_control_ = ParseVgiCacheControl(batch_metadata);
		last_more_output_ = HasMoreOutputStatus(batch_metadata);

		send_next_tick();

		// EOS is the stream ending (null batch), never a 0-row batch.
		return batch;
	}
}

std::unique_ptr<PooledWorker> WebWorkerFunctionConnection::ReleaseForPooling() {
	return nullptr;
}

std::string WebWorkerFunctionConnection::GetExecutionIdHex() const {
	if (execution_id_.empty()) {
		return "";
	}
	return BytesToHex(execution_id_);
}

std::string WebWorkerFunctionConnection::GetAttachOpaqueDataDigest() const {
	if (attach_opaque_data_.empty()) {
		return "";
	}
	return OpaqueDigest(attach_opaque_data_);
}

std::string WebWorkerFunctionConnection::GetTransactionOpaqueDataDigest() const {
	if (transaction_opaque_data_.empty()) {
		return "";
	}
	return OpaqueDigest(transaction_opaque_data_);
}

void WebWorkerFunctionConnection::SetInputSchema(const VgiSchemaPtr &input_schema) {
	input_schema_ = input_schema;
}

void WebWorkerFunctionConnection::UpdateInputSchemaForExecution(const VgiSchemaPtr &input_schema) {
	if (input_writer_opened_) {
		ThrowVgiIOException("WebWorkerFunctionConnection::UpdateInputSchemaForExecution called after OpenInputWriter",
		                    location_, -1, GetExecutionIdHex());
	}
	input_schema_ = input_schema;
}

void WebWorkerFunctionConnection::OpenInputWriter() {
	if (!init_done_) {
		ThrowVgiIOException("WebWorkerFunctionConnection::OpenInputWriter called before PerformInit", location_, -1,
		                    GetExecutionIdHex());
	}
	// Normally already opened by PerformInit: the worker waits for the input
	// schema before writing output.
	if (input_writer_opened_) {
		return;
	}
	if (!input_schema_) {
		ThrowVgiIOException("WebWorkerFunctionConnection::OpenInputWriter called on non-table-in-out function",
		                    location_, -1, GetExecutionIdHex());
	}

	auto sink = std::make_shared<SabOutputStream>(region_offset_, slot_);
	try {
		input_writer_ = VgiIpcWriter::Open(sink, input_schema_);
	} catch (const std::exception &e) {
		ThrowWebWorkerIOException(WebWorkerFunctionInfo(function_name_, function_type_, nullptr), "Failed to create input stream writer: %s", location_, -1, GetExecutionIdHex(),
		                    RawMessageOf(e));
	}
	input_writer_opened_ = true;

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		fields.emplace_back("input_schema_fields", std::to_string(input_schema_->num_fields()));
		VGI_LOG(context_, "function_connection.input_writer_opened", fields);
	}
}

void WebWorkerFunctionConnection::WriteContinuationTick() {
	if (!input_writer_opened_ || input_writer_closed_) {
		ThrowVgiIOException("WebWorkerFunctionConnection::WriteContinuationTick called without an open input writer",
		                    location_, -1, GetExecutionIdHex());
	}
	VgiBatchPtr tick;
	try {
		tick = VgiBatch::MakeEmpty(input_schema_);
	} catch (const std::exception &e) {
		ThrowWebWorkerIOException(WebWorkerFunctionInfo(function_name_, function_type_, nullptr), "Failed to build continuation tick: %s", location_, -1,
		                          GetExecutionIdHex(), RawMessageOf(e));
	}
	try {
		auto metadata = ContinuationTickMetadata();
		input_writer_->Write(*tick, metadata.get());
	} catch (const std::exception &e) {
		ThrowWebWorkerIOException(WebWorkerFunctionInfo(function_name_, function_type_, nullptr), "Failed to write continuation tick: %s", location_, -1,
		                          GetExecutionIdHex(), RawMessageOf(e));
	}
}

void WebWorkerFunctionConnection::WriteInputBatch(const VgiBatchPtr &batch) {
	if (!input_writer_opened_) {
		ThrowVgiIOException("WebWorkerFunctionConnection::WriteInputBatch called before OpenInputWriter", location_, -1,
		                    GetExecutionIdHex());
	}
	if (input_writer_closed_) {
		ThrowVgiIOException("WebWorkerFunctionConnection::WriteInputBatch called after CloseInputWriter", location_, -1,
		                    GetExecutionIdHex());
	}

	// See HttpFunctionConnection::WriteInputBatch.
	auto reconciled = batch;
	if (input_schema_) {
		reconciled = ReconcileBatchToSchema(batch, input_schema_);
	}

	try {
		input_writer_->Write(*reconciled);
	} catch (const std::exception &e) {
		ThrowWebWorkerIOException(WebWorkerFunctionInfo(function_name_, function_type_, nullptr), "Failed to write input batch: %s", location_, -1, GetExecutionIdHex(),
		                    RawMessageOf(e));
	}

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		fields.emplace_back("batch_rows", std::to_string(batch->num_rows()));
		VGI_LOG(context_, "function_connection.input_batch_written", fields);
	}
}

void WebWorkerFunctionConnection::CancelStream(const std::vector<uint8_t> &state_token, ClientContext &live_context) {
	(void)state_token;
	(void)live_context;
	// A closed writer means the worker already saw the stream end.
	if (!input_writer_opened_ || input_writer_closed_ || !input_writer_) {
		return;
	}
	// Producer mode only: the zero-column cancel tick would not match an
	// exchange-mode input schema.
	if (!tick_schema_) {
		return;
	}
	const VgiMetadata metadata {{std::string(generated::VGI_RPC_CANCEL_KEY), "1"}};
	try {
		input_writer_->Write(*EmptyTickBatch(), &metadata);
	} catch (const std::exception &) {
		// Best-effort.
	}
}

void WebWorkerFunctionConnection::CloseInputWriter() {
	if (!input_writer_opened_) {
		ThrowVgiIOException("WebWorkerFunctionConnection::CloseInputWriter called before OpenInputWriter", location_,
		                    -1, GetExecutionIdHex());
	}
	if (input_writer_closed_) {
		return;
	}

	try {
		input_writer_->Close();
	} catch (const std::exception &e) {
		ThrowWebWorkerIOException(WebWorkerFunctionInfo(function_name_, function_type_, nullptr), "Failed to close input stream: %s", location_, -1, GetExecutionIdHex(),
		                    RawMessageOf(e));
	}
	input_writer_.reset();
	input_writer_closed_ = true;
	// Stream EOS only (like the subprocess connection, which keeps stdin open):
	// FINALIZE, buffering RPCs and later splits still use this slot. Ring EOF
	// ends the worker's serve loop and is the destructor's job.

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		VGI_LOG(context_, "function_connection.input_writer_closed", fields);
	}
}

// Buffered table function RPCs: unary request/response on this connection's
// slot, so EnsureWorkerSpawned must have run.

std::vector<uint8_t> WebWorkerFunctionConnection::RpcTableBufferingProcess(
    const std::string &function_name, const std::vector<uint8_t> &execution_id,
    const VgiBatchPtr &input_batch, std::optional<int64_t> batch_index) {
	auto batch_bytes = vgi::SerializeToIpcBytes(input_batch);
	auto rpc_params = vgi::BuildTableBufferingProcessInner(function_name, schema_name_, execution_id, batch_bytes,
	                                                       attach_opaque_data_, batch_index);
	vgi::ValidateRequestSchema(rpc_params, "table_buffering_process", location_);

	auto request = SerializeRpcRequest("table_buffering_process", rpc_params);
	auto out = std::make_shared<SabOutputStream>(region_offset_, slot_);
	try {
		out->Write(request.data(), static_cast<int64_t>(request.size()));
	} catch (const std::exception &e) {
		ThrowWebWorkerIOException(WebWorkerFunctionInfo(function_name_, function_type_, nullptr), "Failed to write table_buffering_process request: %s", location_, -1, GetExecutionIdHex(),
		                    RawMessageOf(e));
	}
	auto response = SabReadUnaryResponse(region_offset_, slot_, &context_, location_, GetExecutionIdHex(),
	                                     GetAttachOpaqueDataDigest(), "", GetConnIdHex());
	auto inner = DecodeOuterResponse(response, "table_buffering_process", location_);
	vgi::ValidateResponseSchema(inner, "table_buffering_process", location_);
	if (!inner || inner->num_rows() == 0) {
		ThrowWebWorkerIOException(WebWorkerFunctionInfo(function_name_, function_type_, error_subtype::kProtocolViolation), "table_buffering_process response missing data", location_, -1, "");
	}
	return vgi::generated::Decode<vgi::generated::TableBufferingProcessResult>(inner, 0, location_).state_id;
}

std::vector<std::vector<uint8_t>>
WebWorkerFunctionConnection::RpcTableBufferingCombine(const std::string &function_name,
                                                      const std::vector<uint8_t> &execution_id,
                                                      const std::vector<std::vector<uint8_t>> &state_ids) {
	auto rpc_params =
	    vgi::BuildTableBufferingCombineInner(function_name, schema_name_, execution_id, state_ids, attach_opaque_data_);
	vgi::ValidateRequestSchema(rpc_params, "table_buffering_combine", location_);

	auto request = SerializeRpcRequest("table_buffering_combine", rpc_params);
	auto out = std::make_shared<SabOutputStream>(region_offset_, slot_);
	try {
		out->Write(request.data(), static_cast<int64_t>(request.size()));
	} catch (const std::exception &e) {
		ThrowWebWorkerIOException(WebWorkerFunctionInfo(function_name_, function_type_, nullptr), "Failed to write table_buffering_combine request: %s", location_, -1, GetExecutionIdHex(),
		                    RawMessageOf(e));
	}
	auto response = SabReadUnaryResponse(region_offset_, slot_, &context_, location_, GetExecutionIdHex(),
	                                     GetAttachOpaqueDataDigest(), "", GetConnIdHex());
	auto inner = DecodeOuterResponse(response, "table_buffering_combine", location_);
	vgi::ValidateResponseSchema(inner, "table_buffering_combine", location_);
	if (!inner || inner->num_rows() == 0) {
		ThrowWebWorkerIOException(WebWorkerFunctionInfo(function_name_, function_type_, error_subtype::kProtocolViolation), "table_buffering_combine response missing data", location_, -1, "");
	}
	return vgi::generated::Decode<vgi::generated::TableBufferingCombineResult>(inner, 0, location_).finalize_state_ids;
}

void WebWorkerFunctionConnection::RpcTableBufferingDestructor(const std::string &function_name,
                                                              const std::vector<uint8_t> &execution_id) {
	auto rpc_params =
	    vgi::BuildTableBufferingDestructorInner(function_name, schema_name_, execution_id, attach_opaque_data_);
	vgi::ValidateRequestSchema(rpc_params, "table_buffering_destructor", location_);

	auto request = SerializeRpcRequest("table_buffering_destructor", rpc_params);
	auto out = std::make_shared<SabOutputStream>(region_offset_, slot_);
	try {
		out->Write(request.data(), static_cast<int64_t>(request.size()));
	} catch (const std::exception &e) {
		ThrowWebWorkerIOException(WebWorkerFunctionInfo(function_name_, function_type_, nullptr), "Failed to write table_buffering_destructor request: %s", location_, -1,
		                    GetExecutionIdHex(), RawMessageOf(e));
	}
	auto response = SabReadUnaryResponse(region_offset_, slot_, &context_, location_, GetExecutionIdHex(),
	                                     GetAttachOpaqueDataDigest(), "", GetConnIdHex());
	auto inner = DecodeOuterResponse(response, "table_buffering_destructor", location_);
	vgi::ValidateResponseSchema(inner, "table_buffering_destructor", location_);
}

} // namespace vgi
} // namespace duckdb

#endif // __EMSCRIPTEN__ || VGI_SAB_NATIVE_TEST
