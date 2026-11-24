// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Protocol version negotiation v1 (docs/protocol_versions.md): range parsing,
// the client window, negotiation, refusal parsing, request encoding, the
// session, and ATTACH over HTTP against a server advertising other ranges.

#include "catch.hpp"

#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "httpfs_extension.hpp"
#include "vgi_exception.hpp"
#include "vgi_extension.hpp"
#include "vgi_protocol_codec.hpp"
#include "vgi_protocol_session.hpp"
#include "vgi_rpc_client.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace duckdb;
using namespace duckdb::vgi;

namespace {

ProtocolVersion V(int major, int minor) {
	return ProtocolVersion {major, minor};
}

ProtocolRange R(int a, int b, int c, int d) {
	return ProtocolRange {V(a, b), V(c, d)};
}

std::string MetadataValue(const std::vector<std::pair<std::string, std::string>> &pairs, const std::string &key) {
	for (const auto &kv : pairs) {
		if (kv.first == key) {
			return kv.second;
		}
	}
	return "<absent>";
}

} // namespace

TEST_CASE("protocol ranges parse and format", "[protocol-versions]") {
	auto ranges = ParseProtocolRanges("vgi.v3=3.4-3.4, vgi.secret.v1=1.0-1.2");
	CHECK(ranges.size() == 2);
	CHECK(ranges.at("vgi.v3") == R(3, 4, 3, 4));
	CHECK(ranges.at("vgi.secret.v1") == R(1, 0, 1, 2));
	CHECK(FormatProtocolRanges(ranges) == "vgi.secret.v1=1.0-1.2, vgi.v3=3.4-3.4");

	// Whitespace, patch components and malformed entries among good ones: the
	// bad ones are skipped, never fatal.
	ranges = ParseProtocolRanges("  vgi.v3 = 3.3.0 - 3.4.7 ,junk, x=1.0, y=2.1-2.0, vgi_rpc.Identity.v2=2.0-2.0");
	CHECK(ranges.size() == 2);
	CHECK(ranges.at("vgi.v3") == R(3, 3, 3, 4));
	CHECK(ranges.at("vgi_rpc.Identity.v2") == R(2, 0, 2, 0));

	// A repeated name keeps its first window.
	ranges = ParseProtocolRanges("vgi.v3=3.4-3.4, vgi.v3=3.0-3.9");
	CHECK(ranges.at("vgi.v3") == R(3, 4, 3, 4));

	// An empty (present) advertisement: the server negotiates, gating nothing.
	CHECK(ParseProtocolRanges("").empty());
	CHECK(ParseProtocolRanges("   ").empty());
	CHECK(ParseProtocolRanges("garbage").empty());
	CHECK(ParseProtocolRanges("vgi.v3=3.x-3.4").empty());
	CHECK(ParseProtocolRanges("vgi.v3=-3.4").empty());
	CHECK(ParseProtocolRanges("bad name=3.4-3.4").empty());

	CHECK(ParseProtocolVersion("3.4") == std::optional<ProtocolVersion>(V(3, 4)));
	CHECK(ParseProtocolVersion("3.4.0") == std::optional<ProtocolVersion>(V(3, 4)));
	CHECK_FALSE(ParseProtocolVersion("3"));
	CHECK_FALSE(ParseProtocolVersion("3.4.x"));
	CHECK_FALSE(ParseProtocolVersion("+3.4"));
}

TEST_CASE("the client window covers the generated versions, each with a codec", "[protocol-versions]") {
	const auto *main = FindClientProtocolWindow(VGI_MAIN_PROTOCOL.name);
	REQUIRE(main);
	auto current = ParseProtocolVersion(VGI_MAIN_PROTOCOL.version);
	REQUIRE(current);
	CHECK(main->range.max == *current);
	CHECK(main->range.Contains(*current));
	CHECK(main->current_wire_version == VGI_MAIN_PROTOCOL.version);
	CHECK(FindClientProtocolWindow(VGI_SECRET_PROTOCOL.name));
	CHECK_FALSE(FindClientProtocolWindow("vgi_rpc.Reflection.v1"));
	CHECK(ClientProtocolVersionsString().find(std::string(VGI_MAIN_PROTOCOL.name) + "=" + main->range.ToString()) !=
	      std::string::npos);
	CHECK(ParseProtocolRanges(ClientProtocolVersionsString()) == ClientProtocolRanges());
	// The windows are the generated ones (vgi-python codegen).
	CHECK(main->range.min == *ParseProtocolVersion(generated::VGI_PROTOCOL_VERSION_MIN));
	CHECK(main->range.max == *ParseProtocolVersion(generated::VGI_PROTOCOL_VERSION_MAX));
	CHECK(ClientProtocolVersionsString().find(std::string(generated::VGI_PROTOCOL_VERSIONS_CLIENT)) !=
	      std::string::npos);
	CHECK(ClientProtocolVersionsString().find(std::string(generated::VGI_SECRET_PROTOCOL_VERSIONS_CLIENT)) !=
	      std::string::npos);
	CHECK(ClientProtocolVersionsString().find("vgi.attach_tickets.v1=") != std::string::npos);
	// Every version a window admits has a codec, and every codec sits in a window.
	for (const auto &window : ClientProtocolWindows()) {
		for (int minor = window.range.min.minor;
		     window.range.min.major == window.range.max.major && minor <= window.range.max.minor; minor++) {
			INFO(std::string(window.protocol) << " " << window.range.min.major << "." << minor);
			CHECK(FindProtocolCodec(window.protocol, V(window.range.min.major, minor)));
		}
	}
	for (const auto *codec : RegisteredProtocolCodecs()) {
		const auto *window = FindClientProtocolWindow(codec->protocol());
		REQUIRE(window);
		CHECK(window->range.Contains(codec->version()));
	}
}

namespace {

class NoopCodec final : public ProtocolCodec {
public:
	using ProtocolCodec::ProtocolCodec;
	VgiBatchPtr EncodeRequest(const std::string &, const VgiBatchPtr &params,
	                          std::vector<std::pair<std::string, std::string>> &) const override {
		return params;
	}
	void DecodeResponse(const std::string &, UnaryResponseResult &) const override {
	}
};

} // namespace

TEST_CASE("a negotiated version carries the client's own patch", "[protocol-versions]") {
	// PROTOCOL_VERSIONS_V1: a 3.5.2 client speaking 3.4 sends 3.4.2.
	NoopCodec older("vgi.v3", V(3, 4), "2");
	CHECK(older.wire_version() == "3.4.2");
	for (const auto *codec : RegisteredProtocolCodecs()) {
		const auto *window = FindClientProtocolWindow(codec->protocol());
		REQUIRE(window);
		const auto patch = window->current_wire_version.substr(window->current_wire_version.rfind('.'));
		CHECK(codec->wire_version() == codec->version().ToString() + std::string(patch));
	}
}

TEST_CASE("negotiation picks the highest common version, or reports disjoint ranges", "[protocol-versions]") {
	// Spec §9: server 3.4-3.4, client 3.4-3.5 -> the client speaks 3.4.
	const ProtocolWindow client {"vgi.v3", R(3, 4, 3, 5), "3.5.0"};
	auto n = NegotiateProtocolVersion(client, R(3, 4, 3, 4));
	CHECK(n.outcome == ProtocolNegotiation::Outcome::AGREED);
	CHECK(n.version == V(3, 4));
	n = NegotiateProtocolVersion(client, R(3, 3, 3, 9));
	CHECK(n.outcome == ProtocolNegotiation::Outcome::AGREED);
	CHECK(n.version == V(3, 5));

	// Unknown server (no header, no describe metadata): the client's newest.
	n = NegotiateProtocolVersion(client, std::nullopt);
	CHECK(n.outcome == ProtocolNegotiation::Outcome::ASSUMED);
	CHECK(n.version == V(3, 5));

	// Disjoint: an older worker.
	n = NegotiateProtocolVersion(client, R(3, 2, 3, 3));
	CHECK(n.outcome == ProtocolNegotiation::Outcome::DISJOINT);
	auto message = DisjointProtocolMessage("vgi.v3", n, "w");
	CHECK(message.find("protocol_version_unsupported") == 0);
	CHECK(message.find("worker supports vgi.v3 3.2-3.3") != std::string::npos);
	CHECK(message.find("extension supports vgi.v3 3.4-3.5") != std::string::npos);
	CHECK(message.find("too old: upgrade the worker") != std::string::npos);

	// Disjoint: a newer worker.
	n = NegotiateProtocolVersion(client, R(3, 6, 3, 7));
	CHECK(n.outcome == ProtocolNegotiation::Outcome::DISJOINT);
	message = DisjointProtocolMessage("vgi.v3", n, "w");
	CHECK(message.find("worker supports vgi.v3 3.6-3.7") != std::string::npos);
	CHECK(message.find("too new for this extension: upgrade the vgi extension") != std::string::npos);
}

TEST_CASE("refusals yield the server's ranges", "[protocol-versions]") {
	{
		ErrorInfo info;
		info.Set(kProtocolVersionsErrorKey, "vgi.v3=3.3-3.3");
		VgiRpcException e(info, "protocol_version_unsupported: client speaks vgi.v3 3.4 ...",
		                  kProtocolVersionUnsupportedKind);
		CHECK(IsProtocolVersionRefusal(e));
		auto ranges = RangesFromRefusal(e);
		REQUIRE(ranges);
		CHECK(ranges->at("vgi.v3") == R(3, 3, 3, 3));
	}
	{
		// A server released before negotiation.
		VgiRpcException e(ErrorInfo(),
		                  "VGI Worker Exception: VGI client/worker protocol_version mismatch for protocol 'vgi.v3'.\n"
		                  "  Client: 3.4.0\n  Server: 3.1.0\n  Direction: client is too new",
		                  kProtocolVersionMismatchKind);
		auto ranges = RangesFromRefusal(e);
		REQUIRE(ranges);
		CHECK(ranges->at("vgi.v3") == R(3, 1, 3, 1));
	}
	{
		// The same, quoted by a precondition description.
		VgiRpcException e(ErrorInfo(),
		                  "protocol_version mismatch for protocol 'vgi.v3': client declares 3.4.0, server requires "
		                  "3.2.1; major and minor must match",
		                  kProtocolVersionMismatchKind);
		auto ranges = RangesFromRefusal(e);
		REQUIRE(ranges);
		CHECK(ranges->at("vgi.v3") == R(3, 2, 3, 2));
	}
	{
		VgiRpcException e(ErrorInfo(), "some other failure", "method_not_implemented");
		CHECK_FALSE(IsProtocolVersionRefusal(e));
		CHECK_FALSE(RangesFromRefusal(e));
		IOException io("transport");
		CHECK_FALSE(IsProtocolVersionRefusal(io));
	}
}

TEST_CASE("requests carry the negotiated version and the client's ranges", "[protocol-versions]") {
	std::vector<std::pair<std::string, std::string>> metadata;
	EncodeRpcRequest("bind", nullptr, VGI_MAIN_PROTOCOL, {{"x", "y"}}, metadata);
	CHECK(MetadataValue(metadata, RPC_PROTOCOL_VERSION_KEY) == std::string(VGI_MAIN_PROTOCOL.version));
	CHECK(MetadataValue(metadata, RPC_PROTOCOL_KEY) == std::string(VGI_MAIN_PROTOCOL.name));
	CHECK(MetadataValue(metadata, kClientProtocolVersionsMetadataKey) == ClientProtocolVersionsString());
	CHECK(metadata.back() == std::make_pair(std::string("x"), std::string("y")));

	// Reflection declares no version and is exempt from the gate.
	const VgiProtocolId reflection {"vgi_rpc.Reflection.v1", ""};
	EncodeRpcRequest("list_protocols", nullptr, reflection, {}, metadata);
	CHECK(MetadataValue(metadata, RPC_PROTOCOL_VERSION_KEY) == "<absent>");
	CHECK(MetadataValue(metadata, kClientProtocolVersionsMetadataKey) == "<absent>");

	// Reserved server-level methods carry no routing key.
	EncodeRpcRequest(UPLOAD_URL_METHOD, nullptr, VGI_MAIN_PROTOCOL, {}, metadata);
	CHECK(MetadataValue(metadata, RPC_PROTOCOL_KEY) == "<absent>");

	// A windowed protocol stamped with a version no codec implements is a bug.
	const VgiProtocolId bogus {VGI_MAIN_PROTOCOL.name, "3.99.0"};
	CHECK_THROWS_AS(EncodeRpcRequest("bind", nullptr, bogus, {}, metadata), InternalException);
}

TEST_CASE("the session keeps ranges, negotiates and answers capability questions", "[protocol-versions]") {
	ProtocolSession session;
	const std::string main(VGI_MAIN_PROTOCOL.name);
	CHECK_FALSE(session.ServerRanges());
	CHECK(session.Negotiate(main).outcome == ProtocolNegotiation::Outcome::ASSUMED);
	CHECK(session.WireVersion(main, "") == VGI_MAIN_PROTOCOL.version);
	CHECK(NegotiatedProtocol(&session, VGI_MAIN_PROTOCOL).version == VGI_MAIN_PROTOCOL.version);
	// Protocols the client does not window keep the version they were given.
	const VgiProtocolId identity {"vgi_rpc.Identity.v2", "2.1.0"};
	CHECK(NegotiatedProtocol(&session, identity).version == "2.1.0");

	ServerCapabilities caps;
	caps.discovered = true;
	caps.accept_max_response_bytes_support = true;
	caps.upload_url_version = kUploadUrlProtocolVersion;
	caps.arguments_ref_bytes = 4096;
	caps.protocol_ranges = ParseProtocolRanges(main + "=" + FindClientProtocolWindow(main)->range.ToString());
	session.Store(caps);
	REQUIRE(session.ServerRange(main));
	CHECK(session.Negotiate(main).outcome == ProtocolNegotiation::Outcome::AGREED);
	CHECK(session.Supports(ProtocolFeature::ACCEPT_MAX_RESPONSE_BYTES));
	CHECK(session.Supports(ProtocolFeature::UPLOAD_URLS));
	CHECK(session.Supports(ProtocolFeature::ARGUMENTS_REF));
	CHECK_FALSE(session.Supports(ProtocolFeature::ADVERTISED_ENCODINGS));
	CHECK(session.UploadUrlVersion() == kUploadUrlProtocolVersion);

	// A later response without the header does not forget the ranges.
	caps.protocol_ranges.reset();
	caps.upload_url_version = 1;
	session.Store(caps);
	CHECK(session.ServerRange(main));
	CHECK_FALSE(session.Supports(ProtocolFeature::UPLOAD_URLS));
	CHECK_FALSE(session.Supports(ProtocolFeature::ARGUMENTS_REF));

	// Disjoint ranges keep the earlier choice; UseVersion stays in the window.
	session.ObserveServerRanges({{main, R(2, 0, 2, 9)}});
	CHECK(session.Negotiate(main).outcome == ProtocolNegotiation::Outcome::DISJOINT);
	CHECK(session.WireVersion(main, "") == VGI_MAIN_PROTOCOL.version);
	CHECK_FALSE(session.UseVersion(main, V(2, 5)));
	CHECK(session.UseVersion(main, FindClientProtocolWindow(main)->range.max));
}

// ---------------------------------------------------------------------------
// ATTACH over HTTP against a server advertising other ranges
// ---------------------------------------------------------------------------

namespace {

// Answers HEAD with the capability headers (plus `protocol_versions`, when not
// empty) and every POST with 400, carrying the same headers. No VGI server
// would answer catalog_attach that way; the point is what the client concludes
// from the headers alone.
class RangeServer {
public:
	explicit RangeServer(std::string protocol_versions) : protocol_versions_(std::move(protocol_versions)) {
		listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
		REQUIRE(listen_fd_ >= 0);
		int one = 1;
		setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
		sockaddr_in addr {};
		addr.sin_family = AF_INET;
		addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		REQUIRE(bind(listen_fd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) == 0);
		REQUIRE(listen(listen_fd_, 8) == 0);
		socklen_t len = sizeof(addr);
		REQUIRE(getsockname(listen_fd_, reinterpret_cast<sockaddr *>(&addr), &len) == 0);
		port_ = ntohs(addr.sin_port);
		thread_ = std::thread([this] { Run(); });
	}
	~RangeServer() {
		stop_.store(true);
		thread_.join();
		close(listen_fd_);
	}
	std::string Url() const {
		return "http://127.0.0.1:" + std::to_string(port_);
	}
	int Posts() const {
		return posts_.load();
	}
	// The VGI-Client-Protocol-Versions header of the last request ("" if none).
	std::string ClientHeader() const {
		std::lock_guard<std::mutex> guard(lock_);
		return client_header_;
	}

private:
	static bool WaitReadable(int fd, int ms) {
		fd_set set;
		FD_ZERO(&set);
		FD_SET(fd, &set);
		timeval tv {ms / 1000, (ms % 1000) * 1000};
		return select(fd + 1, &set, nullptr, nullptr, &tv) > 0;
	}
	void Run() {
		while (!stop_.load()) {
			if (!WaitReadable(listen_fd_, 50)) {
				continue;
			}
			int fd = accept(listen_fd_, nullptr, nullptr);
			if (fd < 0) {
				continue;
			}
			// Keep-alive: serve requests on this connection until it closes.
			std::string buffer;
			char chunk[16384];
			while (!stop_.load()) {
				auto head_end = buffer.find("\r\n\r\n");
				if (head_end == std::string::npos) {
					if (!WaitReadable(fd, 2000)) {
						break;
					}
					auto n = recv(fd, chunk, sizeof(chunk), 0);
					if (n <= 0) {
						break;
					}
					buffer.append(chunk, static_cast<size_t>(n));
					continue;
				}
				const auto head = buffer.substr(0, head_end);
				const auto lower = StringUtil::Lower(head);
				size_t length = 0;
				auto pos = lower.find("content-length:");
				if (pos != std::string::npos) {
					length = std::stoull(lower.substr(pos + 15));
				}
				while (buffer.size() < head_end + 4 + length && WaitReadable(fd, 2000)) {
					auto n = recv(fd, chunk, sizeof(chunk), 0);
					if (n <= 0) {
						break;
					}
					buffer.append(chunk, static_cast<size_t>(n));
				}
				buffer.erase(0, std::min(buffer.size(), head_end + 4 + length));
				auto cpos = lower.find("vgi-client-protocol-versions:");
				{
					std::lock_guard<std::mutex> guard(lock_);
					client_header_.clear();
					if (cpos != std::string::npos) {
						auto end = head.find("\r\n", cpos);
						client_header_ = StringUtil::Lower(
						    std::string(StringUtil::Replace(head.substr(cpos + 29, end - cpos - 29), " ", "")));
					}
				}
				const bool is_head = lower.rfind("head ", 0) == 0;
				std::string headers = "VGI-Accept-Max-Response-Bytes-Support: true\r\n"
				                      "VGI-Supported-Encodings: \r\n";
				// "<absent>" omits the header (a server released before
				// negotiation); "" sends it empty (negotiates, gates nothing).
				if (protocol_versions_ != "<absent>") {
					headers += "VGI-Protocol-Versions: " + protocol_versions_ + "\r\n";
				}
				std::string reply;
				if (is_head) {
					reply = "HTTP/1.1 200 OK\r\n" + headers + "Content-Length: 0\r\n\r\n";
				} else {
					posts_.fetch_add(1);
					const std::string body = "refused";
					reply = "HTTP/1.1 400 Bad Request\r\n" + headers +
					        "Content-Type: text/plain\r\nContent-Length: " + std::to_string(body.size()) + "\r\n\r\n" +
					        body;
				}
				size_t sent = 0;
				while (sent < reply.size()) {
					auto n = send(fd, reply.data() + sent, reply.size() - sent, MSG_NOSIGNAL);
					if (n <= 0) {
						break;
					}
					sent += static_cast<size_t>(n);
				}
			}
			close(fd);
		}
	}

	std::string protocol_versions_;
	int listen_fd_ = -1;
	int port_ = 0;
	std::atomic<bool> stop_ {false};
	std::atomic<int> posts_ {0};
	mutable std::mutex lock_;
	std::string client_header_;
	std::thread thread_;
};

std::string AttachError(const std::string &url) {
	DuckDB db(nullptr);
	db.LoadStaticExtension<HttpfsExtension>();
	db.LoadStaticExtension<VgiExtension>();
	Connection con(db);
	REQUIRE_FALSE(con.Query("SET http_retries = 0")->HasError());
	auto result = con.Query("ATTACH 'x' AS x (TYPE vgi, LOCATION '" + url + "')");
	REQUIRE(result->HasError());
	return result->GetError();
}

} // namespace

TEST_CASE("ATTACH over HTTP fails with both ranges when they are disjoint", "[protocol-versions]") {
	RangeServer server("vgi.v3=3.3-3.3, vgi.secret.v1=1.0-1.0");
	auto error = AttachError(server.Url());
	INFO(error);
	CHECK(error.find("protocol_version_unsupported") != std::string::npos);
	CHECK(error.find("worker supports vgi.v3 3.3-3.3") != std::string::npos);
	CHECK(error.find("extension supports vgi.v3 " + FindClientProtocolWindow("vgi.v3")->range.ToString()) !=
	      std::string::npos);
	CHECK(error.find("upgrade the worker") != std::string::npos);
	// The client advertised its own ranges.
	CHECK(server.ClientHeader() == StringUtil::Lower(StringUtil::Replace(ClientProtocolVersionsString(), " ", "")));
}

TEST_CASE("ATTACH over HTTP without the header, gating nothing, or overlapping, keeps the worker's error",
          "[protocol-versions]") {
	for (const char *advertised : {"<absent>", "", "vgi.secret.v1=1.0-1.0", "vgi.v3=3.4-3.9"}) {
		INFO("advertised '" << advertised << "'");
		RangeServer server(advertised);
		auto error = AttachError(server.Url());
		INFO(error);
		CHECK(error.find("protocol_version_unsupported") == std::string::npos);
		CHECK(error.find("400") != std::string::npos);
		// No retry: the 400 was not a version refusal.
		CHECK(server.Posts() == 1);
	}
}
