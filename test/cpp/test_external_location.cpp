// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// External-location (vgi_rpc.location) client policy: which URLs are fetched,
// the fetched-size cap, and the request-upload decision. The end-to-end paths
// run in test/sql/external_storage/ (make test_http_vendor).

#include "catch.hpp"

#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "httpfs_extension.hpp"
#include "vgi_exception.hpp"
#include "vgi_extension.hpp"
#include "vgi_http_client.hpp"
#include "vgi_ipc.hpp"
#include "vgi_server_capabilities.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <mutex>
#include <thread>
#include <vector>

using namespace duckdb;
using namespace duckdb::vgi;

namespace {

// Sets or clears the loopback hook for one scope, restoring the old value.
class ScopedLoopbackHook {
public:
	explicit ScopedLoopbackHook(const char *value) {
		const char *old = std::getenv(kAllowLoopbackExternalLocationsEnv);
		had_old_ = old != nullptr;
		if (had_old_) {
			old_ = old;
		}
		if (value) {
			setenv(kAllowLoopbackExternalLocationsEnv, value, 1);
		} else {
			unsetenv(kAllowLoopbackExternalLocationsEnv);
		}
	}
	~ScopedLoopbackHook() {
		if (had_old_) {
			setenv(kAllowLoopbackExternalLocationsEnv, old_.c_str(), 1);
		} else {
			unsetenv(kAllowLoopbackExternalLocationsEnv);
		}
	}

private:
	bool had_old_ = false;
	std::string old_;
};

// Answers every GET with `body`, with or without a Content-Length.
class BodyServer {
public:
	BodyServer(std::string body, bool content_length) : body_(std::move(body)), content_length_(content_length) {
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
	~BodyServer() {
		stop_.store(true);
		thread_.join();
		close(listen_fd_);
	}
	std::string Url() const {
		return "http://127.0.0.1:" + std::to_string(port_) + "/blob";
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
			std::string head;
			char buf[4096];
			while (head.find("\r\n\r\n") == std::string::npos && WaitReadable(fd, 2000)) {
				auto n = recv(fd, buf, sizeof(buf), 0);
				if (n <= 0) {
					break;
				}
				head.append(buf, static_cast<size_t>(n));
			}
			std::string reply = "HTTP/1.1 200 OK\r\nConnection: close\r\n";
			if (content_length_) {
				reply += "Content-Length: " + std::to_string(body_.size()) + "\r\n";
			}
			reply += "\r\n" + body_;
			size_t sent = 0;
			while (sent < reply.size()) {
				auto n = send(fd, reply.data() + sent, reply.size() - sent, MSG_NOSIGNAL);
				if (n <= 0) {
					break; // the client stopped reading at its cap
				}
				sent += static_cast<size_t>(n);
			}
			close(fd);
		}
	}

	std::string body_;
	bool content_length_;
	int listen_fd_ = -1;
	int port_ = 0;
	std::atomic<bool> stop_ {false};
	std::thread thread_;
};

struct Fixture {
	Fixture() : db(nullptr), con(db) {
		db.LoadStaticExtension<HttpfsExtension>();
		db.LoadStaticExtension<VgiExtension>();
		REQUIRE_FALSE(con.Query("SET http_retries = 0")->HasError());
	}
	ClientContext &context() {
		return *con.context;
	}
	DuckDB db;
	Connection con;
};

} // namespace

TEST_CASE("external locations must be https unless the loopback hook is set", "[external-location]") {
	ScopedLoopbackHook off(nullptr);
	CHECK(ExternalLocationUrlAllowed("https://store.example.com/o/1?X-Amz-Signature=x"));
	CHECK(ExternalLocationUrlAllowed("HTTPS://store.example.com/o/1"));
	CHECK_FALSE(ExternalLocationUrlAllowed("http://store.example.com/o/1"));
	CHECK_FALSE(ExternalLocationUrlAllowed("http://127.0.0.1:9000/o/1"));
	CHECK_FALSE(ExternalLocationUrlAllowed("file:///etc/passwd"));
	CHECK_FALSE(ExternalLocationUrlAllowed("ftp://store.example.com/o"));
	CHECK_FALSE(ExternalLocationUrlAllowed("httpi://peer/o"));
	CHECK_FALSE(ExternalLocationUrlAllowed(""));
}

TEST_CASE("the loopback hook admits only genuine loopback http", "[external-location]") {
	ScopedLoopbackHook on("1");
	CHECK(ExternalLocationUrlAllowed("http://127.0.0.1:9000/o/1"));
	CHECK(ExternalLocationUrlAllowed("http://localhost:9000/o/1"));
	CHECK(ExternalLocationUrlAllowed("http://[::1]:9000/o/1"));
	CHECK_FALSE(ExternalLocationUrlAllowed("http://127.0.0.1@evil.example/o"));
	CHECK_FALSE(ExternalLocationUrlAllowed("http://127.0.0.1.evil.example/o"));
	CHECK_FALSE(ExternalLocationUrlAllowed("http://store.example.com/o/1"));
	{
		// Only exactly "1" enables it.
		ScopedLoopbackHook yes("yes");
		CHECK_FALSE(ExternalLocationUrlAllowed("http://127.0.0.1:9000/o/1"));
	}
}

TEST_CASE("vgi_external_location_max_bytes defaults to 1 GiB and is per session", "[external-location]") {
	Fixture f;
	CHECK(GetExternalLocationMaxBytes(f.context()) == (1ULL << 30));
	REQUIRE_FALSE(f.con.Query("SET vgi_external_location_max_bytes = 1234")->HasError());
	CHECK(GetExternalLocationMaxBytes(f.context()) == 1234);
}

TEST_CASE("HttpGetBytes stops at max_bytes with or without Content-Length", "[external-location]") {
	const std::string body(100000, 'x');
	for (bool content_length : {true, false}) {
		INFO("content_length=" << content_length);
		BodyServer server(body, content_length);
		Fixture f;
		auto &context = f.context();
		// Under, at and without the cap the whole body arrives.
		CHECK(HttpGetBytes(context, server.Url(), nullptr, HttpSecrets::NONE, 0) == body);
		CHECK(HttpGetBytes(context, server.Url(), nullptr, HttpSecrets::NONE, body.size()) == body);
		try {
			HttpGetBytes(context, server.Url(), nullptr, HttpSecrets::NONE, 1000);
			FAIL("a body over max_bytes was accepted");
		} catch (const IOException &e) {
			// what() is DuckDB's JSON form; the classification rides in extra_info.
			const std::string error = e.what();
			CHECK(error.find("vgi_external_location_max_bytes (1000)") != std::string::npos);
			CHECK(error.find("\"error_subtype\":\"RESPONSE_TOO_LARGE\"") != std::string::npos);
		}
	}
}

TEST_CASE("request bodies within the cap, or without upload support, stay inline", "[external-location]") {
	Fixture f;
	auto &context = f.context();
	const std::vector<uint8_t> original(1000, 0x5a);

	ServerCapabilities caps;
	caps.discovered = true;
	auto body = original;
	// No advertised cap.
	CHECK_FALSE(MaybeExternalizeRequestBody(context, "http://127.0.0.1:9", body, caps, nullptr, nullptr));
	// Under the cap.
	caps.max_request_bytes = 1000;
	caps.upload_url_version = 2;
	CHECK_FALSE(MaybeExternalizeRequestBody(context, "http://127.0.0.1:9", body, caps, nullptr, nullptr));
	// Over the cap without upload support: inline, for the server's 413.
	caps.max_request_bytes = 100;
	caps.upload_url_version = 0;
	CHECK_FALSE(MaybeExternalizeRequestBody(context, "http://127.0.0.1:9", body, caps, nullptr, nullptr));
	CHECK(body == original);
}

TEST_CASE("a server advertising only upload URLs v1 is refused before any request", "[external-location]") {
	Fixture f;
	ServerCapabilities caps;
	caps.discovered = true;
	caps.max_request_bytes = 100;
	caps.upload_url_version = 1; // VGI-Upload-URL-Support: true
	std::vector<uint8_t> body(1000, 0x5a);
	try {
		// Port 9 is closed: reaching the network would fail differently.
		MaybeExternalizeRequestBody(f.context(), "http://127.0.0.1:9", body, caps, nullptr, nullptr);
		FAIL("an upload to a v1 server was attempted");
	} catch (const IOException &e) {
		const std::string error = e.what();
		CHECK(error.find("upload protocol is too old") != std::string::npos);
		CHECK(error.find("\"error_subtype\":\"SERVER_TOO_OLD\"") != std::string::npos);
	}
	CHECK(body.size() == 1000);
}

TEST_CASE("a request over both the request and upload caps is refused before any request", "[external-location]") {
	Fixture f;
	ServerCapabilities caps;
	caps.discovered = true;
	caps.max_request_bytes = 100;
	caps.upload_url_version = 2;
	caps.max_upload_bytes = 500;
	std::vector<uint8_t> body(1000, 0x5a);
	try {
		// Port 9 is closed: reaching the network would fail differently.
		MaybeExternalizeRequestBody(f.context(), "http://127.0.0.1:9", body, caps, nullptr, nullptr);
		FAIL("a request over VGI-Max-Upload-Bytes was accepted");
	} catch (const IOException &e) {
		const std::string error = e.what();
		CHECK(error.find("VGI-Max-Upload-Bytes (500)") != std::string::npos);
		CHECK(error.find("\"error_subtype\":\"LIMIT_EXCEEDED\"") != std::string::npos);
	}
	CHECK(body.size() == 1000);
}

TEST_CASE("a request whose metadata alone reaches the cap stays inline, with no upload", "[external-location]") {
	// A producer tick or a cancel is an empty batch carrying only state
	// tokens. The pointer keeps every metadata key, so uploading cannot bring
	// it under the cap: it goes inline (the server compresses or 413s), and no
	// upload URL is requested (port 9 is closed, so an attempt would throw).
	Fixture f;
	ServerCapabilities caps;
	caps.discovered = true;
	caps.max_request_bytes = 1000;
	caps.upload_url_version = 2;
	VgiMetadata metadata {{"vgi_rpc.stream_state#b64", std::string(2000, 'A')}};
	auto tick = VgiBatch::MakeEmpty(VgiSchema::Empty());
	const auto original = SerializeBatch(*tick, &metadata)->ToVector();
	REQUIRE(original.size() > 1000);
	auto body = original;
	CHECK_FALSE(MaybeExternalizeRequestBody(f.context(), "http://127.0.0.1:9", body, caps, nullptr, nullptr));
	CHECK(body == original);
}

// --- Argument references (protocol 3.4, docs/large-values-3.4.md §4-§5) -----

TEST_CASE("VGI-Arguments-Ref-Bytes parses as a canonical positive integer or not at all", "[external-location]") {
	CHECK(ParseArgumentsRefBytes("4194304") == 4194304);
	CHECK(ParseArgumentsRefBytes("1") == 1);
	CHECK(ParseArgumentsRefBytes("9223372036854775807") == INT64_MAX);
	// Invalid values count as absent: no argument references.
	for (const char *bad : {"", "0", "04", "-1", "+5", "1.5", " 5", "5 ", "1e6", "abc", "9223372036854775808",
	                        "99999999999999999999"}) {
		INFO(bad);
		CHECK(ParseArgumentsRefBytes(bad) == -1);
	}
}

TEST_CASE("arguments travel by reference only under the build-time rule", "[external-location]") {
	ServerCapabilities caps;
	caps.discovered = true;
	caps.upload_url_version = 2;
	caps.arguments_ref_bytes = 1000;

	SECTION("over the worker's threshold, with no request cap") {
		CHECK_FALSE(ShouldExternalizeArguments(caps, 1000));
		CHECK(ShouldExternalizeArguments(caps, 1001));
	}
	SECTION("the request cap lowers the threshold to max_request_bytes / 4 (integer division)") {
		caps.max_request_bytes = 2003; // / 4 = 500
		CHECK_FALSE(ShouldExternalizeArguments(caps, 500));
		CHECK(ShouldExternalizeArguments(caps, 501));
		caps.max_request_bytes = 100000; // / 4 = 25000 > A: A wins
		CHECK_FALSE(ShouldExternalizeArguments(caps, 1000));
		CHECK(ShouldExternalizeArguments(caps, 1001));
	}
	SECTION("an unset or non-positive request cap counts as infinite") {
		caps.max_request_bytes = 0;
		CHECK_FALSE(ShouldExternalizeArguments(caps, 1000));
		CHECK(ShouldExternalizeArguments(caps, 1001));
		caps.max_request_bytes = -1;
		CHECK(ShouldExternalizeArguments(caps, 1001));
	}
	SECTION("the header is the capability signal: without it, never") {
		caps.arguments_ref_bytes = -1;
		caps.max_request_bytes = 100;
		CHECK_FALSE(ShouldExternalizeArguments(caps, 1u << 30));
	}
	SECTION("only upload URLs v2") {
		for (int version : {0, 1, 3}) {
			caps.upload_url_version = version;
			CHECK_FALSE(ShouldExternalizeArguments(caps, 1u << 20));
		}
	}
	SECTION("never over an advertised VGI-Max-Upload-Bytes") {
		caps.max_upload_bytes = 5000;
		CHECK(ShouldExternalizeArguments(caps, 5000));
		CHECK_FALSE(ShouldExternalizeArguments(caps, 5001));
	}
	SECTION("never before discovery") {
		caps.discovered = false;
		CHECK_FALSE(ShouldExternalizeArguments(caps, 1u << 20));
	}
}

// ---------------------------------------------------------------------------
// Storage transfer timeouts: a stall timeout, never a whole-request deadline.
// TrickleServer moves bytes at a fixed slow pace, or stops moving them, so a
// transfer that outlasts the stall timeout must still succeed in one attempt
// and a stalled one must fail within the stall timeout (plus backoff) per
// attempt.
// ---------------------------------------------------------------------------

namespace {

enum class TrickleMode {
	GET_SLOW,       // send the body chunk by chunk with a pause between chunks
	GET_STALL,      // send half the body, then nothing
	GET_STALL_ONCE, // the first request stalls after half the body, later ones get it all
	PUT_SLOW,       // read the request body chunk by chunk with a pause between chunks
	PUT_STALL,      // read the request head, then never read again
	PUT_REJECT,     // read the whole request, answer 400 with the S3 error code in body_
};

class TrickleServer {
public:
	TrickleServer(TrickleMode mode, std::string body, size_t chunk, int pause_ms)
	    : mode_(mode), body_(std::move(body)), chunk_(chunk), pause_ms_(pause_ms) {
		listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
		REQUIRE(listen_fd_ >= 0);
		int one = 1;
		setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
		// Small kernel buffers (inherited by accepted sockets), so the client
		// feels the pace instead of filling megabytes of loopback buffer.
		int small = 32 * 1024;
		setsockopt(listen_fd_, SOL_SOCKET, SO_RCVBUF, &small, sizeof(small));
		setsockopt(listen_fd_, SOL_SOCKET, SO_SNDBUF, &small, sizeof(small));
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
	~TrickleServer() {
		stop_.store(true);
		thread_.join();
		for (auto &t : handlers_) {
			t.join();
		}
		close(listen_fd_);
	}
	// The query stands in for a pre-signed URL's credential.
	std::string Url() const {
		return "http://127.0.0.1:" + std::to_string(port_) + "/bucket/object?X-Amz-Signature=sekrit-sig-value";
	}
	int Requests() const {
		return requests_.load();
	}
	size_t BodyBytesReceived() const {
		return body_received_.load();
	}
	// Seconds between the first and each later request's arrival.
	std::vector<double> ArrivalOffsets() {
		std::lock_guard<std::mutex> guard(lock_);
		std::vector<double> out;
		for (auto &t : arrivals_) {
			out.push_back(std::chrono::duration<double>(t - arrivals_.front()).count());
		}
		return out;
	}

private:
	static bool WaitReadable(int fd, int ms) {
		fd_set set;
		FD_ZERO(&set);
		FD_SET(fd, &set);
		timeval tv {ms / 1000, (ms % 1000) * 1000};
		return select(fd + 1, &set, nullptr, nullptr, &tv) > 0;
	}
	bool SendAll(int fd, const char *data, size_t size) {
		size_t sent = 0;
		while (sent < size && !stop_.load()) {
			auto n = send(fd, data + sent, size - sent, MSG_NOSIGNAL);
			if (n <= 0) {
				return false;
			}
			sent += static_cast<size_t>(n);
		}
		return sent == size;
	}
	// Hold the connection open without moving a byte, until the client gives
	// up (or the test ends).
	void Hold() {
		while (!stop_.load()) {
			std::this_thread::sleep_for(std::chrono::milliseconds(50));
		}
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
			const int index = requests_.fetch_add(1);
			{
				std::lock_guard<std::mutex> guard(lock_);
				arrivals_.push_back(std::chrono::steady_clock::now());
			}
			handlers_.emplace_back([this, fd, index] {
				Handle(fd, index);
				close(fd);
			});
		}
	}
	void Handle(int fd, int index) {
		std::string head;
		char buf[16384];
		while (head.find("\r\n\r\n") == std::string::npos && WaitReadable(fd, 5000)) {
			auto n = recv(fd, buf, sizeof(buf), 0);
			if (n <= 0) {
				return;
			}
			head.append(buf, static_cast<size_t>(n));
		}
		const auto head_end = head.find("\r\n\r\n");
		if (head_end == std::string::npos) {
			return;
		}
		const auto lower = StringUtil::Lower(head.substr(0, head_end));
		if (mode_ == TrickleMode::PUT_SLOW || mode_ == TrickleMode::PUT_STALL || mode_ == TrickleMode::PUT_REJECT) {
			size_t length = 0;
			auto pos = lower.find("content-length:");
			if (pos != std::string::npos) {
				length = std::stoull(lower.substr(pos + 15));
			}
			if (lower.find("expect: 100-continue") != std::string::npos) {
				SendAll(fd, "HTTP/1.1 100 Continue\r\n\r\n", 25);
			}
			size_t got = head.size() - head_end - 4;
			body_received_.fetch_add(got);
			if (mode_ == TrickleMode::PUT_STALL) {
				Hold();
				return;
			}
			while (got < length && !stop_.load()) {
				std::this_thread::sleep_for(std::chrono::milliseconds(pause_ms_));
				if (!WaitReadable(fd, 5000)) {
					return;
				}
				auto n = recv(fd, buf, std::min(chunk_, sizeof(buf)), 0);
				if (n <= 0) {
					return;
				}
				got += static_cast<size_t>(n);
				body_received_.fetch_add(static_cast<size_t>(n));
			}
			std::string reply = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
			if (mode_ == TrickleMode::PUT_REJECT) {
				const auto error = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>" + body_ +
				                   "</Code><Message>checksum</Message></Error>";
				reply = "HTTP/1.1 400 Bad Request\r\nContent-Type: application/xml\r\nConnection: close\r\n"
				        "Content-Length: " +
				        std::to_string(error.size()) + "\r\n\r\n" + error;
			}
			SendAll(fd, reply.data(), reply.size());
			return;
		}
		std::string reply_head =
		    "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: " + std::to_string(body_.size()) + "\r\n\r\n";
		if (!SendAll(fd, reply_head.data(), reply_head.size())) {
			return;
		}
		const bool stall = mode_ == TrickleMode::GET_STALL || (mode_ == TrickleMode::GET_STALL_ONCE && index == 0);
		if (stall) {
			SendAll(fd, body_.data(), body_.size() / 2);
			Hold();
			return;
		}
		if (mode_ == TrickleMode::GET_STALL_ONCE) {
			SendAll(fd, body_.data(), body_.size());
			return;
		}
		for (size_t off = 0; off < body_.size() && !stop_.load(); off += chunk_) {
			if (!SendAll(fd, body_.data() + off, std::min(chunk_, body_.size() - off))) {
				return;
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(pause_ms_));
		}
	}

	TrickleMode mode_;
	std::string body_;
	size_t chunk_;
	int pause_ms_;
	int listen_fd_ = -1;
	int port_ = 0;
	std::atomic<bool> stop_ {false};
	std::atomic<int> requests_ {0};
	std::atomic<size_t> body_received_ {0};
	std::mutex lock_;
	std::vector<std::chrono::steady_clock::time_point> arrivals_;
	std::vector<std::thread> handlers_;
	std::thread thread_;
};

// One stalled attempt at vgi_storage_stall_timeout_seconds = 1 ends within
// this many seconds. curl judges a stall by its average speed over a window of
// about 5 s (CURLOPT_LOW_SPEED_LIMIT 1 KiB/s for CURLOPT_LOW_SPEED_TIME), so an
// attempt that moved a burst of bytes just before stalling ends up to ~6 s
// after the stall timeout; add the backoff and slack.
constexpr double kStallBound = 1.0 + 6.0 + 0.5 + 2.0;

double SecondsSince(std::chrono::steady_clock::time_point start) {
	return std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
}

std::string PatternBody(size_t size) {
	std::string body(size, '\0');
	for (size_t i = 0; i < size; i++) {
		body[i] = static_cast<char>('a' + (i * 7 + i / 4096) % 26);
	}
	return body;
}

} // namespace

TEST_CASE("storage transfers default to a 30 s stall timeout, separate from vgi_http_timeout_seconds",
          "[external-location][storage-timeout]") {
	Fixture f;
	CHECK(GetStorageStallTimeoutSeconds(f.context()) == 30);
	REQUIRE_FALSE(f.con.Query("SET vgi_storage_stall_timeout_seconds = 7")->HasError());
	CHECK(GetStorageStallTimeoutSeconds(f.context()) == 7);
	CHECK(f.con.Query("SET vgi_storage_stall_timeout_seconds = 0")->HasError());
	CHECK_FALSE(StorageAllowsPlainHttp(f.context()));
	REQUIRE_FALSE(f.con.Query("SET vgi_storage_allow_plain_http = true")->HasError());
	CHECK(StorageAllowsPlainHttp(f.context()));
}

TEST_CASE("plain http storage URLs on any host need vgi_storage_allow_plain_http", "[external-location]") {
	ScopedLoopbackHook off(nullptr);
	CHECK_FALSE(ExternalLocationUrlAllowed("http://minio.internal:9000/b/o?X-Amz-Signature=x"));
	CHECK(ExternalLocationUrlAllowed("http://minio.internal:9000/b/o?X-Amz-Signature=x", true));
	CHECK(ExternalLocationUrlAllowed("HTTP://10.0.0.5/b/o", true));
	CHECK(ExternalLocationUrlAllowed("https://s3.eu-central-003.backblazeb2.com/b/o", false));
	CHECK(ExternalLocationUrlAllowed("https://acct.r2.cloudflarestorage.com/b/o", false));
	// The setting is about plaintext, not about other schemes.
	CHECK_FALSE(ExternalLocationUrlAllowed("httpi://peer/o", true));
	CHECK_FALSE(ExternalLocationUrlAllowed("ftp://minio.internal/o", true));
	CHECK_FALSE(ExternalLocationUrlAllowed("http://", true));
}

TEST_CASE("a slow GET that outlasts the stall timeout succeeds in one attempt",
          "[external-location][storage-timeout]") {
	ScopedLoopbackHook on("1");
	// 480 KB in 48 chunks, one every 150 ms: about 7 s, over three times the
	// 2 s stall timeout and seven times vgi_http_timeout_seconds.
	const auto body = PatternBody(480 * 1024);
	TrickleServer server(TrickleMode::GET_SLOW, body, 10 * 1024, 150);
	Fixture f;
	REQUIRE_FALSE(f.con.Query("SET http_retries = 3")->HasError());
	REQUIRE_FALSE(f.con.Query("SET vgi_storage_stall_timeout_seconds = 2")->HasError());
	REQUIRE_FALSE(f.con.Query("SET vgi_http_timeout_seconds = 1")->HasError());
	const auto start = std::chrono::steady_clock::now();
	CHECK(HttpGetBytes(f.context(), server.Url(), nullptr, HttpSecrets::NONE, 0) == body);
	const auto elapsed = SecondsSince(start);
	INFO("elapsed " << elapsed << " s");
	CHECK(elapsed > 5.0);
	CHECK(server.Requests() == 1);
}

TEST_CASE("a stalled GET fails within the stall timeout per attempt, at most 3 attempts",
          "[external-location][storage-timeout]") {
	ScopedLoopbackHook on("1");
	TrickleServer server(TrickleMode::GET_STALL, PatternBody(256 * 1024), 0, 0);
	Fixture f;
	REQUIRE_FALSE(f.con.Query("SET http_retries = 3")->HasError());
	REQUIRE_FALSE(f.con.Query("SET vgi_storage_stall_timeout_seconds = 1")->HasError());
	const auto start = std::chrono::steady_clock::now();
	std::string error;
	try {
		HttpGetBytes(f.context(), server.Url(), nullptr, HttpSecrets::NONE, 0);
		FAIL("a stalled GET succeeded");
	} catch (const std::exception &e) {
		error = e.what();
	}
	const auto elapsed = SecondsSince(start);
	const auto offsets = server.ArrivalOffsets();
	INFO("elapsed " << elapsed << " s, error " << error);
	CHECK(server.Requests() == 3);
	REQUIRE(offsets.size() == 3);
	for (size_t i = 1; i < offsets.size(); i++) {
		const auto gap = offsets[i] - offsets[i - 1];
		INFO("attempt " << i << " lasted " << gap << " s");
		CHECK(gap >= 1.0);
		CHECK(gap < kStallBound);
	}
	CHECK(elapsed < 3 * kStallBound);
	// The pre-signed query never reaches the message.
	CHECK(error.find("sekrit-sig-value") == std::string::npos);
}

TEST_CASE("an http_retries of 0 makes a stalled storage GET a single attempt", "[external-location][storage-timeout]") {
	ScopedLoopbackHook on("1");
	TrickleServer server(TrickleMode::GET_STALL, PatternBody(256 * 1024), 0, 0);
	Fixture f; // http_retries = 0
	REQUIRE_FALSE(f.con.Query("SET vgi_storage_stall_timeout_seconds = 1")->HasError());
	CHECK_THROWS(HttpGetBytes(f.context(), server.Url(), nullptr, HttpSecrets::NONE, 0));
	CHECK(server.Requests() == 1);
}

TEST_CASE("a GET retried after a stall delivers the body once, not the stalled prefix plus the body",
          "[external-location][storage-timeout]") {
	ScopedLoopbackHook on("1");
	const auto body = PatternBody(256 * 1024);
	TrickleServer server(TrickleMode::GET_STALL_ONCE, body, 0, 0);
	Fixture f;
	REQUIRE_FALSE(f.con.Query("SET http_retries = 3")->HasError());
	REQUIRE_FALSE(f.con.Query("SET vgi_storage_stall_timeout_seconds = 1")->HasError());
	const auto got = HttpGetBytes(f.context(), server.Url(), nullptr, HttpSecrets::NONE, 0);
	CHECK(server.Requests() == 2);
	CHECK(got.size() == body.size());
	CHECK(got == body);
}

TEST_CASE("a slow PUT that outlasts the stall timeout succeeds in one attempt",
          "[external-location][storage-timeout]") {
	ScopedLoopbackHook on("1");
	// The server reads 16 KiB every 80 ms (about 200 KB/s): 1.5 MB takes about
	// 7 s against a 2 s stall timeout.
	const auto body = PatternBody(1536 * 1024);
	TrickleServer server(TrickleMode::PUT_SLOW, std::string(), 16 * 1024, 80);
	Fixture f;
	REQUIRE_FALSE(f.con.Query("SET http_retries = 3")->HasError());
	REQUIRE_FALSE(f.con.Query("SET vgi_storage_stall_timeout_seconds = 2")->HasError());
	REQUIRE_FALSE(f.con.Query("SET vgi_http_timeout_seconds = 1")->HasError());
	const auto start = std::chrono::steady_clock::now();
	HttpPutBytes(f.context(), server.Url(), reinterpret_cast<const uint8_t *>(body.data()), body.size(),
	             HttpEncoding::NONE, {});
	const auto elapsed = SecondsSince(start);
	INFO("elapsed " << elapsed << " s");
	CHECK(elapsed > 5.0);
	CHECK(server.Requests() == 1);
	CHECK(server.BodyBytesReceived() == body.size());
}

TEST_CASE("a PUT whose server stops reading fails within the stall timeout per attempt",
          "[external-location][storage-timeout]") {
	ScopedLoopbackHook on("1");
	// Far more than the loopback buffers hold, so the client blocks.
	const auto body = PatternBody(8 * 1024 * 1024);
	TrickleServer server(TrickleMode::PUT_STALL, std::string(), 0, 0);
	Fixture f;
	REQUIRE_FALSE(f.con.Query("SET http_retries = 3")->HasError());
	REQUIRE_FALSE(f.con.Query("SET vgi_storage_stall_timeout_seconds = 1")->HasError());
	const auto start = std::chrono::steady_clock::now();
	std::string error;
	try {
		HttpPutBytes(f.context(), server.Url(), reinterpret_cast<const uint8_t *>(body.data()), body.size(),
		             HttpEncoding::NONE, {});
		FAIL("a PUT to a server that stopped reading succeeded");
	} catch (const std::exception &e) {
		error = e.what();
	}
	const auto elapsed = SecondsSince(start);
	const auto offsets = server.ArrivalOffsets();
	INFO("elapsed " << elapsed << " s, error " << error);
	CHECK(server.Requests() == 3);
	REQUIRE(offsets.size() == 3);
	for (size_t i = 1; i < offsets.size(); i++) {
		const auto gap = offsets[i] - offsets[i - 1];
		INFO("attempt " << i << " lasted " << gap << " s");
		CHECK(gap >= 1.0);
		CHECK(gap < kStallBound);
	}
	CHECK(elapsed < 3 * kStallBound);
	CHECK(error.find("sekrit-sig-value") == std::string::npos);
}

TEST_CASE("S3, R2 and MinIO checksum refusals of a pre-signed PUT are CHECKSUM_MISMATCH", "[external-location]") {
	ScopedLoopbackHook on("1");
	const auto body = PatternBody(4096);
	for (const char *code : {"BadDigest", "XAmzContentSHA256Mismatch", "XAmzContentChecksumMismatch"}) {
		INFO("code " << code);
		TrickleServer server(TrickleMode::PUT_REJECT, code, 64 * 1024, 0);
		Fixture f;
		try {
			HttpPutBytes(f.context(), server.Url(), reinterpret_cast<const uint8_t *>(body.data()), body.size(),
			             HttpEncoding::NONE, {});
			FAIL("a refused PUT succeeded");
		} catch (const IOException &e) {
			const std::string error = e.what();
			CHECK(error.find("storage checksum mismatch") != std::string::npos);
			CHECK(error.find("\"error_subtype\":\"CHECKSUM_MISMATCH\"") != std::string::npos);
			CHECK(error.find("sekrit-sig-value") == std::string::npos);
		}
		CHECK(server.Requests() == 1);
	}
}
