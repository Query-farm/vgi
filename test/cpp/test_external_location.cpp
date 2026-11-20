// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// External-location (vgi_rpc.location) client policy: which URLs are fetched,
// the fetched-size cap, and the request-upload decision. The end-to-end paths
// run in test/sql/external_storage/ (make test_http_vendor).

#include "catch.hpp"

#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "httpfs_extension.hpp"
#include "vgi_exception.hpp"
#include "vgi_extension.hpp"
#include "vgi_http_client.hpp"
#include "vgi_server_capabilities.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cstdlib>
#include <string>
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
