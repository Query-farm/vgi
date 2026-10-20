// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Pre-signed URL requests (externalized batch downloads, upload PUTs) are built
// without a secret lookup: a lookup from inside VgiTransaction::Start
// self-deadlocks on the MetaTransaction mutex (duckdb#22258), and a secret's
// bearer token or extra headers must not reach a third-party storage host.
// The deadlock itself is exercised by `make test_http_demo_storage`.

#include "catch.hpp"

#include "duckdb.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "httpfs_extension.hpp"
#include "vgi_http_client.hpp"
#include "vgi_http_params.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace duckdb;
using namespace duckdb::vgi;

namespace {

// Answers every request with a 200 "ok" and keeps each request's head.
class RecordingServer {
public:
	RecordingServer() {
		listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
		REQUIRE(listen_fd_ >= 0);
		int one = 1;
		setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
		sockaddr_in addr {};
		addr.sin_family = AF_INET;
		addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		addr.sin_port = 0;
		REQUIRE(bind(listen_fd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) == 0);
		REQUIRE(listen(listen_fd_, 8) == 0);
		socklen_t len = sizeof(addr);
		REQUIRE(getsockname(listen_fd_, reinterpret_cast<sockaddr *>(&addr), &len) == 0);
		port_ = ntohs(addr.sin_port);
		thread_ = std::thread([this] { Run(); });
	}

	~RecordingServer() {
		stop_.store(true);
		thread_.join();
		close(listen_fd_);
	}

	std::string Url(const std::string &path) const {
		return "http://127.0.0.1:" + std::to_string(port_) + path;
	}

	std::vector<std::string> Requests() {
		std::lock_guard<std::mutex> lock(mutex_);
		return requests_;
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
			if (fd >= 0) {
				Serve(fd);
			}
		}
	}

	void Serve(int fd) {
		std::string head;
		char buf[4096];
		while (head.find("\r\n\r\n") == std::string::npos && WaitReadable(fd, 2000)) {
			auto n = recv(fd, buf, sizeof(buf), 0);
			if (n <= 0) {
				break;
			}
			head.append(buf, static_cast<size_t>(n));
		}
		{
			std::lock_guard<std::mutex> lock(mutex_);
			requests_.push_back(head);
		}
		static const char reply[] = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok";
		(void)send(fd, reply, sizeof(reply) - 1, 0);
		close(fd);
	}

	int listen_fd_ = -1;
	int port_ = 0;
	std::atomic<bool> stop_ {false};
	std::thread thread_;
	std::mutex mutex_;
	std::vector<std::string> requests_;
};

struct Fixture {
	Fixture() : db(nullptr), con(db) {
		db.LoadStaticExtension<HttpfsExtension>();
	}
	ClientContext &context() {
		return *con.context;
	}
	DuckDB db;
	Connection con;
};

bool Contains(const std::string &haystack, const std::string &needle) {
	return haystack.find(needle) != std::string::npos;
}

// An http secret scoped to the server that adds an Authorization header.
// (EXTRA_HTTP_HEADERS rides the same secret lookup, but its MAP literal needs
// core_functions, which this binary does not link.)
void CreateLeakySecret(Fixture &f, const std::string &scope) {
	auto result = f.con.Query("CREATE SECRET leaky (TYPE http, BEARER_TOKEN 'leaked-token', SCOPE '" + scope + "')");
	INFO("CREATE SECRET: " << (result->HasError() ? result->GetError() : "ok"));
	REQUIRE_FALSE(result->HasError());
}

} // namespace

TEST_CASE("an externalized-batch GET sends no secret headers", "[http-secret-free]") {
	RecordingServer server;
	Fixture f;
	CreateLeakySecret(f, server.Url("/"));
	auto &context = f.context();

	// A secret lookup needs the query's transaction, as it has in a scan.
	context.RunFunctionInTransaction([&] {
		// With secrets the scoped secret applies, so the check below is not vacuous.
		REQUIRE(HttpGetBytes(context, server.Url("/with-secrets"), nullptr, HttpSecrets::LOOKUP) == "ok");
		REQUIRE(HttpGetBytes(context, server.Url("/externalized")) == "ok");
	});

	auto requests = server.Requests();
	REQUIRE(requests.size() == 2);
	INFO("with secrets:\n" << requests[0] << "\nsecret-free:\n" << requests[1]);
	REQUIRE(Contains(requests[0], "leaked-token"));
	REQUIRE_FALSE(Contains(requests[1], "leaked-token"));
	REQUIRE_FALSE(Contains(requests[1], "Authorization"));
	REQUIRE(Contains(requests[1], "User-Agent: "));
}

TEST_CASE("an externalized-batch GET works inside VGI transaction start", "[http-secret-free]") {
	RecordingServer server;
	Fixture f;
	CreateLeakySecret(f, server.Url("/"));
	auto &context = f.context();

	context.RunFunctionInTransaction([&] {
		TransactionStartScope start_scope;
		REQUIRE(HttpGetBytes(context, server.Url("/externalized")) == "ok");
		// A secret lookup here would deadlock under DuckDB's lock; it throws instead.
		REQUIRE_THROWS_WITH(HttpGetBytes(context, server.Url("/with-secrets"), nullptr, HttpSecrets::LOOKUP),
		                    Catch::Contains("would deadlock"));
	});
	REQUIRE(server.Requests().size() == 1);
}

TEST_CASE("secret-free HTTP params keep the session's HTTP settings", "[http-secret-free]") {
	Fixture f;
	REQUIRE_FALSE(f.con.Query("SET http_retries = 7")->HasError());
	REQUIRE_FALSE(f.con.Query("SET http_keep_alive = false")->HasError());
	auto params = InitSecretFreeHttpParams(f.context());
	REQUIRE(params->retries == 7);
	REQUIRE_FALSE(params->keep_alive);
}
