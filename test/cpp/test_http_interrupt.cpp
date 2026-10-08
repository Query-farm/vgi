// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// A VGI HTTP request made for a user's scan is armed with that query's
// interrupt flag, so an interrupt aborts the transfer in flight and surfaces as
// InterruptException. Requests that are not armed (the cancel dispatcher's
// server-side cancel, rollbacks) must still go out while the flag is set.
//
// The server here accepts and reads a request, then answers late or never, so
// the only way out of a stalled request is the interrupt. Needs httpfs's curl
// backend: the in-tree httplib fallback cannot abort a transfer.

#include "catch.hpp"

#include "duckdb.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "httpfs_extension.hpp"
#include "vgi_http_client.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

using namespace duckdb;
using namespace duckdb::vgi;

namespace {

// Answers each connection with an empty 200 after `reply_delay`, holding it
// open until then. Destruction stops it promptly whatever the delay.
class SlowServer {
public:
	explicit SlowServer(std::chrono::milliseconds reply_delay) : reply_delay_(reply_delay) {
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

	~SlowServer() {
		stop_.store(true);
		thread_.join();
		close(listen_fd_);
	}

	std::string Url(const std::string &path) const {
		return "http://127.0.0.1:" + std::to_string(port_) + path;
	}

	int Accepted() const {
		return accepted_.load();
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
		std::vector<std::thread> handlers;
		while (!stop_.load()) {
			if (!WaitReadable(listen_fd_, 50)) {
				continue;
			}
			int fd = accept(listen_fd_, nullptr, nullptr);
			if (fd < 0) {
				continue;
			}
			accepted_.fetch_add(1);
			handlers.emplace_back([this, fd] { Serve(fd); });
		}
		for (auto &h : handlers) {
			h.join();
		}
	}

	void Serve(int fd) {
		// Drain whatever the client sends; the reply never depends on it.
		auto deadline = std::chrono::steady_clock::now() + reply_delay_;
		char buf[4096];
		while (!stop_.load() && std::chrono::steady_clock::now() < deadline) {
			if (WaitReadable(fd, 20) && recv(fd, buf, sizeof(buf), 0) <= 0) {
				break;
			}
		}
		if (!stop_.load()) {
			static const char reply[] = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
			(void)send(fd, reply, sizeof(reply) - 1, 0);
		}
		close(fd);
	}

	std::chrono::milliseconds reply_delay_;
	int listen_fd_ = -1;
	int port_ = 0;
	std::atomic<bool> stop_ {false};
	std::atomic<int> accepted_ {0};
	std::thread thread_;
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

// Flips the query's interrupt flag after `delay`, as Connection::Interrupt does.
std::thread InterruptAfter(ClientContext &context, std::chrono::milliseconds delay) {
	return std::thread([&context, delay] {
		std::this_thread::sleep_for(delay);
		context.Interrupt();
	});
}

double SecondsSince(std::chrono::steady_clock::time_point start) {
	return std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
}

} // namespace

TEST_CASE("an armed POST is aborted mid-flight by an interrupt", "[http-interrupt]") {
	SlowServer server(std::chrono::seconds(30));
	Fixture f;
	auto &context = f.context();
	auto url = server.Url("/vgi/main/init/exchange");
	std::vector<uint8_t> body(16, 0);

	auto start = std::chrono::steady_clock::now();
	auto interrupter = InterruptAfter(context, std::chrono::milliseconds(300));
	bool interrupted = false;
	try {
		context.RunFunctionInTransaction([&] {
			(void)HttpPostArrowIpc(context, url, body, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
			                       &context.interrupted);
		});
	} catch (const InterruptException &) {
		interrupted = true;
	}
	interrupter.join();
	auto elapsed = SecondsSince(start);
	INFO("elapsed " << elapsed << "s");
	REQUIRE(interrupted);
	REQUIRE(server.Accepted() == 1);
	// The server would hold the request for 30s.
	REQUIRE(elapsed < 5.0);
}

TEST_CASE("an armed GET is aborted mid-flight by an interrupt", "[http-interrupt]") {
	SlowServer server(std::chrono::seconds(30));
	Fixture f;
	auto &context = f.context();
	auto url = server.Url("/external/batch");

	auto start = std::chrono::steady_clock::now();
	auto interrupter = InterruptAfter(context, std::chrono::milliseconds(300));
	bool interrupted = false;
	try {
		context.RunFunctionInTransaction([&] { (void)HttpGetBytes(context, url, &context.interrupted); });
	} catch (const InterruptException &) {
		interrupted = true;
	}
	interrupter.join();
	auto elapsed = SecondsSince(start);
	INFO("elapsed " << elapsed << "s");
	REQUIRE(interrupted);
	REQUIRE(elapsed < 5.0);
}

TEST_CASE("an unarmed POST still completes while the query is interrupted", "[http-interrupt]") {
	// The cancel dispatcher's server-side cancel and a rollback both run after
	// the user's query was interrupted; they must reach the server.
	SlowServer server(std::chrono::milliseconds(1000));
	Fixture f;
	auto &context = f.context();
	auto url = server.Url("/vgi/main/init/exchange");
	std::vector<uint8_t> body(16, 0);

	auto start = std::chrono::steady_clock::now();
	auto interrupter = InterruptAfter(context, std::chrono::milliseconds(100));
	std::string error;
	try {
		context.RunFunctionInTransaction(
		    [&] { (void)HttpPostArrowIpc(context, url, body); });
	} catch (const InterruptException &) {
		FAIL("an unarmed request must not be aborted by the query's interrupt");
	} catch (const std::exception &e) {
		error = e.what();
	}
	interrupter.join();
	auto elapsed = SecondsSince(start);
	INFO("elapsed " << elapsed << "s, error: " << error);
	// It waited out the server's reply. The bare 200 is not a VGI response, which
	// is the error it reports — the point is that it got that far.
	REQUIRE(elapsed >= 0.9);
	REQUIRE(error.find("did not respond as a VGI server") != std::string::npos);
}
