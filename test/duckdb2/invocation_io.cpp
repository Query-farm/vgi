// Isolated DuckDB 2 invocation interoperability profile regression.
#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "vgi_arrow_ipc.hpp"
#include <chrono>
#include <fcntl.h>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <unistd.h>

using namespace duckdb;
using namespace duckdb::vgi;

struct Pipe {
	int fds[2];
	Pipe() { if (pipe(fds) != 0) throw std::runtime_error("pipe failed"); }
	~Pipe() { close(fds[0]); close(fds[1]); }
};

static void Require(bool condition, const char *message) {
	if (!condition) throw std::runtime_error(message);
}

static void Fill(Pipe &pipe) {
	auto flags = fcntl(pipe.fds[1], F_GETFL);
	Require(flags >= 0 && fcntl(pipe.fds[1], F_SETFL, flags | O_NONBLOCK) == 0, "fill setup");
	char bytes[4096] = {};
	while (write(pipe.fds[1], bytes, sizeof(bytes)) > 0) {}
	Require(errno == EAGAIN || errno == EWOULDBLOCK, "fill must stop at full pipe");
	Require(fcntl(pipe.fds[1], F_SETFL, flags) == 0, "fill restore");
}

int main(int argc, char **) {
	try {
		DuckDB database(nullptr);
		Connection connection(database);
		auto &context = *connection.context;
		int passed = 0;
		for (int test = 0; test < 3; ++test) {
			Pipe pipe;
			if (test == 1) Require(write(pipe.fds[1], "x", 1) == 1, "partial frame seed");
			if (test == 2) Fill(pipe);
			const auto flags = fcntl(pipe.fds[test == 2 ? 1 : 0], F_GETFL);
			context.ClearInterrupt();
			std::thread cancel([&context]() {
				std::this_thread::sleep_for(std::chrono::milliseconds(150));
				context.Interrupt();
			});
			const auto start = std::chrono::steady_clock::now();
			std::string error;
			if (test == 2) {
				FdOutputStream stream(pipe.fds[1], &context);
				error = stream.Write("test", 4).ToString();
			} else {
				FdInputStream stream(pipe.fds[0], &context);
				char bytes[2];
				auto result = stream.Read(test == 1 ? 2 : 1, bytes);
				error = result.ok() ? "unexpected successful read" : result.status().ToString();
			}
			cancel.join();
			Require(error.find("interrupted") != std::string::npos, "cancellation diagnostic missing");
			Require(std::chrono::steady_clock::now() - start < std::chrono::seconds(2), "cancellation was not prompt");
			Require(fcntl(pipe.fds[test == 2 ? 1 : 0], F_GETFL) == flags, "descriptor flags not restored");
			std::cout << "PASS cancellation " << ++passed << std::endl;
		}
		context.ClearInterrupt();
		{
			Pipe pipe;
			FdOutputStream output(pipe.fds[1], &context);
			Require(output.Write("abc", 3).ok(), "healthy write failed");
			FdInputStream input(pipe.fds[0], &context);
			char bytes[3];
			auto result = input.Read(3, bytes);
			Require(result.ok() && *result == 3 && std::string(bytes, 3) == "abc", "healthy payload changed");
			std::cout << "PASS transfer " << ++passed << std::endl;
		}
		// Explicit opt-in because these two tests exercise the actual 30-second ceiling.
		if (argc > 1) {
			for (bool writing : {false, true}) {
				Pipe pipe;
				if (writing) Fill(pipe);
				const auto start = std::chrono::steady_clock::now();
				std::string error;
				if (writing) {
					FdOutputStream output(pipe.fds[1], &context);
					error = output.Write("x", 1).ToString();
				} else {
					FdInputStream input(pipe.fds[0], &context);
					char byte;
					auto result = input.Read(1, &byte);
					Require(!result.ok(), "idle read unexpectedly succeeded");
					error = result.status().ToString();
				}
				auto elapsed = std::chrono::steady_clock::now() - start;
				Require(error.find("idle I/O timeout") != std::string::npos, "idle timeout diagnostic missing");
				Require(elapsed >= std::chrono::seconds(29) && elapsed < std::chrono::seconds(33), "idle timeout exceeded ceiling");
				std::cout << "PASS idle timeout " << ++passed << std::endl;
			}
		}
		std::cout << "PASS " << passed << " invocation transport cases" << std::endl;
		return 0;
	} catch (const std::exception &error) {
		std::cerr << error.what() << std::endl;
		return 1;
	}
}
