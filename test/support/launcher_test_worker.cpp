// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Minimal AF_UNIX worker that satisfies the cross-language launcher contract:
//   - accepts ``--unix PATH`` and ``--idle-timeout SEC``
//   - binds AF_UNIX, prints ``UNIX:<abs_path>\n`` to stdout (flushed)
//   - accepts connections, then idle-shuts-down per the contract
//
// Standalone — no DuckDB or vgi-rpc dependency.  Used by Layer-3 integration
// tests that exercise the launcher orchestration without requiring Python.

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <mutex>
#include <string>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <thread>
#include <unistd.h>

namespace {

[[noreturn]] void Usage(int rc) {
	std::fprintf(stderr,
	              "usage: launcher_test_worker --unix PATH [--idle-timeout SEC] "
	              "[--noisy-prefix N] [--no-bind] [--exit-with N] "
	              "[--dump-argv-to FILE]\n");
	std::exit(rc);
}

struct Args {
	std::string path;
	double idle_timeout_s = 0.0; // 0 = forever
	int noisy_prefix_lines = 0;  // emit N pre-`UNIX:` noise lines
	bool no_bind = false;        // skip binding entirely (used to test startup-timeout)
	int exit_with = -1;          // if >=0, exit immediately with this code
	std::string dump_argv_path;  // if set, write full argv (one token per line) here
	                             // before doing anything else.  Used by tests to
	                             // verify the launcher passed the expected args.
};

Args ParseArgs(int argc, char **argv) {
	Args a;
	for (int i = 1; i < argc; ++i) {
		std::string s = argv[i];
		if (s == "--unix" && i + 1 < argc) {
			a.path = argv[++i];
		} else if (s == "--idle-timeout" && i + 1 < argc) {
			a.idle_timeout_s = std::strtod(argv[++i], nullptr);
		} else if (s == "--noisy-prefix" && i + 1 < argc) {
			a.noisy_prefix_lines = std::atoi(argv[++i]);
		} else if (s == "--no-bind") {
			a.no_bind = true;
		} else if (s == "--exit-with" && i + 1 < argc) {
			a.exit_with = std::atoi(argv[++i]);
		} else if (s == "--dump-argv-to" && i + 1 < argc) {
			a.dump_argv_path = argv[++i];
		} else if (s == "-h" || s == "--help") {
			Usage(0);
		} else {
			std::fprintf(stderr, "launcher_test_worker: unknown arg: %s\n", s.c_str());
			Usage(2);
		}
	}
	return a;
}

} // namespace

int main(int argc, char **argv) {
	Args args = ParseArgs(argc, argv);

	if (!args.dump_argv_path.empty()) {
		FILE *f = std::fopen(args.dump_argv_path.c_str(), "w");
		if (f) {
			for (int i = 0; i < argc; ++i) {
				std::fprintf(f, "%s\n", argv[i]);
			}
			std::fclose(f);
		}
	}

	if (args.exit_with >= 0) {
		// Test scenario: worker exits before readiness.
		return args.exit_with;
	}
	if (args.no_bind) {
		// Test scenario: worker hangs without ever binding.  Simulates a
		// stuck startup that the launcher should cancel via timeout.
		while (true) {
			std::this_thread::sleep_for(std::chrono::seconds(60));
		}
	}
	if (args.path.empty()) {
		std::fprintf(stderr, "launcher_test_worker: --unix PATH is required\n");
		return 2;
	}

	for (int i = 0; i < args.noisy_prefix_lines; ++i) {
		std::printf("non-discovery noise line %d\n", i);
		std::fflush(stdout);
	}

	int listen_fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
	if (listen_fd < 0) {
		std::perror("launcher_test_worker: socket");
		return 1;
	}
	::unlink(args.path.c_str()); // best-effort stale cleanup
	struct sockaddr_un addr;
	std::memset(&addr, 0, sizeof(addr));
	addr.sun_family = AF_UNIX;
	std::strncpy(addr.sun_path, args.path.c_str(), sizeof(addr.sun_path) - 1);
	mode_t old_umask = ::umask(0077);
	if (::bind(listen_fd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) != 0) {
		std::perror("launcher_test_worker: bind");
		::umask(old_umask);
		return 1;
	}
	::umask(old_umask);
	if (::listen(listen_fd, 16) != 0) {
		std::perror("launcher_test_worker: listen");
		return 1;
	}

	std::printf("UNIX:%s\n", args.path.c_str());
	std::fflush(stdout);

	// Track active connection count + idle timer.  Simple monolithic accept
	// loop with select() so we can wake up on a timer.
	int active = 0;
	std::mutex m;
	auto last_idle_start = std::chrono::steady_clock::now();
	while (true) {
		struct timeval tv;
		tv.tv_sec = 1;
		tv.tv_usec = 0;
		fd_set rfds;
		FD_ZERO(&rfds);
		FD_SET(listen_fd, &rfds);
		int sel = ::select(listen_fd + 1, &rfds, nullptr, nullptr, &tv);
		if (sel > 0 && FD_ISSET(listen_fd, &rfds)) {
			int client_fd = ::accept(listen_fd, nullptr, nullptr);
			if (client_fd < 0) {
				continue;
			}
			{
				std::lock_guard<std::mutex> lk(m);
				++active;
			}
			std::thread([client_fd, &active, &m, &last_idle_start]() {
				char buf[1024];
				while (true) {
					ssize_t r = ::read(client_fd, buf, sizeof(buf));
					if (r <= 0) {
						break;
					}
				}
				::close(client_fd);
				std::lock_guard<std::mutex> lk(m);
				if (--active == 0) {
					last_idle_start = std::chrono::steady_clock::now();
				}
			}).detach();
		}

		if (args.idle_timeout_s > 0) {
			std::lock_guard<std::mutex> lk(m);
			if (active == 0) {
				auto now = std::chrono::steady_clock::now();
				auto idle = std::chrono::duration_cast<std::chrono::duration<double>>(
				    now - last_idle_start);
				if (idle.count() >= args.idle_timeout_s) {
					break;
				}
			} else {
				last_idle_start = std::chrono::steady_clock::now();
			}
		}
	}

	::close(listen_fd);
	::unlink(args.path.c_str());
	return 0;
}
