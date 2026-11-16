// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// AF_UNIX client socket, used by ``unix://`` (operator-managed worker) and
// ``launch:`` (Launch()-managed worker).

#pragma once

#include <chrono>
#include <string>

#include "vgi_platform.hpp"

#if VGI_POSIX_TRANSPORT

namespace duckdb {
namespace vgi {

// Owns an AF_UNIX SOCK_STREAM fd; closes it on destruction.
class UnixSocket {
public:
	UnixSocket() = default;

	UnixSocket(const UnixSocket &) = delete;
	UnixSocket &operator=(const UnixSocket &) = delete;
	UnixSocket(UnixSocket &&other) noexcept;
	UnixSocket &operator=(UnixSocket &&other) noexcept;

	~UnixSocket();

	// Connect a new socket to *path* within ``connect_timeout``; the returned
	// fd is blocking. Throws InvalidInputException for a too-long path and
	// IOException (naming path and errno) for socket/connect failure, refusal
	// or timeout. A full accept queue (IsAcceptQueueFull) is retried until the
	// timeout rather than treated as refusal.
	static UnixSocket Connect(const std::string &path,
	                           std::chrono::milliseconds connect_timeout = std::chrono::seconds(2));

	// One non-blocking connect() to *path*, closed at once, for a liveness
	// probe.  Returns 0 when it connected (or is connecting), else its errno.
	// Never throws.
	static int ProbeOnce(const std::string &path);

	// True for a connect() errno meaning "listener alive, accept queue full"
	// (Linux: EAGAIN). macOS reports ECONNREFUSED, indistinguishable from no
	// listener.
	static bool IsAcceptQueueFull(int err);

	bool IsOpen() const {
		return fd_ >= 0;
	}

	int GetFd() const {
		return fd_;
	}

	// Release ownership of the fd to the caller.
	int Release();

	// Close the fd (idempotent).
	void Close();

private:
	explicit UnixSocket(int fd) : fd_(fd) {}
	int fd_ = -1;
};

} // namespace vgi
} // namespace duckdb

#endif // VGI_POSIX_TRANSPORT
