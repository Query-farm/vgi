// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Thin AF_UNIX client wrapper.  Used by both ``unix://`` (operator-managed
// worker, no spawn) and ``launch:`` (vgi C++ launcher) code paths.

#pragma once

#include <chrono>
#include <string>

#include "vgi_platform.hpp"

#if VGI_POSIX_TRANSPORT

namespace duckdb {
namespace vgi {

// Owns an open AF_UNIX SOCK_STREAM file descriptor; closes it on destruction.
//
// All errors throw ``duckdb::IOException`` with a message that names the
// path and the underlying errno so DuckDB users see a clean diagnostic
// instead of a raw POSIX number.
class UnixSocket {
public:
	UnixSocket() = default;

	// Non-copyable, movable.
	UnixSocket(const UnixSocket &) = delete;
	UnixSocket &operator=(const UnixSocket &) = delete;
	UnixSocket(UnixSocket &&other) noexcept;
	UnixSocket &operator=(UnixSocket &&other) noexcept;

	~UnixSocket();

	// Open a fresh AF_UNIX SOCK_STREAM socket and connect() to *path*.
	// Optional connect timeout (default 2s) bounds how long we wait for
	// the kernel to accept; a hung worker doesn't make us hang forever.
	// Throws ``IOException`` on path-too-long, socket()/connect() failure,
	// timeout, or refused connection.
	static UnixSocket Connect(const std::string &path,
	                           std::chrono::milliseconds connect_timeout = std::chrono::seconds(2));

	// True iff a fd is currently held.
	bool IsOpen() const {
		return fd_ >= 0;
	}

	int GetFd() const {
		return fd_;
	}

	// Release ownership of the fd; caller becomes responsible for closing.
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
