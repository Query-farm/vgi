// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// POSIX-only — see vgi_launcher.cpp's banner for the WASM exclusion policy.

#ifndef __EMSCRIPTEN__

#include "vgi_unix_socket_worker.hpp"

#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace duckdb {
namespace vgi {

UnixSocketWorker::UnixSocketWorker(int connected_fd) : socket_fd_(connected_fd) {
	// SubProcess's stdin_fd_/stdout_fd_/stderr_fd_/pid_ stay at their
	// default -1 sentinels so the base destructor's pid_ > 0 / fd >= 0
	// guards skip the fork-cleanup path.
}

UnixSocketWorker::~UnixSocketWorker() {
	if (socket_fd_ >= 0) {
		::close(socket_fd_);
	}
}

void UnixSocketWorker::CloseStdin() {
	// FunctionConnection calls CloseStdin() to signal EOF to the worker
	// after sending the last input batch.  On a bidirectional AF_UNIX
	// socket, the equivalent is half-closing the write direction so the
	// worker sees EOF on its reads while we can still receive responses
	// on the same fd.
	if (socket_fd_ >= 0 && !write_half_closed_) {
		::shutdown(socket_fd_, SHUT_WR);
		write_half_closed_ = true;
	}
}

int UnixSocketWorker::Wait(bool *exited_normally) {
	// We don't own the worker process — its lifetime is governed by the
	// launcher's idle timeout, not by us.  The Wait contract from
	// FunctionConnection is "wait for the process to exit"; for AF_UNIX
	// we treat this as "the connection has gone away cleanly."
	if (exited_normally) {
		*exited_normally = true;
	}
	return 0;
}

bool UnixSocketWorker::IsLikelyAlive() const {
	if (socket_fd_ < 0) {
		return false;
	}
	struct pollfd pfd;
	pfd.fd = socket_fd_;
	pfd.events = POLLIN; // Pure liveness — we don't actually want to read.
	pfd.revents = 0;
	// Zero-ms poll: check for HUP/ERR without blocking.  A peer-side
	// shutdown raises POLLHUP; a transport-level error (rare for AF_UNIX
	// stream sockets but possible) raises POLLERR.  POLLIN with no error
	// flags means there's pending data — also a "still alive" signal.
	int rc = ::poll(&pfd, 1, 0);
	if (rc < 0) {
		// Polling failed — be conservative; assume alive and let the
		// next real I/O operation surface the truth.
		return true;
	}
	if (rc == 0) {
		// No events, no errors — connection is idle but presumably alive.
		return true;
	}
	// Any of these means the peer is gone or in an error state.
	if (pfd.revents & (POLLHUP | POLLERR | POLLNVAL)) {
		return false;
	}
	return true;
}

} // namespace vgi
} // namespace duckdb

#endif // !__EMSCRIPTEN__
