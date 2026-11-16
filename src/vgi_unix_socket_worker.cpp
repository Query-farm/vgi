// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// POSIX-only — see vgi_launcher.cpp's banner for the WASM exclusion policy.

#include "vgi_platform.hpp"

#if VGI_POSIX_TRANSPORT

#include "vgi_unix_socket_worker.hpp"

#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace duckdb {
namespace vgi {

UnixSocketWorker::UnixSocketWorker(int connected_fd) : socket_fd_(connected_fd) {
	// Base pid_/fds stay -1 so ~SubProcess skips child cleanup.
}

UnixSocketWorker::~UnixSocketWorker() {
	if (socket_fd_ >= 0) {
		::close(socket_fd_);
	}
}

void UnixSocketWorker::CloseStdin() {
	if (socket_fd_ >= 0 && !write_half_closed_) {
		::shutdown(socket_fd_, SHUT_WR);
		write_half_closed_ = true;
	}
}

int UnixSocketWorker::Wait(bool *exited_normally) {
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
	pfd.events = POLLIN;
	pfd.revents = 0;
	int rc = ::poll(&pfd, 1, 0);
	if (rc < 0) {
		// Assume alive; the next real I/O surfaces the truth.
		return true;
	}
	if (rc == 0) {
		return true;
	}
	if (pfd.revents & (POLLHUP | POLLERR | POLLNVAL)) {
		return false;
	}
	return true;
}

} // namespace vgi
} // namespace duckdb

#endif // VGI_POSIX_TRANSPORT
