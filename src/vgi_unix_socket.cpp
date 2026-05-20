// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// POSIX-only — under emscripten this translation unit is empty.  See
// vgi_launcher.cpp's banner comment for the policy.

#ifndef __EMSCRIPTEN__

#include "vgi_unix_socket.hpp"

#include "duckdb/common/exception.hpp"
#include "vgi_launcher_internal.hpp"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

namespace duckdb {
namespace vgi {

namespace {

// RAII helper that closes a non-negative fd on scope exit unless `Release()`
// is called first.  Used inside Connect() to make the early-return error
// paths leak-free without an explicit close on each branch.
class FdGuard {
public:
	explicit FdGuard(int fd) : fd_(fd) {}
	FdGuard(const FdGuard &) = delete;
	FdGuard &operator=(const FdGuard &) = delete;
	~FdGuard() {
		if (fd_ >= 0) {
			::close(fd_);
		}
	}
	int Release() {
		int fd = fd_;
		fd_ = -1;
		return fd;
	}

private:
	int fd_;
};

// Set/clear O_NONBLOCK on `fd`.  Returns false on failure (errno is set).
bool SetNonBlocking(int fd, bool nonblock) {
	int flags = ::fcntl(fd, F_GETFL, 0);
	if (flags < 0) {
		return false;
	}
	if (nonblock) {
		flags |= O_NONBLOCK;
	} else {
		flags &= ~O_NONBLOCK;
	}
	return ::fcntl(fd, F_SETFL, flags) == 0;
}

} // namespace

UnixSocket::UnixSocket(UnixSocket &&other) noexcept : fd_(other.fd_) {
	other.fd_ = -1;
}

UnixSocket &UnixSocket::operator=(UnixSocket &&other) noexcept {
	if (this != &other) {
		Close();
		fd_ = other.fd_;
		other.fd_ = -1;
	}
	return *this;
}

UnixSocket::~UnixSocket() {
	Close();
}

void UnixSocket::Close() {
	if (fd_ >= 0) {
		::close(fd_);
		fd_ = -1;
	}
}

int UnixSocket::Release() {
	int fd = fd_;
	fd_ = -1;
	return fd;
}

UnixSocket UnixSocket::Connect(const std::string &path, std::chrono::milliseconds connect_timeout) {
	// Cheap, deterministic check before we touch the kernel.  Translates
	// std::invalid_argument from ValidateUnixPathLength into the DuckDB
	// exception users expect at the boundary.
	try {
		launcher::ValidateUnixPathLength(path);
	} catch (const std::invalid_argument &e) {
		throw InvalidInputException(e.what());
	}

	int raw_fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
	if (raw_fd < 0) {
		throw IOException("vgi unix socket: socket() failed for %s: %s", path,
		                   std::strerror(errno));
	}
	FdGuard guard(raw_fd);

	// We use a non-blocking connect + select() to enforce the timeout.
	// AF_UNIX connect normally returns immediately, but a hung worker
	// could leave the kernel queue in a state that blocks us.
	if (!SetNonBlocking(raw_fd, true)) {
		throw IOException("vgi unix socket: fcntl(O_NONBLOCK) failed for %s: %s", path,
		                   std::strerror(errno));
	}

	struct sockaddr_un addr;
	std::memset(&addr, 0, sizeof(addr));
	addr.sun_family = AF_UNIX;
	// Length already validated above so the strncpy is safe.
	std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);

	int rc = ::connect(raw_fd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr));
	if (rc == 0) {
		// Connected immediately — common case for an alive AF_UNIX worker.
	} else if (errno == EINPROGRESS) {
		// Wait for the socket to become writable, signalling that connect
		// finished.  ``select()`` (rather than poll) keeps the dependency
		// surface aligned with the rest of vgi, which already uses it via
		// ``WaitForReadable``.
		fd_set wfds;
		FD_ZERO(&wfds);
		FD_SET(raw_fd, &wfds);
		struct timeval tv;
		tv.tv_sec = static_cast<time_t>(connect_timeout.count() / 1000);
		tv.tv_usec = static_cast<suseconds_t>((connect_timeout.count() % 1000) * 1000);
		int sel = ::select(raw_fd + 1, nullptr, &wfds, nullptr, &tv);
		if (sel == 0) {
			throw IOException("vgi unix socket: connect to %s timed out after %lldms", path,
			                   static_cast<long long>(connect_timeout.count()));
		}
		if (sel < 0) {
			throw IOException("vgi unix socket: select() failed for %s: %s", path,
			                   std::strerror(errno));
		}
		// select() said writable; check SO_ERROR to confirm connect succeeded.
		int so_err = 0;
		socklen_t so_err_len = sizeof(so_err);
		if (::getsockopt(raw_fd, SOL_SOCKET, SO_ERROR, &so_err, &so_err_len) < 0) {
			throw IOException("vgi unix socket: getsockopt(SO_ERROR) failed for %s: %s", path,
			                   std::strerror(errno));
		}
		if (so_err != 0) {
			throw IOException("vgi unix socket: connect to %s failed: %s", path,
			                   std::strerror(so_err));
		}
	} else {
		throw IOException("vgi unix socket: connect to %s failed: %s", path,
		                   std::strerror(errno));
	}

	// Restore blocking mode for downstream read/write — vgi's IPC code
	// expects classic blocking semantics.
	if (!SetNonBlocking(raw_fd, false)) {
		throw IOException("vgi unix socket: fcntl(clear O_NONBLOCK) failed for %s: %s", path,
		                   std::strerror(errno));
	}

	return UnixSocket(guard.Release());
}

} // namespace vgi
} // namespace duckdb

#endif // !__EMSCRIPTEN__
