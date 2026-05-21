// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Windows named-pipe client + worker. See vgi_named_pipe.hpp. Windows-only;
// empty TU elsewhere.

#include "vgi_platform.hpp"

#if defined(_WIN32)

#include "vgi_named_pipe.hpp"

#include "duckdb/common/exception.hpp"

#include <fcntl.h> // _O_BINARY
#include <io.h>     // _open_osfhandle, _get_osfhandle, _close
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>

namespace duckdb {
namespace vgi {

int NamedPipeConnect(const std::string &pipe_name, std::chrono::milliseconds timeout) {
	const auto deadline = std::chrono::steady_clock::now() + timeout;
	while (true) {
		HANDLE h = CreateFileA(pipe_name.c_str(), GENERIC_READ | GENERIC_WRITE, 0, nullptr, OPEN_EXISTING,
		                       0, nullptr);
		if (h != INVALID_HANDLE_VALUE) {
			// The server pipe is byte-mode; make sure our handle reads in byte
			// mode too (default, but be explicit).
			DWORD mode = PIPE_READMODE_BYTE;
			SetNamedPipeHandleState(h, &mode, nullptr, nullptr);
			int fd = _open_osfhandle(reinterpret_cast<intptr_t>(h), _O_BINARY);
			if (fd < 0) {
				CloseHandle(h);
				throw IOException("vgi named pipe: _open_osfhandle failed for %s", pipe_name);
			}
			return fd;
		}
		DWORD err = GetLastError();
		auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
		    deadline - std::chrono::steady_clock::now());
		if (remaining.count() <= 0) {
			throw IOException("vgi named pipe: connect to %s timed out (last GetLastError=%lu)", pipe_name,
			                  (unsigned long)err);
		}
		if (err == ERROR_PIPE_BUSY) {
			// All instances busy — wait for one to free up, then retry.
			DWORD wait_ms = static_cast<DWORD>(remaining.count());
			WaitNamedPipeA(pipe_name.c_str(), wait_ms);
			continue;
		}
		if (err == ERROR_FILE_NOT_FOUND) {
			// Server not listening yet — brief backoff and retry until deadline.
			Sleep(50);
			continue;
		}
		throw IOException("vgi named pipe: connect to %s failed (GetLastError=%lu)", pipe_name,
		                  (unsigned long)err);
	}
}

NamedPipeWorker::NamedPipeWorker(int connected_fd) : fd_(connected_fd) {
}

NamedPipeWorker::~NamedPipeWorker() {
	if (fd_ >= 0) {
		_close(fd_);
		fd_ = -1;
	}
}

bool NamedPipeWorker::IsLikelyAlive() const {
	if (fd_ < 0) {
		return false;
	}
	HANDLE h = reinterpret_cast<HANDLE>(_get_osfhandle(fd_));
	if (h == INVALID_HANDLE_VALUE) {
		return false;
	}
	DWORD avail = 0;
	// PeekNamedPipe fails with ERROR_BROKEN_PIPE once the peer is gone.
	if (!PeekNamedPipe(h, nullptr, 0, nullptr, &avail, nullptr)) {
		return GetLastError() != ERROR_BROKEN_PIPE;
	}
	return true;
}

} // namespace vgi
} // namespace duckdb

#endif // _WIN32
