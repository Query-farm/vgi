// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_stderr_drainer.hpp"

#include <cerrno>
#include <chrono>

#if VGI_POSIX_TRANSPORT
#include <poll.h>
#include <unistd.h>
#elif defined(_WIN32)
#include <io.h> // _read, _get_osfhandle, _close
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN // keep <windows.h> from dragging in legacy <winsock.h>
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h> // PeekNamedPipe
#endif

#include "vgi_logging.hpp"

namespace duckdb {
namespace vgi {

#if VGI_SUBPROCESS_TRANSPORT
StderrDrainer::StderrDrainer(int fd) : fd_(fd) {
	if (fd_ < 0) {
		return;
	}
	thread_ = std::thread(&StderrDrainer::ThreadLoop, this);
}

StderrDrainer::~StderrDrainer() {
	stop_.store(true, std::memory_order_relaxed);
	if (thread_.joinable()) {
		thread_.join();
	}
	if (fd_ >= 0) {
#if defined(_WIN32)
		_close(fd_);
#else
		close(fd_);
#endif
		fd_ = -1;
	}
}

int StderrDrainer::ReleaseFd() {
	stop_.store(true, std::memory_order_relaxed);
	if (thread_.joinable()) {
		thread_.join();
	}
	int fd = fd_;
	fd_ = -1;
	return fd;
}
#else  // Emscripten (HTTP-only)
// No subprocess stderr pipe exists on this build; the drainer is never
// constructed with a real fd. Defined so the always-compiled worker pool /
// FunctionConnection (which hold a unique_ptr<StderrDrainer>) link.
StderrDrainer::StderrDrainer(int) : fd_(-1) {
}
StderrDrainer::~StderrDrainer() {
}
int StderrDrainer::ReleaseFd() {
	return -1;
}
#endif // VGI_SUBPROCESS_TRANSPORT

void StderrDrainer::DrainToLog(ClientContext &context, const std::string &worker_path, pid_t worker_pid) {
	std::vector<std::string> lines;
	size_t dropped = 0;
	{
		std::lock_guard<std::mutex> lock(mutex_);
		lines.swap(lines_);
		dropped = dropped_lines_;
		dropped_lines_ = 0;
	}
	if (dropped > 0) {
		VGI_LOG(context, "worker.stderr.truncated",
		        {{"worker_path", worker_path},
		         {"worker_pid", std::to_string(worker_pid)},
		         {"dropped_lines", std::to_string(dropped)}});
	}
	for (const auto &line : lines) {
		VGI_LOG(context, "worker.stderr",
		        {{"worker_path", worker_path}, {"worker_pid", std::to_string(worker_pid)}, {"message", line}});
	}
}

#if VGI_SUBPROCESS_TRANSPORT
void StderrDrainer::ThreadLoop() {
	char buffer[4096];
	std::string line_buffer;

#if VGI_POSIX_TRANSPORT
	struct pollfd pfd;
	pfd.fd = fd_;
	pfd.events = POLLIN;
#elif defined(_WIN32)
	HANDLE h = reinterpret_cast<HANDLE>(_get_osfhandle(fd_));
#endif

	while (!stop_.load(std::memory_order_relaxed)) {
		// Wait (with a timeout / poll quantum) so we can periodically check the
		// stop flag — a blocking read would prevent the destructor from joining.
#if VGI_POSIX_TRANSPORT
		int poll_result = poll(&pfd, 1, 100); // 100ms
		if (poll_result < 0) {
			if (errno == EINTR) {
				continue;
			}
			break;
		}
		if (poll_result == 0) {
			continue;
		}
		ssize_t bytes_read = read(fd_, buffer, sizeof(buffer) - 1);
#elif defined(_WIN32)
		DWORD avail = 0;
		if (h == INVALID_HANDLE_VALUE || !PeekNamedPipe(h, nullptr, 0, nullptr, &avail, nullptr)) {
			break; // pipe closed / broken
		}
		if (avail == 0) {
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
			continue;
		}
		int bytes_read = _read(fd_, buffer, sizeof(buffer) - 1);
#endif
		if (bytes_read <= 0) {
			break; // EOF or error
		}
		buffer[bytes_read] = '\0';

		line_buffer.append(buffer, bytes_read);
		size_t pos;
		while ((pos = line_buffer.find('\n')) != std::string::npos) {
			std::string line = line_buffer.substr(0, pos);
			line_buffer.erase(0, pos + 1);
			if (!line.empty() && line.back() == '\r') {
				line.pop_back();
			}
			if (!line.empty()) {
				std::lock_guard<std::mutex> lock(mutex_);
				if (lines_.size() >= kMaxBufferedLines) {
					// Drop the oldest line. erase(begin) is O(n) but at
					// 1024 it's a few microseconds and only happens when
					// the consumer is slow.
					lines_.erase(lines_.begin());
					dropped_lines_++;
				}
				lines_.push_back(std::move(line));
			}
		}
	}

	if (!line_buffer.empty()) {
		std::lock_guard<std::mutex> lock(mutex_);
		if (lines_.size() >= kMaxBufferedLines) {
			lines_.erase(lines_.begin());
			dropped_lines_++;
		}
		lines_.push_back(std::move(line_buffer));
	}

	// Note: fd is NOT closed here — the destructor or ReleaseFd() decides.
}
#endif // VGI_SUBPROCESS_TRANSPORT

} // namespace vgi
} // namespace duckdb
