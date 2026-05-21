// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_stderr_drainer.hpp"

#include <cerrno>

#if VGI_POSIX_TRANSPORT
#include <poll.h>
#include <unistd.h>
#endif

#include "vgi_logging.hpp"

namespace duckdb {
namespace vgi {

#if VGI_POSIX_TRANSPORT
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
		close(fd_);
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
#else  // !VGI_POSIX_TRANSPORT
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
#endif // VGI_POSIX_TRANSPORT

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

#if VGI_POSIX_TRANSPORT
void StderrDrainer::ThreadLoop() {
	char buffer[4096];
	std::string line_buffer;

	struct pollfd pfd;
	pfd.fd = fd_;
	pfd.events = POLLIN;

	while (!stop_.load(std::memory_order_relaxed)) {
		// poll() with timeout so we can periodically check the stop flag —
		// avoids blocking read() from preventing the destructor from joining.
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
#endif // VGI_POSIX_TRANSPORT

} // namespace vgi
} // namespace duckdb
