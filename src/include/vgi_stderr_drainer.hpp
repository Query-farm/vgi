// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <atomic>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "vgi_platform.hpp" // pid_t (real on POSIX, shim on Windows)

namespace duckdb {

class ClientContext;

namespace vgi {

// Background stderr reader for a subprocess worker. Owns `fd` (the read end of
// the worker's stderr pipe) and a thread that buffers lines for DrainToLog().
// Pooling hands the whole drainer to the pool with the worker. With fd < 0
// (stderr passthrough, no pipe) it is a no-op.
class StderrDrainer {
public:
	explicit StderrDrainer(int fd);
	~StderrDrainer();

	StderrDrainer(const StderrDrainer &) = delete;
	StderrDrainer &operator=(const StderrDrainer &) = delete;
	StderrDrainer(StderrDrainer &&) = delete;
	StderrDrainer &operator=(StderrDrainer &&) = delete;

	// Forward buffered lines to VGI_LOG as worker.stderr events.
	void DrainToLog(ClientContext &context, const std::string &worker_path, pid_t worker_pid);

	// Error path only: let the reader drain what is in the pipe, join it, and
	// return the buffered stderr newline-joined. The reader is stopped
	// afterwards, so call it just before tearing the connection down. Does NOT
	// close the fd.
	std::string CaptureStderrSnapshot();

private:
	// Bound on lines_: an idle pooled worker that keeps writing would otherwise
	// grow it without limit. Over the cap the oldest line is dropped and counted;
	// DrainToLog logs a worker.stderr.truncated event, CaptureStderrSnapshot a
	// "[N earlier stderr line(s) dropped]" prefix.
	static constexpr size_t kMaxBufferedLines = 1024;

	int fd_ = -1;
	std::thread thread_;
	std::atomic<bool> stop_ {false};
	// Set by CaptureStderrSnapshot: "finish what is in the pipe, then stop".
	// Distinct from stop_ (hard stop, used by the destructor) because
	// a hard stop checked at the top of the reader loop can race a thread that
	// has not been scheduled yet and drop the whole capture unread.
	std::atomic<bool> drain_to_eof_ {false};
	std::mutex mutex_;
	std::vector<std::string> lines_;
	size_t dropped_lines_ = 0;

	void ThreadLoop();
};

} // namespace vgi
} // namespace duckdb
