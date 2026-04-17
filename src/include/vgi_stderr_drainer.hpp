#pragma once

#include <atomic>
#include <mutex>
#include <string>
#include <sys/types.h>
#include <thread>
#include <vector>

namespace duckdb {

class ClientContext;

namespace vgi {

// Background stderr reader/buffer for a subprocess worker.
//
// Takes ownership of a file descriptor (typically a pipe read-end connected to
// the worker's stderr). Spawns a thread that reads lines and stashes them for
// later retrieval via DrainToLog(). Ownership of the fd can be released with
// ReleaseFd() when handing the worker back to the pool — the thread stops but
// the fd stays open.
//
// A drainer constructed with fd < 0 is a no-op: no thread, no fd, no buffering.
// This lets callers uniformly construct one even when the SubProcess was
// spawned with stderr_passthrough (no pipe).
class StderrDrainer {
public:
	explicit StderrDrainer(int fd);
	~StderrDrainer();

	StderrDrainer(const StderrDrainer &) = delete;
	StderrDrainer &operator=(const StderrDrainer &) = delete;
	StderrDrainer(StderrDrainer &&) = delete;
	StderrDrainer &operator=(StderrDrainer &&) = delete;

	// Stop the reader thread and return the fd to the caller (ownership
	// transferred; fd remains open). Returns -1 if no fd was held.
	// After this call the drainer is empty; the destructor is a no-op.
	int ReleaseFd();

	// Forward any buffered lines to VGI_LOG as worker.stderr events.
	// Safe to call with no fd (does nothing). Main-thread only.
	void DrainToLog(ClientContext &context, const std::string &worker_path, pid_t worker_pid);

private:
	int fd_ = -1;
	std::thread thread_;
	std::atomic<bool> stop_ {false};
	std::mutex mutex_;
	std::vector<std::string> lines_;

	void ThreadLoop();
};

} // namespace vgi
} // namespace duckdb
