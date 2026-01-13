#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#ifdef _WIN32
#error "VGI subprocess support is currently Unix-only. Windows support is not yet implemented."
#else
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace duckdb {
namespace vgi {

// Pipe wrapper for RAII cleanup
class Pipe {
public:
	int read_fd = -1;
	int write_fd = -1;

	Pipe();
	~Pipe();

	void CloseRead();
	void CloseWrite();

	// Non-copyable
	Pipe(const Pipe &) = delete;
	Pipe &operator=(const Pipe &) = delete;
};

// Subprocess wrapper for RAII cleanup.
// Exception safety: If the constructor throws after fork(), the child process
// will be orphaned. In practice, the only operations that can fail after fork()
// are the pipe fd transfers, which cannot fail. The child calls _exit() if
// exec fails, so it won't become a zombie.
class SubProcess {
public:
	// Create a subprocess running the given command.
	// If stderr_passthrough is true, worker stderr goes directly to the terminal
	// instead of being captured (useful for debugging).
	explicit SubProcess(const std::string &command, bool stderr_passthrough = false);
	~SubProcess();

	// Accessors for process info and file descriptors
	pid_t GetPid() const {
		return pid_;
	}
	int GetStdinFd() const {
		return stdin_fd_;
	}
	int GetStdoutFd() const {
		return stdout_fd_;
	}
	int GetStderrFd() const {
		return stderr_fd_;
	}

	// Close stdin to signal EOF to the child process
	void CloseStdin();

	// Close stderr (typically after transferring ownership to a reader)
	void CloseStderr();

	// Wait for the process to exit and return the exit status.
	// Returns 0 on success, or the exit code/signal number on failure.
	// Sets exited_normally to true if the process exited via exit(), false if killed by signal.
	int Wait(bool *exited_normally = nullptr);

	// Release ownership of stderr fd (caller takes responsibility for closing)
	int ReleaseStderrFd() {
		int fd = stderr_fd_;
		stderr_fd_ = -1;
		return fd;
	}

	// Non-copyable
	SubProcess(const SubProcess &) = delete;
	SubProcess &operator=(const SubProcess &) = delete;

private:
	pid_t pid_ = -1;
	int stdin_fd_ = -1;
	int stdout_fd_ = -1;
	int stderr_fd_ = -1;
};

// Helper to write all bytes to a file descriptor
void WriteAll(int fd, const uint8_t *data, size_t len);

// Default timeout for catalog operations (5 seconds)
constexpr int CATALOG_OPERATION_TIMEOUT_SECONDS = 5;

// Wait for a file descriptor to become readable with a timeout.
// Returns true if readable, throws IOException on timeout or error.
void WaitForReadable(int fd, int timeout_seconds = CATALOG_OPERATION_TIMEOUT_SECONDS);

} // namespace vgi
} // namespace duckdb
