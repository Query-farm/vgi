// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_subprocess.hpp"

#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <thread>

#if VGI_POSIX_TRANSPORT
#include <fcntl.h>
#include <sys/select.h>
#endif

#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {
namespace vgi {

#if VGI_POSIX_TRANSPORT

// Pipe implementation
Pipe::Pipe() {
	int fds[2];
	if (pipe(fds) != 0) {
		throw IOException("Failed to create pipe");
	}
	read_fd = fds[0];
	write_fd = fds[1];
	// Mark both ends close-on-exec so subsequent fork+execs in the parent
	// don't leak this pipe into sibling worker processes.
	//
	// Why this matters: SubProcess keeps the parent-side FDs (write end of the
	// child's stdin, read ends of stdout/stderr) for the lifetime of the worker.
	// When DuckDB's subprocess pool spawns a SECOND worker via fork+execl,
	// without CLOEXEC the new child inherits the previous worker's pipe FDs.
	// Then if DuckDB exits abnormally and every worker reparents to init,
	// none of them ever sees EOF on its stdin — sibling workers are still
	// holding the write end alive — and the orphaned cohort lives forever.
	//
	// In the child, dup2() onto STDIN/STDOUT/STDERR clears the FD_CLOEXEC flag
	// on the destination FD, so the worker's stdio survives execl normally.
	// macOS lacks pipe2(), so we set the flag explicitly via fcntl after pipe().
	if (::fcntl(read_fd, F_SETFD, FD_CLOEXEC) != 0 || ::fcntl(write_fd, F_SETFD, FD_CLOEXEC) != 0) {
		int saved = errno;
		::close(read_fd);
		::close(write_fd);
		read_fd = -1;
		write_fd = -1;
		throw IOException("Failed to set FD_CLOEXEC on pipe: %s", std::strerror(saved));
	}
}

Pipe::~Pipe() {
	CloseRead();
	CloseWrite();
}

void Pipe::CloseRead() {
	if (read_fd >= 0) {
		close(read_fd);
		read_fd = -1;
	}
}

void Pipe::CloseWrite() {
	if (write_fd >= 0) {
		close(write_fd);
		write_fd = -1;
	}
}

// SubProcess implementation
SubProcess::SubProcess(const std::string &command, bool stderr_passthrough) {
	// Also check environment variables
	if (!stderr_passthrough) {
		const char *passthrough_env = std::getenv("VGI_WORKER_STDERR_PASSTHROUGH");
		stderr_passthrough = passthrough_env && std::string(passthrough_env) == "1";
	}
	if (!stderr_passthrough) {
		const char *debug_env = std::getenv("VGI_WORKER_DEBUG");
		stderr_passthrough = debug_env && std::string(debug_env) == "1";
	}

	Pipe stdin_pipe;
	Pipe stdout_pipe;
	Pipe stderr_pipe; // Only used if not passthrough

	pid_ = fork();
	if (pid_ < 0) {
		throw IOException("Failed to fork process");
	}

	if (pid_ == 0) {
		// Child process
		// Redirect stdin
		if (dup2(stdin_pipe.read_fd, STDIN_FILENO) < 0) {
			_exit(126); // dup2 failed
		}
		stdin_pipe.CloseRead();
		stdin_pipe.CloseWrite();

		// Redirect stdout
		if (dup2(stdout_pipe.write_fd, STDOUT_FILENO) < 0) {
			_exit(126); // dup2 failed
		}
		stdout_pipe.CloseRead();
		stdout_pipe.CloseWrite();

		// Redirect stderr (only if not passthrough)
		if (!stderr_passthrough) {
			if (dup2(stderr_pipe.write_fd, STDERR_FILENO) < 0) {
				_exit(126); // dup2 failed
			}
			stderr_pipe.CloseRead();
			stderr_pipe.CloseWrite();
		} else {
			// Enable IPC debug output in the worker when debugging
			setenv("VGI_IPC_DEBUG", "1", 1);
		}
		// If passthrough, stderr remains connected to parent's stderr

		// Execute command via shell
		execl("/bin/sh", "sh", "-c", command.c_str(), nullptr);
		_exit(127); // exec failed
	}

	// Parent process
	stdin_fd_ = stdin_pipe.write_fd;
	stdin_pipe.write_fd = -1; // Transfer ownership

	stdout_fd_ = stdout_pipe.read_fd;
	stdout_pipe.read_fd = -1; // Transfer ownership

	if (!stderr_passthrough) {
		stderr_fd_ = stderr_pipe.read_fd;
		stderr_pipe.read_fd = -1; // Transfer ownership
	} else {
		stderr_fd_ = -1; // No stderr pipe when passthrough
	}

	// Close unused ends
	stdin_pipe.CloseRead();
	stdout_pipe.CloseWrite();
	if (!stderr_passthrough) {
		stderr_pipe.CloseWrite();
	}
}

SubProcess::~SubProcess() {
	if (stdin_fd_ >= 0) {
		close(stdin_fd_);
	}
	if (stdout_fd_ >= 0) {
		close(stdout_fd_);
	}
	if (stderr_fd_ >= 0) {
		close(stderr_fd_);
	}
	if (pid_ > 0) {
		// Fast path: process already exited.
		int status;
		pid_t result = waitpid(pid_, &status, WNOHANG);
		if (result != 0) {
			// result > 0: already reaped. result < 0: gone (errno=ECHILD).
			return;
		}

		// Still running — escalate. SIGTERM first, give it ~2 s to exit;
		// then SIGKILL (uncatchable, so the second wait is bounded). The
		// previous code did an unbounded blocking waitpid here, so a
		// worker that ignored SIGTERM could hang the destructor (and any
		// caller — pool teardown, FunctionConnection cleanup, the host
		// process exit) indefinitely.
		kill(pid_, SIGTERM);
		constexpr auto kSigtermGrace = std::chrono::milliseconds(2000);
		constexpr auto kPollInterval = std::chrono::milliseconds(50);
		auto deadline = std::chrono::steady_clock::now() + kSigtermGrace;
		while (std::chrono::steady_clock::now() < deadline) {
			result = waitpid(pid_, &status, WNOHANG);
			if (result != 0) {
				return; // exited (>0) or already gone (<0)
			}
			std::this_thread::sleep_for(kPollInterval);
		}

		// SIGTERM grace expired — escalate.
		kill(pid_, SIGKILL);
		// SIGKILL cannot be caught/blocked/ignored, so this returns promptly.
		waitpid(pid_, &status, 0);
	}
}

void SubProcess::CloseStdin() {
	if (stdin_fd_ >= 0) {
		close(stdin_fd_);
		stdin_fd_ = -1;
	}
}

void SubProcess::CloseStderr() {
	if (stderr_fd_ >= 0) {
		close(stderr_fd_);
		stderr_fd_ = -1;
	}
}

int SubProcess::Wait(bool *exited_normally) {
	if (pid_ <= 0) {
		if (exited_normally) {
			*exited_normally = true;
		}
		return 0;
	}

	int status;
	pid_t result;
	while (true) {
		result = waitpid(pid_, &status, 0);
		if (result >= 0) {
			break;
		}
		if (errno == EINTR) {
			continue; // Interrupted by signal, retry
		}
		// Error waiting for process
		if (exited_normally) {
			*exited_normally = false;
		}
		return -1;
	}

	pid_ = -1; // Mark as waited

	if (WIFEXITED(status)) {
		if (exited_normally) {
			*exited_normally = true;
		}
		return WEXITSTATUS(status);
	} else if (WIFSIGNALED(status)) {
		if (exited_normally) {
			*exited_normally = false;
		}
		return -WTERMSIG(status);
	}

	if (exited_normally) {
		*exited_normally = false;
	}
	return -1;
}

bool SubProcess::TryWait(int *exit_status) {
	if (pid_ <= 0) {
		return true; // Already waited or invalid
	}

	int status;
	pid_t result = waitpid(pid_, &status, WNOHANG);

	if (result == 0) {
		// Process still running
		return false;
	}

	if (result < 0) {
		// Error (process doesn't exist)
		return true;
	}

	// Process has exited
	pid_ = -1; // Mark as waited

	if (exit_status) {
		if (WIFEXITED(status)) {
			*exit_status = WEXITSTATUS(status);
		} else if (WIFSIGNALED(status)) {
			*exit_status = -WTERMSIG(status); // Negative to indicate signal
		} else {
			*exit_status = -1;
		}
	}

	return true;
}

// WriteAll implementation
void WriteAll(int fd, const uint8_t *data, size_t len) {
	size_t written = 0;
	while (written < len) {
		ssize_t result = write(fd, data + written, len - written);
		if (result < 0) {
			if (errno == EINTR) {
				continue; // Interrupted by signal, retry
			}
			if (errno == EPIPE) {
				// Worker closed its stdin - it likely crashed or exited early
				throw IOException("Worker closed pipe (EPIPE). Worker may have crashed - "
				                  "use VGI_WORKER_STDERR_PASSTHROUGH=1 for diagnostics");
			}
			throw IOException("Failed to write to pipe: %s", strerror(errno));
		}
		written += result;
	}
}

// WaitForReadable implementation - wait for fd to be readable with timeout.
// Retries on EINTR with deadline-based remaining time calculation.
void WaitForReadable(int fd, int timeout_seconds) {
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_seconds);

	while (true) {
		auto now = std::chrono::steady_clock::now();
		auto remaining = std::chrono::duration_cast<std::chrono::microseconds>(deadline - now);
		if (remaining.count() <= 0) {
			throw IOException("VGI catalog operation timed out after %d seconds", timeout_seconds);
		}

		fd_set read_fds;
		FD_ZERO(&read_fds);
		FD_SET(fd, &read_fds);

		struct timeval tv;
		tv.tv_sec = remaining.count() / 1000000;
		tv.tv_usec = remaining.count() % 1000000;

		int result = select(fd + 1, &read_fds, nullptr, nullptr, &tv);

		if (result < 0) {
			if (errno == EINTR) {
				continue; // Recalculate remaining time and retry
			}
			throw IOException("VGI catalog operation failed: select error: %s", strerror(errno));
		}

		if (result == 0) {
			throw IOException("VGI catalog operation timed out after %d seconds", timeout_seconds);
		}

		// fd is readable, return successfully
		return;
	}
}

#else // !VGI_POSIX_TRANSPORT

// Windows / Emscripten stubs. The subprocess transport is unavailable on these
// builds; the transport-dispatch layer (CreateFunctionConnection /
// InvokePooledUnaryRpc) throws a clear InvalidInputException before any of these
// is reached on a live path. They exist only so that the always-compiled
// vgi_worker_pool.cpp (which holds unique_ptr<SubProcess>) and the
// per-method-guarded FunctionConnection link cleanly.
Pipe::Pipe() {
	throw NotImplementedException("vgi: subprocess transport unavailable in this build");
}
Pipe::~Pipe() {
}
void Pipe::CloseRead() {
}
void Pipe::CloseWrite() {
}
SubProcess::SubProcess(const std::string &, bool) {
	throw InvalidInputException("vgi: subprocess (bare command) LOCATIONs require fork() and are "
	                            "not available in this build; use http://… instead");
}
SubProcess::~SubProcess() {
}
void SubProcess::CloseStdin() {
}
void SubProcess::CloseStderr() {
}
int SubProcess::Wait(bool *exited_normally) {
	if (exited_normally) {
		*exited_normally = true;
	}
	return 0;
}
bool SubProcess::TryWait(int *exit_status) {
	if (exit_status) {
		*exit_status = 0;
	}
	return true; // treat as exited; pool is always empty on this build
}
void WriteAll(int, const uint8_t *, size_t) {
	throw NotImplementedException("vgi: subprocess transport unavailable in this build");
}
void WaitForReadable(int, int) {
	throw NotImplementedException("vgi: subprocess transport unavailable in this build");
}

#endif // VGI_POSIX_TRANSPORT

int GetCatalogTimeout(ClientContext *context) {
	if (context) {
		Value val;
		if (context->TryGetCurrentSetting("vgi_catalog_timeout_seconds", val)) {
			return static_cast<int>(val.GetValue<int64_t>());
		}
	}
	return CATALOG_OPERATION_TIMEOUT_SECONDS;
}

#if VGI_POSIX_TRANSPORT
void WaitForReadableUntilCancel(int fd, ClientContext *context) {
	// No wall-clock deadline. Match the streaming data-phase model: block
	// until the worker speaks. Cancellation comes via the context's
	// `interrupted` flag (Ctrl-C, query cancel) — we poll it every 250ms.
	constexpr int64_t kPollIntervalUs = 250 * 1000;

	while (true) {
		if (context && context->interrupted) {
			throw IOException("VGI operation interrupted (query cancelled)");
		}

		fd_set read_fds;
		FD_ZERO(&read_fds);
		FD_SET(fd, &read_fds);

		struct timeval tv;
		tv.tv_sec = 0;
		tv.tv_usec = kPollIntervalUs;

		int result = select(fd + 1, &read_fds, nullptr, nullptr, &tv);
		if (result < 0) {
			if (errno == EINTR) {
				continue;
			}
			throw IOException("VGI operation failed: select error: %s", strerror(errno));
		}
		if (result == 0) {
			// Quantum elapsed without data — loop to re-poll interrupted.
			continue;
		}
		return;
	}
}
#else  // !VGI_POSIX_TRANSPORT
void WaitForReadableUntilCancel(int, ClientContext *) {
	throw NotImplementedException("vgi: subprocess transport unavailable in this build");
}
#endif // VGI_POSIX_TRANSPORT

} // namespace vgi
} // namespace duckdb
