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
#elif defined(_WIN32)
#include <fcntl.h> // _O_BINARY / _O_RDONLY / _O_WRONLY
#include <io.h>     // _open_osfhandle, _get_osfhandle, _read, _write, _close
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN // keep <windows.h> from dragging in legacy <winsock.h>
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
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

#elif defined(_WIN32)

// ---------------------------------------------------------------------------
// Windows subprocess backend.
//
// Realizes the same fd-based seam the POSIX path uses, via the CRT fd layer:
// CreatePipe gives HANDLEs, _open_osfhandle wraps the parent ends as int fds so
// the rest of the codebase keeps its `int fd` model unchanged. Readiness waits
// use PeekNamedPipe polling (Windows anonymous pipes are not selectable). The
// child is launched with CreateProcess via `cmd.exe /c <command>` (POSIX uses
// `/bin/sh -c`). UNTESTED on macOS CI — validated on the Windows build machine.
// ---------------------------------------------------------------------------

// Pipe is fork-oriented (FD_CLOEXEC); the Windows SubProcess manages its pipes
// inline, so this type is never instantiated here. Throwing stubs keep it linkable.
Pipe::Pipe() {
	throw NotImplementedException("vgi: Pipe is not used by the Windows subprocess backend");
}
Pipe::~Pipe() {
}
void Pipe::CloseRead() {
}
void Pipe::CloseWrite() {
}

SubProcess::SubProcess(const std::string &command, bool stderr_passthrough) {
	if (!stderr_passthrough) {
		const char *passthrough_env = std::getenv("VGI_WORKER_STDERR_PASSTHROUGH");
		stderr_passthrough = passthrough_env && std::string(passthrough_env) == "1";
	}
	if (!stderr_passthrough) {
		const char *debug_env = std::getenv("VGI_WORKER_DEBUG");
		stderr_passthrough = debug_env && std::string(debug_env) == "1";
	}

	SECURITY_ATTRIBUTES sa {};
	sa.nLength = sizeof(sa);
	sa.bInheritHandle = TRUE;
	sa.lpSecurityDescriptor = nullptr;

	HANDLE child_stdin_rd = nullptr, parent_stdin_wr = nullptr;
	HANDLE parent_stdout_rd = nullptr, child_stdout_wr = nullptr;
	HANDLE parent_stderr_rd = nullptr, child_stderr_wr = nullptr;

	auto fail = [&](const char *what) -> IOException {
		DWORD e = GetLastError();
		for (HANDLE h : {child_stdin_rd, parent_stdin_wr, parent_stdout_rd, child_stdout_wr,
		                 parent_stderr_rd, child_stderr_wr}) {
			if (h) {
				CloseHandle(h);
			}
		}
		return IOException("vgi: %s failed (GetLastError=%lu)", what, (unsigned long)e);
	};

	if (!CreatePipe(&child_stdin_rd, &parent_stdin_wr, &sa, 0)) {
		throw fail("CreatePipe(stdin)");
	}
	SetHandleInformation(parent_stdin_wr, HANDLE_FLAG_INHERIT, 0); // parent end not inherited
	if (!CreatePipe(&parent_stdout_rd, &child_stdout_wr, &sa, 0)) {
		throw fail("CreatePipe(stdout)");
	}
	SetHandleInformation(parent_stdout_rd, HANDLE_FLAG_INHERIT, 0);
	if (!stderr_passthrough) {
		if (!CreatePipe(&parent_stderr_rd, &child_stderr_wr, &sa, 0)) {
			throw fail("CreatePipe(stderr)");
		}
		SetHandleInformation(parent_stderr_rd, HANDLE_FLAG_INHERIT, 0);
	}

	STARTUPINFOA si {};
	si.cb = sizeof(si);
	si.dwFlags = STARTF_USESTDHANDLES;
	si.hStdInput = child_stdin_rd;
	si.hStdOutput = child_stdout_wr;
	si.hStdError = stderr_passthrough ? GetStdHandle(STD_ERROR_HANDLE) : child_stderr_wr;

	// POSIX runs `/bin/sh -c <command>`; the Windows equivalent is `cmd.exe /c`.
	std::string cmdline = "cmd.exe /c " + command;
	std::vector<char> mutable_cmd(cmdline.begin(), cmdline.end());
	mutable_cmd.push_back('\0');

	PROCESS_INFORMATION pi {};
	BOOL ok = CreateProcessA(nullptr, mutable_cmd.data(), nullptr, nullptr, /*bInheritHandles=*/TRUE,
	                         0, nullptr, nullptr, &si, &pi);

	// The child's ends are now owned by the child; close our copies regardless.
	CloseHandle(child_stdin_rd);
	child_stdin_rd = nullptr;
	CloseHandle(child_stdout_wr);
	child_stdout_wr = nullptr;
	if (child_stderr_wr) {
		CloseHandle(child_stderr_wr);
		child_stderr_wr = nullptr;
	}

	if (!ok) {
		throw fail("CreateProcess");
	}
	CloseHandle(pi.hThread);
	process_handle_ = pi.hProcess;
	pid_ = static_cast<pid_t>(GetProcessId(pi.hProcess));

	// Wrap the parent ends as CRT fds. After a successful _open_osfhandle the fd
	// owns the HANDLE (closing the fd closes the HANDLE).
	stdin_fd_ = _open_osfhandle(reinterpret_cast<intptr_t>(parent_stdin_wr), _O_BINARY | _O_WRONLY);
	stdout_fd_ = _open_osfhandle(reinterpret_cast<intptr_t>(parent_stdout_rd), _O_BINARY | _O_RDONLY);
	stderr_fd_ =
	    stderr_passthrough ? -1 : _open_osfhandle(reinterpret_cast<intptr_t>(parent_stderr_rd), _O_BINARY | _O_RDONLY);
}

SubProcess::~SubProcess() {
	if (stdin_fd_ >= 0) {
		_close(stdin_fd_);
	}
	if (stdout_fd_ >= 0) {
		_close(stdout_fd_);
	}
	if (stderr_fd_ >= 0) {
		_close(stderr_fd_);
	}
	if (process_handle_) {
		HANDLE h = static_cast<HANDLE>(process_handle_);
		if (WaitForSingleObject(h, 0) != WAIT_OBJECT_0) {
			TerminateProcess(h, 1);
			WaitForSingleObject(h, 2000);
		}
		CloseHandle(h);
		process_handle_ = nullptr;
	}
}

void SubProcess::CloseStdin() {
	if (stdin_fd_ >= 0) {
		_close(stdin_fd_);
		stdin_fd_ = -1;
	}
}

void SubProcess::CloseStderr() {
	if (stderr_fd_ >= 0) {
		_close(stderr_fd_);
		stderr_fd_ = -1;
	}
}

int SubProcess::Wait(bool *exited_normally) {
	if (!process_handle_) {
		if (exited_normally) {
			*exited_normally = true;
		}
		return 0;
	}
	HANDLE h = static_cast<HANDLE>(process_handle_);
	WaitForSingleObject(h, INFINITE);
	DWORD code = 0;
	GetExitCodeProcess(h, &code);
	CloseHandle(h);
	process_handle_ = nullptr;
	pid_ = -1;
	if (exited_normally) {
		*exited_normally = true; // Windows has no signal-vs-exit distinction here
	}
	return static_cast<int>(code);
}

bool SubProcess::TryWait(int *exit_status) {
	if (!process_handle_) {
		return true;
	}
	HANDLE h = static_cast<HANDLE>(process_handle_);
	if (WaitForSingleObject(h, 0) == WAIT_TIMEOUT) {
		return false; // still running
	}
	DWORD code = 0;
	GetExitCodeProcess(h, &code);
	if (exit_status) {
		*exit_status = static_cast<int>(code);
	}
	CloseHandle(h);
	process_handle_ = nullptr;
	pid_ = -1;
	return true;
}

void WriteAll(int fd, const uint8_t *data, size_t len) {
	size_t written = 0;
	while (written < len) {
		int result = _write(fd, data + written, static_cast<unsigned int>(len - written));
		if (result < 0) {
			if (errno == EINTR) {
				continue;
			}
			if (errno == EPIPE) {
				throw IOException("Worker closed pipe (EPIPE). Worker may have crashed - "
				                  "check worker stderr output");
			}
			throw IOException("Failed to write to worker: %s", std::strerror(errno));
		}
		written += static_cast<size_t>(result);
	}
}

void WaitForReadable(int fd, int timeout_seconds) {
	HANDLE h = reinterpret_cast<HANDLE>(_get_osfhandle(fd));
	if (h == INVALID_HANDLE_VALUE) {
		throw IOException("VGI WaitForReadable: invalid file descriptor");
	}
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_seconds);
	while (true) {
		DWORD avail = 0;
		if (!PeekNamedPipe(h, nullptr, 0, nullptr, &avail, nullptr)) {
			// Broken pipe → treat as readable so the following read sees EOF,
			// matching POSIX select() returning readable at EOF.
			if (GetLastError() == ERROR_BROKEN_PIPE) {
				return;
			}
			throw IOException("VGI catalog operation failed: PeekNamedPipe error %lu",
			                  (unsigned long)GetLastError());
		}
		if (avail > 0) {
			return;
		}
		if (std::chrono::steady_clock::now() >= deadline) {
			throw IOException("VGI catalog operation timed out after %d seconds", timeout_seconds);
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(5));
	}
}

#else // Emscripten (HTTP-only): throwing stubs so worker pool / FunctionConnection link

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
	return true;
}
void WriteAll(int, const uint8_t *, size_t) {
	throw NotImplementedException("vgi: subprocess transport unavailable in this build");
}
void WaitForReadable(int, int) {
	throw NotImplementedException("vgi: subprocess transport unavailable in this build");
}

#endif // VGI_POSIX_TRANSPORT / _WIN32 / emscripten

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
#elif defined(_WIN32)
void WaitForReadableUntilCancel(int fd, ClientContext *context) {
	HANDLE h = reinterpret_cast<HANDLE>(_get_osfhandle(fd));
	if (h == INVALID_HANDLE_VALUE) {
		throw IOException("VGI WaitForReadableUntilCancel: invalid file descriptor");
	}
	while (true) {
		if (context && context->interrupted) {
			throw IOException("VGI operation interrupted (query cancelled)");
		}
		DWORD avail = 0;
		if (!PeekNamedPipe(h, nullptr, 0, nullptr, &avail, nullptr)) {
			if (GetLastError() == ERROR_BROKEN_PIPE) {
				return; // EOF — let the read observe it
			}
			throw IOException("VGI operation failed: PeekNamedPipe error %lu",
			                  (unsigned long)GetLastError());
		}
		if (avail > 0) {
			return;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(250)); // poll quantum
	}
}
#else // Emscripten
void WaitForReadableUntilCancel(int, ClientContext *) {
	throw NotImplementedException("vgi: subprocess transport unavailable in this build");
}
#endif // VGI_POSIX_TRANSPORT / _WIN32 / emscripten

} // namespace vgi
} // namespace duckdb
