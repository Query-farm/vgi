// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_subprocess.hpp"

#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <thread>

#if VGI_POSIX_TRANSPORT
#include <fcntl.h>
#include <poll.h>
#elif defined(_WIN32)
#include <fcntl.h> // _O_BINARY / _O_RDONLY / _O_WRONLY
#include <io.h>     // _open_osfhandle, _get_osfhandle, _read, _write, _close
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN // keep <windows.h> from dragging in legacy <winsock.h>
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h> // SOCKET, WSAPoll, getsockopt — MUST precede <windows.h>
#include <ws2tcpip.h> // inet_pton
#pragma comment(lib, "ws2_32")
#include <windows.h>
#endif

#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {
namespace vgi {

#if VGI_POSIX_TRANSPORT

void ResetChildSignalDispositions() {
	// SIGKILL / SIGSTOP cannot be reset; sigaction() on them just fails, so
	// walking the whole range and ignoring errors is correct and cheapest.
	for (int sig = 1; sig < NSIG; ++sig) {
		struct sigaction sa;
		std::memset(&sa, 0, sizeof(sa));
		sa.sa_handler = SIG_DFL;
		sigemptyset(&sa.sa_mask);
		// Preserve the process-wide SIGPIPE ignore (see header).
		if (sig == SIGPIPE) {
			continue;
		}
		(void)::sigaction(sig, &sa, nullptr);
	}
	// The child inherits the forking thread's signal mask, and exec() preserves
	// it — a blocked signal would otherwise stay blocked in the worker.
	sigset_t empty;
	sigemptyset(&empty);
	(void)::sigprocmask(SIG_SETMASK, &empty, nullptr);
}

ScopedForkSignalBlock::ScopedForkSignalBlock() {
	sigset_t all;
	sigfillset(&all);
	// SIGKILL/SIGSTOP are silently not blockable, which is fine — neither can
	// run a handler. SIGPIPE is blocked here too and restored on scope exit;
	// the process-wide SIG_IGN disposition it relies on is untouched.
	if (::pthread_sigmask(SIG_SETMASK, &all, &saved_) == 0) {
		active_ = true;
	}
}

void ScopedForkSignalBlock::Restore() noexcept {
	if (active_) {
		(void)::pthread_sigmask(SIG_SETMASK, &saved_, nullptr);
		active_ = false;
	}
}

ScopedForkSignalBlock::~ScopedForkSignalBlock() {
	Restore();
}

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

	// Build the final shell command BEFORE fork(). When passthrough/debug is on
	// we want VGI_IPC_DEBUG=1 in the worker's environment; do it by prefixing the
	// shell command with `export` rather than calling setenv() in the child after
	// fork(). setenv() is not async-signal-safe (it may take the allocator/environ
	// lock); DuckDB forks workers from query threads, so a peer thread holding that
	// lock at fork time could deadlock the child. Building the string pre-fork keeps
	// the child path to only async-signal-safe calls (dup2/close/execl/_exit).
	std::string shell_command =
	    stderr_passthrough ? ("export VGI_IPC_DEBUG=1; " + command) : command;

	Pipe stdin_pipe;
	Pipe stdout_pipe;
	Pipe stderr_pipe; // Only used if not passthrough

	// Block signals across the fork so nothing can be delivered to the child
	// before it resets the host's inherited handlers (see ScopedForkSignalBlock).
	ScopedForkSignalBlock fork_signal_block;
	pid_ = fork();
	if (pid_ < 0) {
		throw IOException("Failed to fork process");
	}

	if (pid_ == 0) {
		// Child process
		// Disarm the host's signal handlers before anything else — while stdout
		// is still the host's, a handler firing here writes to the host's
		// console under the host's identity. See ResetChildSignalDispositions().
		ResetChildSignalDispositions();

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
		}
		// If passthrough, stderr remains connected to parent's stderr and
		// VGI_IPC_DEBUG was already baked into shell_command above (pre-fork).

		// Execute command via shell
		execl("/bin/sh", "sh", "-c", shell_command.c_str(), nullptr);
		_exit(127); // exec failed
	}

	// Parent process — unblock as soon as the fork has returned; the remaining
	// setup is ordinary parent-side bookkeeping that must not run masked.
	fork_signal_block.Restore();
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

// Create the worker's stdout pipe as an OVERLAPPED named pipe so the parent
// (engine) read end supports async ReadFile -- letting FdInputStream block at
// 0% CPU and wake instantly on data instead of busy-polling an anonymous pipe.
// parent_read: overlapped, not inheritable. child_write: synchronous,
// inheritable (becomes the child's stdout). The MyCreatePipeEx pattern.
static BOOL CreateOverlappedStdoutPipe(HANDLE *parent_read, HANDLE *child_write, DWORD bufsize) {
	static volatile LONG serial = 0;
	char name[80];
	_snprintf_s(name, sizeof(name), _TRUNCATE, "\\\\.\\pipe\\vgi-data-%lu-%lu",
	            (unsigned long)::GetCurrentProcessId(), (unsigned long)::InterlockedIncrement(&serial));
	HANDLE rd = ::CreateNamedPipeA(
	    name, PIPE_ACCESS_INBOUND | FILE_FLAG_OVERLAPPED | FILE_FLAG_FIRST_PIPE_INSTANCE,
	    PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT, 1, bufsize, bufsize, 0, nullptr);
	if (rd == INVALID_HANDLE_VALUE) {
		return FALSE;
	}
	SECURITY_ATTRIBUTES sa = {};
	sa.nLength = sizeof(sa);
	sa.bInheritHandle = TRUE;
	HANDLE wr = ::CreateFileA(name, GENERIC_WRITE, 0, &sa, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
	if (wr == INVALID_HANDLE_VALUE) {
		::CloseHandle(rd);
		return FALSE;
	}
	*parent_read = rd;
	*child_write = wr;
	return TRUE;
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

	// Pipe buffer hint for the two DATA pipes (stdin carries exchange-mode input
	// + producer ticks; stdout carries the worker's Arrow IPC batches). The
	// default CreatePipe buffer is ~4 KiB, which is far smaller than a typical
	// Arrow batch — so the worker blocks part-way through every batch write and
	// ping-pongs with the reader, adding context switches on the hot path. A
	// larger buffer lets a whole batch flow without blocking. 256 KiB is a
	// deliberate balance: big enough for typical/large batches, while bounding
	// nonpaged-pool use across pooled workers (the size is advisory — Windows
	// caps the actual allocation). stderr stays at the default (low-volume logs).
	constexpr DWORD kDataPipeBufferBytes = 1u << 18; // 256 KiB
	if (!CreatePipe(&child_stdin_rd, &parent_stdin_wr, &sa, kDataPipeBufferBytes)) {
		throw fail("CreatePipe(stdin)");
	}
	SetHandleInformation(parent_stdin_wr, HANDLE_FLAG_INHERIT, 0); // parent end not inherited
	if (!CreateOverlappedStdoutPipe(&parent_stdout_rd, &child_stdout_wr, kDataPipeBufferBytes)) {
		throw fail("CreateOverlappedStdoutPipe");
	}
	// parent_stdout_rd is FILE_FLAG_OVERLAPPED + not inheritable; child_stdout_wr inheritable.
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

#endif // VGI_POSIX_TRANSPORT / _WIN32 / emscripten

#if VGI_POSIX_TRANSPORT
void WaitForReadableUntilCancel(int fd, ClientContext *context) {
	// No wall-clock deadline. Match the streaming data-phase model: block
	// until the worker speaks. Cancellation comes via the context's
	// `interrupted` flag (Ctrl-C, query cancel) — we poll it every 250ms.
	constexpr int kPollIntervalMs = 250;

	while (true) {
		if (context && context->interrupted) {
			throw IOException("VGI operation interrupted (query cancelled)");
		}

		// poll() (not select()) so high-numbered fds don't overrun fd_set.
		struct pollfd pfd;
		pfd.fd = fd;
		pfd.events = POLLIN;

		int result = poll(&pfd, 1, kPollIntervalMs);
		if (result < 0) {
			if (errno == EINTR) {
				continue;
			}
			throw IOException("VGI operation failed: poll error: %s", strerror(errno));
		}
		if (result == 0) {
			// Quantum elapsed without data — loop to re-poll interrupted.
			continue;
		}
		return;
	}
}
#elif defined(_WIN32)
// Windows raw-TCP connect (Winsock). The POSIX definition lives in
// vgi_container_runtime.cpp; this satisfies the same VGI_SUBPROCESS_TRANSPORT-
// declared TcpConnect for the tcp:// transport on Windows. Returns a CRT fd that
// owns the socket (close with _close), so the existing fd-based
// FunctionConnection / Fd{Input,Output}Stream path drives it unchanged.
int TcpConnect(const std::string &host, int port, int timeout_ms) {
	static const bool wsa_ok = []() {
		WSADATA w;
		return WSAStartup(MAKEWORD(2, 2), &w) == 0;
	}();
	(void)wsa_ok;
	// WSASocket with dwFlags=0 (NOT the implicit WSA_FLAG_OVERLAPPED that plain
	// socket() sets) — a non-overlapped socket is what lets the CRT fd layer's
	// _read/_write (ReadFile/WriteFile) drive it synchronously, exactly like a pipe.
	SOCKET s = ::WSASocketW(AF_INET, SOCK_STREAM, IPPROTO_TCP, nullptr, 0, 0);
	if (s == INVALID_SOCKET) {
		return -1;
	}
	u_long nb = 1;
	::ioctlsocket(s, FIONBIO, &nb); // non-blocking for the timed connect
	struct sockaddr_in addr;
	ZeroMemory(&addr, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(static_cast<uint16_t>(port));
	::inet_pton(AF_INET, host.c_str(), &addr.sin_addr);
	bool ok = false;
	if (::connect(s, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) == 0) {
		ok = true;
	} else if (WSAGetLastError() == WSAEWOULDBLOCK) {
		fd_set wf;
		FD_ZERO(&wf);
		FD_SET(s, &wf);
		timeval tv;
		tv.tv_sec = timeout_ms / 1000;
		tv.tv_usec = (timeout_ms % 1000) * 1000;
		if (::select(0, nullptr, &wf, nullptr, &tv) > 0) {
			int soerr = 0;
			int l = static_cast<int>(sizeof(soerr));
			::getsockopt(s, SOL_SOCKET, SO_ERROR, reinterpret_cast<char *>(&soerr), &l);
			ok = (soerr == 0);
		}
	}
	if (!ok) {
		::closesocket(s);
		return -1;
	}
	nb = 0;
	::ioctlsocket(s, FIONBIO, &nb); // restore blocking for the fd I/O layer
	// Lockstep request/response RPC — disable Nagle so a small request isn't held
	// waiting to coalesce (the worker can't reply until it arrives).
	BOOL nodelay = TRUE;
	::setsockopt(s, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char *>(&nodelay), sizeof(nodelay));
	int fd = _open_osfhandle(static_cast<intptr_t>(s), _O_BINARY);
	if (fd < 0) {
		::closesocket(s);
		return -1;
	}
	return fd;
}

void WaitForReadableUntilCancel(int fd, ClientContext *context) {
	HANDLE h = reinterpret_cast<HANDLE>(_get_osfhandle(fd));
	if (h == INVALID_HANDLE_VALUE) {
		throw IOException("VGI WaitForReadableUntilCancel: invalid file descriptor");
	}
	// A tcp:// worker's fd is a socket, not a pipe — PeekNamedPipe can't probe it.
	// Sockets ARE selectable, so block in WSAPoll (the Windows poll()): it wakes
	// the instant data arrives, with a 250ms cap only to re-check cancellation.
	// getsockopt fails (WSAENOTSOCK / WSANOTINITIALISED) on a pipe handle, so the
	// subprocess pipe path falls through to the adaptive-backoff peek loop below.
	{
		SOCKET sock = reinterpret_cast<SOCKET>(h);
		int sotype = 0;
		int slen = static_cast<int>(sizeof(sotype));
		if (::getsockopt(sock, SOL_SOCKET, SO_TYPE, reinterpret_cast<char *>(&sotype), &slen) == 0) {
			// Same adaptive ladder as the pipe path so a tcp:// worker isn't
			// penalised vs subprocess: ioctlsocket(FIONREAD) is the non-blocking
			// peek; spin (YieldProcessor) → yield (SwitchToThread) → WSAPoll(1ms)
			// backstop. WSAPoll also wakes on EOF/hangup, so a closed peer doesn't
			// spin forever (recv() then observes the EOF). interrupted re-checked
			// each iteration → prompt cancel.
			for (unsigned attempt = 0;; attempt++) {
				if (context && context->interrupted) {
					throw IOException("VGI operation interrupted (query cancelled)");
				}
				u_long avail = 0;
				if (::ioctlsocket(sock, FIONREAD, &avail) == 0 && avail > 0) {
					return; // data ready
				}
				if (attempt < 64) {
					YieldProcessor();
				} else if (attempt < 4096) {
					SwitchToThread();
				} else {
					WSAPOLLFD pfd;
					pfd.fd = sock;
					pfd.events = POLLRDNORM;
					pfd.revents = 0;
					if (::WSAPoll(&pfd, 1, 1) > 0) {
						return; // readable (data or EOF — recv observes which)
					}
				}
			}
		}
	}
	// Pipe path: the subprocess stdout pipe is OVERLAPPED, so the actual read
	// (FdInputStream::Read) blocks at 0% CPU via overlapped ReadFile +
	// WaitForSingleObject and wakes the instant data arrives -- a real
	// poll()-equivalent that also handles cancellation. Nothing to poll here.
	return;
}
#else // Emscripten
void WaitForReadableUntilCancel(int, ClientContext *) {
	throw NotImplementedException("vgi: subprocess transport unavailable in this build");
}
#endif // VGI_POSIX_TRANSPORT / _WIN32 / emscripten

} // namespace vgi
} // namespace duckdb
