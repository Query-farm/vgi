// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "vgi_platform.hpp"
#if VGI_POSIX_TRANSPORT
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
//
// As of the AF_UNIX launcher work, this class is polymorphic: the existing
// fork()/execl() concrete implementation is the default; a subclass
// :class:`UnixSocketWorker` (see ``vgi_unix_socket_worker.hpp``) plugs an
// AF_UNIX-connected socket into the same shape, so ``FunctionConnection``
// — which talks to ``proc_->GetStdinFd()`` etc. via this interface — works
// transparently against either transport.
//
// Exception safety: If the constructor throws after fork(), the child
// process will be orphaned.  In practice, the only operations that can fail
// after fork() are the pipe fd transfers, which cannot fail.  The child
// calls _exit() if exec fails, so it won't become a zombie.
class SubProcess {
public:
	// Create a subprocess running the given command.
	// If stderr_passthrough is true, worker stderr goes directly to the terminal
	// instead of being captured (useful for debugging).
	explicit SubProcess(const std::string &command, bool stderr_passthrough = false);
	virtual ~SubProcess();

	// Accessors for process info and file descriptors.  Virtual so a
	// non-fork-based transport (``UnixSocketWorker``) can substitute its
	// own fds without re-implementing FunctionConnection's wire-protocol
	// machinery.
	virtual pid_t GetPid() const {
		return pid_;
	}
	virtual int GetStdinFd() const {
		return stdin_fd_;
	}
	virtual int GetStdoutFd() const {
		return stdout_fd_;
	}
	virtual int GetStderrFd() const {
		return stderr_fd_;
	}

	// Close stdin to signal EOF to the child process
	virtual void CloseStdin();

	// Close stderr (typically after transferring ownership to a reader)
	virtual void CloseStderr();

	// Wait for the process to exit and return the exit status.
	// Returns 0 on success, positive exit code on normal exit, or negative signal number
	// (i.e. -WTERMSIG) if killed by a signal. This matches TryWait() convention.
	// Sets exited_normally to true if the process exited via exit(), false if killed by signal.
	virtual int Wait(bool *exited_normally = nullptr);

	// Non-blocking check if the process has exited.
	// Returns true if the process has exited (and populates exit_status if provided).
	// Returns false if the process is still running.
	// Useful for detecting early failures (e.g., command not found).
	virtual bool TryWait(int *exit_status = nullptr);

	// Release ownership of stderr fd (caller takes responsibility for closing)
	virtual int ReleaseStderrFd() {
		int fd = stderr_fd_;
		stderr_fd_ = -1;
		return fd;
	}

	// Subprocess workers can be returned to the per-path worker pool after a
	// query finishes; AF_UNIX-backed workers (where the same long-lived worker
	// is shared across DuckDB processes via the launcher) cannot — they're
	// already conceptually pooled by the OS-level socket.  Defaults true for
	// the fork-based concrete class; ``UnixSocketWorker`` overrides to false.
	virtual bool IsPoolable() const {
		return true;
	}

	// Best-effort liveness probe for *connection-oriented* transports.
	// The base class always returns true: a fork-spawned subprocess's
	// liveness is authoritatively answered by ``TryWait()`` (kernel-side
	// process table) rather than by an I/O probe, so this method exists
	// purely so AF_UNIX-backed workers (``UnixSocketWorker``) can answer
	// "is the peer still up?" without a full read attempt.  For AF_UNIX
	// the override does ``poll(POLLIN | POLLERR | POLLHUP)`` with a
	// 0-ms timeout: if POLLERR or POLLHUP is set, the peer is gone.
	//
	// Note: returning ``true`` here does *not* prove the peer will accept
	// the next byte — only that we have no observable signal it's gone.
	// Callers should treat this as a hint, not a contract.  Use it to
	// short-circuit obviously-dead sockets, not as a replacement for
	// per-call I/O error handling.
	virtual bool IsLikelyAlive() const {
		return true;
	}

	// Non-copyable
	SubProcess(const SubProcess &) = delete;
	SubProcess &operator=(const SubProcess &) = delete;

protected:
	// Default constructor for subclasses that supply their own fds (e.g.
	// UnixSocketWorker).  Skips the fork+exec path entirely.
	SubProcess() = default;

	pid_t pid_ = -1;
	int stdin_fd_ = -1;
	int stdout_fd_ = -1;
	int stderr_fd_ = -1;
#if defined(_WIN32)
	// Windows: the spawned child's process HANDLE (owned; closed in the dtor /
	// after Wait). pid_ holds GetProcessId(process_handle_) for diagnostics, but
	// Wait()/TryWait() need the HANDLE itself. void* avoids dragging <windows.h>
	// into this widely-included header. The fd members above are CRT fds wrapping
	// the parent ends of the stdio pipes via _open_osfhandle().
	void *process_handle_ = nullptr;
#endif
};

// Helper to write all bytes to a file descriptor
void WriteAll(int fd, const uint8_t *data, size_t len);

} // namespace vgi

class ClientContext;

namespace vgi {
// Wait for a file descriptor to become readable, polling the context's
// `interrupted` flag every 250ms so a user Ctrl-C breaks out of long blocking
// reads. No deadline — used by data-phase RPCs (table_buffering_*) that can
// legitimately run for arbitrary durations. Cleanup of an interrupted-mid-RPC
// connection is the caller's responsibility (route through
// VgiCancelDispatcher so the worker unblocks).
void WaitForReadableUntilCancel(int fd, ClientContext *context);
} // namespace vgi

} // namespace duckdb
