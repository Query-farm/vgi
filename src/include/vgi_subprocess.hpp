// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "vgi_platform.hpp"
#if VGI_POSIX_TRANSPORT
#include <pthread.h> // pthread_sigmask (ScopedForkSignalBlock)
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace duckdb {
namespace vgi {

#if VGI_POSIX_TRANSPORT
// Restore default signal dispositions and an empty signal mask in a freshly
// forked child, BEFORE it touches fds or execs. exec() resets caught signals,
// but between fork() and exec() the host's handlers (e.g. Catch2's) are still
// armed in the child and would run there, writing to the host's stdout.
// SIGPIPE is left alone: workers rely on the inherited process-wide SIG_IGN so
// broken pipes surface as EPIPE. Async-signal-safe, as required post-fork.
void ResetChildSignalDispositions();

// Block every signal on the calling thread across a fork(), restoring the
// previous mask on scope exit or Restore(). This closes the gap before the
// child's ResetChildSignalDispositions() runs (e.g. a SIGTERM from ~SubProcess
// on a just-spawned worker): a pending signal is delivered only after the
// reset, so it takes its default action.
class ScopedForkSignalBlock {
public:
	ScopedForkSignalBlock();
	~ScopedForkSignalBlock();
	// Idempotent. The parent calls it once fork() returns; the child never does
	// (ResetChildSignalDispositions empties its mask).
	void Restore() noexcept;

	ScopedForkSignalBlock(const ScopedForkSignalBlock &) = delete;
	ScopedForkSignalBlock &operator=(const ScopedForkSignalBlock &) = delete;

private:
	sigset_t saved_;
	bool active_ = false;
};
#endif

// pipe() pair, both ends FD_CLOEXEC; closed on destruction.
class Pipe {
public:
	int read_fd = -1;
	int write_fd = -1;

	Pipe();
	~Pipe();

	void CloseRead();
	void CloseWrite();

	Pipe(const Pipe &) = delete;
	Pipe &operator=(const Pipe &) = delete;
};

struct DirectExecutable {
	std::string path;
	std::shared_ptr<void> lifetime_anchor;
};

// A worker process with piped stdio (fork + exec on POSIX, CreateProcess on
// Windows). Subclasses such as UnixSocketWorker supply their own fds through
// the same virtual accessors, so FunctionConnection works over any of them.
// Nothing after fork() in the constructor can throw, so a child is never
// orphaned by a failed construction; a failed exec _exits(127).
class SubProcess {
public:
	// `command` runs via /bin/sh -c (cmd.exe /c on Windows); a DirectExecutable
	// is exec'd without a shell. stderr_passthrough (also enabled by
	// VGI_WORKER_STDERR_PASSTHROUGH=1 / VGI_WORKER_DEBUG=1) leaves worker stderr
	// on the terminal and sets VGI_IPC_DEBUG=1 in the worker.
	explicit SubProcess(const std::string &command, bool stderr_passthrough = false);
	explicit SubProcess(DirectExecutable executable, bool stderr_passthrough = false);
	virtual ~SubProcess();

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

	virtual void CloseStdin();

	virtual void CloseStderr();

	// Blocking. Returns the exit code, or -WTERMSIG if killed by a signal (same
	// convention as TryWait); exited_normally is false only for a signal.
	virtual int Wait(bool *exited_normally = nullptr);

	// Non-blocking: true (with exit_status) once the process has exited.
	virtual bool TryWait(int *exit_status = nullptr);

	// Caller takes ownership of (and must close) the stderr fd.
	virtual int ReleaseStderrFd() {
		int fd = stderr_fd_;
		stderr_fd_ = -1;
		return fd;
	}

	// Whether the worker may return to the per-path subprocess pool. Socket
	// workers (UnixSocketWorker, named pipes) override to false: the launcher
	// already shares them at the OS level.
	virtual bool IsPoolable() const {
		return true;
	}

	// Hint for connection-oriented transports (a 0-ms poll for hangup in
	// UnixSocketWorker); a forked process answers via TryWait(), so the base
	// returns true. `true` only means no sign the peer is gone — it never
	// replaces per-call I/O error handling.
	virtual bool IsLikelyAlive() const {
		return true;
	}

	SubProcess(const SubProcess &) = delete;
	SubProcess &operator=(const SubProcess &) = delete;

protected:
	// For subclasses that supply their own fds; spawns nothing.
	SubProcess() = default;
	SubProcess(const std::string &command, bool stderr_passthrough, bool direct_executable,
	           std::shared_ptr<void> lifetime_anchor);

	pid_t pid_ = -1;
	int stdin_fd_ = -1;
	int stdout_fd_ = -1;
	int stderr_fd_ = -1;
	std::shared_ptr<void> lifetime_anchor_;
#if defined(_WIN32)
	// Owned child process HANDLE (Wait/TryWait need it; pid_ is for
	// diagnostics). void* keeps <windows.h> out of this header. The fds above
	// are CRT fds wrapping the parent pipe ends (_open_osfhandle).
	void *process_handle_ = nullptr;
#endif
};

// Writes every byte, retrying EINTR; throws IOException (e.g. worker gone).
void WriteAll(int fd, const uint8_t *data, size_t len);

} // namespace vgi

class ClientContext;

namespace vgi {
// Wait until `fd` is readable, with no deadline (data-phase RPCs can run
// arbitrarily long), checking RpcInterrupted every 250ms and throwing
// InterruptException. The caller must cancel-dispatch an interrupted
// connection (VgiCancelDispatcher) so the worker unblocks.
void WaitForReadableUntilCancel(int fd, ClientContext *context);

// Marks this thread as running a cleanup RPC that must reach the worker even
// though its query is marked interrupted (catalog_transaction_rollback, the
// drop of a failed CTAS's table): Executor::PushError marks the context
// interrupted when any task fails, before the rollback runs. Thread-local, so
// scans and other tasks keep honouring the flag.
//
// Known limitation: such an RPC cannot be cancelled on subprocess, launcher,
// shm or native Iroh transports; a worker that never answers blocks it until
// the worker exits (Iroh still applies io_timeout_seconds). HTTP unary RPCs
// never take the query as cancellation and are bounded by HTTP timeouts.
// Native Iroh's CheckDuckDBInterrupted runs on the calling thread (block_on
// polls it there), so the scope applies to it too.
class CleanupRpcScope {
public:
	CleanupRpcScope();
	~CleanupRpcScope();
	CleanupRpcScope(const CleanupRpcScope &) = delete;
	CleanupRpcScope &operator=(const CleanupRpcScope &) = delete;

	static bool Active();
};

// True when a blocking transport wait on this thread should give up because
// `context`'s query was interrupted (never inside a CleanupRpcScope).
bool RpcInterrupted(const ClientContext *context);
} // namespace vgi

} // namespace duckdb
