// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Windows named-pipe client + worker — the Windows analog of vgi_unix_socket /
// UnixSocketWorker. CPython has no AF_UNIX on Windows, so the launch:/unix://
// rendezvous uses a Windows named pipe (\\.\pipe\...) there. The connected pipe
// HANDLE is wrapped as a CRT fd (via _open_osfhandle), so the existing fd-based
// FunctionConnection wire-protocol code drives it unchanged.

#pragma once

#include "vgi_platform.hpp"

#if defined(_WIN32)

#include "vgi_subprocess.hpp"

#include <chrono>
#include <string>

namespace duckdb {
namespace vgi {

// Connect to a Windows named pipe by name (e.g. "\\.\pipe\vgi-rpc-<hash>").
// Retries on ERROR_PIPE_BUSY (via WaitNamedPipe) and ERROR_FILE_NOT_FOUND
// (server not up yet) until `timeout` elapses. Returns a CRT file descriptor
// owning the pipe HANDLE (close with _close). Throws IOException on failure.
int NamedPipeConnect(const std::string &pipe_name, std::chrono::milliseconds timeout);

// SubProcess wrapping a connected named-pipe fd. Both stdin and stdout reference
// the same duplex pipe, so FunctionConnection's writes (GetStdinFd) and reads
// (GetStdoutFd) interleave on it — exactly like UnixSocketWorker over AF_UNIX.
// We don't own the worker process (it's launched out-of-band / by the launcher),
// so GetPid()==-1 and Wait/TryWait are no-ops; not poolable.
class NamedPipeWorker : public SubProcess {
public:
	explicit NamedPipeWorker(int connected_fd);
	~NamedPipeWorker() override;

	pid_t GetPid() const override {
		return -1;
	}
	int GetStdinFd() const override {
		return fd_;
	}
	int GetStdoutFd() const override {
		return fd_;
	}
	int GetStderrFd() const override {
		return -1;
	}

	// Named pipes have no half-close; the wire protocol delimits each request
	// with an in-band Arrow IPC EOS, so signalling input-EOF at the fd level is
	// unnecessary. No-op.
	void CloseStdin() override {
	}
	void CloseStderr() override {
	}

	int Wait(bool *exited_normally = nullptr) override {
		if (exited_normally) {
			*exited_normally = true;
		}
		return 0;
	}
	bool TryWait(int *exit_status = nullptr) override {
		(void)exit_status;
		return false;
	}
	bool IsLikelyAlive() const override;
	int ReleaseStderrFd() override {
		return -1;
	}
	bool IsPoolable() const override {
		// Shared via the OS named pipe / launcher, not DuckDB's subprocess pool.
		return false;
	}

private:
	int fd_ = -1;
};

} // namespace vgi
} // namespace duckdb

#endif // _WIN32
