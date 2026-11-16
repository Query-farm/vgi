// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// SubProcess over a connected AF_UNIX socket: the socket fd serves as both
// stdin and stdout, so FunctionConnection's subprocess code path drives it
// unchanged. The worker process is not ours (it exits on its own idle timeout),
// so GetPid() is -1 and Wait()/TryWait() are no-ops.

#pragma once

#include "vgi_subprocess.hpp"

#if VGI_POSIX_TRANSPORT

namespace duckdb {
namespace vgi {

class UnixSocketWorker : public SubProcess {
public:
	// Takes ownership of a connected AF_UNIX SOCK_STREAM fd.
	explicit UnixSocketWorker(int connected_fd);
	~UnixSocketWorker() override;

	pid_t GetPid() const override {
		return -1;
	}
	int GetStdinFd() const override {
		return socket_fd_;
	}
	int GetStdoutFd() const override {
		return socket_fd_;
	}
	int GetStderrFd() const override {
		return -1;
	}

	// shutdown(SHUT_WR): the worker sees EOF while responses can still be read.
	void CloseStdin() override;

	void CloseStderr() override {
	}

	// "Process exited?" has no answer here (no child to reap). Wait() reports a
	// clean exit (teardown only; closing the fd ends the connection). TryWait()
	// reports "still running": ReleaseForPooling then rejects via IsPoolable(),
	// and on the read path the I/O failure itself is the liveness signal. Use
	// IsLikelyAlive() for a real hint.
	int Wait(bool *exited_normally = nullptr) override;
	bool TryWait(int *exit_status = nullptr) override {
		(void)exit_status;
		return false;
	}

	// Zero-timeout poll; false only on POLLHUP/POLLERR/POLLNVAL (or a closed fd).
	bool IsLikelyAlive() const override;

	int ReleaseStderrFd() override {
		return -1;
	}
	bool IsPoolable() const override {
		// Shared via the OS socket, not DuckDB's worker pool.
		return false;
	}

private:
	int socket_fd_ = -1;
	bool write_half_closed_ = false;
};

} // namespace vgi
} // namespace duckdb

#endif // VGI_POSIX_TRANSPORT
