// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// SubProcess subclass that wraps a connected AF_UNIX socket.
//
// FunctionConnection drives the wire protocol via ``proc_->GetStdinFd()`` /
// ``proc_->GetStdoutFd()`` / ``proc_->TryWait()`` etc.  By substituting a
// SubProcess subclass that returns the AF_UNIX socket fd for both stdin
// and stdout, the entire RPC machinery works against an AF_UNIX-served
// worker without duplicating any wire-protocol code.
//
// We don't *own* the worker process — it was spawned (or already running)
// outside of this class's scope, and is garbage-collected by its own idle
// timeout — so ``GetPid()`` returns -1 and ``Wait()`` / ``TryWait()`` are
// no-ops.

#pragma once

#include "vgi_subprocess.hpp"

namespace duckdb {
namespace vgi {

class UnixSocketWorker : public SubProcess {
public:
	// Adopt a connected AF_UNIX SOCK_STREAM file descriptor.  This class
	// takes ownership: the fd is closed in the destructor.
	explicit UnixSocketWorker(int connected_fd);
	~UnixSocketWorker() override;

	// Both stdin and stdout reference the same bidirectional socket.
	// FunctionConnection's writes via GetStdinFd() and reads via
	// GetStdoutFd() naturally interleave on the single full-duplex
	// AF_UNIX stream.
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

	// Half-close the write side of the socket so the worker sees EOF on
	// reads — equivalent semantics to closing a subprocess's stdin.
	void CloseStdin() override;

	// No-ops — the worker isn't our child.
	void CloseStderr() override {
	}

	// ``Wait``/``TryWait`` model "process exited?", which is meaningless
	// for AF_UNIX: we don't own the worker process, the kernel will not
	// let us reap it via ``waitpid``, and there is no PID to track.
	//
	// The honest answer to both is "I don't know."  We picked these
	// values to make existing FunctionConnection call sites behave
	// correctly:
	//
	// - ``Wait()`` returning ``0`` (success) — only invoked from the
	//   destructor / explicit teardown.  No-op is right; closing our fd
	//   already breaks the connection.
	// - ``TryWait()`` returning ``false`` (still running) — paired with
	//   ``IsPoolable() == false`` so ``ReleaseForPooling`` bails out
	//   *before* it asks ``TryWait``.  The other call sites use
	//   ``TryWait`` to enrich error messages on the read path; for
	//   AF_UNIX the read failure itself is the authoritative liveness
	//   signal, so a non-committal ``false`` here is still correct.
	//
	// For callers that genuinely need a liveness hint, prefer
	// :func:`IsLikelyAlive`, which polls the socket for HUP/ERR.
	int Wait(bool *exited_normally = nullptr) override;
	bool TryWait(int *exit_status = nullptr) override {
		(void)exit_status;
		return false;
	}

	// Connection-oriented liveness probe: poll the socket for
	// ``POLLERR`` / ``POLLHUP`` with a zero-ms timeout.  Returns false
	// only when the kernel has observable evidence the peer is gone; the
	// "no signal" case still returns true.  Useful as a short-circuit
	// before issuing an RPC against a possibly-stale connection.
	bool IsLikelyAlive() const override;

	int ReleaseStderrFd() override {
		return -1;
	}
	bool IsPoolable() const override {
		// AF_UNIX workers are shared via the OS socket, not via DuckDB's
		// per-process worker pool.  Returning false here causes
		// FunctionConnection::ReleaseForPooling() to bail out cleanly.
		return false;
	}

private:
	int socket_fd_ = -1;
	bool write_half_closed_ = false;
};

} // namespace vgi
} // namespace duckdb
