// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_unary_rpc.hpp"

#include <exception>
#include <algorithm>
#include <chrono>
#include <thread>

#include "duckdb/common/exception.hpp"

#include "vgi_container_runtime.hpp"
#include "vgi_exception.hpp"
#include "vgi_hosted_protocols.hpp"
#include "vgi_http_client.hpp"
#include "vgi_iroh_native.hpp"
#include "vgi_logging.hpp"
#include "vgi_stderr_drainer.hpp"
#include "vgi_github.hpp"
#include "vgi_subprocess.hpp"
#include "vgi_transport.hpp"
#include "vgi_unary_retry.hpp"
#include "vgi_protocol_session.hpp"
#if defined(__EMSCRIPTEN__)
#include "vgi_webworker_function_connection.hpp" // WebWorkerInvokeUnary (worker: unary branch)
#endif
#if VGI_POSIX_TRANSPORT
// Launcher / AF_UNIX transport — POSIX only.
#include "vgi_launcher_cache.hpp"
#include "vgi_unix_socket.hpp"
#include "vgi_unix_socket_worker.hpp"
#elif defined(_WIN32)
// Windows: launch:/unix:// rendezvous uses Windows named pipes instead of AF_UNIX.
#include "vgi_launcher.hpp"
#include "vgi_launcher_internal.hpp"
#include "vgi_named_pipe.hpp"
#endif
#include "vgi_worker_pool.hpp"

namespace duckdb {
namespace vgi {

namespace {

// A failed HTTP call still taught something: the capability headers (and
// VGI-Protocol-Versions) of the response that refused it, which protocol
// negotiation reads. The success path stores the snapshot itself.
struct HarvestOnFailure {
	ProtocolSession *session;
	const ServerCapabilities &caps;
	~HarvestOnFailure() {
		if (session && std::uncaught_exceptions() > 0) {
			session->Store(caps);
		}
	}
};

} // namespace

namespace {

#if VGI_SUBPROCESS_TRANSPORT
// One attempt; throws IOException on any transport error and the caller decides
// whether to retry. `force_fresh` skips pool acquisition (the retry path).
UnaryResponseResult AttemptUnaryRpc(const UnaryRpcOptions &opts, const std::string &method_name,
                                     const VgiBatchPtr &params, bool force_fresh) {
	std::unique_ptr<SubProcess> proc;
	std::unique_ptr<StderrDrainer> drainer;
	bool from_pool = false;
	PoolKey pool_key {opts.worker_path, opts.data_version_spec, opts.implementation_version};

	if (!force_fresh && opts.use_pool) {
		auto pooled = VgiWorkerPool::Instance().TryAcquire(pool_key);
		if (pooled) {
			// Take over the drainer that's been keeping stderr empty while
			// the worker sat idle; its reader thread is already running.
			drainer = pooled->ReleaseDrainer();
			proc = pooled->Release();
			from_pool = true;
		}
	}
	if (!proc) {
		// Resolve github:// / github-auto:// to the local cached entrypoint before
		// spawning (no-op for every other scheme). opts.worker_path stays the
		// LOCATION above so the pool keys consistently.
		proc = SpawnWorker(ResolveWorkerPath(opts.worker_path, opts.context), opts.worker_debug);
		drainer = std::make_unique<StderrDrainer>(proc->ReleaseStderrFd());
		if (opts.use_pool && !force_fresh) {
			VgiWorkerPool::Instance().RecordMiss(opts.worker_path);
		}
	}

	const char *result_tag =
	    from_pool ? "hit" : (force_fresh ? "retry_after_stale" : (opts.use_pool ? "miss" : "disabled"));
	if (opts.enable_logging) {
		VGI_LOG(opts.context, "worker_pool.acquire",
		        {{"worker_path", opts.worker_path},
		         {"worker_pid", std::to_string(proc->GetPid())},
		         {"result", result_tag},
		         {"phase", opts.phase}});
	}

	UnaryResponseResult response;
	try {
		if (params) {
			WriteRpcRequest(proc->GetStdinFd(), method_name, params, /*extra_metadata=*/nullptr, opts.protocol);
		} else {
			WriteEmptyRpcRequest(proc->GetStdinFd(), method_name, opts.protocol);
		}
		// HandleBatchLogMessage would otherwise call VGI_LOG from this thread.
		auto *log_ctx = opts.enable_logging ? &opts.context : nullptr;
		response = ReadUnaryResponse(proc->GetStdoutFd(), log_ctx, opts.worker_path, proc->GetPid());
	} catch (const IOException &e) {
		if (from_pool) {
			if (opts.enable_logging) {
				VGI_LOG(opts.context, "worker_pool.stale",
				        {{"worker_path", opts.worker_path},
				         {"worker_pid", std::to_string(proc->GetPid())},
				         {"method_name", method_name},
				         {"error", e.what()},
				         {"phase", opts.phase}});
			}
		} else {
			// A freshly spawned worker that died here never answered its first
			// request — usually a startup failure (missing runtime, bad project).
			CheckWorkerExitStatus(*proc, opts.worker_path, "exited before answering its first request", "",
			                      drainer ? drainer->CaptureStderrSnapshot() : std::string());
		}
		throw;
	}

	// Only where logging is safe (see UnaryRpcOptions::enable_logging); otherwise
	// buffered lines ride along with the pooled worker or die with it.
	if (drainer && opts.enable_logging) {
		drainer->DrainToLog(opts.context, opts.worker_path, proc->GetPid());
	}

	if (opts.use_pool) {
		int exit_status = 0;
		if (!proc->TryWait(&exit_status)) {
			// Keep the drainer alive across the pool idle period.
			auto to_pool = std::make_unique<PooledWorker>(std::move(proc), pool_key, std::move(drainer));
			auto released_pid = to_pool->GetPid();
			auto rr = VgiWorkerPool::Instance().Release(std::move(to_pool));
			if (opts.enable_logging) {
				PoolReleaseLogFields lf;
				lf.worker_path = opts.worker_path;
				lf.worker_pid = released_pid;
				lf.method_name = method_name;
				lf.phase = opts.phase;
				LogWorkerPoolRelease(opts.context, lf, rr.pooled, rr.skip_reason, rr.pool_size, rr.total_pool_size);
			}
		}
	}

	return response;
}
#endif // VGI_SUBPROCESS_TRANSPORT

UnaryResponseResult InvokeUnaryRpcOnTransport(const UnaryRpcOptions &opts, const std::string &method_name,
                                              const VgiBatchPtr &params) {
	if (IsHttpTransport(opts.worker_path)) {
		// Keep-alive: check a client out of the per-catalog pool for this call
		// and return it only on success. If HttpInvokeUnary throws, ``pooled``
		// is destroyed here — a client whose request failed may hold a wedged
		// connection, so it is dropped rather than repooled.
		duckdb::unique_ptr<HTTPClient> pooled;
		if (opts.http_client_pool) {
			pooled = opts.http_client_pool->Checkout();
		}
		// Snapshot in to pick the request codec; refreshed from the response.
		ServerCapabilities caps;
		if (opts.protocol_session) {
			caps = opts.protocol_session->Load();
		}
		HarvestOnFailure harvest_on_failure {opts.protocol_session.get(), caps};
		auto result = HttpInvokeUnary(
		    opts.context, opts.worker_path, method_name, params, opts.auth, opts.cookie_jar, opts.cached_http_params,
		                              /*execution_id_hex=*/"", /*attach_opaque_data_digest=*/"",
		    /*transaction_opaque_data_digest=*/"", /*conn_id_hex=*/"", opts.protocol,
		    opts.http_client_pool ? &pooled : nullptr, opts.protocol_session ? &caps : nullptr, opts.iroh);
		if (opts.protocol_session) {
			opts.protocol_session->Store(caps);
		}
		if (opts.http_client_pool) {
			opts.http_client_pool->Return(std::move(pooled));
		}
		return result;
	}
	if (IsHttpiTransport(opts.worker_path)) {
		ServerCapabilities caps;
		if (opts.protocol_session) {
			caps = opts.protocol_session->Load();
		}
		HarvestOnFailure harvest_on_failure {opts.protocol_session.get(), caps};
#if defined(__EMSCRIPTEN__)
		auto result = HttpInvokeUnary(opts.context, CanonicalizeHttpiLocation(opts.worker_path), method_name, params,
		                       opts.auth, opts.cookie_jar, opts.cached_http_params, "", "", "", "",
		                       opts.protocol, nullptr,
		                       opts.protocol_session ? &caps : nullptr);
#else
		auto result = HttpInvokeUnary(opts.context, CanonicalizeHttpiLocation(opts.worker_path), method_name, params,
		                       opts.auth, opts.cookie_jar, nullptr, "", "", "", "",
		                       opts.protocol, nullptr,
		                       opts.protocol_session ? &caps : nullptr, opts.iroh);
#endif
		if (opts.protocol_session) {
			opts.protocol_session->Store(caps);
		}
		return result;
	}

#if defined(__EMSCRIPTEN__)
	// Browser worker: SAB transport — catalog RPCs (ATTACH, discovery) as a
	// self-contained unary over a transient SAB slot (open/write+EOS/read/release).
	if (IsWebWorkerTransport(opts.worker_path) || IsIrohTransport(opts.worker_path)) {
		return WebWorkerInvokeUnary(opts.context, opts.worker_path, method_name, params,
		                            opts.protocol);
	}
#endif
#if !defined(__EMSCRIPTEN__)
	if (IsIrohTransport(opts.worker_path)) {
		if (!opts.iroh) {
			throw InvalidInputException("vgi: native iroh:// unary RPC is missing its ATTACH configuration");
		}
		auto duplex = OpenIrohArrowMuxStream(opts.iroh, &opts.context);
		if (params) {
			WriteRpcRequest(duplex.output, method_name, params, /*extra_metadata=*/nullptr, opts.protocol);
		} else {
			// Zero-field schema, one row (the no-params request shape).
			auto empty = VgiBatch::FromColumns(VgiSchema::Empty(), 1, {});
			WriteRpcRequest(duplex.output, method_name, empty, /*extra_metadata=*/nullptr, opts.protocol);
		}
		return ReadUnaryResponse(duplex.input, &opts.context, opts.worker_path);
	}
#endif

	// Shared container: resolve the live endpoint at call time, then dispatch to
	// the resolved transport (http reuses HttpInvokeUnary on the loopback URL).
	if (IsContainerSharedLocation(opts.worker_path)) {
#if VGI_SUBPROCESS_TRANSPORT
		ContainerSpec shared_spec;
		ContainerConnMode shared_mode;
		if (!LookupSharedContainer(opts.worker_path, shared_spec, shared_mode)) {
			throw IOException("vgi: no resolved shared-container info for '%s'", opts.worker_path);
		}
		// The connect + RPC is the readiness check: a fresh shared container may
		// still be starting, so retry with backoff until a 60 s deadline (a broken
		// image then fails rather than spinning). The first RPC of an ATTACH lands
		// here, so this absorbs cold start; a warm container returns at once.
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
		auto backoff = std::chrono::milliseconds(150);
		for (;;) {
			bool request_dispatched = false;
			try {
				auto ep = EnsureSharedContainer(shared_spec, shared_mode);
				if (ep.mode == ContainerConnMode::HTTP) {
					request_dispatched = true;
					// Same capability-snapshot handling as the http:// branch above.
					ServerCapabilities caps;
					if (opts.protocol_session) {
						caps = opts.protocol_session->Load();
					}
					HarvestOnFailure harvest_on_failure {opts.protocol_session.get(), caps};
					auto result = HttpInvokeUnary(opts.context, ep.url, method_name, params, opts.auth,
					                              opts.cookie_jar, opts.cached_http_params, "", "", "", "",
					                              opts.protocol, nullptr, opts.protocol_session ? &caps : nullptr);
					if (opts.protocol_session) {
						opts.protocol_session->Store(caps);
					}
					return result;
				}
				// tcp: native vgi-rpc over the connected fd.
				auto worker = ConnectSharedContainer(ep);
				request_dispatched = true;
				if (params) {
					WriteRpcRequest(worker->GetStdinFd(), method_name, params, /*extra_metadata=*/nullptr, opts.protocol);
				} else {
					WriteEmptyRpcRequest(worker->GetStdinFd(), method_name, opts.protocol);
				}
				auto *log_ctx = opts.enable_logging ? &opts.context : nullptr;
				return ReadUnaryResponse(worker->GetStdoutFd(), log_ctx, opts.worker_path, /*pid=*/-1);
			} catch (const IOException &) {
				if (request_dispatched && RequiresSingleAttemptUnary(method_name)) {
					// Readiness failures before dispatch may reconnect; a lost
					// response after mutation dispatch must not repeat the call.
					throw;
				}
				if (std::chrono::steady_clock::now() >= deadline) {
					throw;
				}
				InvalidateSharedContainer(shared_spec);
				NoteWorkerConnectionReestablished(opts.worker_path);
				std::this_thread::sleep_for(backoff);
				backoff = std::min(backoff * 2, std::chrono::milliseconds(1000));
			}
		}
#else
		throw InvalidInputException("vgi: shared containers are unavailable in this build");
#endif
	}

	if (IsTcpTransport(opts.worker_path)) {
#if VGI_POSIX_TRANSPORT
		// tcp://host:port — connect-only against an out-of-band worker on a raw-TCP
		// socket; drive the same wire-protocol code through a UnixSocketWorker (the
		// connected fd). No pooling — the long-lived worker is itself the pool.
		std::string tcp_host;
		int tcp_port;
		ParseTcpLocation(opts.worker_path, tcp_host, tcp_port);
		std::string connect_error;
		int fd = TcpConnect(tcp_host, tcp_port, 10000, &connect_error, opts.tcp_proxy);
		if (fd < 0) {
			throw IOException(ErrorInfo(error_subtype::kConnectFailed)
			                      .Worker(opts.worker_path)
			                      .Set(error_key::kHost, tcp_host)
			                      .Set(error_key::kPort, static_cast<int64_t>(tcp_port)),
			                  "vgi: failed to connect to tcp worker %s: %s", opts.worker_path, connect_error);
		}
		UnixSocketWorker worker(fd);
		if (params) {
			WriteRpcRequest(worker.GetStdinFd(), method_name, params, /*extra_metadata=*/nullptr, opts.protocol);
		} else {
			WriteEmptyRpcRequest(worker.GetStdinFd(), method_name, opts.protocol);
		}
		auto *log_ctx = opts.enable_logging ? &opts.context : nullptr;
		return ReadUnaryResponse(worker.GetStdoutFd(), log_ctx, opts.worker_path, /*pid=*/-1);
#elif defined(_WIN32)
		// tcp://host:port on Windows — raw TCP socket (Winsock), wrapped in a
		// NamedPipeWorker fd worker; same wire-protocol path as POSIX unix://.
		std::string tcp_host;
		int tcp_port;
		ParseTcpLocation(opts.worker_path, tcp_host, tcp_port);
		std::string connect_error;
		int fd = TcpConnect(tcp_host, tcp_port, 10000, &connect_error, opts.tcp_proxy);
		if (fd < 0) {
			throw IOException(ErrorInfo(error_subtype::kConnectFailed)
			                      .Worker(opts.worker_path)
			                      .Set(error_key::kHost, tcp_host)
			                      .Set(error_key::kPort, static_cast<int64_t>(tcp_port)),
			                  "vgi: failed to connect to tcp worker %s: %s", opts.worker_path, connect_error);
		}
		NamedPipeWorker worker(fd);
		if (params) {
			WriteRpcRequest(worker.GetStdinFd(), method_name, params, /*extra_metadata=*/nullptr, opts.protocol);
		} else {
			WriteEmptyRpcRequest(worker.GetStdinFd(), method_name, opts.protocol);
		}
		auto *log_ctx = opts.enable_logging ? &opts.context : nullptr;
		return ReadUnaryResponse(worker.GetStdoutFd(), log_ctx, opts.worker_path, /*pid=*/-1);
#else
		throw InvalidInputException("vgi: tcp:// LOCATIONs are not available in this build");
#endif
	}

	if (IsLaunchLocation(opts.worker_path) || IsUnixLocation(opts.worker_path)) {
#if VGI_POSIX_TRANSPORT
		// Fresh AF_UNIX connection per call; no pooling, the long-lived worker
		// behind the socket is itself the pool. ResolveAndConnect fires the
		// launcher on first use and retries once if the cached worker idled out.
		LaunchOverrides overrides;
		if (opts.launcher_idle_timeout.has_value()) {
			overrides.idle_timeout = *opts.launcher_idle_timeout;
		}
		if (opts.launcher_state_dir.has_value()) {
			overrides.state_dir = *opts.launcher_state_dir;
		}
		auto sock = ResolveAndConnect(opts.worker_path, std::chrono::seconds(10), overrides);
		UnixSocketWorker worker(sock.Release());
		if (params) {
			WriteRpcRequest(worker.GetStdinFd(), method_name, params, /*extra_metadata=*/nullptr, opts.protocol);
		} else {
			WriteEmptyRpcRequest(worker.GetStdinFd(), method_name, opts.protocol);
		}
		auto *log_ctx = opts.enable_logging ? &opts.context : nullptr;
		return ReadUnaryResponse(worker.GetStdoutFd(), log_ctx, opts.worker_path, /*pid=*/-1);
#elif defined(_WIN32)
		// Windows: launch:/unix:// over a Windows named pipe. unix:// connects to
		// an out-of-band worker; launch: spawns (or reuses) one via the launcher.
		std::string pipe_name;
		if (IsUnixLocation(opts.worker_path)) {
			pipe_name = StripUnixScheme(opts.worker_path);
		} else {
			LaunchConfig cfg;
			cfg.worker_argv = launcher::ParseLaunchArgv(StripLaunchScheme(opts.worker_path));
			if (opts.launcher_idle_timeout.has_value()) {
				cfg.idle_timeout = *opts.launcher_idle_timeout;
			}
			if (opts.launcher_state_dir.has_value()) {
				cfg.state_dir_override = *opts.launcher_state_dir;
			}
			pipe_name = Launch(cfg);
		}
		NamedPipeWorker worker(NamedPipeConnect(pipe_name, std::chrono::seconds(10)));
		if (params) {
			WriteRpcRequest(worker.GetStdinFd(), method_name, params, /*extra_metadata=*/nullptr, opts.protocol);
		} else {
			WriteEmptyRpcRequest(worker.GetStdinFd(), method_name, opts.protocol);
		}
		auto *log_ctx = opts.enable_logging ? &opts.context : nullptr;
		return ReadUnaryResponse(worker.GetStdoutFd(), log_ctx, opts.worker_path, /*pid=*/-1);
#else
		throw InvalidInputException("vgi: launch:/unix:// LOCATION schemes are not available in this build "
		    "(worker_path=%s); use http://… instead",
		    opts.worker_path);
#endif
	}

	// Bare-command path → subprocess transport (POSIX fork/exec or Windows
	// CreateProcess); only Emscripten lacks it.
#if !VGI_SUBPROCESS_TRANSPORT
	throw InvalidInputException("vgi: subprocess (bare command) LOCATIONs require a child-process transport "
	    "not available in this build (worker_path=%s); use http://… instead",
	    opts.worker_path);
#else
	try {
		return AttemptUnaryRpc(opts, method_name, params, /*force_fresh=*/false);
	} catch (const IOException &) {
		if (!opts.use_pool || RequiresSingleAttemptUnary(method_name)) {
			// Reusing a healthy worker does not make a lost mutation reply safe
			// to repeat. Keep pool acquisition independent of retry policy.
			throw;
		}
		// Single retry with a fresh worker. If that also fails, propagate.
		NoteWorkerConnectionReestablished(opts.worker_path);
		return AttemptUnaryRpc(opts, method_name, params, /*force_fresh=*/true);
	}
#endif
}

} // anonymous namespace

UnaryResponseResult InvokePooledUnaryRpc(const UnaryRpcOptions &opts, const std::string &method_name,
                                          const VgiBatchPtr &params) {
	auto result = InvokeUnaryRpcOnTransport(opts, method_name, params);
	// Externalized results can come back on any transport. Resolved outside
	// the respawn retry so a failed fetch does not cost a worker; HTTP already
	// resolved it.
	return MaybeResolveExternalLocation(opts.context, result, opts.worker_path);
}

} // namespace vgi
} // namespace duckdb
