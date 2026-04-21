#include "vgi_unary_rpc.hpp"

#include "duckdb/common/exception.hpp"

#include "vgi_exception.hpp"
#include "vgi_http_client.hpp"
#include "vgi_logging.hpp"
#include "vgi_stderr_drainer.hpp"
#include "vgi_subprocess.hpp"
#include "vgi_transport.hpp"
#include "vgi_worker_pool.hpp"

namespace duckdb {
namespace vgi {

namespace {

// One attempt. Throws IOException on any transport error; caller decides whether
// to retry. `force_fresh` bypasses the pool acquire entirely — used for the
// retry path so a freshly-spawned worker does the work.
UnaryResponseResult AttemptUnaryRpc(const UnaryRpcOptions &opts, const std::string &method_name,
                                     const std::shared_ptr<arrow::RecordBatch> &params, bool force_fresh) {
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
		proc = std::make_unique<SubProcess>(opts.worker_path, opts.worker_debug);
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
			WriteRpcRequest(proc->GetStdinFd(), method_name, params);
		} else {
			WriteEmptyRpcRequest(proc->GetStdinFd(), method_name);
		}
		// Disable in-band log message forwarding when enable_logging is false —
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
			CheckWorkerExitStatus(*proc, opts.worker_path, "failed during unary RPC");
		}
		throw;
	}

	// Drain stderr into DuckDB's logger only when the caller guaranteed we're
	// on a thread where that's safe. Off-main threads (aggregate destructors
	// during pipeline teardown) skip this; buffered lines either ride along
	// with the pooled worker to its next use or are dropped with the
	// subprocess. StderrDrainer itself keeps reading in its own thread.
	if (drainer && opts.enable_logging) {
		drainer->DrainToLog(opts.context, opts.worker_path, proc->GetPid());
	}

	if (opts.use_pool) {
		int exit_status = 0;
		if (!proc->TryWait(&exit_status)) {
			// Keep the drainer alive across the pool idle period.
			auto to_pool = std::make_unique<PooledWorker>(std::move(proc), pool_key, std::move(drainer));
			if (opts.enable_logging) {
				VGI_LOG(opts.context, "worker_pool.release",
				        {{"worker_path", opts.worker_path},
				         {"worker_pid", std::to_string(to_pool->GetPid())},
				         {"method_name", method_name},
				         {"phase", opts.phase}});
			}
			VgiWorkerPool::Instance().Release(std::move(to_pool));
		}
	}

	return response;
}

} // anonymous namespace

UnaryResponseResult InvokePooledUnaryRpc(const UnaryRpcOptions &opts, const std::string &method_name,
                                          const std::shared_ptr<arrow::RecordBatch> &params) {
	if (IsHttpTransport(opts.worker_path)) {
		return HttpInvokeUnary(opts.context, opts.worker_path, method_name, params, opts.auth,
		                        opts.cookie_jar);
	}

	try {
		return AttemptUnaryRpc(opts, method_name, params, /*force_fresh=*/false);
	} catch (const IOException &) {
		if (!opts.use_pool) {
			throw; // nothing to retry — not pooled
		}
		// Single retry with a fresh worker. If that also fails, propagate.
		return AttemptUnaryRpc(opts, method_name, params, /*force_fresh=*/true);
	}
}

} // namespace vgi
} // namespace duckdb
