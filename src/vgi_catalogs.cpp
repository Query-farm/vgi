#include "vgi_catalogs.hpp"
#include "vgi_arrow_ipc.hpp"
#include "vgi_logging.hpp"
#include "vgi_protocol.hpp"
#include "vgi_subprocess.hpp"

#include <cerrno>
#include <chrono>
#include <cstring>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/logging/logging.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

namespace {

// Helper to log a single line from stderr
void LogStderrLine(ClientContext &context, const std::string &worker_path, pid_t worker_pid, const std::string &line) {
	if (!line.empty()) {
		DUCKDB_LOG(context, VgiLogType, "worker.stderr",
		           {{"worker_path", worker_path},
		            {"worker_pid", std::to_string(worker_pid)},
		            {"message", line}});
	}
}

// Stderr reader that runs on a separate thread.
// Reads lines from stderr_fd until EOF and logs each line.
// This prevents the worker from blocking if it writes a lot to stderr.
class StderrReader {
public:
	StderrReader(ClientContext &context, int stderr_fd, const std::string &worker_path, pid_t worker_pid)
	    : context_(context), stderr_fd_(stderr_fd), worker_path_(worker_path), worker_pid_(worker_pid) {
		try {
			thread_ = std::thread(&StderrReader::Run, this);
		} catch (...) {
			// If thread creation fails, close the fd we took ownership of
			if (stderr_fd_ >= 0) {
				close(stderr_fd_);
				stderr_fd_ = -1;
			}
			throw;
		}
	}

	~StderrReader() {
		JoinWithTimeout(std::chrono::seconds(5));
	}

	// Join with a timeout. Returns true if thread finished, false if timed out.
	// Note: True timeout isn't possible with std::thread, so we close the fd
	// to unblock any pending read, then join.
	bool JoinWithTimeout(std::chrono::seconds timeout) {
		if (!thread_.joinable()) {
			return true;
		}

		auto start = std::chrono::steady_clock::now();

		// Close the fd to unblock any pending read
		if (stderr_fd_ >= 0) {
			close(stderr_fd_);
			stderr_fd_ = -1;
		}

		thread_.join();

		auto elapsed = std::chrono::steady_clock::now() - start;
		return elapsed < timeout;
	}

	// Non-copyable, non-movable
	StderrReader(const StderrReader &) = delete;
	StderrReader &operator=(const StderrReader &) = delete;
	StderrReader(StderrReader &&) = delete;
	StderrReader &operator=(StderrReader &&) = delete;

private:
	void Run() {
		std::string buffer;
		char read_buf[4096];

		while (true) {
			ssize_t bytes_read;
			while (true) {
				bytes_read = read(stderr_fd_, read_buf, sizeof(read_buf) - 1);
				if (bytes_read >= 0) {
					break;
				}
				if (errno == EINTR) {
					continue; // Interrupted by signal, retry
				}
				// Real error (including EBADF if fd was closed)
				bytes_read = 0;
				break;
			}

			if (bytes_read <= 0) {
				break; // EOF or error
			}

			read_buf[bytes_read] = '\0';
			buffer += read_buf;

			// Process complete lines
			size_t pos = 0;
			size_t newline_pos;
			while ((newline_pos = buffer.find('\n', pos)) != std::string::npos) {
				std::string line = buffer.substr(pos, newline_pos - pos);
				LogStderrLine(context_, worker_path_, worker_pid_, line);
				pos = newline_pos + 1;
			}

			// Keep any incomplete line in the buffer (erase processed content in-place)
			if (pos > 0) {
				buffer.erase(0, pos);
			}
		}

		// Log any remaining content without a trailing newline
		if (!buffer.empty()) {
			LogStderrLine(context_, worker_path_, worker_pid_, buffer);
		}
	}

	ClientContext &context_;
	int stderr_fd_;
	std::string worker_path_;
	pid_t worker_pid_;
	std::thread thread_;
};

// Call the VGI worker to get catalog names
std::vector<std::string> GetCatalogsFromWorker(ClientContext &context, const std::string &worker_path) {
	auto start_time = std::chrono::steady_clock::now();

	try {
		// Spawn the worker process
		vgi::SubProcess proc(worker_path);

		DUCKDB_LOG(context, VgiLogType, "catalog.start",
		           {{"worker_path", worker_path}, {"method", "catalogs"}, {"worker_pid", std::to_string(proc.GetPid())}});

		// Start reading stderr on a separate thread to prevent blocking
		// if the worker writes a lot to stderr while we're reading stdout.
		// Transfer ownership of stderr_fd to the reader (it will close it).
		StderrReader stderr_reader(context, proc.ReleaseStderrFd(), worker_path, proc.GetPid());

		// Create and send the invocation
		auto invocation = vgi::CreateCatalogInvocation("catalogs");
		auto invocation_bytes = vgi::SerializeRecordBatch(invocation);
		vgi::WriteAll(proc.GetStdinFd(), invocation_bytes->data(), invocation_bytes->size());

		// Create and send the empty arguments batch
		auto args = vgi::CreateEmptyArgsBatch();
		auto args_bytes = vgi::SerializeRecordBatch(args);
		vgi::WriteAll(proc.GetStdinFd(), args_bytes->data(), args_bytes->size());

		// Close stdin to signal end of input
		proc.CloseStdin();

		// Read the result batch
		auto result_batch = vgi::ReadRecordBatch(proc.GetStdoutFd());

		// Extract catalog names from the "value" column
		auto catalogs = vgi::ExtractStringColumn(result_batch, "value");

		// Wait for stderr reader to finish (will happen when worker closes stderr/exits)
		// Use timeout to avoid hanging if worker doesn't close stderr properly
		stderr_reader.JoinWithTimeout(std::chrono::seconds(5));

		// Wait for the worker process to exit and check its status
		// Save pid before Wait() since it clears the pid
		auto worker_pid = proc.GetPid();
		bool exited_normally = false;
		int exit_status = proc.Wait(&exited_normally);

		auto end_time = std::chrono::steady_clock::now();
		auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

		DUCKDB_LOG(context, VgiLogType, "catalog.complete",
		           {{"worker_path", worker_path},
		            {"method", "catalogs"},
		            {"worker_pid", std::to_string(worker_pid)},
		            {"num_catalogs", std::to_string(catalogs.size())},
		            {"bytes_sent", std::to_string(invocation_bytes->size() + args_bytes->size())},
		            {"duration_ms", std::to_string(duration_ms)},
		            {"exit_status", std::to_string(exit_status)},
		            {"exited_normally", exited_normally ? "true" : "false"}});

		// If the worker exited abnormally or with an error, throw an exception
		if (!exited_normally) {
			throw IOException("VGI worker '%s' was killed by signal %d", worker_path, exit_status);
		}
		if (exit_status != 0) {
			throw IOException("VGI worker '%s' exited with status %d", worker_path, exit_status);
		}

		return catalogs;
	} catch (const Exception &e) {
		throw IOException("VGI worker '%s' failed: %s", worker_path, e.what());
	} catch (const std::exception &e) {
		throw IOException("VGI worker '%s' failed: %s", worker_path, e.what());
	}
}

// Bind data for the vgi_catalogs function
struct VgiCatalogsBindData : public TableFunctionData {
	std::string worker_path;
};

// Global state for the vgi_catalogs function
struct VgiCatalogsGlobalState : public GlobalTableFunctionState {
	std::vector<std::string> catalogs;
	idx_t current_idx = 0;
	bool done = false;

	idx_t MaxThreads() const override {
		return 1;
	}
};

// Bind function - sets up return types and extracts worker path
unique_ptr<FunctionData> VgiCatalogsBind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<VgiCatalogsBindData>();

	// Get the worker path from the first argument
	bind_data->worker_path = input.inputs[0].ToString();

	// Return type is a single column "catalog" of type VARCHAR
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("catalog");

	return bind_data;
}

// Init function - spawns worker and gets catalog list
unique_ptr<GlobalTableFunctionState> VgiCatalogsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<VgiCatalogsBindData>();
	auto state = make_uniq<VgiCatalogsGlobalState>();

	// Get catalogs from the worker
	state->catalogs = GetCatalogsFromWorker(context, bind_data.worker_path);

	return state;
}

// Main function - outputs catalog rows
void VgiCatalogsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<VgiCatalogsGlobalState>();

	if (state.done) {
		output.SetCardinality(0);
		return;
	}

	idx_t count = 0;
	auto max_count = STANDARD_VECTOR_SIZE;

	while (state.current_idx < state.catalogs.size() && count < max_count) {
		auto &catalog_name = state.catalogs[state.current_idx];
		output.data[0].SetValue(count, Value(catalog_name));
		count++;
		state.current_idx++;
	}

	if (state.current_idx >= state.catalogs.size()) {
		state.done = true;
	}

	output.SetCardinality(count);
}

} // anonymous namespace

void RegisterVgiCatalogsFunction(ExtensionLoader &loader) {
	auto vgi_catalogs_function = TableFunction("vgi_catalogs", {LogicalType::VARCHAR}, VgiCatalogsFunction,
	                                           VgiCatalogsBind, VgiCatalogsInit);

	loader.RegisterFunction(vgi_catalogs_function);
}

} // namespace duckdb
