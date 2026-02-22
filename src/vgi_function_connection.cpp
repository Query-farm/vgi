#include "vgi_function_connection.hpp"

#include <poll.h>

#include "duckdb.hpp"
#include "duckdb/logging/log_manager.hpp"

#include "vgi_arrow_ipc.hpp"
#include "vgi_exception.hpp"
#include "vgi_logging.hpp"
#include "vgi_rpc_client.hpp"
#include "vgi_rpc_types.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// Connection Acquisition with Retry
// ============================================================================

AcquireAndBindResult AcquireAndBindConnection(ClientContext &context, const FunctionConnectionParams &params) {
	std::unique_ptr<FunctionConnection> conn;
	BindResult bind_result;
	bool from_pool = false;

	// Lambda to create a fresh connection
	auto create_fresh_connection = [&]() {
		return make_uniq<FunctionConnection>(params.worker_path, params.function_name, params.arguments,
		                                     params.attach_id, context, params.function_type,
		                                     params.global_execution_id, params.worker_debug,
		                                     params.settings);
	};

	// Lambda to attempt bind, returns true on success, false if retry needed
	auto try_bind = [&](bool is_retry) -> bool {
		try {
			bind_result = conn->PerformBindFull();
			return true; // Success
		} catch (const IOException &e) {
			if (!is_retry && from_pool) {
				// Pooled worker was stale, signal retry needed
				VGI_LOG(context, "worker_pool.stale",
				        {{"worker_path", params.worker_path},
				         {"worker_pid", std::to_string(conn->GetPid())},
				         {"error", e.what()},
				         {"phase", params.phase}});
				return false; // Trigger retry
			}
			throw; // Fresh connection or retry failed, propagate
		}
	};

	// Try pool first
	if (params.use_pool) {
		auto pooled = VgiWorkerPool::Instance().TryAcquire(params.worker_path);
		if (pooled) {
			auto pooled_pid = pooled->GetPid();
			conn = make_uniq<FunctionConnection>(std::move(pooled), params.function_name, params.arguments,
			                                     params.attach_id, context, params.function_type,
			                                     params.global_execution_id,
			                                     params.worker_debug, params.settings);
			from_pool = true;
			VGI_LOG(context, "worker_pool.acquire",
			        {{"worker_path", params.worker_path},
			         {"worker_pid", std::to_string(pooled_pid)},
			         {"result", "hit"},
			         {"phase", params.phase}});
		}
	}

	// Create fresh if pool miss
	if (!conn) {
		conn = create_fresh_connection();
		if (params.use_pool) {
			VgiWorkerPool::Instance().RecordMiss(params.worker_path);
		}
		VGI_LOG(context, "worker_pool.acquire",
		        {{"worker_path", params.worker_path},
		         {"worker_pid", std::to_string(conn->GetPid())},
		         {"result", params.use_pool ? "miss" : "disabled"},
		         {"phase", params.phase}});
	}

	// Attempt bind with single retry for stale pool connections
	if (!try_bind(false)) {
		// Pooled worker was stale, retry with fresh
		conn = create_fresh_connection();
		VGI_LOG(context, "worker_pool.acquire",
		        {{"worker_path", params.worker_path},
		         {"worker_pid", std::to_string(conn->GetPid())},
		         {"result", "retry_after_stale"},
		         {"phase", params.phase}});
		try_bind(true); // Throws if fails, no more retries
	}

	return AcquireAndBindResult {std::move(conn), std::move(bind_result)};
}

// ============================================================================
// FunctionConnection - vgi_rpc Protocol Implementation
// ============================================================================

FunctionConnection::FunctionConnection(const std::string &worker_path, const std::string &function_name,
                                       const ArrowArguments &arguments, const std::vector<uint8_t> &attach_id,
                                       ClientContext &context, const std::string &function_type,
                                       const std::vector<uint8_t> &global_execution_id,
                                       bool worker_debug, const std::map<std::string, std::string> &settings)
    : worker_path_(worker_path), function_name_(function_name), function_type_(function_type),
      arguments_type_(arguments.type), arguments_array_(arguments.array), attach_id_(attach_id),
      global_execution_id_(global_execution_id), context_(context), worker_debug_(worker_debug), settings_(settings) {
}

FunctionConnection::FunctionConnection(std::unique_ptr<PooledWorker> pooled_worker, const std::string &function_name,
                                       const ArrowArguments &arguments, const std::vector<uint8_t> &attach_id,
                                       ClientContext &context, const std::string &function_type,
                                       const std::vector<uint8_t> &global_execution_id,
                                       bool worker_debug, const std::map<std::string, std::string> &settings)
    : worker_path_(pooled_worker->GetWorkerPath()), function_name_(function_name), function_type_(function_type),
      arguments_type_(arguments.type), arguments_array_(arguments.array), attach_id_(attach_id),
      global_execution_id_(global_execution_id), context_(context), worker_debug_(worker_debug), settings_(settings),
      proc_(pooled_worker->Release()), stderr_fd_(pooled_worker->ReleaseStderrFd()) {
	// Start stderr reader thread for the existing subprocess (reusing the fd)
	StartStderrReader();
}

FunctionConnection::~FunctionConnection() {
	// Terminate the subprocess first - this causes EOF on stderr which unblocks
	// the stderr reader thread. Without this, StopStderrReader() would block forever
	// waiting for the thread that's blocked on read().
	proc_.reset();
	// Now stop stderr reader thread (should exit quickly due to EOF)
	StopStderrReader();
	// Drain any remaining stderr (can't log without context, just discard)
	{
		std::lock_guard<std::mutex> lock(stderr_mutex_);
		stderr_lines_.clear();
	}
}

BindResult FunctionConnection::PerformBindFull() {
	if (bind_done_) {
		// Return cached results
		return bind_result_;
	}

	// Spawn the worker process (unless we already have one from the pool)
	if (!proc_) {
		proc_ = std::make_unique<SubProcess>(worker_path_, worker_debug_);
		// Start stderr reader thread to prevent pipe buffer from blocking worker
		StartStderrReader();
	}

	// Log the invocation request
	int64_t num_args = arguments_array_ ? arguments_array_->length() : 0;
	VGI_LOG(context_, "function_connection.bind",
	        {{"worker_path", worker_path_},
	         {"worker_pid", std::to_string(proc_->GetPid())},
	         {"function_name", function_name_},
	         {"function_type", function_type_},
	         {"num_args", std::to_string(num_args)}});

	// Build BindRequest using vgi_rpc types

	// 1. Convert arguments to IPC bytes
	// Python expects a single-column batch with an "args" struct column
	std::vector<uint8_t> arguments_bytes;
	if (arguments_array_) {
		// Wrap the struct array in a batch with a single "args" column
		auto args_schema = arrow::schema({arrow::field("args", arguments_array_->type())});
		auto args_batch = arrow::RecordBatch::Make(args_schema, arguments_array_->length(), {arguments_array_});
		arguments_bytes = SerializeToIpcBytes(args_batch);
	} else {
		// Empty arguments: create batch with empty struct "args" column
		auto empty_struct_type = arrow::struct_({});
		auto empty_struct_result = arrow::MakeEmptyArray(empty_struct_type);
		if (!empty_struct_result.ok()) {
			ThrowVgiIOException("Failed to create empty struct array: %s", worker_path_, proc_->GetPid(),
			                    "", empty_struct_result.status().ToString());
		}
		auto args_schema = arrow::schema({arrow::field("args", empty_struct_type)});
		auto args_batch = arrow::RecordBatch::Make(args_schema, 0, {empty_struct_result.ValueUnsafe()});
		arguments_bytes = SerializeToIpcBytes(args_batch);
	}

	// 2. Serialize settings if non-empty
	std::vector<uint8_t> settings_bytes;
	if (!settings_.empty()) {
		std::vector<std::shared_ptr<arrow::Field>> fields;
		std::vector<std::shared_ptr<arrow::Array>> arrays;
		for (const auto &[key, value] : settings_) {
			fields.push_back(arrow::field(key, arrow::utf8()));
			arrow::StringBuilder builder;
			auto status = builder.Append(value);
			if (!status.ok()) {
				ThrowVgiIOException("Failed to build settings array: %s", worker_path_, proc_->GetPid(), "",
				                    status.ToString());
			}
			auto result = builder.Finish();
			if (!result.ok()) {
				ThrowVgiIOException("Failed to finish settings array: %s", worker_path_, proc_->GetPid(), "",
				                    result.status().ToString());
			}
			arrays.push_back(result.ValueUnsafe());
		}
		auto settings_schema = arrow::schema(fields);
		auto settings_batch = arrow::RecordBatch::Make(settings_schema, 1, arrays);
		settings_bytes = SerializeToIpcBytes(settings_batch);
	}

	// 3. Serialize input_schema if set
	std::vector<uint8_t> input_schema_bytes;
	if (input_schema_) {
		input_schema_bytes = SerializeSchemaToIpcBytes(input_schema_);
	}

	// 4. Build BindRequest
	auto bind_request = BuildBindRequest(function_name_, arguments_bytes, function_type_,
	                                     input_schema_bytes, settings_bytes, attach_id_);
	auto bind_request_bytes = SerializeToIpcBytes(bind_request);

	// 5. Build RPC params and send request
	auto params = BuildBindRpcParams(bind_request_bytes);
	try {
		WriteRpcRequest(proc_->GetStdinFd(), "bind", params);
	} catch (const IOException &e) {
		CheckWorkerExitStatus(*proc_, worker_path_, "failed to start");
		throw;
	}

	// 6. Read unary response
	UnaryResponseResult response;
	try {
		response = ReadUnaryResponse(proc_->GetStdoutFd(), &context_, worker_path_, proc_->GetPid());
	} catch (const IOException &e) {
		CheckWorkerExitStatus(*proc_, worker_path_, "failed during bind");
		throw;
	}

	// 7. Extract and parse BindResponse
	if (!response.batch || response.batch->num_rows() == 0) {
		ThrowVgiIOException("Empty bind response from worker", worker_path_, proc_->GetPid(), "");
	}

	auto result_col = response.batch->GetColumnByName("result");
	if (!result_col) {
		ThrowVgiIOException("Bind response missing 'result' column", worker_path_, proc_->GetPid(), "");
	}

	auto binary_array = std::dynamic_pointer_cast<arrow::BinaryArray>(result_col);
	if (!binary_array || binary_array->IsNull(0)) {
		ThrowVgiIOException("Bind response 'result' column is null", worker_path_, proc_->GetPid(), "");
	}

	auto view = binary_array->GetView(0);
	auto bind_response_batch = DeserializeFromIpcBytes(
	    reinterpret_cast<const uint8_t *>(view.data()), view.size());
	auto bind_response = ParseBindResponse(bind_response_batch, worker_path_);

	// 8. Cache the output schema as IPC bytes for InitRequest
	auto output_schema_bytes = SerializeSchemaToIpcBytes(bind_response.output_schema);

	// 9. Store result
	bind_result_ = BindResult {
	    bind_response.output_schema,
	    bind_response.opaque_data,
	    bind_request_bytes,
	    output_schema_bytes
	};
	bind_done_ = true;

	// Drain any buffered stderr from bind phase
	DrainStderrLog();

	VGI_LOG(context_, "function_connection.bind_result",
	        {{"worker_path", worker_path_},
	         {"worker_pid", std::to_string(proc_->GetPid())},
	         {"function_name", function_name_},
	         {"num_output_columns", std::to_string(bind_response.output_schema->num_fields())},
	         {"has_opaque_data", bind_response.opaque_data.empty() ? "false" : "true"}});

	return bind_result_;
}

InitResult FunctionConnection::PerformInit(const std::vector<int32_t> &projection_ids,
                                           std::shared_ptr<arrow::Buffer> pushdown_filters,
                                           const std::string &phase) {
	if (!bind_done_) {
		ThrowVgiIOException("FunctionConnection::PerformInit called before PerformBind", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}
	if (init_done_) {
		ThrowVgiIOException("FunctionConnection::PerformInit called twice", worker_path_, proc_ ? proc_->GetPid() : -1,
		                    GetExecutionIdHex());
	}

	// Convert projection_ids to int64_t
	std::vector<int64_t> projection_ids_64;
	projection_ids_64.reserve(projection_ids.size());
	for (auto id : projection_ids) {
		projection_ids_64.push_back(static_cast<int64_t>(id));
	}

	// Serialize pushdown_filters if present
	std::vector<uint8_t> pushdown_filters_bytes;
	if (pushdown_filters) {
		pushdown_filters_bytes.assign(pushdown_filters->data(),
		                              pushdown_filters->data() + pushdown_filters->size());
	}

	// Determine execution_id: use global_execution_id_ for secondary workers
	auto &execution_id = global_execution_id_;

	// Build InitRequest
	auto init_request = BuildInitRequest(
	    bind_result_.bind_request_bytes,
	    bind_result_.output_schema_bytes,
	    bind_result_.opaque_data,
	    projection_ids_64,
	    pushdown_filters_bytes,
	    phase,
	    execution_id);
	auto init_request_bytes = SerializeToIpcBytes(init_request);

	// Build RPC params and send request
	auto rpc_params = BuildInitRpcParams(init_request_bytes);
	try {
		WriteRpcRequest(proc_->GetStdinFd(), "init", rpc_params);
	} catch (const IOException &e) {
		CheckWorkerExitStatus(*proc_, worker_path_, "failed during init request");
		throw;
	}

	// Read stream header (GlobalInitResponse)
	StreamHeaderResult header;
	try {
		header = ReadStreamHeader(proc_->GetStdoutFd(), &context_, worker_path_, proc_->GetPid());
	} catch (const IOException &e) {
		CheckWorkerExitStatus(*proc_, worker_path_, "failed during init response");
		throw;
	}

	auto init_response = ParseGlobalInitResponse(header.header_batch, worker_path_);
	execution_id_ = init_response.execution_id;

	// Open data exchange streams based on mode.
	//
	// IMPORTANT: pyarrow's ipc.new_stream() does NOT write the IPC schema to the
	// output stream immediately — it defers the schema write until the first
	// write_batch() call. This means:
	//   - C++ MakeStreamWriter writes the input schema to stdin immediately (via write() syscall)
	//   - The Python server reads the input schema and opens its output writer
	//   - But the output writer's schema is NOT written to stdout yet
	//   - The output schema only appears when the server processes the first input batch
	//     and calls write_batch(), which flushes schema + data together
	//
	// Therefore:
	//   - Producer mode: we must send the first tick batch before opening data_reader_,
	//     so the server can process it and flush the output schema to stdout.
	//   - Exchange mode: we defer opening data_reader_ to the first ReadDataBatch() call,
	//     which happens after the caller has written at least one input batch via
	//     WriteInputBatch(), giving the server data to process and flush.
	if (!input_schema_) {
		// Regular table function: producer mode (tick-based)
		is_producer_mode_ = true;
		tick_schema_ = arrow::schema({});

		// Create tick writer on stdin (C++ MakeStreamWriter writes schema immediately)
		auto sink = std::make_shared<FdOutputStream>(proc_->GetStdinFd());
		auto writer_result = arrow::ipc::MakeStreamWriter(sink, tick_schema_);
		if (!writer_result.ok()) {
			ThrowVgiIOException("Failed to create tick writer: %s", worker_path_, proc_->GetPid(),
			                    GetExecutionIdHex(), writer_result.status().ToString());
		}
		input_writer_ = writer_result.ValueUnsafe();
		input_writer_opened_ = true;

		// Send the first tick batch immediately. The server needs to receive and
		// process a tick before it calls write_batch() on the output, which is
		// what actually flushes the output IPC schema to stdout.
		auto tick_batch = arrow::RecordBatch::Make(
		    tick_schema_, 0, std::vector<std::shared_ptr<arrow::Array>>{});
		auto write_status = input_writer_->WriteRecordBatch(*tick_batch);
		if (!write_status.ok()) {
			ThrowVgiIOException("Failed to write initial tick batch: %s", worker_path_, proc_->GetPid(),
			                    GetExecutionIdHex(), write_status.ToString());
		}

		// Now open data reader on stdout — the server has processed the tick and
		// flushed the output schema + first data batch to stdout
		data_stream_ = std::make_shared<FdInputStream>(proc_->GetStdoutFd());
		auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(data_stream_);
		if (!reader_result.ok()) {
			ThrowVgiIOException("Failed to open data stream: %s", worker_path_, proc_->GetPid(),
			                    GetExecutionIdHex(), reader_result.status().ToString());
		}
		data_reader_ = reader_result.ValueUnsafe();
	} else {
		// Scalar or table-in-out function: exchange mode
		is_producer_mode_ = false;

		// Create input writer on stdin (writes schema immediately)
		auto sink = std::make_shared<FdOutputStream>(proc_->GetStdinFd());
		auto writer_result = arrow::ipc::MakeStreamWriter(sink, input_schema_);
		if (!writer_result.ok()) {
			ThrowVgiIOException("Failed to create input writer: %s", worker_path_, proc_->GetPid(),
			                    GetExecutionIdHex(), writer_result.status().ToString());
		}
		input_writer_ = writer_result.ValueUnsafe();
		input_writer_opened_ = true;

		// Do NOT open data_reader_ here. The server's output schema is only flushed
		// to stdout when it processes the first input batch and calls write_batch().
		// data_reader_ will be opened lazily in ReadDataBatch() after the caller
		// has written at least one input batch via WriteInputBatch().
	}

	init_done_ = true;

	// Drain any buffered stderr from init phase
	DrainStderrLog();

	VGI_LOG(context_, "function_connection.init_result",
	        {{"worker_path", worker_path_},
	         {"worker_pid", std::to_string(proc_->GetPid())},
	         {"function_name", function_name_},
	         {"execution_id", BytesToHex(execution_id_)},
	         {"max_workers", std::to_string(init_response.max_workers)},
	         {"is_producer_mode", is_producer_mode_ ? "true" : "false"},
	         {"phase", phase.empty() ? "default" : phase}});

	return InitResult {init_response.execution_id, init_response.max_workers, init_response.opaque_data};
}

void FunctionConnection::PerformFinalizeInit() {
	if (!init_done_) {
		ThrowVgiIOException("FunctionConnection::PerformFinalizeInit called before PerformInit", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}

	VGI_LOG(context_, "function_connection.finalize_init",
	        {{"worker_path", worker_path_},
	         {"worker_pid", std::to_string(proc_->GetPid())},
	         {"function_name", function_name_},
	         {"execution_id", GetExecutionIdHex()}});

	// Close current data exchange streams
	if (input_writer_ && !input_writer_closed_) {
		auto close_status = input_writer_->Close();
		if (!close_status.ok()) {
			ThrowVgiIOException("Failed to close input writer for finalize: %s", worker_path_,
			                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex(), close_status.ToString());
		}
	}
	input_writer_.reset();
	input_writer_opened_ = false;
	input_writer_closed_ = false;

	// If data_reader_ was never opened (e.g., DuckDB skipped calling the in-out
	// function because 0 input rows were produced), the worker's output data stream
	// from the INPUT phase still sits unconsumed on stdout. We must open and drain
	// it before sending the FINALIZE init request, otherwise ReadStreamHeader will
	// read the leftover INPUT-phase output instead of the FINALIZE response header.
	if (!data_reader_ && proc_) {
		data_stream_ = std::make_shared<FdInputStream>(proc_->GetStdoutFd());
		auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(data_stream_);
		if (reader_result.ok()) {
			data_reader_ = reader_result.ValueUnsafe();
		}
	}

	// Drain remaining output from current stream
	while (data_reader_) {
		auto drain_result = data_reader_->ReadNext();
		if (!drain_result.ok() || !drain_result.ValueUnsafe().batch) {
			break;
		}
	}
	data_reader_.reset();
	data_stream_.reset();

	// Reset init state so PerformInit can be called again
	init_done_ = false;
	data_finished_ = false;

	// Clear input_schema_ so PerformInit enters producer mode (tick-based).
	// The server's FINALIZE phase uses producer mode (no input schema), not exchange mode.
	auto saved_input_schema = input_schema_;
	auto saved_global_exec_id = global_execution_id_;
	input_schema_.reset();
	global_execution_id_ = execution_id_;  // Use the execution_id from our init

	// RAII scope guard to restore state even if PerformInit throws
	struct RestoreGuard {
		std::shared_ptr<arrow::Schema> &input_schema;
		std::vector<uint8_t> &global_exec_id;
		std::shared_ptr<arrow::Schema> saved_schema;
		std::vector<uint8_t> saved_id;
		~RestoreGuard() {
			input_schema = std::move(saved_schema);
			global_exec_id = std::move(saved_id);
		}
	} guard{input_schema_, global_execution_id_, std::move(saved_input_schema), std::move(saved_global_exec_id)};

	// Call PerformInit with phase=FINALIZE and the stored execution_id
	PerformInit({}, nullptr, "FINALIZE");
}

std::shared_ptr<arrow::RecordBatch> FunctionConnection::ReadDataBatch() {
	if (!init_done_) {
		ThrowVgiIOException("FunctionConnection::ReadDataBatch called before PerformInit", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}
	if (data_finished_) {
		return nullptr;
	}

	// Lazily open data reader if not yet opened (exchange mode defers this)
	if (!data_reader_) {
		if (!proc_) {
			ThrowVgiIOException("FunctionConnection::ReadDataBatch proc_ is null", worker_path_,
			                    -1, GetExecutionIdHex());
		}
		data_stream_ = std::make_shared<FdInputStream>(proc_->GetStdoutFd());
		auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(data_stream_);
		if (!reader_result.ok()) {
			ThrowVgiIOException("Failed to open data stream: %s", worker_path_, proc_->GetPid(),
			                    GetExecutionIdHex(), reader_result.status().ToString());
		}
		data_reader_ = reader_result.ValueUnsafe();
	}

	// For producer mode (table functions): send tick batch before reading.
	// The first tick was already sent during PerformInit to bootstrap the
	// data stream, so each ReadDataBatch sends the NEXT tick and reads the
	// response from the PREVIOUS tick — a one-ahead pipeline.
	// Send one tick OUTSIDE the loop (only once per call).
	if (is_producer_mode_ && input_writer_ && !input_writer_closed_) {
		auto tick_batch = arrow::RecordBatch::Make(tick_schema_, 0, std::vector<std::shared_ptr<arrow::Array>>{});
		auto write_status = input_writer_->WriteRecordBatch(*tick_batch);
		if (!write_status.ok()) {
			// Tick write can fail with EPIPE/broken pipe if the server has already
			// finished (e.g., finalize with no output). This is not an error — just
			// mark the writer closed and proceed to read, which will return EOS.
			input_writer_closed_ = true;
		}
	}

	// Loop to skip log batches (replaces recursive ReadDataBatch call)
	while (true) {
		// Drain any buffered stderr to logging
		DrainStderrLog();

		// Read from the data stream reader
		auto read_result = data_reader_->ReadNext();
		if (!read_result.ok()) {
			auto status = read_result.status();
			// Invalid status from IPC reader typically indicates end-of-stream
			if (status.IsInvalid()) {
				data_finished_ = true;
				return nullptr;
			}
			ThrowVgiIOException("Failed to read data batch: %s", worker_path_, proc_ ? proc_->GetPid() : -1,
			                    GetExecutionIdHex(), status.ToString());
		}
		auto result = read_result.ValueUnsafe();

		// Null batch means end of stream (EOS)
		if (!result.batch) {
			data_finished_ = true;
			return nullptr;
		}

		// Check for log/error batches via HandleBatchLogMessage
		if (HandleBatchLogMessage(result.batch, result.custom_metadata, &context_, worker_path_, proc_->GetPid(),
		                          GetExecutionIdHex(), GetAttachIdHex(), "")) {
			continue;  // Skip log batch, read next
		}

		// In the vgi_rpc protocol, EOS is signaled by the IPC stream closing (null
		// batch from ReadNext above), not by 0-row batches. 0-row batches are valid
		// responses in exchange mode (e.g., function buffered input without output).
		return result.batch;
	}
}

std::string FunctionConnection::GetExecutionIdHex() const {
	if (execution_id_.empty()) {
		return "";
	}
	return BytesToHex(execution_id_);
}

std::string FunctionConnection::GetAttachIdHex() const {
	if (attach_id_.empty()) {
		return "";
	}
	return BytesToHex(attach_id_);
}

void FunctionConnection::SetInputSchema(const std::shared_ptr<arrow::Schema> &input_schema) {
	if (bind_done_) {
		ThrowVgiIOException("FunctionConnection::SetInputSchema called after bind", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}
	input_schema_ = input_schema;
}

void FunctionConnection::OpenInputWriter() {
	if (!init_done_) {
		ThrowVgiIOException("FunctionConnection::OpenInputWriter called before PerformInit", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}
	// Input writer is now opened during PerformInit to avoid deadlock
	// (server waits for input stream schema before writing output stream)
	if (input_writer_opened_) {
		return; // Already opened during init
	}
	if (!input_schema_) {
		ThrowVgiIOException("FunctionConnection::OpenInputWriter called on non-table-in-out function", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}

	// Create an IPC stream writer for stdin
	auto sink = std::make_shared<FdOutputStream>(proc_->GetStdinFd());
	auto writer_result = arrow::ipc::MakeStreamWriter(sink, input_schema_);
	if (!writer_result.ok()) {
		ThrowVgiIOException("Failed to create input stream writer: %s", worker_path_, proc_ ? proc_->GetPid() : -1,
		                    GetExecutionIdHex(), writer_result.status().ToString());
	}
	input_writer_ = writer_result.ValueUnsafe();
	input_writer_opened_ = true;

	VGI_LOG(context_, "function_connection.input_writer_opened",
	        {{"worker_path", worker_path_},
	         {"worker_pid", std::to_string(proc_->GetPid())},
	         {"execution_id", GetExecutionIdHex()},
	         {"function_name", function_name_},
	         {"input_schema_fields", std::to_string(input_schema_->num_fields())}});
}

void FunctionConnection::WriteInputBatch(const std::shared_ptr<arrow::RecordBatch> &batch) {
	if (!input_writer_opened_) {
		ThrowVgiIOException("FunctionConnection::WriteInputBatch called before OpenInputWriter", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}
	if (input_writer_closed_) {
		ThrowVgiIOException("FunctionConnection::WriteInputBatch called after CloseInputWriter", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}

	// Write the batch (no protocol state metadata in vgi_rpc protocol)
	auto write_status = input_writer_->WriteRecordBatch(*batch);
	if (!write_status.ok()) {
		ThrowVgiIOException("Failed to write input batch: %s", worker_path_, proc_ ? proc_->GetPid() : -1,
		                    GetExecutionIdHex(), write_status.ToString());
	}

	VGI_LOG(context_, "function_connection.input_batch_written",
	        {{"worker_path", worker_path_},
	         {"worker_pid", std::to_string(proc_->GetPid())},
	         {"execution_id", GetExecutionIdHex()},
	         {"function_name", function_name_},
	         {"batch_rows", std::to_string(batch->num_rows())}});

	// Drain any buffered stderr
	DrainStderrLog();
}

void FunctionConnection::CloseInputWriter() {
	if (!input_writer_opened_) {
		ThrowVgiIOException("FunctionConnection::CloseInputWriter called before OpenInputWriter", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}
	if (input_writer_closed_) {
		// Already closed, no-op
		return;
	}

	// Close the IPC stream writer (sends end-of-stream marker)
	auto close_status = input_writer_->Close();
	if (!close_status.ok()) {
		ThrowVgiIOException("Failed to close input stream: %s", worker_path_, proc_ ? proc_->GetPid() : -1,
		                    GetExecutionIdHex(), close_status.ToString());
	}
	input_writer_.reset();
	input_writer_closed_ = true;

	VGI_LOG(context_, "function_connection.input_writer_closed",
	        {{"worker_path", worker_path_},
	         {"worker_pid", std::to_string(proc_->GetPid())},
	         {"execution_id", GetExecutionIdHex()},
	         {"function_name", function_name_}});

	// Drain any buffered stderr
	DrainStderrLog();
}

int FunctionConnection::Wait() {
	if (!proc_) {
		return 0;
	}
	return proc_->Wait();
}

bool FunctionConnection::CanBePooled() const {
	// Can only pool if:
	// 1. Data phase completed (data_finished_ is true)
	// 2. Subprocess is still alive
	if (!data_finished_ || !proc_) {
		return false;
	}
	return !proc_->TryWait(); // TryWait returns true if process exited
}

std::unique_ptr<PooledWorker> FunctionConnection::ReleaseForPooling() {
	if (!CanBePooled()) {
		return nullptr;
	}

	// Properly close the data exchange streams before releasing to the pool.
	// The input writer must be closed (writes EOS to stdin) so the server
	// exits its data loop. The data reader must be drained to EOS so the
	// stdout pipe is clean for the next operation.
	if (input_writer_ && !input_writer_closed_) {
		auto close_status = input_writer_->Close();
		// Ignore close errors during cleanup (worker might have died)
		(void)close_status;
		input_writer_closed_ = true;
	}
	if (data_reader_) {
		// Drain remaining output to EOS
		while (true) {
			auto drain_result = data_reader_->ReadNext();
			if (!drain_result.ok() || !drain_result.ValueUnsafe().batch) {
				break;
			}
		}
	}
	input_writer_.reset();
	data_reader_.reset();
	data_stream_.reset();

	// Stop stderr thread but keep the fd open for reuse
	StopStderrReader(false);

	// Create pooled worker with our subprocess and stderr fd
	auto pooled = std::make_unique<PooledWorker>(std::move(proc_), worker_path_, stderr_fd_);
	stderr_fd_ = -1; // Ownership transferred to pooled worker

	// Clear our state so destructor doesn't try to use proc_
	proc_.reset();

	return pooled;
}

void FunctionConnection::StartStderrReader() {
	// If we don't have an fd yet, try to get one from the subprocess
	if (stderr_fd_ < 0) {
		if (!proc_ || proc_->GetStderrFd() < 0) {
			return; // No stderr to read (passthrough mode or no process)
		}
		stderr_fd_ = proc_->ReleaseStderrFd();
	}

	// Reset stop flag for new thread
	stderr_stop_.store(false, std::memory_order_relaxed);

	stderr_thread_ = std::thread([this]() {
		char buffer[4096];
		std::string line_buffer;

		struct pollfd pfd;
		pfd.fd = stderr_fd_;
		pfd.events = POLLIN;

		while (!stderr_stop_.load(std::memory_order_relaxed)) {
			// Use poll() with timeout to allow periodic checking of stderr_stop_
			// This avoids blocking indefinitely on read() which could cause
			// the destructor to hang waiting for the thread to join
			int poll_result = poll(&pfd, 1, 100); // 100ms timeout
			if (poll_result < 0) {
				if (errno == EINTR) {
					continue; // Interrupted by signal, retry
				}
				break; // Error
			}
			if (poll_result == 0) {
				continue; // Timeout, check stop flag and poll again
			}

			// Data available or hangup - read what's available
			ssize_t bytes_read = read(stderr_fd_, buffer, sizeof(buffer) - 1);
			if (bytes_read <= 0) {
				break; // EOF or error
			}
			buffer[bytes_read] = '\0';

			// Append to line buffer and process complete lines
			line_buffer.append(buffer, bytes_read);
			size_t pos;
			while ((pos = line_buffer.find('\n')) != std::string::npos) {
				std::string line = line_buffer.substr(0, pos);
				line_buffer.erase(0, pos + 1);

				// Trim trailing \r if present
				if (!line.empty() && line.back() == '\r') {
					line.pop_back();
				}
				if (!line.empty()) {
					std::lock_guard<std::mutex> lock(stderr_mutex_);
					stderr_lines_.push_back(std::move(line));
				}
			}
		}

		// Buffer any remaining partial line
		if (!line_buffer.empty()) {
			std::lock_guard<std::mutex> lock(stderr_mutex_);
			stderr_lines_.push_back(std::move(line_buffer));
		}

		// Note: fd is NOT closed here - it's owned by the class and closed in StopStderrReader(true)
	});
}

void FunctionConnection::StopStderrReader(bool close_fd) {
	stderr_stop_.store(true, std::memory_order_relaxed);
	if (stderr_thread_.joinable()) {
		stderr_thread_.join();
	}
	if (close_fd && stderr_fd_ >= 0) {
		close(stderr_fd_);
		stderr_fd_ = -1;
	}
}

void FunctionConnection::DrainStderrLog() {
	std::vector<std::string> lines;
	{
		std::lock_guard<std::mutex> lock(stderr_mutex_);
		lines.swap(stderr_lines_);
	}

	pid_t worker_pid = proc_ ? proc_->GetPid() : -1;
	for (const auto &line : lines) {
		VGI_LOG(context_, "worker.stderr",
		        {{"worker_path", worker_path_}, {"worker_pid", std::to_string(worker_pid)}, {"message", line}});
	}
}

} // namespace vgi
} // namespace duckdb
