#include "vgi_function_connection.hpp"

#include "duckdb.hpp"
#include "duckdb/logging/log_manager.hpp"

#include "vgi_arrow_ipc.hpp"
#include "vgi_bind_protocol.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_exception.hpp"
#include "vgi_http_function_connection.hpp"
#include "vgi_logging.hpp"
#include "vgi_rpc_client.hpp"
#include "vgi_rpc_types.hpp"
#include "vgi_schema_registry.hpp"
#include "vgi_transport.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// FunctionConnectionParams Accessors (out-of-line, needs VgiAttachParameters)
// ============================================================================

const std::string &FunctionConnectionParams::worker_path() const { return attach_params->worker_path(); }
bool FunctionConnectionParams::worker_debug() const { return attach_params->worker_debug(); }
bool FunctionConnectionParams::use_pool() const { return attach_params->use_pool(); }
const std::string &FunctionConnectionParams::data_version_spec() const {
	return attach_params->data_version_spec();
}
const std::string &FunctionConnectionParams::implementation_version() const {
	return attach_params->implementation_version();
}

// ============================================================================
// Connection Acquisition with Retry
// ============================================================================

AcquireAndBindResult AcquireAndBindConnection(ClientContext &context, const FunctionConnectionParams &params) {
	std::unique_ptr<IFunctionConnection> conn;
	BindResult bind_result;
	bool from_pool = false;

	// Lambda to create a fresh connection (uses factory for HTTP/subprocess dispatch)
	auto create_fresh_connection = [&]() {
		return CreateFunctionConnection(params.worker_path(), params.function_name, params.arguments,
		                                params.attach_id, params.transaction_id, context,
		                                params.function_type, params.global_execution_id,
		                                params.worker_debug(), params.settings, params.required_secrets,
		                                params.attach_params);
	};

	// Lambda to attempt bind, returns true on success, false if retry needed
	auto try_bind = [&](bool is_retry) -> bool {
		try {
			if (params.input_schema) {
				conn->SetInputSchema(params.input_schema);
			}
			bind_result = conn->PerformBindFull();
			return true; // Success
		} catch (const IOException &e) {
			if (!is_retry && from_pool) {
				// Pooled worker was stale, signal retry needed
				auto fields = BuildConnLogFields(*conn);
				fields.emplace_back("error", e.what());
				fields.emplace_back("phase", params.phase);
				VGI_LOG(context, "worker_pool.stale", fields);
				return false; // Trigger retry
			}
			throw; // Fresh connection or retry failed, propagate
		}
	};

	// Try pool first (only for subprocess transport)
	if (params.use_pool() && !IsHttpTransport(params.worker_path())) {
		PoolKey pool_key {params.worker_path(), params.data_version_spec(), params.implementation_version()};
		auto pooled = VgiWorkerPool::Instance().TryAcquire(pool_key);
		if (pooled) {
			conn = CreateFunctionConnectionFromPool(std::move(pooled), params.function_name, params.arguments,
			                                        params.attach_id, params.transaction_id, context,
			                                        params.function_type, params.global_execution_id,
			                                        params.worker_debug(), params.settings,
			                                        params.required_secrets);
			from_pool = true;
			auto fields = BuildConnLogFields(*conn);
			fields.emplace_back("worker_path", params.worker_path());
			fields.emplace_back("result", "hit");
			fields.emplace_back("phase", params.phase);
			VGI_LOG(context, "worker_pool.acquire", fields);
		}
	}

	// Create fresh if pool miss
	if (!conn) {
		conn = create_fresh_connection();
		if (params.use_pool() && !IsHttpTransport(params.worker_path())) {
			VgiWorkerPool::Instance().RecordMiss(params.worker_path());
		}
		auto fields = BuildConnLogFields(*conn);
		fields.emplace_back("worker_path", params.worker_path());
		fields.emplace_back("result", params.use_pool() ? "miss" : "disabled");
		fields.emplace_back("phase", params.phase);
		VGI_LOG(context, "worker_pool.acquire", fields);
	}

	// Attempt bind with single retry for stale pool connections
	if (!try_bind(false)) {
		// Pooled worker was stale, retry with fresh
		conn = create_fresh_connection();
		auto fields = BuildConnLogFields(*conn);
		fields.emplace_back("worker_path", params.worker_path());
		fields.emplace_back("result", "retry_after_stale");
		fields.emplace_back("phase", params.phase);
		VGI_LOG(context, "worker_pool.acquire", fields);
		try_bind(true); // Throws if fails, no more retries
	}

	return AcquireAndBindResult {std::move(conn), std::move(bind_result)};
}

// ============================================================================
// FunctionConnection - vgi_rpc Protocol Implementation
// ============================================================================

FunctionConnection::FunctionConnection(const std::string &worker_path, const std::string &function_name,
                                       const ArrowArguments &arguments, const std::vector<uint8_t> &attach_id,
                                       const std::vector<uint8_t> &transaction_id,
                                       ClientContext &context, const std::string &function_type,
                                       const std::vector<uint8_t> &global_execution_id,
                                       bool worker_debug, const std::map<std::string, Value> &settings,
                                       const std::vector<VgiSecretRequirement> &required_secrets,
                                       const std::string &data_version_spec,
                                       const std::string &implementation_version)
    : conn_id_hex_(VgiGenerateConnId()), worker_path_(worker_path), data_version_spec_(data_version_spec),
      implementation_version_(implementation_version), function_name_(function_name), function_type_(function_type),
      arguments_type_(arguments.type), arguments_array_(arguments.array), attach_id_(attach_id),
      transaction_id_(transaction_id), global_execution_id_(global_execution_id), context_(context),
      worker_debug_(worker_debug), settings_(settings), required_secrets_(required_secrets) {
}

FunctionConnection::FunctionConnection(std::unique_ptr<PooledWorker> pooled_worker, const std::string &function_name,
                                       const ArrowArguments &arguments, const std::vector<uint8_t> &attach_id,
                                       const std::vector<uint8_t> &transaction_id,
                                       ClientContext &context, const std::string &function_type,
                                       const std::vector<uint8_t> &global_execution_id,
                                       bool worker_debug, const std::map<std::string, Value> &settings,
                                       const std::vector<VgiSecretRequirement> &required_secrets)
    : conn_id_hex_(VgiGenerateConnId()),
      worker_path_(pooled_worker->GetKey().worker_path),
      data_version_spec_(pooled_worker->GetKey().data_version_spec),
      implementation_version_(pooled_worker->GetKey().implementation_version),
      function_name_(function_name), function_type_(function_type),
      arguments_type_(arguments.type), arguments_array_(arguments.array), attach_id_(attach_id),
      transaction_id_(transaction_id), global_execution_id_(global_execution_id), context_(context),
      worker_debug_(worker_debug), settings_(settings), required_secrets_(required_secrets),
      proc_(pooled_worker->Release()) {
	// Adopt the drainer that was kept running while the worker was idle
	// in the pool. Falling back to a fresh drainer is unexpected (pool
	// invariant: workers always carry a drainer), but cheap if needed.
	stderr_drainer_ = pooled_worker->ReleaseDrainer();
	if (!stderr_drainer_ && proc_ && proc_->GetStderrFd() >= 0) {
		stderr_drainer_ = std::make_unique<StderrDrainer>(proc_->ReleaseStderrFd());
	}
}

FunctionConnection::~FunctionConnection() {
	// Terminate the subprocess first — EOF on stderr unblocks the drainer's
	// blocking read(), so the drainer's destructor can join its thread quickly.
	proc_.reset();
	stderr_drainer_.reset();
}

BindResult FunctionConnection::PerformBindFull() {
	if (bind_done_) {
		return bind_result_;
	}

	// Spawn the worker process (unless we already have one from the pool)
	if (!proc_) {
		proc_ = std::make_unique<SubProcess>(worker_path_, worker_debug_);
		StartStderrReader();
	}

	int64_t num_args = arguments_array_ ? arguments_array_->length() : 0;
	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		fields.emplace_back("function_type", function_type_);
		fields.emplace_back("num_args", std::to_string(num_args));
		VGI_LOG(context_, "function_connection.bind", fields);
	}

	// Transport: send bind via subprocess stdin/stdout
	auto transport_fn = [&](const std::vector<uint8_t> &request_bytes) -> std::shared_ptr<arrow::RecordBatch> {
		auto rpc_params = BuildBindRpcParams(request_bytes);
		ValidateRequestSchema(rpc_params, "bind", worker_path_);
		try {
			WriteRpcRequest(proc_->GetStdinFd(), "bind", rpc_params);
		} catch (const IOException &e) {
			CheckWorkerExitStatus(*proc_, worker_path_, "failed to start");
			throw;
		}

		UnaryResponseResult response;
		try {
			response = ReadUnaryResponse(proc_->GetStdoutFd(), &context_, worker_path_, proc_->GetPid());
		} catch (const IOException &e) {
			CheckWorkerExitStatus(*proc_, worker_path_, "failed during bind");
			throw;
		}

		if (!response.batch || response.batch->num_rows() == 0) {
			ThrowVgiIOException("Empty bind response from worker", worker_path_, proc_->GetPid(), "");
		}

		auto result_col = response.batch->GetColumnByName("result");
		if (!result_col) {
			ThrowVgiIOException("Bind response missing 'result' column", worker_path_, proc_->GetPid(), "");
		}

		auto bin_array = std::dynamic_pointer_cast<arrow::BinaryArray>(result_col);
		if (!bin_array || bin_array->IsNull(0)) {
			ThrowVgiIOException("Bind response 'result' column is null", worker_path_, proc_->GetPid(), "");
		}

		auto v = bin_array->GetView(0);
		auto bind_batch = DeserializeFromIpcBytes(reinterpret_cast<const uint8_t *>(v.data()), v.size());
		ValidateResponseSchema(bind_batch, "bind", worker_path_);
		return bind_batch;
	};

	bind_result_ = PerformBindProtocol(context_, function_name_, function_type_,
	                                    arguments_array_, input_schema_, attach_id_,
	                                    transaction_id_, settings_, required_secrets_,
	                                    worker_path_, transport_fn);
	bind_done_ = true;

	DrainStderrLog();

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		fields.emplace_back("num_output_columns", std::to_string(bind_result_.output_schema->num_fields()));
		fields.emplace_back("has_opaque_data", bind_result_.opaque_data.empty() ? "false" : "true");
		VGI_LOG(context_, "function_connection.bind_result", fields);
	}

	return bind_result_;
}

InitResult FunctionConnection::PerformInit(const std::vector<int32_t> &projection_ids,
                                           std::shared_ptr<arrow::Buffer> pushdown_filters,
                                           std::vector<std::shared_ptr<arrow::Buffer>> join_keys,
                                           const std::string &phase,
                                           const std::optional<OrderByHint> &order_by,
                                           const std::optional<TableSampleHint> &table_sample) {
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

	// Determine execution_id: use global_execution_id_ for secondary workers
	auto &execution_id = global_execution_id_;

	// Extract order hint fields (empty strings / -1 when no hint)
	std::string ob_col, ob_dir, ob_null;
	int64_t ob_limit = -1;
	if (order_by.has_value()) {
		ob_col = order_by->column_name;
		ob_dir = order_by->direction;
		ob_null = order_by->null_order;
		ob_limit = order_by->row_limit;
	}

	// Extract table sample hint fields (-1.0 / -1 when no hint)
	double ts_percentage = -1.0;
	int64_t ts_seed = -1;
	if (table_sample.has_value()) {
		ts_percentage = table_sample->sample_percentage;
		ts_seed = table_sample->seed;
	}

	// Build InitRequest — pass arrow::Buffer directly to avoid copying
	auto init_request = BuildInitRequest(
	    bind_result_.bind_request_bytes,
	    bind_result_.output_schema_bytes,
	    bind_result_.opaque_data,
	    projection_ids_64,
	    pushdown_filters,
	    join_keys,
	    phase,
	    execution_id,
	    {},  // init_opaque_data
	    ob_col, ob_dir, ob_null, ob_limit,
	    ts_percentage, ts_seed);
	auto init_request_bytes = SerializeToIpcBytes(init_request);

	// Build RPC params and send request
	auto rpc_params = BuildInitRpcParams(init_request_bytes);
	ValidateRequestSchema(rpc_params, "init", worker_path_);
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

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		fields.emplace_back("max_workers", std::to_string(init_response.max_workers));
		fields.emplace_back("is_producer_mode", is_producer_mode_ ? "true" : "false");
		fields.emplace_back("phase", phase.empty() ? "default" : phase);
		VGI_LOG(context_, "function_connection.init_result", fields);
	}

	return InitResult {init_response.execution_id, init_response.max_workers, init_response.opaque_data};
}

void FunctionConnection::PerformFinalizeInit() {
	if (!init_done_) {
		ThrowVgiIOException("FunctionConnection::PerformFinalizeInit called before PerformInit", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		VGI_LOG(context_, "function_connection.finalize_init", fields);
	}

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
	PerformInit({}, nullptr, {}, "FINALIZE");
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

		// Attach dynamic filter metadata as IPC custom metadata if available
		std::shared_ptr<const arrow::KeyValueMetadata> tick_metadata;
		if (tick_filter_state_) {
			lock_guard<mutex> l(tick_filter_state_->lock);
			if (tick_filter_state_->has_filters) {
				tick_metadata = arrow::KeyValueMetadata::Make(
				    {"vgi_pushdown_filters"}, {tick_filter_state_->encoded_filters});
			}
		}

		auto write_status = tick_metadata
		    ? input_writer_->WriteRecordBatch(*tick_batch, tick_metadata)
		    : input_writer_->WriteRecordBatch(*tick_batch);
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

std::string FunctionConnection::GetTransactionIdHex() const {
	if (transaction_id_.empty()) {
		return "";
	}
	return BytesToHex(transaction_id_);
}

void FunctionConnection::SetInputSchema(const std::shared_ptr<arrow::Schema> &input_schema) {
	if (bind_done_) {
		ThrowVgiIOException("FunctionConnection::SetInputSchema called after bind", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}
	input_schema_ = input_schema;
}

void FunctionConnection::UpdateInputSchemaForExecution(const std::shared_ptr<arrow::Schema> &input_schema) {
	// Allow updating input schema after bind but before OpenInputWriter.
	// Used when reusing a bind connection and the actual DataChunk types
	// differ from the bind-time expression types (e.g., DECIMAL vs DOUBLE).
	if (input_writer_opened_) {
		ThrowVgiIOException("FunctionConnection::UpdateInputSchemaForExecution called after OpenInputWriter",
		                    worker_path_, proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
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

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		fields.emplace_back("input_schema_fields", std::to_string(input_schema_->num_fields()));
		VGI_LOG(context_, "function_connection.input_writer_opened", fields);
	}
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

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		fields.emplace_back("batch_rows", std::to_string(batch->num_rows()));
		VGI_LOG(context_, "function_connection.input_batch_written", fields);
	}

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

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		VGI_LOG(context_, "function_connection.input_writer_closed", fields);
	}

	// Drain any buffered stderr
	DrainStderrLog();
}

int FunctionConnection::Wait() {
	if (!proc_) {
		return 0;
	}
	return proc_->Wait();
}

std::unique_ptr<PooledWorker> FunctionConnection::ReleaseForPooling() {
	// The worker can be handed back to the pool only if it's parked at its
	// RPC accept-loop and the process is still alive. A streaming data
	// phase is in-flight iff init was sent and we haven't seen EOS. Every
	// other state — never-bound, post-bind pre-init, or post-EOS — means
	// the worker has looped back and is ready to accept another RPC.
	if (!proc_ || proc_->TryWait()) {
		return nullptr; // process missing or exited
	}
	bool streaming_in_flight = init_done_ && !data_finished_;
	if (streaming_in_flight) {
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

	// Hand the (still-running) drainer off to the pool so it keeps consuming
	// stderr while the worker is idle. This prevents the Python worker from
	// blocking on a full stderr pipe buffer between RPCs.
	auto drainer = std::move(stderr_drainer_);

	// Create pooled worker with our subprocess and the live drainer.
	PoolKey pool_key {worker_path_, data_version_spec_, implementation_version_};
	auto pooled = std::make_unique<PooledWorker>(std::move(proc_), pool_key, std::move(drainer));

	// Clear our state so destructor doesn't try to use proc_
	proc_.reset();

	return pooled;
}

void FunctionConnection::StartStderrReader() {
	if (stderr_drainer_) {
		return; // Already running
	}
	if (!proc_ || proc_->GetStderrFd() < 0) {
		return; // No stderr pipe (passthrough mode or no process)
	}
	stderr_drainer_ = std::make_unique<StderrDrainer>(proc_->ReleaseStderrFd());
}

int FunctionConnection::ReleaseStderrReaderFd() {
	if (!stderr_drainer_) {
		return -1;
	}
	int fd = stderr_drainer_->ReleaseFd();
	stderr_drainer_.reset();
	return fd;
}

void FunctionConnection::StopStderrReader() {
	// Dropping the drainer joins the thread and closes the fd.
	stderr_drainer_.reset();
}

void FunctionConnection::DrainStderrLog() {
	if (!stderr_drainer_) {
		return;
	}
	stderr_drainer_->DrainToLog(context_, worker_path_, proc_ ? proc_->GetPid() : -1);
}

// ============================================================================
// Factory Functions
// ============================================================================

std::unique_ptr<IFunctionConnection> CreateFunctionConnection(
    const std::string &worker_path, const std::string &function_name,
    const ArrowArguments &arguments, const std::vector<uint8_t> &attach_id,
    const std::vector<uint8_t> &transaction_id,
    ClientContext &context, const std::string &function_type,
    const std::vector<uint8_t> &global_execution_id,
    bool worker_debug,
    const std::map<std::string, Value> &settings,
    const std::vector<VgiSecretRequirement> &required_secrets,
    const std::shared_ptr<VgiAttachParameters> &attach_params) {
	if (IsHttpTransport(worker_path)) {
		return std::make_unique<HttpFunctionConnection>(
		    worker_path, function_name, arguments, attach_id, transaction_id, context,
		    function_type, global_execution_id, worker_debug, settings, required_secrets,
		    attach_params);
	}
	// Forward version-key fields so ReleaseForPooling routes this worker back
	// to the same pool bucket on next release.
	std::string data_version_spec;
	std::string implementation_version;
	if (attach_params) {
		data_version_spec = attach_params->data_version_spec();
		implementation_version = attach_params->implementation_version();
	}
	return std::make_unique<FunctionConnection>(
	    worker_path, function_name, arguments, attach_id, transaction_id, context,
	    function_type, global_execution_id, worker_debug, settings, required_secrets,
	    data_version_spec, implementation_version);
}

std::unique_ptr<IFunctionConnection> CreateFunctionConnectionFromPool(
    std::unique_ptr<PooledWorker> pooled_worker, const std::string &function_name,
    const ArrowArguments &arguments, const std::vector<uint8_t> &attach_id,
    const std::vector<uint8_t> &transaction_id,
    ClientContext &context, const std::string &function_type,
    const std::vector<uint8_t> &global_execution_id,
    bool worker_debug,
    const std::map<std::string, Value> &settings,
    const std::vector<VgiSecretRequirement> &required_secrets) {
	// Only subprocess connections use the pool
	return std::make_unique<FunctionConnection>(
	    std::move(pooled_worker), function_name, arguments, attach_id, transaction_id, context,
	    function_type, global_execution_id, worker_debug, settings, required_secrets);
}

} // namespace vgi
} // namespace duckdb
