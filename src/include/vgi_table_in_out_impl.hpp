#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"

#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_api.hpp"

namespace duckdb {

class DatabaseInstance;

namespace vgi {

// Forward declaration
class IFunctionConnection;

// ============================================================================
// Bind Data for Table-In-Out Functions
// ============================================================================

struct VgiTableInOutBindData : public TableFunctionData {
	VgiTableInOutBindData();
	~VgiTableInOutBindData() override;

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other) const override;

	// Connection parameters
	std::shared_ptr<VgiAttachParameters> attach_params;  // replaces worker_path, worker_debug, use_pool
	std::vector<uint8_t> attach_id;
	std::vector<uint8_t> transaction_id;
	std::string function_name;
	std::map<std::string, Value> settings;
	std::vector<vgi::VgiSecretRequirement> required_secrets;

	// Convenience accessors
	const std::string &worker_path() const { return attach_params->worker_path(); }
	bool worker_debug() const { return attach_params->worker_debug(); }
	bool use_pool() const { return attach_params->use_pool(); }
	const std::string &data_version_spec() const { return attach_params->data_version_spec(); }
	const std::string &implementation_version() const { return attach_params->implementation_version(); }

	// Arguments (excluding TABLE input)
	ArrowArguments arguments;

	// Output schema from bind handshake
	std::shared_ptr<arrow::Schema> output_schema;

	// Arrow C ABI schema and table schema for ArrowToDuckDB conversion
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;

	// Input schema (built from table input types/names)
	std::shared_ptr<arrow::Schema> input_schema;

	// Bind output retained for init phase. The init payload (BuildInitRequest)
	// carries bind_request_bytes + output_schema_bytes + opaque_data inline, so
	// the worker reconstructs bind context entirely from the init request — no
	// second on-wire bind needed.
	BindResult bind_result;

	// Worker capabilities
	int32_t max_processes = 1;
	int64_t cardinality_estimate = -1;
};

// ============================================================================
// Global State for Table-In-Out Functions
// ============================================================================

struct VgiTableInOutGlobalState : public GlobalTableFunctionState {
	explicit VgiTableInOutGlobalState(DatabaseInstance *db = nullptr);
	~VgiTableInOutGlobalState() override;

	// Primary connection for this execution
	std::unique_ptr<IFunctionConnection> connection;

	// BindResult from the bind RPC sent at InitGlobal time. Held here so
	// that the later PerformInit (INPUT phase, called below at InitGlobal)
	// and PerformFinalizeInit (called from VgiTableInOutFunction at the
	// end of the input stream) can both reference the same bind.
	BindResult bind_result;

	// Global execution ID for multi-worker coordination
	std::vector<uint8_t> global_execution_id;

	// Whether finalize signal has been sent to the worker
	bool finalize_sent = false;

	// Set when the stream reaches a natural end (worker returned
	// FINISHED or Finalize drained). The destructor uses this to
	// decide between cancel-dispatch and plain connection drop.
	bool stream_finished = false;

	// Captured at InitGlobal from the ClientContext setting.
	bool cancel_enabled = true;

	// Most recent HTTP stream-state token seen on exchange responses.
	// Empty for subprocess. Used to address the right worker on cancel
	// when HTTP max_workers > 1.
	std::vector<uint8_t> last_state_token;

	// DatabaseInstance used to locate the per-instance cancel
	// dispatcher. nullptr is valid (cancel dispatch is skipped).
	DatabaseInstance *db = nullptr;

	idx_t MaxThreads() const override {
		return 1; // Table-in-out functions are single-threaded for now
	}
};

// ============================================================================
// Local State for Table-In-Out Functions
// ============================================================================

struct VgiTableInOutLocalState : public ArrowScanLocalState {
	VgiTableInOutLocalState(unique_ptr<ArrowArrayWrapper> current_chunk, ClientContext &ctx)
	    : ArrowScanLocalState(std::move(current_chunk), ctx) {
	}
	~VgiTableInOutLocalState() override;
};

// ============================================================================
// Parameters for Bind Function
// ============================================================================

struct VgiTableInOutBindParams {
	std::shared_ptr<VgiAttachParameters> attach_params;  // replaces worker_path, worker_debug, use_pool
	std::string function_name;
	std::vector<uint8_t> attach_id;
	std::vector<uint8_t> transaction_id;
	std::map<std::string, Value> settings;
	std::vector<vgi::VgiSecretRequirement> required_secrets;

	// Convenience accessors
	const std::string &worker_path() const { return attach_params->worker_path(); }
	bool worker_debug() const { return attach_params->worker_debug(); }
	bool use_pool() const { return attach_params->use_pool(); }
	const std::string &data_version_spec() const { return attach_params->data_version_spec(); }
	const std::string &implementation_version() const { return attach_params->implementation_version(); }
};

// ============================================================================
// Function Implementations
// ============================================================================

// Bind function for table-in-out functions
// Receives table input schema via input.input_table_types/input_table_names
unique_ptr<FunctionData> VgiTableInOutBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names,
                                            const VgiTableInOutBindParams &params);

// Init functions
unique_ptr<GlobalTableFunctionState> VgiTableInOutInitGlobal(ClientContext &context, TableFunctionInitInput &input);
unique_ptr<LocalTableFunctionState> VgiTableInOutInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                            GlobalTableFunctionState *global_state);

// In-out function - processes input chunks and produces output chunks
OperatorResultType VgiTableInOutFunction(ExecutionContext &context, TableFunctionInput &data,
                                          DataChunk &input, DataChunk &output);

// Finalize function - called when all input has been processed
OperatorFinalizeResultType VgiTableInOutFinalize(ExecutionContext &context, TableFunctionInput &data, DataChunk &output);

} // namespace vgi
} // namespace duckdb
