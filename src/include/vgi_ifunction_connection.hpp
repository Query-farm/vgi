#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include <sys/types.h>

#include "duckdb/common/types/value.hpp"

#include "vgi_arrow_utils.hpp"
#include "vgi_protocol.hpp"

namespace duckdb {

class ClientContext;

namespace vgi {

class PooledWorker;
struct VgiSecretRequirement;

// ============================================================================
// IFunctionConnection - Abstract interface for VGI function connections
// ============================================================================
// Both subprocess (FunctionConnection) and HTTP (HttpFunctionConnection) implement
// this interface. All call sites store unique_ptr<IFunctionConnection>.

// Shared state for dynamic filter values that tighten during execution.
// Written by the table scan (after Top-N sink updates), read by the connection (before tick).
struct TickFilterState {
	mutex lock;
	//! Base64-encoded Arrow IPC bytes of the complete filter set (static + dynamic merged)
	string encoded_filters;
	//! True if encoded_filters has been set at least once
	bool has_filters = false;
};

class IFunctionConnection {
public:
	virtual ~IFunctionConnection() = default;

	// Tick metadata: set shared state for dynamic filter pushdown via tick batches.
	// The connection reads this before each tick and includes it as custom metadata.
	virtual void SetTickFilterState(shared_ptr<TickFilterState> state) = 0;

	// Phase 1: Bind
	virtual BindResult PerformBindFull() = 0;
	virtual void SetInputSchema(const std::shared_ptr<arrow::Schema> &input_schema) = 0;

	// Update input schema for execute phase (after bind, before OpenInputWriter)
	// Used when reusing bind connection and actual DataChunk types differ from bind types
	virtual void UpdateInputSchemaForExecution(const std::shared_ptr<arrow::Schema> &input_schema) = 0;

	// Phase 2: Init
	virtual InitResult PerformInit(const std::vector<int32_t> &projection_ids = {},
	                               std::shared_ptr<arrow::Buffer> pushdown_filters = nullptr,
	                               const std::string &phase = "") = 0;
	virtual void PerformFinalizeInit() = 0;

	// Phase 3: Data exchange
	virtual void OpenInputWriter() = 0;
	virtual void WriteInputBatch(const std::shared_ptr<arrow::RecordBatch> &batch) = 0;
	virtual void CloseInputWriter() = 0;
	virtual std::shared_ptr<arrow::RecordBatch> ReadDataBatch() = 0;

	// State queries
	virtual bool IsTableInOut() const = 0;
	virtual bool IsFinished() const = 0;
	virtual void MarkDataFinished() = 0;

	// Identity/diagnostics
	virtual pid_t GetPid() const = 0;
	virtual std::string GetExecutionIdHex() const = 0;
	virtual std::string GetAttachIdHex() const = 0;

	// Lifecycle
	virtual int Wait() = 0;
	virtual bool CanBePooled() const = 0;
	virtual std::unique_ptr<PooledWorker> ReleaseForPooling() = 0;
};

// ============================================================================
// Factory functions
// ============================================================================

// Create a connection of the appropriate type (subprocess or HTTP) based on worker_path.
std::unique_ptr<IFunctionConnection> CreateFunctionConnection(
    const std::string &worker_path, const std::string &function_name,
    const ArrowArguments &arguments, const std::vector<uint8_t> &attach_id,
    const std::vector<uint8_t> &transaction_id,
    ClientContext &context, const std::string &function_type = "TABLE",
    const std::vector<uint8_t> &global_execution_id = {},
    bool worker_debug = false,
    const std::map<std::string, Value> &settings = {},
    const std::vector<VgiSecretRequirement> &required_secrets = {});

// Create from a pooled worker (subprocess only — HTTP connections are never pooled).
std::unique_ptr<IFunctionConnection> CreateFunctionConnectionFromPool(
    std::unique_ptr<PooledWorker> pooled_worker, const std::string &function_name,
    const ArrowArguments &arguments, const std::vector<uint8_t> &attach_id,
    const std::vector<uint8_t> &transaction_id,
    ClientContext &context, const std::string &function_type = "TABLE",
    const std::vector<uint8_t> &global_execution_id = {},
    bool worker_debug = false,
    const std::map<std::string, Value> &settings = {},
    const std::vector<VgiSecretRequirement> &required_secrets = {});

} // namespace vgi
} // namespace duckdb
