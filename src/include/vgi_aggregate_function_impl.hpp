// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"

#include <atomic>
#include <memory>
#include <string>
#include <vector>


#include "duckdb/function/aggregate_function.hpp"

#include "vgi_catalog_metadata.hpp"
#include "vgi_exception.hpp"
#include "../generated/vgi_protocol_records.hpp"

namespace duckdb {
namespace vgi {

// Which struct owns an aggregate state buffer. The buffer (sized by
// VgiAggregateStateSize) holds either a VgiAggregateState (update/combine/
// finalize) or a VgiAggregateWindowLocalState (window callback); the shared
// destructor dispatches on the tag.
enum class VgiAggregateStateTag : uint8_t {
	UNSET = 0,
	AGGREGATE = 1,
	WINDOW = 2,
};

struct VgiAggregateState {
	// Must be the first field (in both state structs) so it can be read whichever
	// variant owns the buffer.
	VgiAggregateStateTag tag = VgiAggregateStateTag::UNSET;
	int64_t group_id = -1;
};

struct VgiAggregateFunctionInfo : public AggregateFunctionInfo {
	std::shared_ptr<VgiAttachParameters> attach_params;
	std::vector<uint8_t> attach_opaque_data;
	optional_ptr<Catalog> catalog;
	// Attach alias of the owning catalog. For a global (system.main) aggregate,
	// which outlives DETACH, it re-resolves the live catalog at bind
	// (ResolveVgiGlobalBinding).
	std::string catalog_name;
	//! True when this registration lives in system.main rather than the VGI
	//! catalog's own schema.
	bool global = false;
	std::string function_name;
	// Owning catalog schema, carried on every aggregate RPC: a function name is
	// unique only within a schema. Empty serializes as null (by-name lookup).
	std::string schema_name;
	std::map<std::string, Value> settings;
	std::vector<std::string> setting_names;
	std::vector<VgiSecretRequirement> required_secrets;
	VgiSchemaPtr output_schema;

	std::vector<bool> positional_is_const;
	std::vector<std::string> positional_names;

	// Worker implements window(): enables DuckDB's WindowCustomAggregator path.
	bool supports_window = false;

	// Worker opts into streaming-partitioned windows: an optimizer rule swaps
	// LogicalWindow for an operator that streams input chunks to the worker
	// without materializing partitions.
	bool streaming_partitioned = false;

	const std::string &worker_path() const {
		return attach_params->worker_path();
	}
	bool worker_debug() const {
		return attach_params->worker_debug();
	}
	bool use_pool() const {
		return attach_params->use_pool();
	}
};

struct VgiAggregateBindData : public FunctionData {
	std::shared_ptr<VgiAttachParameters> attach_params;
	std::vector<uint8_t> attach_opaque_data;
	std::string function_name;
	// See VgiAggregateFunctionInfo::schema_name. Part of Equals().
	std::string schema_name;
	std::map<std::string, Value> settings;
	std::vector<VgiSecretRequirement> required_secrets;
	VgiSchemaPtr resolved_output_schema;
	VgiSchemaPtr input_schema;
	optional_ptr<Catalog> catalog;
	// Weak: state destructors can fire on a scheduler thread after the session
	// is gone. Lock at the use site; if expired, skip the RPC.
	weak_ptr<ClientContext> context;
	vector<Value> const_values;

	// Shared by all copies of this bind data (DuckDB copies during planning).
	struct ExecState {
		std::atomic<int64_t> group_id_counter{0};
		std::atomic<int64_t> destroy_counter{0};
		// Assigns a partition_id per OVER partition (VgiAggregateWindowInit);
		// the worker keys its window storage on it.
		std::atomic<int64_t> partition_id_counter{0};
		std::vector<uint8_t> execution_id;
	};
	std::shared_ptr<ExecState> exec_state = std::make_shared<ExecState>();

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other_p) const override;
};

// Aggregate function callbacks
idx_t VgiAggregateStateSize(AggregateStateInput &input);
void VgiAggregateInitialize(AggregateStateInput &input, data_ptr_t *states, idx_t count);
void VgiAggregateUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, Vector &state,
                        idx_t count);
void VgiAggregateCombine(Vector &state, Vector &combined, AggregateInputData &aggr_input_data, idx_t count);
void VgiAggregateFinalize(Vector &state, AggregateFinalizeInputData &aggr_input_data, Vector &result, idx_t count,
                          idx_t offset);
void VgiAggregateDestroy(Vector &state, AggregateInputData &aggr_input_data, idx_t count);

unique_ptr<FunctionData> VgiAggregateFunctionBind(BindAggregateFunctionInput &input);

// RPC helpers, shared with vgi_aggregate_window_impl.cpp.

struct AggregateRpcResult {
	// Outer vgi_rpc unary envelope: {result: Binary}.
	VgiBatchPtr response_batch;
	// Unwrapped and schema-validated inner response; null if the result is null
	// or the envelope empty. Decode with DecodeAggregateResult.
	VgiBatchPtr inner_batch;
};

// Dispatch an aggregate unary RPC through InvokePooledUnaryRpc, with the
// catalog's HTTP params/pool/capabilities and launcher overrides.
// enable_logging=false for destructor RPCs (see UnaryRpcOptions::enable_logging).
AggregateRpcResult InvokeAggregateRpc(ClientContext &context, const VgiAggregateBindData &bind_data,
                                      const std::string &method_name,
                                      const VgiBatchPtr &params,
                                      bool enable_logging = true);

// extra_info for a malformed aggregate (incl. window / streaming) RPC response.
ErrorInfo AggregateViolation(const VgiAggregateBindData &bind_data, const std::string &method_name);

// The worker that serves `bind_data`'s aggregate.
const std::string &AggregateWorkerPath(const VgiAggregateBindData &bind_data);

[[noreturn]] void ThrowMissingAggregateResult(const VgiAggregateBindData &bind_data, const std::string &method_name);

// The typed result of an aggregate RPC (the generated record for the method).
template <class T>
T DecodeAggregateResult(const AggregateRpcResult &rpc_result, const VgiAggregateBindData &bind_data,
                        const std::string &method_name) {
	if (!rpc_result.inner_batch) {
		ThrowMissingAggregateResult(bind_data, method_name);
	}
	return generated::Decode<T>(rpc_result.inner_batch, 0, AggregateWorkerPath(bind_data));
}

} // namespace vgi
} // namespace duckdb
