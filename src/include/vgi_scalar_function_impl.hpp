// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"

#include <memory>
#include <optional>
#include <string>
#include <vector>
#include <map>


#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/main/client_properties.hpp" // ClientProperties

#include "vgi_catalog_metadata.hpp"
#include "vgi_result_cache.hpp" // VgiResultCacheKey (per-value memo)

namespace duckdb {

namespace vgi {
class IFunctionConnection;
}

// Attached to each scalar function at registration.
struct VgiScalarFunctionInfo : public ScalarFunctionInfo {
	VgiScalarFunctionInfo() = default;
	~VgiScalarFunctionInfo() override = default;

	std::shared_ptr<vgi::VgiAttachParameters> attach_params;
	std::vector<uint8_t> attach_opaque_data;
	optional_ptr<Catalog> catalog;
	// Attach alias of the owning catalog. For a global function (system.main,
	// `catalog` null because it outlives DETACH) the key that re-resolves the
	// live catalog at bind (ResolveVgiGlobalBinding).
	std::string catalog_name;
	//! True when this registration lives in system.main rather than the VGI
	//! catalog's own schema.
	bool global = false;
	std::string function_name;
	// Owning schema, sent in the bind request (one name may exist in several).
	std::string schema_name;
	std::map<std::string, Value> settings;
	std::vector<std::string> setting_names;
	std::vector<vgi::VgiSecretRequirement> required_secrets;

	const std::string &worker_path() const { return attach_params->worker_path(); }
	bool worker_debug() const { return attach_params->worker_debug(); }
	bool use_pool() const { return attach_params->use_pool(); }
	const std::string &data_version_spec() const { return attach_params->data_version_spec(); }
	const std::string &implementation_version() const { return attach_params->implementation_version(); }

	vgi::VgiSchemaPtr output_schema;  // Single "result" column, from registration

	// Return type (vgi:any) resolved at bind.
	bool has_dynamic_return_type = false;

	// Which positional params are constants (passed at bind, erased from the call).
	std::vector<bool> positional_is_const;
	std::vector<std::string> positional_names;
	// Parsed and validated but not yet installed on the ScalarFunction.
	std::optional<std::vector<vgi::VgiArgumentMonotonicity>> argument_monotonicity;
};

struct VgiScalarFunctionBindData : public FunctionData {
	VgiScalarFunctionBindData() = default;
	~VgiScalarFunctionBindData() override = default;

	std::shared_ptr<vgi::VgiAttachParameters> attach_params;
	std::vector<uint8_t> attach_opaque_data;
	std::string function_name;
	// Owning catalog schema; see VgiScalarFunctionInfo::schema_name.
	std::string schema_name;
	std::map<std::string, Value> settings;
	std::vector<vgi::VgiSecretRequirement> required_secrets;
	// Sanitized union of metadata-declared and bind-discovered secret usage.
	bool secret_dependent = false;

	const std::string &worker_path() const { return attach_params->worker_path(); }
	bool worker_debug() const { return attach_params->worker_debug(); }
	bool use_pool() const { return attach_params->use_pool(); }
	const std::string &data_version_spec() const { return attach_params->data_version_spec(); }
	const std::string &implementation_version() const { return attach_params->implementation_version(); }

	// Output schema resolved at bind (concrete types).
	vgi::VgiSchemaPtr resolved_output_schema;

	// Input schema from the declared argument types (const params erased).
	vgi::VgiSchemaPtr input_schema;
	std::optional<std::vector<std::optional<std::string>>> argument_names;

	// DuckDB types of `input_schema`. Execute casts incoming columns to them:
	// DuckDB may pass a narrower type than declared (e.g. a DECIMAL(3,2) literal
	// for a DOUBLE param, with no Cast inserted), and the wire must match bind.
	vector<LogicalType> input_duckdb_types;

	// Values of the const params erased at bind.
	vector<Value> const_values;

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<VgiScalarFunctionBindData>();
		copy->attach_params = attach_params;
		copy->attach_opaque_data = attach_opaque_data;
		copy->function_name = function_name;
		copy->schema_name = schema_name;
		copy->settings = settings;
		copy->required_secrets = required_secrets;
		copy->secret_dependent = secret_dependent;
		copy->resolved_output_schema = resolved_output_schema;
		copy->input_schema = input_schema;
		copy->argument_names = argument_names;
		copy->input_duckdb_types = input_duckdb_types;
		copy->const_values = const_values;
		return copy;
	}

	// Const args are erased from the expression's children, so they (and the
	// settings sent at bind) must be compared here, or CSE merges f(x, 1) with
	// f(x, 2). Schemas derive from these and the argument types.
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<VgiScalarFunctionBindData>();
		return worker_path() == other.worker_path() && attach_opaque_data == other.attach_opaque_data &&
		       function_name == other.function_name && schema_name == other.schema_name &&
		       worker_debug() == other.worker_debug() && use_pool() == other.use_pool() &&
		       data_version_spec() == other.data_version_spec() &&
		       implementation_version() == other.implementation_version() &&
		       SameValues(const_values, other.const_values) && SameSettings(settings, other.settings) &&
		       input_duckdb_types == other.input_duckdb_types && argument_names == other.argument_names;
	}

private:
	// BoundConstantExpression::Equals semantics: same type, NOT DISTINCT FROM.
	static bool SameValue(const Value &a, const Value &b) {
		return a.type() == b.type() && Value::NotDistinctFrom(a, b);
	}
	static bool SameValues(const vector<Value> &a, const vector<Value> &b) {
		if (a.size() != b.size()) {
			return false;
		}
		for (idx_t i = 0; i < a.size(); i++) {
			if (!SameValue(a[i], b[i])) {
				return false;
			}
		}
		return true;
	}
	static bool SameSettings(const std::map<std::string, Value> &a, const std::map<std::string, Value> &b) {
		if (a.size() != b.size()) {
			return false;
		}
		for (auto ia = a.begin(), ib = b.begin(); ia != a.end(); ++ia, ++ib) {
			if (ia->first != ib->first || !SameValue(ia->second, ib->second)) {
				return false;
			}
		}
		return true;
	}
};

// Per-thread execution state.
struct VgiScalarFunctionLocalState : public FunctionLocalState {
	VgiScalarFunctionLocalState();
	~VgiScalarFunctionLocalState() override;

	// Created on first execute.
	std::unique_ptr<vgi::IFunctionConnection> connection;

	bool initialized = false;

	vgi::VgiSchemaPtr input_schema;

	// Captured at init for the destructor.
	bool use_pool = false;

	// Output schema parse, from the first batch: the output schema is fixed at bind.
	ArrowSchemaWrapper output_c_schema;
	ArrowTableSchema output_arrow_table;
	vector<LogicalType> output_types;
	vector<string> output_names;
	bool output_schema_cached = false;

	// Input conversion state for DataChunkToArrowCached, from the first batch.
	vector<LogicalType> input_arrow_types;
	vector<string> input_arrow_names;
	unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> input_extension_types;
	std::optional<ClientProperties> input_client_props;
	bool input_convert_cached = false;

	// Per-value memoization: the static cache key is built once; per-value keys
	// add each distinct tuple's input_hash. cache_eligible gates the whole tier.
	// Execute re-binds, so its secret provenance lives here, not in the shared
	// FunctionData (which parallel threads read).
	bool secret_dependent = false;
	// That execute-time bind's BindResult::secret_scope: the cache key's secret
	// fingerprint, since its secrets are the ones this connection's worker holds.
	std::string secret_scope;
	bool cache_key_built = false;
	bool cache_eligible = false;
	vgi::VgiResultCacheKey cache_static_key;
	std::string cache_catalog_name;
	// Fingerprint of cache_static_key: the per-value arena registry key.
	std::string cache_static_fp;
	int64_t cache_default_ttl_seconds = 0;
	// Latched `vgi.cache.per_value` advertisement (see VGI_CACHE_PER_VALUE_KEY).
	bool cache_pv_opt_in = false;
};

namespace vgi {

unique_ptr<FunctionLocalState> VgiScalarFunctionInitLocalState(ExpressionState &state,
                                                                const BoundFunctionExpression &expr,
                                                                FunctionData *bind_data);

void VgiScalarFunctionExecute(DataChunk &args, ExpressionState &state, Vector &result);

// Binds against the worker to resolve the return type for the argument types,
// and extracts const params.
unique_ptr<FunctionData> VgiScalarFunctionBind(BindScalarFunctionInput &input);

} // namespace vgi
} // namespace duckdb
