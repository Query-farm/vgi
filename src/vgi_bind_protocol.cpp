// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_bind_protocol.hpp"

#include "duckdb/common/exception.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_rpc.hpp"
#include "vgi_rpc_types.hpp"

namespace duckdb {
namespace vgi {

std::vector<uint8_t> BuildBindRequestBytes(
    ClientContext &context,
    const std::string &function_name,
    const std::string &function_type,
    const std::shared_ptr<arrow::Array> &arguments_array,
    const std::shared_ptr<arrow::Schema> &input_schema,
    const std::vector<uint8_t> &attach_opaque_data,
    const std::vector<uint8_t> &transaction_opaque_data,
    const std::map<std::string, Value> &settings,
    const std::map<std::string, std::map<std::string, Value>> &resolved_secrets,
    bool resolved_secrets_provided,
    const std::string &worker_label,
    const std::string &at_unit,
    const std::string &at_value,
    const CopyFromBindContext *copy_from) {

	// 1. Convert arguments to IPC bytes (Python expects a struct column "args")
	std::vector<uint8_t> arguments_bytes;
	if (arguments_array) {
		auto args_schema = arrow::schema({arrow::field("args", arguments_array->type())});
		auto args_batch = arrow::RecordBatch::Make(args_schema, arguments_array->length(), {arguments_array});
		arguments_bytes = SerializeToIpcBytes(args_batch);
	} else {
		auto empty_struct_type = arrow::struct_({});
		auto empty_struct_result = arrow::MakeEmptyArray(empty_struct_type);
		if (!empty_struct_result.ok()) {
			throw IOException("Failed to create empty struct array: %s [worker: %s]",
			                  empty_struct_result.status().ToString(), worker_label);
		}
		auto args_schema = arrow::schema({arrow::field("args", empty_struct_type)});
		auto args_batch = arrow::RecordBatch::Make(args_schema, 0, {empty_struct_result.ValueUnsafe()});
		arguments_bytes = SerializeToIpcBytes(args_batch);
	}

	// 2. Serialize settings if non-empty
	std::vector<uint8_t> settings_bytes;
	if (!settings.empty()) {
		auto settings_batch = BuildSettingsBatch(context, settings);
		settings_bytes = SerializeToIpcBytes(settings_batch);
	}

	// 3. Serialize input_schema if set
	std::vector<uint8_t> input_schema_bytes;
	if (input_schema) {
		input_schema_bytes = SerializeSchemaToIpcBytes(input_schema);
	}

	// 4. Build secrets bytes from already-resolved secrets (BuildSecretsBatch
	// returns IPC bytes directly, despite its name).
	auto secrets_bytes = BuildSecretsBatch(context, resolved_secrets);

	// 5. Build BindRequest and serialize.
	auto bind_request = BuildBindRequest(function_name, arguments_bytes, function_type,
	                                     input_schema_bytes, settings_bytes, secrets_bytes,
	                                     attach_opaque_data, transaction_opaque_data, resolved_secrets_provided,
	                                     at_unit, at_value, copy_from);
	return SerializeToIpcBytes(bind_request);
}

BindResult BuildBindResultFromInlinedBytes(
    std::vector<uint8_t> bind_request_bytes,
    const std::vector<uint8_t> &bind_response_bytes,
    const std::string &worker_label) {

	// Deserialize the inlined BindResponse blob.
	auto bind_response_batch = DeserializeFromIpcBytes(
	    bind_response_bytes.data(), bind_response_bytes.size());
	if (!bind_response_batch || bind_response_batch->num_rows() == 0) {
		throw IOException("Inlined bind_result is empty or malformed [worker: %s]", worker_label);
	}

	// Reject secret-scope responses inline-bound — those require an RPC retry
	// dance that the inlined path cannot drive. The worker shouldn't have
	// produced one in this position, but if it did, fall back is the right
	// behavior; surfacing as an exception lets PerformVgiTableFunctionBind's
	// try/catch route through the on-demand RPC path.
	auto scope_request = TryParseBindSecretScopeResponse(bind_response_batch);
	if (scope_request) {
		throw IOException(
		    "Inlined bind_result is a secret-scope request, not a bind response — "
		    "secret resolution requires the on-demand RPC path [worker: %s]",
		    worker_label);
	}

	auto bind_response = ParseBindResponse(bind_response_batch, worker_label);
	auto output_schema_bytes = SerializeSchemaToIpcBytes(bind_response.output_schema);

	return BindResult {
	    bind_response.output_schema,
	    bind_response.opaque_data,
	    std::move(bind_request_bytes),
	    output_schema_bytes
	};
}

BindResult PerformBindProtocol(
    ClientContext &context,
    const std::string &function_name,
    const std::string &function_type,
    const std::shared_ptr<arrow::Array> &arguments_array,
    const std::shared_ptr<arrow::Schema> &input_schema,
    const std::vector<uint8_t> &attach_opaque_data,
    const std::vector<uint8_t> &transaction_opaque_data,
    const std::map<std::string, Value> &settings,
    const std::vector<VgiSecretRequirement> &required_secrets,
    const std::string &worker_label,
    const BindTransportFn &transport_fn,
    const std::string &at_unit,
    const std::string &at_value,
    const CopyFromBindContext *copy_from) {

	// Resolve unscoped + static-scope/name secrets up front. Scoped secrets
	// requested by the worker mid-bind are merged in below if needed.
	auto secrets = ExtractVgiSecrets(context, required_secrets);

	// First-attempt request bytes (resolved_secrets_provided=false).
	auto bind_request_bytes = BuildBindRequestBytes(
	    context, function_name, function_type, arguments_array, input_schema,
	    attach_opaque_data, transaction_opaque_data, settings, secrets,
	    /*resolved_secrets_provided=*/false, worker_label, at_unit, at_value, copy_from);

	// Send first bind and read response.
	auto bind_response_batch = transport_fn(bind_request_bytes);

	// Two-phase secret-scope bind: worker can request additional scoped
	// secrets, we resolve them and re-send with resolved_secrets_provided=true.
	auto scope_request = TryParseBindSecretScopeResponse(bind_response_batch);
	if (scope_request) {
		std::vector<VgiSecretRequirement> scoped_reqs;
		for (const auto &lookup : scope_request->lookups) {
			VgiSecretRequirement req;
			req.secret_type = lookup.secret_type;
			req.name = lookup.name;
			req.scope = lookup.scope;
			scoped_reqs.push_back(std::move(req));
		}

		auto scoped = ExtractVgiSecrets(context, scoped_reqs);
		for (auto &[k, v] : scoped) {
			secrets[k] = std::move(v);
		}

		bind_request_bytes = BuildBindRequestBytes(
		    context, function_name, function_type, arguments_array, input_schema,
		    attach_opaque_data, transaction_opaque_data, settings, secrets,
		    /*resolved_secrets_provided=*/true, worker_label, at_unit, at_value, copy_from);

		bind_response_batch = transport_fn(bind_request_bytes);

		if (TryParseBindSecretScopeResponse(bind_response_batch)) {
			throw IOException("Worker requested scoped secrets twice — only one retry allowed [worker: %s]",
			                  worker_label);
		}
	}

	auto bind_response = ParseBindResponse(bind_response_batch, worker_label);
	auto output_schema_bytes = SerializeSchemaToIpcBytes(bind_response.output_schema);

	return BindResult {
	    bind_response.output_schema,
	    bind_response.opaque_data,
	    std::move(bind_request_bytes),
	    output_schema_bytes
	};
}

} // namespace vgi
} // namespace duckdb
