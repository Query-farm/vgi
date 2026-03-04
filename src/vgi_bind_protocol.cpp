#include "vgi_bind_protocol.hpp"

#include "duckdb/common/exception.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_rpc_types.hpp"

namespace duckdb {
namespace vgi {

BindResult PerformBindProtocol(
    ClientContext &context,
    const std::string &function_name,
    const std::string &function_type,
    const std::shared_ptr<arrow::Array> &arguments_array,
    const std::shared_ptr<arrow::Schema> &input_schema,
    const std::vector<uint8_t> &attach_id,
    const std::map<std::string, Value> &settings,
    const std::vector<VgiSecretRequirement> &required_secrets,
    const std::string &worker_label,
    const BindTransportFn &transport_fn) {

	// 1. Convert arguments to IPC bytes
	// Python expects a single-column batch with an "args" struct column
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

	// 4. Extract secrets from SecretManager (unscoped + static scope/name from metadata)
	auto secrets = ExtractVgiSecrets(context, required_secrets);
	auto secrets_bytes = BuildSecretsBatch(context, secrets);

	// 5. Build BindRequest (first attempt, resolved_secrets_provided=false)
	auto bind_request = BuildBindRequest(function_name, arguments_bytes, function_type,
	                                     input_schema_bytes, settings_bytes, secrets_bytes,
	                                     attach_id, {}, false);
	auto bind_request_bytes = SerializeToIpcBytes(bind_request);

	// 6. Send first bind and read response
	auto bind_response_batch = transport_fn(bind_request_bytes);

	// 7. Check if worker needs scoped secrets (two-phase bind)
	auto scope_request = TryParseBindSecretScopeResponse(bind_response_batch);
	if (scope_request) {
		// Worker requested dynamically-scoped secrets — resolve and retry
		std::vector<VgiSecretRequirement> scoped_reqs;
		for (const auto &lookup : scope_request->lookups) {
			VgiSecretRequirement req;
			req.secret_type = lookup.secret_type;
			req.name = lookup.name;
			req.scope = lookup.scope;
			scoped_reqs.push_back(std::move(req));
		}

		// Resolve scoped secrets and merge with existing
		auto scoped = ExtractVgiSecrets(context, scoped_reqs);
		for (auto &[k, v] : scoped) {
			secrets[k] = std::move(v);
		}

		// Rebuild BindRequest with all secrets and resolved_secrets_provided=true
		secrets_bytes = BuildSecretsBatch(context, secrets);
		bind_request = BuildBindRequest(function_name, arguments_bytes, function_type,
		                                input_schema_bytes, settings_bytes, secrets_bytes,
		                                attach_id, {}, true);
		bind_request_bytes = SerializeToIpcBytes(bind_request);

		// Send retry bind
		bind_response_batch = transport_fn(bind_request_bytes);

		// If worker requests secrets again, that's an error
		if (TryParseBindSecretScopeResponse(bind_response_batch)) {
			throw IOException("Worker requested scoped secrets twice — only one retry allowed [worker: %s]",
			                  worker_label);
		}
	}

	// 8. Parse final BindResponse
	auto bind_response = ParseBindResponse(bind_response_batch, worker_label);

	// 9. Cache the output schema as IPC bytes for InitRequest
	auto output_schema_bytes = SerializeSchemaToIpcBytes(bind_response.output_schema);

	// 10. Build and return BindResult
	return BindResult {
	    bind_response.output_schema,
	    bind_response.opaque_data,
	    bind_request_bytes,
	    output_schema_bytes
	};
}

} // namespace vgi
} // namespace duckdb
