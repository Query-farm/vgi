// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_bind_protocol.hpp"

#include "duckdb/common/exception.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_cache_identity.hpp"
#include "vgi_catalog_rpc.hpp"
#include "vgi_rpc_types.hpp"
#include "vgi_query_rpc.hpp"

namespace duckdb {
namespace vgi {

//! The result-cache identity of a bind's secrets: empty unless the bind
//! depended on a secret, then a fingerprint of the exact set it resolved.
static std::string SecretScopeFor(bool secret_dependent,
                                  const std::map<std::string, std::map<std::string, Value>> &resolved_secrets) {
	return secret_dependent ? ComputeSecretCacheFingerprint(resolved_secrets) : std::string();
}

//! The one-row {args: struct} IPC stream a BindRequest's `arguments` holds.
static std::vector<uint8_t> SerializeArgumentsIpc(const VgiBatchPtr &arguments_batch) {
	// ArrowArguments::batch already is the one-row {args: struct} batch.
	if (arguments_batch) {
		return SerializeToIpcBytes(arguments_batch);
	}
	// One valid empty row: Python's Arguments.deserialize_from_bytes reads row 0.
	VgiBatchBuilder builder(MakeSchema({type::Struct({}).Named("args")}));
	append::FinishElement(builder.column(0));
	builder.FinishRow();
	return SerializeToIpcBytes(builder.Finish());
}

//! BuildBindRequestBytes over already-serialized arguments; with
//! `arguments_location` set, the arguments travel by reference instead.
static std::vector<uint8_t> BuildBindRequestBytesFrom(
    ClientContext &context, const std::string &function_name, const std::string &function_type,
    const std::vector<uint8_t> &arguments_bytes, const ArgumentsLocationRef *arguments_location,
    const VgiSchemaPtr &input_schema, const std::vector<uint8_t> &attach_opaque_data,
    const std::vector<uint8_t> &transaction_opaque_data, const std::map<std::string, Value> &settings,
    const std::map<std::string, std::map<std::string, Value>> &resolved_secrets, bool resolved_secrets_provided,
    const std::string &at_unit, const std::string &at_value, const CopyFromBindContext *copy_from,
    const CopyToBindContext *copy_to, const std::string &schema_name,
    const std::optional<std::vector<std::optional<std::string>>> &argument_names) {
	std::vector<uint8_t> settings_bytes;
	if (!settings.empty()) {
		auto settings_batch = BuildSettingsBatch(context, settings);
		settings_bytes = SerializeToIpcBytes(settings_batch);
	}

	std::vector<uint8_t> input_schema_bytes;
	if (input_schema) {
		input_schema_bytes = SerializeSchemaToIpcBytes(input_schema);
	}

	// BuildSecretsBatch returns IPC bytes, despite its name.
	auto secrets_bytes = BuildSecretsBatch(context, resolved_secrets);

	auto bind_request = BuildBindRequest(function_name, arguments_bytes, function_type,
	                                     input_schema_bytes, settings_bytes, secrets_bytes,
	                                     attach_opaque_data, transaction_opaque_data, resolved_secrets_provided,
	                                     at_unit, at_value, copy_from, copy_to, schema_name, argument_names,
	                                     arguments_location);
	return SerializeToIpcBytes(bind_request);
}

std::vector<uint8_t> BuildBindRequestBytes(
    ClientContext &context,
    const std::string &function_name,
    const std::string &function_type,
    const VgiBatchPtr &arguments_batch,
    const VgiSchemaPtr &input_schema,
    const std::vector<uint8_t> &attach_opaque_data,
    const std::vector<uint8_t> &transaction_opaque_data,
    const std::map<std::string, Value> &settings,
    const std::map<std::string, std::map<std::string, Value>> &resolved_secrets,
    bool resolved_secrets_provided,
    const std::string &worker_label,
    const std::string &at_unit,
    const std::string &at_value,
    const CopyFromBindContext *copy_from,
    const CopyToBindContext *copy_to,
    const std::string &schema_name,
    const std::optional<std::vector<std::optional<std::string>>> &argument_names) {
	(void)worker_label;
	return BuildBindRequestBytesFrom(context, function_name, function_type, SerializeArgumentsIpc(arguments_batch),
	                                 /*arguments_location=*/nullptr, input_schema, attach_opaque_data,
	                                 transaction_opaque_data, settings, resolved_secrets, resolved_secrets_provided,
	                                 at_unit, at_value, copy_from, copy_to, schema_name, argument_names);
}

BindResult BuildBindResultFromInlinedBytes(std::vector<uint8_t> bind_request_bytes,
                                           const std::vector<uint8_t> &bind_response_bytes,
                                           const std::string &worker_label, bool secret_dependent,
                                           const std::map<std::string, std::map<std::string, Value>> &resolved_secrets) {
	auto bind_response_batch = DeserializeFromIpcBytes(
	    bind_response_bytes.data(), bind_response_bytes.size());
	if (!bind_response_batch || bind_response_batch->num_rows() == 0) {
		throw IOException("Inlined bind_result is empty or malformed [worker: %s]", worker_label);
	}

	// A secret-scope request needs the RPC retry; throwing lets the caller fall
	// back to the on-demand bind RPC.
	auto scope_request = TryParseBindSecretScopeResponse(bind_response_batch);
	if (scope_request) {
		throw IOException(
		    "Inlined bind_result is a secret-scope request, not a bind response — "
		    "secret resolution requires the on-demand RPC path [worker: %s]",
		    worker_label);
	}

	auto bind_response = ParseBindResponse(bind_response_batch, worker_label);
	auto output_schema_bytes = SerializeSchemaToIpcBytes(bind_response.output_schema);

	return BindResult {bind_response.output_schema,
	                   bind_response.opaque_data,
	                   std::move(bind_request_bytes),
	                   output_schema_bytes,
	                   secret_dependent,
	                   SecretScopeFor(secret_dependent, resolved_secrets),
	                   bind_response.projection_expression_profiles,
	                   SerializeToIpcBytes(bind_response_batch),
	                   bind_response.repeatability};
}

BindResult PerformBindProtocol(
    ClientContext &context,
    const std::string &function_name,
    const std::string &function_type,
    const VgiBatchPtr &arguments_batch,
    const VgiSchemaPtr &input_schema,
    const std::vector<uint8_t> &attach_opaque_data,
    const std::vector<uint8_t> &transaction_opaque_data,
    const std::map<std::string, Value> &settings,
    const std::vector<VgiSecretRequirement> &required_secrets,
    const std::string &worker_label,
    const BindTransportFn &transport_fn,
    const std::string &at_unit,
    const std::string &at_value,
    const CopyFromBindContext *copy_from,
    const CopyToBindContext *copy_to,
    const std::string &schema_name,
    const std::optional<std::vector<std::optional<std::string>>> &argument_names,
    const ArgumentsExternalizerFn &externalize_arguments) {

	// Resolve declared secrets up front; scoped secrets the worker requests
	// mid-bind are merged in below.
	const bool query_executor = function_name == VGI_QUERY_EXECUTOR;
	if (query_executor && !required_secrets.empty()) {
		throw IOException("vgi_query does not allow external secret resolution");
	}
	auto secrets = ExtractVgiSecrets(context, required_secrets);
	bool secret_dependent = !required_secrets.empty();

	// Decided once per bind (docs/large-values-3.4.md §4): the arguments go
	// inline, or up once and by reference. Every BindRequest this bind builds,
	// including the scoped-secret retry below, reuses that decision, so no
	// retry uploads again and later requests embed these exact bytes.
	auto arguments_bytes = SerializeArgumentsIpc(arguments_batch);
	std::optional<ArgumentsLocationRef> arguments_location;
	if (externalize_arguments) {
		arguments_location = externalize_arguments(arguments_bytes);
		if (arguments_location) {
			// Only the reference travels from here on.
			std::vector<uint8_t>().swap(arguments_bytes);
		}
	}
	const auto *location = arguments_location ? &*arguments_location : nullptr;

	auto bind_request_bytes = BuildBindRequestBytesFrom(
	    context, function_name, function_type, arguments_bytes, location, input_schema, attach_opaque_data,
	    transaction_opaque_data, settings, secrets, /*resolved_secrets_provided=*/false, at_unit, at_value, copy_from,
	    copy_to, schema_name, argument_names);

	auto bind_response_batch = transport_fn(bind_request_bytes);

	// Two-phase bind: resolve the requested scoped secrets and resend once.
	auto scope_request = TryParseBindSecretScopeResponse(bind_response_batch);
	if (scope_request) {
		if (query_executor) {
			throw IOException("vgi_query executor cannot request secret negotiation");
		}
		secret_dependent = true;
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
			// Keyed by secret name: a distinct secret (e.g. another s3 bucket)
			// lands under its own key; re-resolving the same secret is idempotent.
			secrets[k] = std::move(v);
		}

		bind_request_bytes = BuildBindRequestBytesFrom(
		    context, function_name, function_type, arguments_bytes, location, input_schema, attach_opaque_data,
		    transaction_opaque_data, settings, secrets, /*resolved_secrets_provided=*/true, at_unit, at_value,
		    copy_from, copy_to, schema_name, argument_names);

		bind_response_batch = transport_fn(bind_request_bytes);

		if (TryParseBindSecretScopeResponse(bind_response_batch)) {
			throw IOException("Worker requested scoped secrets twice — only one retry allowed [worker: %s]",
			                  worker_label);
		}
	}

	auto bind_response = ParseBindResponse(bind_response_batch, worker_label);
	auto output_schema_bytes = SerializeSchemaToIpcBytes(bind_response.output_schema);

	// `secrets` is exactly what the final bind_request_bytes carried, including
	// scoped secrets merged by the retry.
	return BindResult {bind_response.output_schema,
	                   bind_response.opaque_data,
	                   std::move(bind_request_bytes),
	                   output_schema_bytes,
	                   secret_dependent,
	                   SecretScopeFor(secret_dependent, secrets),
	                   bind_response.projection_expression_profiles,
	                   SerializeToIpcBytes(bind_response_batch),
	                   bind_response.repeatability};
}

} // namespace vgi
} // namespace duckdb
