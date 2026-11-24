// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_settings_defaults.hpp"
#include "nanoarrow/nanoarrow.h"
#include "vgi_catalog_api.hpp"
#include "vgi_column_expression.hpp"
#include "vgi_column_metadata.hpp"
#include "vgi_macro_contract.hpp"
#include "vgi_query_rpc.hpp"
#include "vgi_constraint_contract.hpp"
#include "vgi_index_contract.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/constraints/check_constraint.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "vgi_arrow_ipc.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_record_access.hpp"
#include "vgi_exception.hpp"
#include "vgi_http_client.hpp"
#include "vgi_http_params.hpp"
#include "vgi_logging.hpp"
#include "vgi_profiling.hpp"
#include "vgi_protocol_constants.hpp"
#include "vgi_protocol_codec.hpp"
#include "vgi_reflection.hpp"

#include <cstdlib>
#include <deque>
#include <memory>
#include <set>
#include <typeinfo>
#include "vgi_rpc_client.hpp"
#include "vgi_rpc_types.hpp"
#include "generated/vgi_request_builders.hpp"
#include "generated/vgi_protocol_records.hpp"
#include "vgi_schema_registry.hpp"
#include "vgi_wire_codec.hpp"
#include "vgi_batch_validation.hpp"
#include "vgi_transport.hpp"
#include "vgi_unary_rpc.hpp"
#include "vgi_unary_retry.hpp"
#include "yyjson.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {
namespace vgi {

namespace {

// CatalogRpcContext uses empty-as-absent; the generated builders take optionals.
inline std::optional<std::vector<uint8_t>> OptTxn(const CatalogRpcContext &ctx) {
	return ctx.transaction_opaque_data.empty()
	           ? std::nullopt
	           : std::optional<std::vector<uint8_t>>(ctx.transaction_opaque_data);
}

// Empty-string-as-absent (e.g. at_unit/at_value) to optional.
inline std::optional<std::string> OptStrIfNonEmpty(const std::string &s) {
	return s.empty() ? std::nullopt : std::optional<std::string>(s);
}

// (value, is_null) pairs, e.g. COMMENT ON ... IS NULL, to optional.
inline std::optional<std::string> OptStrNullable(const std::string &s, bool is_null) {
	return is_null ? std::nullopt : std::optional<std::string>(s);
}

// Exactly one `catalog.rpc` event per dispatch, with outcome=ok|error,
// duration_ms and the CatalogRpcContext entity context. Callers MarkOk() on
// success; otherwise the destructor logs an error. Also a ScopedTimer for
// VGI_PROFILE=1.
class CatalogRpcInstrumentation {
public:
	CatalogRpcInstrumentation(ClientContext &context, const CatalogRpcContext &ctx,
	                          const std::string &method_name)
	    : context_(context),
	      ctx_(ctx),
	      method_name_(method_name),
	      timer_("catalog." + method_name),
	      start_(std::chrono::steady_clock::now()) {
	}

	void MarkOk() {
		ok_ = true;
	}

	// Called from a catch-then-rethrow: the destructor runs during unwind,
	// where the in-flight exception cannot be inspected without a catch.
	void NoteException(const std::string &kind, const std::string &what) {
		err_kind_ = kind;
		err_what_ = what;
	}

	~CatalogRpcInstrumentation() {
		auto end = std::chrono::steady_clock::now();
		double duration_ms = std::chrono::duration<double, std::milli>(end - start_).count();
		vector<pair<string, string>> info;
		info.reserve(8);
		info.emplace_back("method", method_name_);
		info.emplace_back("worker_path", ctx_.params->worker_path());
		// Short digests only, never the raw values (OpaqueDigest).
		auto attach_digest = OpaqueDigest(ctx_.attach_opaque_data);
		if (!attach_digest.empty()) {
			info.emplace_back("attach_opaque_data_sha256", attach_digest);
		}
		auto txn_digest = OpaqueDigest(ctx_.transaction_opaque_data);
		if (!txn_digest.empty()) {
			info.emplace_back("transaction_opaque_data_sha256", txn_digest);
		}
		if (!ctx_.entity_kind.empty()) {
			info.emplace_back("entity_kind", ctx_.entity_kind);
		}
		if (!ctx_.entity_qualifier.empty()) {
			info.emplace_back("entity_qualifier", ctx_.entity_qualifier);
		}
		info.emplace_back("duration_ms", std::to_string(duration_ms));
		if (ok_) {
			info.emplace_back("outcome", "ok");
		} else {
			info.emplace_back("outcome", "error");
			if (!err_kind_.empty()) {
				info.emplace_back("error_kind", err_kind_);
			}
			if (!err_what_.empty()) {
				info.emplace_back("error_message", err_what_);
			}
		}
		VGI_LOG(context_, "catalog.rpc", info);
	}

private:
	ClientContext &context_;
	const CatalogRpcContext &ctx_;
	std::string method_name_;
	vgi::ScopedTimer timer_;
	std::chrono::steady_clock::time_point start_;
	bool ok_ = false;
	std::string err_kind_;
	std::string err_what_;
};

}  // namespace

// Initialized once per catalog; see the declaration in vgi_attach_parameters.hpp
// for why BEGIN must never reach the secret manager (duckdb#22258).
std::shared_ptr<HTTPParams> VgiAttachParameters::GetOrInitHttpParams(
    ClientContext &context, const std::string &url, bool single_attempt) const {
	std::lock_guard<std::mutex> lock(http_params_mutex_);
	if (!cached_http_params_) {
		auto owned = InitHttpParamsWithSecrets(context, url);
		// Initialize both sets now: copying an HTTPParams base would lose the
		// HTTPFS TLS and credential fields, and doing it later inside BEGIN
		// would deadlock.
		auto single_owned = InitHttpParamsWithSecrets(context, url);
		single_owned->retries = 0;
		// Captured once: a later vgi_http_timeout_seconds needs a re-ATTACH.
		Value timeout_val;
		if (context.TryGetCurrentSetting("vgi_http_timeout_seconds", timeout_val)) {
			owned->timeout = static_cast<uint64_t>(timeout_val.GetValue<int64_t>());
		} else {
			owned->timeout = defaults::HTTP_TIMEOUT_SECONDS;
		}
		single_owned->timeout = owned->timeout;
		auto regular_params = std::shared_ptr<HTTPParams>(owned.release());
		auto single_params = std::shared_ptr<HTTPParams>(single_owned.release());
		cached_http_params_ = std::move(regular_params);
		cached_single_attempt_http_params_ = std::move(single_params);
	}
	return single_attempt ? cached_single_attempt_http_params_ : cached_http_params_;
}

std::shared_ptr<VgiHttpClientPool> VgiAttachParameters::GetOrInitHttpClientPool() const {
	std::lock_guard<std::mutex> lock(http_params_mutex_);
	if (!http_client_pool_) {
		http_client_pool_ = std::make_shared<VgiHttpClientPool>();
	}
	return http_client_pool_;
}

// extra_info for a worker that answered a catalog RPC but broke the protocol
// (missing/empty/mis-shaped response).
static ErrorInfo CatalogProtocolViolation(const std::string &method_name, const std::string &worker_path) {
	return std::move(ErrorInfo(error_subtype::kProtocolViolation).Rpc(method_name).Worker(worker_path));
}

// Call from inside a catch block: rethrow with the RPC's method / catalog /
// entity added to extra_info, keeping the concrete type and the message
// byte-for-byte (fields the exception already carries win).
//   - VgiRpcException stays a VgiRpcException: callers match on its error_kind
//     (capability fallback, catalog_version probe, COPY-format discovery).
//   - InterruptException, types ThrowTypedException cannot rebuild faithfully
//     (HTTP, INTERNAL, PERMISSION, ...) and non-DuckDB exceptions pass through.
[[noreturn]] static void RethrowWithCatalogRpcContext(const CatalogRpcContext &ctx, const std::string &method_name) {
	auto context_info = [&]() {
		ErrorInfo info;
		info.Set(error_key::kRpcMethod, method_name);
		if (ctx.params) {
			info.Set(error_key::kCatalog, ctx.params->catalog_name());
		}
		info.Set(error_key::kEntityKind, ctx.entity_kind);
		info.Set(error_key::kEntity, ctx.entity_qualifier);
		return info;
	};
	try {
		throw;
	} catch (const VgiRpcException &e) {
		ErrorData err(e);
		auto info = context_info();
		info.Merge(err.ExtraInfo());
		throw VgiRpcException(info, err.RawMessage(), e.GetErrorKind());
	} catch (const InterruptException &) {
		throw;
	} catch (const Exception &e) {
		ErrorData err(e);
		switch (err.Type()) {
		case ExceptionType::IO:
		case ExceptionType::INVALID_INPUT:
		case ExceptionType::BINDER:
		case ExceptionType::CATALOG:
		case ExceptionType::NOT_IMPLEMENTED: {
			auto info = context_info();
			info.Merge(err.ExtraInfo());
			ThrowTypedException(err.Type(), info, err.RawMessage());
		}
		default:
			throw;
		}
	}
}

// Every catalog RPC goes through here (InvokePooledUnaryRpc does pooling,
// stderr draining and stale-pool retry). `protocol` selects the routing key
// (the URL's protocol segment on HTTP). Only vgi.v3 methods are in the schema
// registry, so only they get request validation.
static UnaryResponseResult InvokeRpcMethod(const CatalogRpcContext &ctx, const std::string &method_name,
                                            const VgiBatchPtr &params, ClientContext &context,
                                            const VgiProtocolId &protocol = VGI_MAIN_PROTOCOL) {
	CatalogRpcInstrumentation instr(context, ctx, method_name);
	try {
		// Catch request-builder drift here rather than as an opaque worker error.
		if (protocol.name == VGI_MAIN_PROTOCOL.name) {
			ValidateRequestSchema(params, method_name, ctx.params->worker_path());
		}

		UnaryRpcOptions opts {context,
		                      ctx.params->worker_path(),
		                      ctx.params->worker_debug(),
		                      ctx.params->use_pool(),
		                      ctx.params->data_version_spec(),
		                      ctx.params->implementation_version(),
		                      "rpc_catalog",
		                      ctx.params->auth(),
		                      ctx.params->cookie_jar()};
		// Replay policy (single attempt for mutations) is the dispatcher's, not
		// tied to pool selection.
		opts.tcp_proxy = ctx.params->tcp_proxy();
		opts.iroh = ctx.params->iroh();
		// Cached HTTPParams: RPCs from inside VgiTransaction::Start must not reach
		// the secret manager (see VgiAttachParameters::GetOrInitHttpParams).
		if (IsHttpTransport(ctx.params->worker_path()) || IsHttpiTransport(ctx.params->worker_path())) {
			if (IsHttpTransport(ctx.params->worker_path())) {
				opts.cached_http_params = ctx.params->GetOrInitHttpParams(
				    context, ctx.params->worker_path(), RequiresSingleAttemptUnary(method_name));
			}
			// Keep-alive pool: reuse TCP/TLS connections across catalog RPCs.
			opts.http_client_pool = ctx.params->GetOrInitHttpClientPool();
			// Shared with the streaming connections: whichever first sees
			// VGI-Supported-Encodings settles the codec for both.
			opts.protocol_session = ctx.params->protocol_session();
		} else if (IsContainerSharedLocation(ctx.params->worker_path())) {
			// An oci:// container served over HTTP needs the same caches: without
			// them each RPC re-probes /health and does a secret lookup, which
			// throws inside VgiTransaction::Start (catalog_transaction_begin).
			opts.cached_http_params = ctx.params->GetOrInitHttpParams(
			    context, ctx.params->worker_path(), RequiresSingleAttemptUnary(method_name));
			opts.protocol_session = ctx.params->protocol_session();
		}
		// Launcher overrides (`launch:` LOCATIONs only).
		if (ctx.params->launcher_idle_timeout_seconds().has_value()) {
			opts.launcher_idle_timeout =
			    std::chrono::seconds(*ctx.params->launcher_idle_timeout_seconds());
		}
		if (ctx.params->launcher_state_dir().has_value()) {
			opts.launcher_state_dir = *ctx.params->launcher_state_dir();
		}
		// The version this endpoint negotiated; its codec translates the reply
		// back to the current version (identity for 3.4).
		opts.protocol = NegotiatedProtocol(ctx.params->protocol_session().get(), protocol);
		auto result = InvokePooledUnaryRpc(opts, method_name, params);
		if (const auto *codec = IsReservedRpcMethod(method_name) ? nullptr : RequestCodec(opts.protocol)) {
			codec->DecodeResponse(method_name, result);
		}
		instr.MarkOk();
		return result;
	} catch (const std::exception &e) {
		// typeid().name() may be mangled; good enough for triage.
		instr.NoteException(typeid(e).name(), e.what());
		RethrowWithCatalogRpcContext(ctx, method_name);
	}
}

// Unwrap the `result` envelope and validate it against the schema registry.
static VgiBatchPtr ExtractAndDeserializeResult(
    const UnaryResponseResult &response, const std::string &method_name, const std::string &worker_path) {
	// An empty envelope or a NULL result is a void/absent result, not an error.
	VgiBatchPtr result;
	if (response.batch && response.batch->num_rows() != 0) {
		result = UnwrapResultEnvelope(response.batch, method_name, CatalogProtocolViolation(method_name, worker_path),
		                              /*null_result_ok=*/true);
	}

	ValidateResponseSchema(result, method_name, worker_path);
	return result;
}

// For void methods the registry requires an empty envelope (or empty inner
// batch); a payload is a mismatch error, not silently dropped.
static void InvokeVoidRpc(const CatalogRpcContext &ctx, const std::string &method_name,
                          const VgiBatchPtr &params, ClientContext &context) {
	auto response = InvokeRpcMethod(ctx, method_name, params, context);
	ExtractAndDeserializeResult(response, method_name, ctx.params->worker_path());
}

// Non-vgi.v3 protocols (vgi_rpc.Reflection.v1, ...)

VgiBatchPtr InvokeProtocolUnary(const CatalogRpcContext &ctx, ClientContext &context,
                                const VgiProtocolId &protocol, const std::string &method_name,
                                const VgiBatchPtr &params, VgiMetadataPtr *response_metadata) {
	auto response = InvokeRpcMethod(ctx, method_name, params, context, protocol);
	if (response_metadata) {
		*response_metadata = response.metadata;
	}
	const auto &worker_path = ctx.params->worker_path();
	const std::string protocol_name(protocol.name);
	auto reflection_violation = [&]() {
		return ErrorInfo(error_subtype::kProtocolViolation).Rpc(protocol_name + "/" + method_name).Worker(worker_path);
	};
	// The structured return is one serialized IPC batch in a binary `result` column.
	auto result_col = response.batch->GetColumnByName("result");
	if (!result_col || result_col.type().type != NANOARROW_TYPE_BINARY) {
		throw IOException(reflection_violation(), "%s/%s reply carried no binary 'result' column [worker: %s]",
		                  protocol_name, method_name, worker_path);
	}
	auto binary_array = result_col;
	if (binary_array.IsNull(0)) {
		throw IOException(reflection_violation(), "%s/%s reply carried a NULL 'result' [worker: %s]", protocol_name,
		                  method_name, worker_path);
	}
	VgiBatchPtr payload;
	try {
		if (method_name == "table_function_projection") {
			auto bytes = binary_array.GetView(0);
			if (bytes.size() > 8 * 1024 * 1024)
				throw IOException("Projection response exceeds 8 MiB");
			payload = DeserializeVgiQueryRecord(std::vector<uint8_t>(bytes.begin(), bytes.end()));
		} else {
			payload = DeserializeFromIpcBytesZeroCopy(std::make_shared<const VgiBuffer>(response.batch,
			    reinterpret_cast<const uint8_t *>(binary_array.GetView(0).data()), binary_array.GetView(0).size()));
		}
	} catch (const std::exception &e) {
		throw IOException(reflection_violation().Merge(ExtraInfoOf(e)), "Could not decode the %s/%s reply [worker: %s]: %s",
		                  protocol_name, method_name, worker_path, RawMessageOf(e));
	}
	if (!payload || payload->num_rows() < 1) {
		throw IOException(reflection_violation(), "%s/%s reply carried no rows [worker: %s]", protocol_name, method_name,
		                  worker_path);
	}
	return payload;
}

VgiBatchPtr InvokeReflectionListProtocols(const CatalogRpcContext &ctx, ClientContext &context,
                                          VgiMetadataPtr *response_metadata) {
	// Reflection declares no protocol_version and is exempt from the version
	// gate, so the request carries the routing key and no version.
	static constexpr VgiProtocolId kReflection {REFLECTION_PROTOCOL_NAME, ""};
	return InvokeProtocolUnary(ctx, context, kReflection, "list_protocols", nullptr, response_metadata);
}

// ---------------------------------------------------------------------------
// Protocol version negotiation at ATTACH (docs/protocol_versions.md)
// ---------------------------------------------------------------------------

namespace {

bool HeaderTransport(const std::string &worker_path) {
	return IsHttpTransport(worker_path) || IsHttpiTransport(worker_path) || IsContainerSharedLocation(worker_path);
}

// Byte transports: the worker's ranges from its reflection answer, before any
// other call. vgi_rpc.protocol_versions when the worker sends it; otherwise
// (a worker released before negotiation) the exact versions its listing
// declares. A worker without reflection, or one whose answer fails, leaves the
// ranges unknown: requests then carry the client's own version, as before.
void LearnRangesFromReflection(const CatalogRpcContext &ctx, ClientContext &context) {
	std::string advertised;
	VgiProtocolListing listing;
	try {
		listing = InvokeListProtocols(ctx, context, /*tolerate_pre_reflection=*/true, &advertised);
	} catch (const InterruptException &) {
		throw;
	} catch (const std::exception &e) {
		VGI_LOG(context, "protocol.reflection_unavailable",
		        {{"worker_path", ctx.params->worker_path()}, {"error", e.what()}});
		return;
	}
	std::optional<ProtocolRanges> ranges;
	if (!advertised.empty()) {
		ranges = ParseProtocolRanges(advertised);
	}
	if (!ranges && !listing.predates_reflection) {
		ProtocolRanges declared;
		for (const auto &p : listing.protocols) {
			if (auto v = ParseProtocolVersion(p.version)) {
				declared[p.name] = ProtocolRange {*v, *v};
			}
		}
		if (!declared.empty()) {
			ranges = std::move(declared);
		}
	}
	VGI_LOG(context, "protocol.server_ranges",
	        {{"worker_path", ctx.params->worker_path()},
	         {"source", !advertised.empty() ? "vgi_rpc.protocol_versions" : "reflection listing"},
	         {"ranges", ranges ? FormatProtocolRanges(*ranges) : std::string("unknown")}});
	if (ranges) {
		ctx.params->protocol_session()->ObserveServerRanges(*ranges);
	}
}

[[noreturn]] void ThrowDisjointProtocol(const ProtocolNegotiation &negotiation, const std::string &worker_path) {
	const std::string name(VGI_MAIN_PROTOCOL.name);
	ErrorInfo info;
	info.Set(error_key::kErrorKind, kProtocolVersionUnsupportedKind)
	    .Set("client_protocol_versions", name + "=" + negotiation.client.ToString())
	    .Set("server_protocol_versions",
	         negotiation.server ? name + "=" + negotiation.server->ToString() : std::string())
	    .Worker(worker_path, -1, "");
	throw InvalidInputException(info, DisjointProtocolMessage(name, negotiation, worker_path));
}

} // namespace

// Typed catalog RPCs

std::vector<VgiCatalogInfo> InvokeCatalogs(const std::string &worker_path, ClientContext &context,
                                            bool worker_debug, bool use_pool,
	                                            const std::shared_ptr<CatalogAuth> &auth,
	                                            std::optional<int64_t> launcher_idle_timeout_seconds,
	                                            std::optional<std::string> launcher_state_dir,
	                                            std::shared_ptr<void> worker_artifact_anchor,
	                                            const std::string &tcp_proxy,
	                                            std::shared_ptr<IrohClientConfig> iroh) {
	// Launcher overrides must reach temp_params (see the declaration).
	VgiAttachParametersConfig temp_cfg;
	temp_cfg.worker_path = worker_path;
	temp_cfg.worker_debug = worker_debug;
	temp_cfg.use_pool = use_pool;
	temp_cfg.auth = auth;
	temp_cfg.iroh = std::move(iroh);
	temp_cfg.launcher_idle_timeout_seconds = launcher_idle_timeout_seconds;
	temp_cfg.launcher_state_dir = launcher_state_dir;
	temp_cfg.worker_artifact_anchor = std::move(worker_artifact_anchor);
	temp_cfg.tcp_proxy = tcp_proxy;
	auto temp_params = std::make_shared<VgiAttachParameters>(std::move(temp_cfg));
	CatalogRpcContext ctx{temp_params, {}, {}};
	auto response = InvokeRpcMethod(ctx, "catalog_catalogs", nullptr, context);
	auto result_batch = ExtractAndDeserializeResult(response, "catalog_catalogs", worker_path);
	if (!result_batch) {
		return {};
	}

	// CatalogsResponse = {items: List<Binary>} — each item is an IPC-serialized
	// CatalogInfo batch. Both outer and item schemas are validated by the
	// schema registry; see vgi-python/vgi/catalog/catalog_interface.py:CatalogInfo.
	auto info_batches = UnwrapAndValidateItems(result_batch, "catalog_catalogs", worker_path);
	std::vector<VgiCatalogInfo> out;
	out.reserve(info_batches.size());
	for (const auto &info_batch : info_batches) {
		auto wire = generated::Decode<generated::CatalogInfo>(info_batch, 0, worker_path);
		VgiCatalogInfo info;
		info.name = std::move(wire.name);
		info.implementation_version = wire.implementation_version.value_or("");
		info.data_version_spec = wire.data_version_spec.value_or("");
		for (const auto &spec_bytes : wire.attach_option_specs) {
			info.attach_option_specs.push_back(ParseAttachOptionSpec(spec_bytes, worker_path, context));
		}
		for (auto &release : wire.releases) {
			VgiCatalogReleaseInfo r;
			r.version = std::move(release.version);
			r.released_at_us = release.released_at.value_or(0);
			r.summary = std::move(release.summary);
			r.notes_url = release.notes_url.value_or("");
			info.releases.push_back(std::move(r));
		}
		info.source_url = wire.source_url.value_or("");

		out.push_back(std::move(info));
	}
	return out;
}

CatalogAttachResult InvokeCatalogAttach(const std::string &worker_path, const std::string &catalog_name,
                                        ClientContext &context, bool worker_debug, bool use_pool,
                                        const std::shared_ptr<CatalogAuth> &auth,
                                        const std::string &data_version_spec,
                                        const std::string &implementation_version,
                                        const std::shared_ptr<SessionCookieJar> &cookie_jar,
                                        const std::map<std::string, Value> &attach_options,
	                                    std::optional<int64_t> launcher_idle_timeout_seconds,
	                                    std::optional<std::string> launcher_state_dir,
	                                    std::shared_ptr<void> worker_artifact_anchor,
	                                    const std::string &tcp_proxy,
	                                    std::shared_ptr<IrohClientConfig> iroh,
	                                    std::shared_ptr<ProtocolSession> *session_out) {
	// Launcher overrides must reach temp_params, as in InvokeCatalogs.
	VgiAttachParametersConfig temp_cfg;
	temp_cfg.worker_path = worker_path;
	temp_cfg.worker_debug = worker_debug;
	temp_cfg.use_pool = use_pool;
	temp_cfg.auth = auth;
	temp_cfg.data_version_spec = data_version_spec;
	temp_cfg.implementation_version = implementation_version;
	temp_cfg.cookie_jar = cookie_jar;
	temp_cfg.iroh = std::move(iroh);
	temp_cfg.launcher_idle_timeout_seconds = launcher_idle_timeout_seconds;
	temp_cfg.launcher_state_dir = launcher_state_dir;
	temp_cfg.worker_artifact_anchor = std::move(worker_artifact_anchor);
	temp_cfg.tcp_proxy = tcp_proxy;
	auto temp_params = std::make_shared<VgiAttachParameters>(std::move(temp_cfg));
	CatalogRpcContext ctx{temp_params, {}, {}};

	std::vector<uint8_t> options_ipc_bytes;
	if (!attach_options.empty()) {
		auto options_batch = BuildSettingsBatch(context, attach_options);
		options_ipc_bytes = SerializeToIpcBytes(options_batch);
	}

	const bool query_offered = IsVgiQueryEnabled(context);
	auto request_batch = BuildCatalogAttachRequest(catalog_name, options_ipc_bytes, data_version_spec, implementation_version, query_offered);
	auto request_bytes = SerializeToIpcBytes(request_batch);
	auto params = generated::BuildCatalogAttachParams(request_bytes);

	auto &session = *temp_params->protocol_session();
	const std::string main_protocol(VGI_MAIN_PROTOCOL.name);
	if (!HeaderTransport(worker_path)) {
		LearnRangesFromReflection(ctx, context);
	}
	// Before anything else when the ranges are already known (byte transports);
	// over HTTP they arrive with the first response, so this is ASSUMED here.
	auto negotiation = session.Negotiate(main_protocol);
	if (negotiation.outcome == ProtocolNegotiation::Outcome::DISJOINT) {
		ThrowDisjointProtocol(negotiation, worker_path);
	}

	UnaryResponseResult response;
	try {
		response = InvokeRpcMethod(ctx, "catalog_attach", params, context);
	} catch (const InterruptException &) {
		throw;
	} catch (const std::exception &e) {
		// What the refusal (or, over HTTP, any response's header) says the
		// worker speaks.
		if (auto refused = RangesFromRefusal(e)) {
			session.ObserveServerRanges(*refused);
		}
		const auto sent = session.Version(main_protocol);
		negotiation = session.Negotiate(main_protocol);
		if (negotiation.outcome == ProtocolNegotiation::Outcome::DISJOINT) {
			ThrowDisjointProtocol(negotiation, worker_path);
		}
		if (!IsProtocolVersionRefusal(e) || negotiation.outcome != ProtocolNegotiation::Outcome::AGREED ||
		    negotiation.version == sent) {
			// Not a version problem, or nothing better to offer: the worker's
			// own error stands.
			throw;
		}
		// catalog_attach is idempotent: one retry at the negotiated version.
		VGI_LOG(context, "protocol.retry_negotiated",
		        {{"worker_path", worker_path},
		         {"protocol", main_protocol},
		         {"refused", sent.ToString()},
		         {"retry", negotiation.version.ToString()}});
		response = InvokeRpcMethod(ctx, "catalog_attach", params, context);
	}
	// Record what was agreed (HTTP learns the ranges on that first response).
	negotiation = session.Negotiate(main_protocol);
	VGI_LOG(context, "protocol.negotiated",
	        {{"worker_path", worker_path},
	         {"protocol", main_protocol},
	         {"version", session.Version(main_protocol).ToString()},
	         {"server", negotiation.server ? negotiation.server->ToString() : std::string("unknown")}});
	auto result_batch = ExtractAndDeserializeResult(response, "catalog_attach", worker_path);
	if (!result_batch) {
		throw IOException(CatalogProtocolViolation("catalog_attach", worker_path), "Empty response from catalog_attach [worker: %s]", worker_path);
	}
	auto result = ParseCatalogAttachResult(result_batch, worker_path, context, query_offered);
	if (session_out) {
		*session_out = temp_params->protocol_session();
	}
	return result;
}

void InvokeCatalogDetach(const CatalogRpcContext &ctx, ClientContext &context) {
	InvokeVoidRpc(ctx, "catalog_detach", generated::BuildCatalogDetachParams(ctx.attach_opaque_data), context);
}

VgiQueryPrepareResult InvokeCatalogQueryPrepare(const CatalogRpcContext &ctx,
                                              const VgiQueryCapabilities &capabilities, const std::string &sql,
                                              const VgiSchemaPtr &parameter_schema,
                                              ClientContext &context, const std::string &required_ordering) {
    if (!IsVgiQueryEnabled(context)) throw InvalidInputException("vgi_query_enabled must be true before preparing a remote query");
    auto request = SerializeToIpcBytes(BuildVgiQueryPrepareRequest(ctx, capabilities, sql, parameter_schema, required_ordering));
    if (request.size() > VGI_QUERY_MAX_RECORD_BYTES) throw InvalidInputException("VGI query request is too large");
    auto response = InvokeRpcMethod(ctx, "catalog_query_prepare", generated::BuildCatalogQueryPrepareParams(request), context);
    if (!response.batch ||
        response.batch->num_rows() != 1 || response.batch->num_columns() != 1 ||
        response.batch->column_name(0) != "result" || response.batch->column(0).type().type != NANOARROW_TYPE_BINARY) {
        throw IOException("Malformed catalog_query_prepare response envelope");
    }
    ValidateWorkerBatch(response.batch.get(), WorkerBatchValidation::FULL, ctx.params->worker_path());
    auto payload = response.batch->column(0);
    if (payload.IsNull(0) || payload.GetView(0).size() > VGI_QUERY_MAX_RECORD_BYTES) {
        throw IOException("Invalid catalog_query_prepare response size");
    }
    RecordBatchSingleRow envelope(response.batch, 0, "catalog_query_prepare response", ctx.params->worker_path());
    auto batch = DeserializeVgiQueryRecord(envelope["result"].value_not_null<std::vector<uint8_t>>());
    try {
        ValidateResponseSchema(batch, "catalog_query_prepare", ctx.params->worker_path());
        return ParseVgiQueryPrepareResult(batch, capabilities, required_ordering, QueryEpochMillisNow(), ctx.params->worker_path());
    } catch (...) {
        // A valid token can already own a preparation even when another result
        // field is malformed. Release best-effort without hiding the primary error.
        try {
            RecordBatchSingleRow row(batch, 0, "CatalogQueryPrepareResult", ctx.params->worker_path());
            auto token = row["prepared_query_token"].as<std::vector<uint8_t>>();
            if (token && !token->empty() && token->size() <= VGI_QUERY_MAX_TOKEN_BYTES) {
                InvokeCatalogQueryRelease(ctx, *token, context);
            }
        } catch (...) { }
        throw;
    }
}

void InvokeCatalogQueryRelease(const CatalogRpcContext &ctx, const std::vector<uint8_t> &prepared_query_token,
                               ClientContext &context) {
    if (ctx.attach_opaque_data.empty() || ctx.attach_opaque_data.size() > VGI_QUERY_MAX_TOKEN_BYTES ||
        prepared_query_token.empty() || prepared_query_token.size() > VGI_QUERY_MAX_TOKEN_BYTES) {
        throw InvalidInputException("Invalid VGI query attachment or preparation token");
    }
    InvokeVoidRpc(ctx, "catalog_query_release",
                  generated::BuildCatalogQueryReleaseParams(ctx.attach_opaque_data, prepared_query_token, OptTxn(ctx)), context);
}

std::vector<VgiSchemaInfo> InvokeCatalogSchemas(const CatalogRpcContext &ctx, ClientContext &context) {
	auto &worker_path = ctx.params->worker_path();
	auto params = generated::BuildCatalogSchemasParams(ctx.attach_opaque_data, OptTxn(ctx));
	auto response = InvokeRpcMethod(ctx, "catalog_schemas", params, context);
	auto result_batch = ExtractAndDeserializeResult(response, "catalog_schemas", worker_path);
	if (!result_batch) {
		return {};
	}

	auto info_batches = UnwrapAndValidateItems(result_batch, "catalog_schemas", worker_path);
	std::vector<VgiSchemaInfo> schemas;
	schemas.reserve(info_batches.size());
	for (const auto &info_batch : info_batches) {
		schemas.push_back(ParseSchemaInfo(info_batch, worker_path));
	}
	return schemas;
}

namespace {

// A (non-dictionary) column of exactly nanoarrow type `type`.
bool HasType(const VgiColumn &column, int type) {
	return column && column.type().type == type;
}

// Binary or utf8 only — not the large or view variants.
bool IsBinaryOrUtf8(const VgiColumn &column) {
	if (!column) {
		return false;
	}
	const auto type = column.type().type;
	return type == NANOARROW_TYPE_BINARY || type == NANOARROW_TYPE_STRING;
}

// Cell `i` of binary column `values` as a zero-copy buffer that keeps `owner`
// (the batch holding `values`) alive.
VgiBufferPtr BinaryCell(const VgiBatchPtr &owner, const VgiColumn &values, int64_t i) {
	auto view = values.GetView(i);
	return std::make_shared<const VgiBuffer>(owner, reinterpret_cast<const uint8_t *>(view.data()),
	                                         static_cast<int64_t>(view.size()));
}

struct ContentsCategory {
	const char *column;
	const char *items_method;
};

const ContentsCategory kContentsCategories[] = {
    {"schemas", "catalog_schemas"},
    {"tables", "catalog_schema_contents_tables"},
    {"views", "catalog_schema_contents_views"},
    {"functions", "catalog_schema_contents_functions"},
    {"macros", "catalog_schema_contents_macros"},
    {"indexes", "catalog_schema_contents_indexes"},
};

// Open each nested stream once and retain the response backing buffers.
std::vector<VgiBatchPtr> ReadContentsStream(const VgiBatchPtr &owner, const VgiColumn &values,
                                           const ContentsCategory &category, const std::string &worker_path) {
	std::vector<VgiBatchPtr> batches;
	if (values.IsNull(0)) {
		return batches;
	}
	auto violation = CatalogProtocolViolation("catalog_contents", worker_path).Set(error_key::kField, category.column);
	auto input = std::make_shared<VgiBufferReader>(BinaryCell(owner, values, 0));
	auto reader = VgiIpcReader::Open(input);
	const auto *contract = LookupResponseSchema(category.items_method);
	if (!contract || !contract->item_schema) {
		throw InternalException("Missing catalog metadata schema for %s", category.column);
	}
	if (!reader->schema()->Equals(*contract->item_schema, /*check_metadata=*/false)) {
		throw IOException(violation, "catalog_contents: '%s' stream has an incompatible metadata schema", category.column);
	}
	while (auto batch = reader->ReadNext()) {
		ValidateWorkerBatch(batch.get(), WorkerBatchValidation::FULL, worker_path);
		for (int column = 0; column < batch->num_columns(); column++) {
			if (batch->schema()->field_nullable(column)) {
				continue;
			}
			auto values = batch->column(column);
			for (int64_t row = 0; row < batch->num_rows(); row++) {
				wire::Ctx ctx {"catalog_contents", &worker_path, batch->column_name(column), row};
				if (wire::IsNullValue(values, row, ctx)) {
					throw IOException(violation, "catalog_contents: required '%s.%s' contains nulls", category.column,
					                  batch->column_name(column));
				}
			}
		}
		batches.push_back(std::move(batch));
	}
	if (input->remaining() != 0) {
		throw IOException(violation, "catalog_contents: trailing bytes after '%s' IPC stream", category.column);
	}
	return batches;
}

std::vector<std::string> ContentsPath(const VgiBatchPtr &batch, int64_t row,
                                    const char *field, const std::string &worker_path) {
	auto values = batch->GetColumnByName(field);
	auto parts = values.child(0);
	std::vector<std::string> path;
	int64_t begin = 0, end = 0;
	values.ListRange(row, begin, end);
	for (int64_t i = begin; i < end; i++) {
		if (parts.IsNull(i) || parts.GetView(i).empty()) {
			throw IOException(CatalogProtocolViolation("catalog_contents", worker_path).Set(error_key::kField, field),
			                  "catalog_contents: schema path contains a null or empty component");
		}
		path.push_back(parts.GetString(i));
	}
	if (path.empty()) {
		throw IOException(CatalogProtocolViolation("catalog_contents", worker_path).Set(error_key::kField, field),
		                  "catalog_contents: schema path must not be empty");
	}
	return path;
}

const std::vector<VgiCatalogItemRef> &ContentsItems(const VgiSchemaContents &contents, const std::string &kind) {
	static const std::vector<VgiCatalogItemRef> empty;
	auto found = contents.items.find(kind);
	return found == contents.items.end() ? empty : found->second;
}

} // namespace

bool UseCatalogContents(ClientContext &context) {
	Value val;
	if (context.TryGetCurrentSetting("vgi_catalog_contents", val) && !val.IsNull()) {
		return val.GetValue<bool>();
	}
	return true;
}

VgiCatalogContents InvokeCatalogContents(const CatalogRpcContext &ctx, ClientContext &context,
                                         const std::optional<std::string> &if_none_match) {
	auto &worker_path = ctx.params->worker_path();
	auto params = generated::BuildCatalogContentsParams(ctx.attach_opaque_data, if_none_match);
	auto response = InvokeRpcMethod(ctx, "catalog_contents", params, context);
	auto result_batch = ExtractAndDeserializeResult(response, "catalog_contents", worker_path);
	return ParseCatalogContents(result_batch, worker_path, if_none_match);
}

VgiCatalogContents ParseCatalogContents(const VgiBatchPtr &result_batch,
                                         const std::string &worker_path,
                                         const std::optional<std::string> &if_none_match) {
	ValidateResponseSchema(result_batch, "catalog_contents", worker_path);
	if (!result_batch || result_batch->num_rows() != 1) {
		throw IOException(CatalogProtocolViolation("catalog_contents", worker_path),
		                  "catalog_contents must contain exactly one response row [worker: %s]", worker_path);
	}
	ValidateWorkerBatch(result_batch.get(), WorkerBatchValidation::FULL, worker_path);
	for (int column = 0; column < result_batch->num_columns(); column++) {
		if (!result_batch->schema()->field_nullable(column) && result_batch->column(column).IsNull(0)) {
			throw IOException(CatalogProtocolViolation("catalog_contents", worker_path),
			                  "catalog_contents required field '%s' is null", result_batch->column_name(column));
		}
	}

	VgiCatalogContents result;
	auto version_col = result_batch->GetColumnByName("catalog_version");
	result.catalog_version = version_col.GetInt(0);
	auto etag_col = result_batch->GetColumnByName("etag");
	if (!etag_col.IsNull(0)) {
		result.etag = etag_col.GetString(0);
	}
	auto not_modified_col = result_batch->GetColumnByName("not_modified");
	result.not_modified = !not_modified_col.IsNull(0) && not_modified_col.GetBool(0);
	if (result.not_modified) {
		// Only meaningful as the answer to a matching if_none_match.
		if (!if_none_match || result.etag != if_none_match) {
			throw IOException(CatalogProtocolViolation("catalog_contents", worker_path).Set(error_key::kField, "not_modified"),
			                  "catalog_contents answered not_modified without a matching if_none_match [worker: %s]",
			                  worker_path);
		}
		for (auto &category : kContentsCategories) {
			if (!result_batch->GetColumnByName(category.column).IsNull(0)) {
				throw IOException(CatalogProtocolViolation("catalog_contents", worker_path),
				                  "catalog_contents not_modified response contains '%s' metadata", category.column);
			}
		}
		return result;
	}

	std::map<std::vector<std::string>, size_t> schema_index;
	for (auto &category : kContentsCategories) {
		auto values = result_batch->GetColumnByName(category.column);
		for (auto &batch : ReadContentsStream(result_batch, values, category, worker_path)) {
			const bool is_schema = std::string(category.column) == "schemas";
			for (int64_t row_idx = 0; row_idx < batch->num_rows(); row_idx++) {
				auto path = ContentsPath(batch, row_idx, is_schema ? "path" : "schema_path", worker_path);
				auto path_key = path;
				for (auto &part : path_key) {
					part = StringUtil::Lower(part);
				}
				if (is_schema) {
					if (!schema_index.emplace(path_key, result.schemas.size()).second) {
						throw IOException(CatalogProtocolViolation("catalog_contents", worker_path),
						                  "catalog_contents contains a duplicate schema path");
					}
					if (path_key.size() > 1 &&
					    schema_index.find(std::vector<std::string>(path_key.begin(), path_key.end() - 1)) == schema_index.end()) {
						throw IOException(CatalogProtocolViolation("catalog_contents", worker_path),
						                  "catalog_contents schema appears before its parent");
					}
					VgiSchemaContents contents;
					contents.name = SchemaNameFromPath(path, "SchemaInfo.path");
					contents.path = std::move(path);
					contents.schema = {batch, row_idx};
					contents.worker_path = worker_path;
					result.schemas.push_back(std::move(contents));
					continue;
				}
				auto schema = schema_index.find(path_key);
				if (schema == schema_index.end()) {
					throw IOException(CatalogProtocolViolation("catalog_contents", worker_path).Set(error_key::kField, category.column),
					                  "catalog_contents '%s' metadata refers to an absent schema", category.column);
				}
				std::string kind = category.column;
				wire::RowReader row(batch, row_idx, category.column, worker_path);
				if (kind == "functions") {
					std::string type_string;
					row.Required("function_type", type_string);
					auto type = generated::FunctionTypeFromWire(type_string);
					if (!type) {
						throw IOException(CatalogProtocolViolation("catalog_contents", worker_path),
						                  "catalog_contents has unknown function_type '%s'", type_string);
					}
					kind = *type == generated::FunctionType::Scalar ? "SCALAR_FUNCTION"
					       : *type == generated::FunctionType::Aggregate ? "AGGREGATE_FUNCTION" : "TABLE_FUNCTION";
				} else if (kind == "macros") {
					std::string type;
					row.Required("macro_type", type);
					type = StringUtil::Upper(type);
					if (type != "SCALAR" && type != "TABLE") {
						throw IOException(CatalogProtocolViolation("catalog_contents", worker_path),
						                  "catalog_contents has unknown macro_type '%s'", type);
					}
					kind = type + "_MACRO";
				}
				result.schemas[schema->second].items[kind].push_back({batch, row_idx});
			}
		}
	}
	return result;
}

VgiSchemaInfo DecodeContentsSchemaInfo(const VgiSchemaContents &contents) {
	return ParseSchemaInfo(contents.schema.batch->Slice(contents.schema.row, 1), contents.worker_path);
}

std::vector<VgiTableInfo> DecodeContentsTables(const VgiSchemaContents &contents, ClientContext &context) {
	std::vector<VgiTableInfo> out;
	for (auto &item : ContentsItems(contents, "tables")) {
		out.push_back(ParseTableInfo(context, item.batch, item.row, contents.worker_path));
	}
	return out;
}

std::vector<VgiIndexInfo> DecodeContentsIndexes(const VgiSchemaContents &contents) {
	std::vector<VgiIndexInfo> out;
	for (const auto &item : ContentsItems(contents, "indexes"))
		out.push_back(ParseNativeIndexInfo(item.batch->Slice(item.row, 1)));
	return out;
}

std::vector<VgiIndexInfo> InvokeCatalogSchemaContentsIndexes(const CatalogRpcContext &ctx,
    const std::string &schema_name, ClientContext &context) {
	auto params = generated::BuildCatalogSchemaContentsIndexesParams(ctx.attach_opaque_data,
	    SingleSchemaPath(schema_name), OptTxn(ctx));
	auto response = InvokeRpcMethod(ctx, "catalog_schema_contents_indexes", params, context);
	auto result = ExtractAndDeserializeResult(response, "catalog_schema_contents_indexes", ctx.params->worker_path());
	if (!result) throw IOException("Missing typed index listing response");
	std::vector<VgiIndexInfo> out;
	for (const auto &batch : UnwrapAndValidateItems(result, "catalog_schema_contents_indexes", ctx.params->worker_path())) {
		auto info = ParseNativeIndexInfo(batch);
		if (info.schema_name != schema_name) throw IOException("Index response belongs to another schema");
		out.push_back(std::move(info));
	}
	return out;
}
std::optional<VgiIndexInfo> InvokeCatalogIndexGet(const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &name, ClientContext &context) {
	auto params = generated::BuildCatalogIndexGetParams(ctx.attach_opaque_data, SingleSchemaPath(schema_name), name, OptTxn(ctx));
	auto response = InvokeRpcMethod(ctx, "catalog_index_get", params, context);
	auto result = ExtractAndDeserializeResult(response, "catalog_index_get", ctx.params->worker_path());
	if (!result) throw IOException("Missing typed index lookup response");
	auto items = UnwrapAndValidateItems(result, "catalog_index_get", ctx.params->worker_path());
	if (items.empty()) return std::nullopt;
	if (items.size() != 1) throw IOException("Index lookup returned multiple records");
	auto info = ParseNativeIndexInfo(items[0]);
	if (info.schema_name != schema_name || !StringUtil::CIEquals(info.name, name))
		throw IOException("Index lookup returned another identity");
	return info;
}
void RequireCatalogIndexAlgorithm(const CatalogRpcContext &ctx, const std::string &schema,
    const std::string &table, const std::string &algorithm, ClientContext &context) {
	auto params = generated::BuildCatalogIndexOptionDefinitionsParams(ctx.attach_opaque_data,
	    BuildNativeIndexTableRef(schema, table), algorithm, OptTxn(ctx));
	auto response = InvokeRpcMethod(ctx, "catalog_index_option_definitions", params, context);
	auto result = ExtractAndDeserializeResult(response, "catalog_index_option_definitions", ctx.params->worker_path());
	ValidateNativeIndexOptionDefinitions(result);
}
void InvokeCatalogIndexCreate(const CatalogRpcContext &ctx, const std::string &schema_name,
    const std::string &name, const std::vector<uint8_t> &definition, const std::string &on_conflict,
    ClientContext &context) {
	auto request = BuildNativeIndexCreate(ctx.attach_opaque_data, ctx.transaction_opaque_data, schema_name, name,
	                                     definition, on_conflict);
	InvokeVoidRpc(ctx, "catalog_index_create", generated::BuildCatalogIndexCreateParams(request), context);
}
void InvokeCatalogIndexDrop(const CatalogRpcContext &ctx, const std::string &schema_name,
    const std::string &name, bool ignore_not_found, bool cascade, ClientContext &context) {
	InvokeVoidRpc(ctx, "catalog_index_drop", generated::BuildCatalogIndexDropParams(ctx.attach_opaque_data,
	    SingleSchemaPath(schema_name), name, ignore_not_found, cascade, OptTxn(ctx)), context);
}

std::vector<VgiViewInfo> DecodeContentsViews(const VgiSchemaContents &contents) {
	std::vector<VgiViewInfo> out;
	for (auto &item : ContentsItems(contents, "views")) {
		out.push_back(ParseViewInfo(item.batch->Slice(item.row, 1), contents.worker_path));
	}
	return out;
}

std::vector<VgiFunctionInfo> DecodeContentsFunctions(const VgiSchemaContents &contents,
                                                     const std::string &function_type) {
	if (function_type != "SCALAR_FUNCTION" && function_type != "AGGREGATE_FUNCTION" && function_type != "TABLE_FUNCTION") {
		throw InternalException("catalog_contents: unknown function type %s", function_type);
	}
	std::vector<VgiFunctionInfo> out;
	for (auto &item : ContentsItems(contents, function_type)) {
		out.push_back(ParseFunctionInfo(item.batch, item.row, contents.worker_path));
	}
	return out;
}

std::vector<VgiMacroInfo> DecodeContentsMacros(const VgiSchemaContents &contents, const std::string &macro_type) {
	if (macro_type != "SCALAR_MACRO" && macro_type != "TABLE_MACRO") {
		throw InternalException("catalog_contents: unknown macro type %s", macro_type);
	}
	std::vector<VgiMacroInfo> out;
	for (auto &item : ContentsItems(contents, macro_type)) {
		out.push_back(ParseMacroInfo(item.batch->Slice(item.row, 1), contents.worker_path));
	}
	return out;
}

std::vector<VgiTableInfo> InvokeCatalogSchemaContentsTables(const CatalogRpcContext &ctx,
                                                            const std::string &schema_name, ClientContext &context) {
	auto &worker_path = ctx.params->worker_path();
	auto params = generated::BuildCatalogSchemaContentsTablesParams(ctx.attach_opaque_data,
	                                                                SingleSchemaPath(schema_name), OptTxn(ctx));
	auto response = InvokeRpcMethod(ctx, "catalog_schema_contents_tables", params, context);
	auto result_batch = ExtractAndDeserializeResult(response, "catalog_schema_contents_tables", worker_path);
	if (!result_batch) {
		return {};
	}

	auto info_batches = UnwrapAndValidateItems(result_batch, "catalog_schema_contents_tables", worker_path);
	std::vector<VgiTableInfo> tables;
	tables.reserve(info_batches.size());
	for (const auto &info_batch : info_batches) {
		tables.push_back(ParseTableInfo(context, info_batch, worker_path));
	}
	return tables;
}

std::vector<VgiViewInfo> InvokeCatalogSchemaContentsViews(const CatalogRpcContext &ctx,
                                                          const std::string &schema_name, ClientContext &context) {
	auto &worker_path = ctx.params->worker_path();
	auto params = generated::BuildCatalogSchemaContentsViewsParams(ctx.attach_opaque_data,
	                                                               SingleSchemaPath(schema_name), OptTxn(ctx));
	auto response = InvokeRpcMethod(ctx, "catalog_schema_contents_views", params, context);
	auto result_batch = ExtractAndDeserializeResult(response, "catalog_schema_contents_views", worker_path);
	if (!result_batch) {
		return {};
	}

	auto info_batches = UnwrapAndValidateItems(result_batch, "catalog_schema_contents_views", worker_path);
	std::vector<VgiViewInfo> views;
	views.reserve(info_batches.size());
	for (const auto &info_batch : info_batches) {
		views.push_back(ParseViewInfo(info_batch, worker_path));
	}
	return views;
}

std::vector<VgiFunctionInfo> InvokeCatalogSchemaContentsFunctions(
    const CatalogRpcContext &ctx, const std::string &schema_name,
    const std::string &function_type, ClientContext &context) {
	auto &worker_path = ctx.params->worker_path();
	auto params = generated::BuildCatalogSchemaContentsFunctionsParams(
	    ctx.attach_opaque_data, SingleSchemaPath(schema_name), function_type, OptTxn(ctx));
	auto response = InvokeRpcMethod(ctx, "catalog_schema_contents_functions", params, context);
	auto result_batch = ExtractAndDeserializeResult(response, "catalog_schema_contents_functions", worker_path);
	if (!result_batch) {
		return {};
	}

	auto info_batches = UnwrapAndValidateItems(result_batch, "catalog_schema_contents_functions", worker_path);
	std::vector<VgiFunctionInfo> functions;
	functions.reserve(info_batches.size());
	for (const auto &info_batch : info_batches) {
		functions.push_back(ParseFunctionInfo(info_batch, 0, worker_path));
	}
	return functions;
}

std::vector<VgiMacroInfo> InvokeCatalogSchemaContentsMacros(
    const CatalogRpcContext &ctx, const std::string &schema_name,
    const std::string &macro_type, ClientContext &context) {
	auto &worker_path = ctx.params->worker_path();
	auto params = generated::BuildCatalogSchemaContentsMacrosParams(
	    ctx.attach_opaque_data, SingleSchemaPath(schema_name), macro_type, OptTxn(ctx));
	auto response = InvokeRpcMethod(ctx, "catalog_schema_contents_macros", params, context);
	auto result_batch = ExtractAndDeserializeResult(response, "catalog_schema_contents_macros", worker_path);
	if (!result_batch) {
		return {};
	}

	auto info_batches = UnwrapAndValidateItems(result_batch, "catalog_schema_contents_macros", worker_path);
	std::vector<VgiMacroInfo> macros;
	macros.reserve(info_batches.size());
	for (const auto &info_batch : info_batches) {
		macros.push_back(ParseMacroInfo(info_batch, worker_path));
	}
	return macros;
}

std::vector<VgiCopyFromFormatInfo> InvokeCatalogCopyFromFormats(const CatalogRpcContext &ctx, ClientContext &context) {
	auto &worker_path = ctx.params->worker_path();
	auto params = generated::BuildCatalogCopyFromFormatsParams(ctx.attach_opaque_data, OptTxn(ctx));
	auto response = InvokeRpcMethod(ctx, "catalog_copy_from_formats", params, context);
	auto result_batch = ExtractAndDeserializeResult(response, "catalog_copy_from_formats", worker_path);
	if (!result_batch) {
		return {};
	}

	auto info_batches = UnwrapAndValidateItems(result_batch, "catalog_copy_from_formats", worker_path);
	std::vector<VgiCopyFromFormatInfo> formats;
	formats.reserve(info_batches.size());
	for (const auto &info_batch : info_batches) {
		formats.push_back(ParseCopyFromFormatInfo(info_batch, worker_path));
	}
	return formats;
}

std::optional<VgiTableInfo> InvokeCatalogTableGet(const CatalogRpcContext &ctx,
                                                   const std::string &schema_name, const std::string &table_name,
                                                   ClientContext &context) {
	auto &worker_path = ctx.params->worker_path();
	// at_unit/at_value are always present (nullable) on the wire.
	auto params =
	    generated::BuildCatalogTableGetParams(ctx.attach_opaque_data, SingleSchemaPath(schema_name), table_name,
	                                          /*at_unit=*/std::nullopt, /*at_value=*/std::nullopt, OptTxn(ctx));
	auto response = InvokeRpcMethod(ctx, "catalog_table_get", params, context);
	auto result_batch = ExtractAndDeserializeResult(response, "catalog_table_get", worker_path);
	if (!result_batch) {
		return std::nullopt;
	}

	// Result is TablesResponse with items: list<binary> (0 or 1 items)
	auto info_batches = UnwrapAndValidateItems(result_batch, "catalog_table_get", worker_path);
	if (info_batches.empty()) {
		return std::nullopt;
	}
	return ParseTableInfo(context, info_batches[0], worker_path);
}

std::optional<VgiTableInfo> InvokeCatalogTableGet(const CatalogRpcContext &ctx,
                                                   const std::string &schema_name, const std::string &table_name,
                                                   ClientContext &context,
                                                   const std::string &at_unit, const std::string &at_value) {
	auto &worker_path = ctx.params->worker_path();
	auto params =
	    generated::BuildCatalogTableGetParams(ctx.attach_opaque_data, SingleSchemaPath(schema_name), table_name,
	                                          OptStrIfNonEmpty(at_unit), OptStrIfNonEmpty(at_value), OptTxn(ctx));
	auto response = InvokeRpcMethod(ctx, "catalog_table_get", params, context);
	auto result_batch = ExtractAndDeserializeResult(response, "catalog_table_get", worker_path);
	if (!result_batch) {
		return std::nullopt;
	}

	auto info_batches = UnwrapAndValidateItems(result_batch, "catalog_table_get", worker_path);
	if (info_batches.empty()) {
		return std::nullopt;
	}
	return ParseTableInfo(context, info_batches[0], worker_path);
}

std::optional<VgiViewInfo> InvokeCatalogViewGet(const CatalogRpcContext &ctx,
                                                 const std::string &schema_name, const std::string &view_name,
                                                 ClientContext &context) {
	auto &worker_path = ctx.params->worker_path();
	auto params = generated::BuildCatalogViewGetParams(ctx.attach_opaque_data, SingleSchemaPath(schema_name), view_name,
	                                                   OptTxn(ctx));
	auto response = InvokeRpcMethod(ctx, "catalog_view_get", params, context);
	auto result_batch = ExtractAndDeserializeResult(response, "catalog_view_get", worker_path);
	if (!result_batch) {
		return std::nullopt;
	}

	// Result is ViewsResponse with items: list<binary> (0 or 1 items)
	auto info_batches = UnwrapAndValidateItems(result_batch, "catalog_view_get", worker_path);
	if (info_batches.empty()) {
		return std::nullopt;
	}
	return ParseViewInfo(info_batches[0], worker_path);
}

VgiScanBranchesResult InvokeCatalogTableScanBranchesGet(
    const CatalogRpcContext &ctx, const std::string &schema_name,
    const std::string &table_name, ClientContext &context, const std::string &at_unit, const std::string &at_value,
    const std::optional<std::vector<uint8_t>> &pushdown_filters) {
	auto &worker_path = ctx.params->worker_path();
	auto params = generated::BuildCatalogTableScanBranchesGetParams(
	    ctx.attach_opaque_data, SingleSchemaPath(schema_name), table_name, OptStrIfNonEmpty(at_unit),
	    OptStrIfNonEmpty(at_value), OptTxn(ctx), pushdown_filters);
	UnaryResponseResult response;
	try {
		response = InvokeRpcMethod(ctx, "catalog_table_scan_branches_get", params, context);
	} catch (const VgiRpcException &e) {
		if (e.GetErrorKind() == error_kind::kMethodNotImplemented) {
			throw IOException(CatalogProtocolViolation("catalog_table_scan_branches_get", worker_path),
			                  "VGI worker does not implement catalog_table_scan_branches_get, which protocol 3.3 "
			                  "requires to scan a table (table '%s.%s') [worker: %s]. The single-function "
			                  "catalog_table_scan_function_get RPC was removed in protocol 3.3; the worker's SDK "
			                  "must serve the branches RPC.",
			                  schema_name, table_name, worker_path);
		}
		throw;
	}
	auto result_batch = ExtractAndDeserializeResult(response, "catalog_table_scan_branches_get", worker_path);
	if (!result_batch || result_batch->num_rows() == 0) {
		throw IOException(CatalogProtocolViolation("catalog_table_scan_branches_get", worker_path), "Empty response from catalog_table_scan_branches_get [worker: %s]", worker_path);
	}
	return ParseScanBranchesResult(context, result_batch, worker_path);
}

// Table write function get

static VgiWriteFunctionResult InvokeCatalogTableWriteFunctionGet(
    const CatalogRpcContext &ctx, const std::string &rpc_method,
    const std::string &schema_name, const std::string &table_name, ClientContext &context,
    const std::optional<std::string> &writable_branch_function_name = std::nullopt) {
	auto &worker_path = ctx.params->worker_path();
	VgiBatchPtr params;
	if (rpc_method == "catalog_table_insert_function_get") {
		params =
		    generated::BuildCatalogTableInsertFunctionGetParams(ctx.attach_opaque_data, SingleSchemaPath(schema_name),
		                                                        table_name, OptTxn(ctx), writable_branch_function_name);
	} else if (rpc_method == "catalog_table_update_function_get") {
		params = generated::BuildCatalogTableUpdateFunctionGetParams(
		    ctx.attach_opaque_data, SingleSchemaPath(schema_name), table_name, OptTxn(ctx));
	} else {
		params = generated::BuildCatalogTableDeleteFunctionGetParams(
		    ctx.attach_opaque_data, SingleSchemaPath(schema_name), table_name, OptTxn(ctx));
	}
	auto response = InvokeRpcMethod(ctx, rpc_method, params, context);
	auto result_batch = ExtractAndDeserializeResult(response, rpc_method, worker_path);
	if (!result_batch || result_batch->num_rows() == 0) {
		throw IOException(CatalogProtocolViolation(rpc_method, worker_path), "Empty response from %s [worker: %s]", rpc_method, worker_path);
	}
	return ParseScanFunctionResult(context, result_batch, worker_path);
}

VgiWriteFunctionResult InvokeCatalogTableInsertFunctionGet(
    const CatalogRpcContext &ctx, const std::string &schema_name,
    const std::string &table_name, ClientContext &context,
    const std::optional<std::string> &writable_branch_function_name) {
	return InvokeCatalogTableWriteFunctionGet(ctx, "catalog_table_insert_function_get",
	                                          schema_name, table_name, context,
	                                          writable_branch_function_name);
}

VgiWriteFunctionResult InvokeCatalogTableUpdateFunctionGet(
    const CatalogRpcContext &ctx, const std::string &schema_name,
    const std::string &table_name, ClientContext &context) {
	return InvokeCatalogTableWriteFunctionGet(ctx, "catalog_table_update_function_get",
	                                          schema_name, table_name, context);
}

VgiWriteFunctionResult InvokeCatalogTableDeleteFunctionGet(
    const CatalogRpcContext &ctx, const std::string &schema_name,
    const std::string &table_name, ClientContext &context) {
	return InvokeCatalogTableWriteFunctionGet(ctx, "catalog_table_delete_function_get",
	                                          schema_name, table_name, context);
}

// Transaction lifecycle

int64_t InvokeCatalogVersion(const CatalogRpcContext &ctx, ClientContext &context) {
	auto &worker_path = ctx.params->worker_path();
	auto params = generated::BuildCatalogVersionParams(ctx.attach_opaque_data, OptTxn(ctx));
	try {
		auto response = InvokeRpcMethod(ctx, "catalog_version", params, context);
		auto result_batch = ExtractAndDeserializeResult(response, "catalog_version", worker_path);
		if (!result_batch || result_batch->num_rows() == 0) {
			return 0;  // Not implemented by worker
		}
		return generated::Decode<generated::CatalogVersionResponse>(result_batch, 0, worker_path).version;
	} catch (const VgiRpcException &e) {
		// Only method_not_implemented maps to 0 ("unknown version": the caller
		// clears its cache). Interrupts, auth, transport and worker failures
		// must reach the user, not turn into a silent cache clear.
		if (e.GetErrorKind() == error_kind::kMethodNotImplemented) {
			return 0;
		}
		throw;
	}
}

std::vector<uint8_t> InvokeCatalogTransactionBegin(const CatalogRpcContext &ctx, ClientContext &context) {
	auto &worker_path = ctx.params->worker_path();
	auto params = generated::BuildCatalogTransactionBeginParams(ctx.attach_opaque_data);
	auto response = InvokeRpcMethod(ctx, "catalog_transaction_begin", params, context);
	auto result_batch = ExtractAndDeserializeResult(response, "catalog_transaction_begin", worker_path);
	if (!result_batch || result_batch->num_rows() == 0) {
		return {};  // Transaction not supported
	}

	return generated::Decode<generated::TransactionBeginResponse>(result_batch, 0, worker_path)
	    .transaction_opaque_data.value_or(generated::Bytes {});
}

void InvokeCatalogTransactionCommit(const CatalogRpcContext &ctx, ClientContext &context) {
	auto params = generated::BuildCatalogTransactionCommitParams(ctx.attach_opaque_data, ctx.transaction_opaque_data);
	InvokeVoidRpc(ctx, "catalog_transaction_commit", params, context);
}

void InvokeCatalogTransactionRollback(const CatalogRpcContext &ctx, ClientContext &context) {
	auto params = generated::BuildCatalogTransactionRollbackParams(ctx.attach_opaque_data, ctx.transaction_opaque_data);
	InvokeVoidRpc(ctx, "catalog_transaction_rollback", params, context);
}

// DDL

bool InvokeCatalogTableCreate(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const VgiSchemaPtr &columns_schema,
    const std::string &on_conflict,
    const std::vector<std::vector<uint8_t>> &constraint_definitions,
    ClientContext &context) {
	ValidateNativeColumnMetadata(columns_schema);
	auto request_batch =
	    BuildTableCreateRequest(ctx.attach_opaque_data, SingleSchemaPath(schema_name), table_name, columns_schema,
	                            on_conflict, constraint_definitions, ctx.transaction_opaque_data);
	auto request_bytes = SerializeToIpcBytes(request_batch);
	auto params = generated::BuildCatalogTableCreateParams(request_bytes);
	// `created` is required: a CTAS appends to the table only when this call
	// created it.
	const auto &worker_path = ctx.params->worker_path();
	auto response = InvokeRpcMethod(ctx, "catalog_table_create", params, context);
	auto result_batch = ExtractAndDeserializeResult(response, "catalog_table_create", worker_path);
	if (!result_batch || result_batch->num_rows() == 0) {
		throw IOException(CatalogProtocolViolation("catalog_table_create", worker_path),
		                  "VGI worker sent no catalog_table_create result for table '%s.%s'; protocol 3.3 requires "
		                  "'created: bool not null' [worker: %s]",
		                  schema_name, table_name, worker_path);
	}
	return generated::Decode<generated::TableCreateResponse>(result_batch, 0, worker_path).created;
}

void InvokeCatalogTableConstraintsApply(const CatalogRpcContext &ctx, const VgiTableInfo &table,
                                      const std::vector<uint8_t> &change, ClientContext &context) {
	auto request = BuildNativeConstraintsApply(ctx.attach_opaque_data, ctx.transaction_opaque_data, table, change);
	auto params = generated::BuildCatalogTableConstraintsApplyParams(request);
	auto response = InvokeRpcMethod(ctx, "catalog_table_constraints_apply", params, context);
	if (!response.batch || response.batch->num_rows() != 1 ||
	    !response.batch->GetColumnByName("result") ||
	    response.batch->GetColumnByName("result").type().type != NANOARROW_TYPE_BINARY) {
		throw IOException("Malformed catalog_table_constraints_apply response envelope");
	}
	RecordBatchSingleRow row(response.batch, 0, "ConstraintsApplyResponse", ctx.params->worker_path());
	ValidateNativeConstraintSnapshot(row["result"].value_not_null<std::vector<uint8_t>>(), table);
}

void InvokeCatalogTableDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    bool ignore_not_found, bool cascade,
    ClientContext &context) {
	auto params = generated::BuildCatalogTableDropParams(ctx.attach_opaque_data, SingleSchemaPath(schema_name),
	                                                     table_name, ignore_not_found, cascade, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_table_drop", params, context);
}

void InvokeCatalogTableRename(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &new_name, bool ignore_not_found,
    ClientContext &context) {
	auto params = generated::BuildCatalogTableRenameParams(ctx.attach_opaque_data, SingleSchemaPath(schema_name),
	                                                       table_name, new_name, ignore_not_found, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_table_rename", params, context);
}

void InvokeCatalogTableColumnAdd(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const VgiSchemaPtr &column_definition,
    bool if_column_not_exists,
    ClientContext &context) {
	ValidateNativeColumnMetadata(column_definition);
	auto column_bytes = SerializeSchemaToIpcBytes(column_definition);
	auto params = generated::BuildCatalogTableColumnAddParams(
	    ctx.attach_opaque_data, SingleSchemaPath(schema_name), table_name,
	    std::vector<std::string> {column_definition->field_name(0)}, column_bytes,
	    /*ignore_not_found=*/false, if_column_not_exists, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_table_column_add", params, context);
}

void InvokeCatalogTableColumnDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name, bool if_column_exists, bool cascade,
    ClientContext &context) {
	auto params = generated::BuildCatalogTableColumnDropParams(
	    ctx.attach_opaque_data, SingleSchemaPath(schema_name), table_name, std::vector<std::string> {column_name},
	    /*ignore_not_found=*/false, if_column_exists, cascade, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_table_column_drop", params, context);
}

void InvokeCatalogTableColumnRename(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &old_column_name, const std::string &new_column_name,
    ClientContext &context) {
	auto params = generated::BuildCatalogTableColumnRenameParams(ctx.attach_opaque_data, SingleSchemaPath(schema_name),
	                                                             table_name, std::vector<std::string> {old_column_name}, new_column_name,
	                                                             /*ignore_not_found=*/false, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_table_column_rename", params, context);
}

void InvokeCatalogTableCommentSet(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &comment, bool comment_is_null,
    bool ignore_not_found,
    ClientContext &context) {
	auto params = generated::BuildCatalogTableCommentSetParams(ctx.attach_opaque_data, SingleSchemaPath(schema_name),
	                                                           table_name, OptStrNullable(comment, comment_is_null),
	                                                           ignore_not_found, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_table_comment_set", params, context);
}

void InvokeCatalogTableColumnCommentSet(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name,
    const std::string &comment, bool comment_is_null,
    bool ignore_not_found,
    ClientContext &context) {
	auto params = generated::BuildCatalogTableColumnCommentSetParams(
	    ctx.attach_opaque_data, SingleSchemaPath(schema_name), table_name, std::vector<std::string> {column_name},
	    OptStrNullable(comment, comment_is_null), ignore_not_found, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_table_column_comment_set", params, context);
}

void InvokeCatalogTableColumnTypeChange(const CatalogRpcContext &ctx, const std::string &schema_name,
                                        const std::string &table_name,
                                        const VgiSchemaPtr &column_definition,
                                        const std::optional<std::vector<uint8_t>> &expression, ClientContext &context) {
	auto column_bytes = SerializeSchemaToIpcBytes(column_definition);
	auto params = generated::BuildCatalogTableColumnTypeChangeParams(
	    ctx.attach_opaque_data, SingleSchemaPath(schema_name), table_name,
	    std::vector<std::string> {column_definition->field_name(0)}, column_bytes, expression,
	    /*ignore_not_found=*/false, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_table_column_type_change", params, context);
}

void InvokeCatalogTableColumnDefaultSet(const CatalogRpcContext &ctx, const std::string &schema_name,
                                        const std::string &table_name, const std::string &column_name,
                                        const std::vector<uint8_t> &expression, ClientContext &context) {
	auto params = generated::BuildCatalogTableColumnDefaultSetParams(
	    ctx.attach_opaque_data, SingleSchemaPath(schema_name), table_name, std::vector<std::string> {column_name}, expression,
	    /*ignore_not_found=*/false, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_table_column_default_set", params, context);
}

void InvokeCatalogTableColumnDefaultDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name,
    ClientContext &context) {
	auto params = generated::BuildCatalogTableColumnDefaultDropParams(
	    ctx.attach_opaque_data, SingleSchemaPath(schema_name), table_name, std::vector<std::string> {column_name},
	    /*ignore_not_found=*/false, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_table_column_default_drop", params, context);
}

void InvokeCatalogTableNotNullSet(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name,
    ClientContext &context) {
	auto params = generated::BuildCatalogTableNotNullSetParams(ctx.attach_opaque_data, SingleSchemaPath(schema_name),
	                                                           table_name, std::vector<std::string> {column_name},
	                                                           /*ignore_not_found=*/false, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_table_not_null_set", params, context);
}

void InvokeCatalogTableNotNullDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name,
    ClientContext &context) {
	auto params = generated::BuildCatalogTableNotNullDropParams(ctx.attach_opaque_data, SingleSchemaPath(schema_name),
	                                                            table_name, std::vector<std::string> {column_name},
	                                                            /*ignore_not_found=*/false, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_table_not_null_drop", params, context);
}

// View DDL

void InvokeCatalogViewCreate(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &view_name,
    const std::string &definition, const std::vector<std::string> &aliases, const std::string &on_conflict,
    const std::optional<std::string> &comment, const std::map<std::string, std::string> &tags,
    const std::map<std::string, std::string> &column_comments,
    ClientContext &context) {
	auto params = generated::BuildCatalogViewCreateParams(ctx.attach_opaque_data, SingleSchemaPath(schema_name),
	                                                              view_name, definition, on_conflict, OptTxn(ctx), aliases, "regular", comment,
	                                                              std::vector<std::pair<std::string, std::string>>(tags.begin(), tags.end()),
	                                                              std::vector<std::pair<std::string, std::string>>(column_comments.begin(), column_comments.end()));
	InvokeVoidRpc(ctx, "catalog_view_create", params, context);
}

void InvokeCatalogViewDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &view_name,
    bool ignore_not_found, bool cascade,
    ClientContext &context) {
	auto params = generated::BuildCatalogViewDropParams(ctx.attach_opaque_data, SingleSchemaPath(schema_name),
	                                                    view_name, ignore_not_found, cascade, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_view_drop", params, context);
}

void InvokeCatalogViewRename(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &view_name,
    const std::string &new_name, bool ignore_not_found,
    ClientContext &context) {
	auto params = generated::BuildCatalogViewRenameParams(ctx.attach_opaque_data, SingleSchemaPath(schema_name),
	                                                      view_name, new_name, ignore_not_found, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_view_rename", params, context);
}

void InvokeCatalogViewColumnCommentSet(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &view_name, const std::string &column_name,
    const std::string &comment, bool comment_is_null, bool ignore_not_found,
    ClientContext &context) {
	auto params = generated::BuildCatalogViewColumnCommentSetParams(
	    ctx.attach_opaque_data, SingleSchemaPath(schema_name), view_name, {column_name},
	    OptStrNullable(comment, comment_is_null), ignore_not_found, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_view_column_comment_set", params, context);
}

void InvokeCatalogViewCommentSet(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &view_name,
    const std::string &comment, bool comment_is_null,
    bool ignore_not_found,
    ClientContext &context) {
	auto params = generated::BuildCatalogViewCommentSetParams(ctx.attach_opaque_data, SingleSchemaPath(schema_name),
	                                                          view_name, OptStrNullable(comment, comment_is_null),
	                                                          ignore_not_found, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_view_comment_set", params, context);
}

void InvokeCatalogSchemaCreate(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &on_conflict,
    const std::optional<std::string> &comment, const std::map<std::string, std::string> &tags,
    ClientContext &context) {
	auto params =
	    generated::BuildCatalogSchemaCreateParams(ctx.attach_opaque_data, SingleSchemaPath(schema_name), on_conflict,
	                                              comment, std::vector<std::pair<std::string, std::string>>(tags.begin(), tags.end()),
	                                              OptTxn(ctx), /*options=*/std::nullopt);
	InvokeVoidRpc(ctx, "catalog_schema_create", params, context);
}

void InvokeCatalogSchemaDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, bool ignore_not_found, bool cascade,
    ClientContext &context) {
	auto params = generated::BuildCatalogSchemaDropParams(ctx.attach_opaque_data, SingleSchemaPath(schema_name),
	                                                      ignore_not_found, cascade, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_schema_drop", params, context);
}

// Table function cardinality

TableFunctionCardinalityResult InvokeTableFunctionCardinality(
    const CatalogRpcContext &ctx, const std::vector<uint8_t> &bind_request_bytes,
    const std::vector<uint8_t> &bind_opaque_data, ClientContext &context) {
	auto &worker_path = ctx.params->worker_path();

	auto request_batch = BuildTableFunctionCardinalityRequest(bind_request_bytes, bind_opaque_data);
	auto request_bytes = SerializeToIpcBytes(request_batch);

	// Wrapped as {request: binary}.
	auto params = generated::BuildTableFunctionCardinalityParams(request_bytes);

	auto response = InvokeRpcMethod(ctx, "table_function_cardinality", params, context);
	auto result_batch = ExtractAndDeserializeResult(response, "table_function_cardinality", worker_path);
	if (!result_batch) {
		return {};  // Unknown cardinality
	}
	return ParseTableFunctionCardinalityResult(result_batch, worker_path);
}

// Scan planning (splits)

// `splits` and `scope` are required. An EMPTY splits list is legal: a
// fully-pruned scan plans zero splits and yields an empty result.
VgiScanPlanPage ParseVgiScanPlanPage(const VgiBatchPtr &batch,
                                     const std::string &worker_path) {
	VgiScanPlanPage page;
	auto violation = [&](const char *field) {
		return CatalogProtocolViolation("table_function_plan", worker_path).Set(error_key::kField, field);
	};
	if (!batch || batch->num_rows() != 1) {
		const auto rows = batch ? batch->num_rows() : 0;
		throw IOException(violation("result").ExpectedActual("1", std::to_string(rows)),
		                  "VGI worker '%s' returned a plan response with %lld rows; expected exactly one",
		                  worker_path, static_cast<long long>(rows));
	}

	// splits: list<binary>, one serialized ScanSplit per entry.
	auto list = batch->GetColumnByName("splits");
	if (!list || !HasType(list, NANOARROW_TYPE_LIST) || list.IsNull(0)) {
		throw IOException(violation("splits"),
		                  "VGI worker '%s' returned a plan without a non-null list<binary> 'splits' field",
		                  worker_path);
	}
	{
		auto values = list.child(0);
		if (!HasType(values, NANOARROW_TYPE_BINARY)) {
			throw IOException(violation("splits"),
			                  "VGI worker '%s' returned a plan whose 'splits' entries are not binary",
			                  worker_path);
		}
		int64_t start = 0;
		int64_t end = 0;
		list.ListRange(0, start, end);
		page.splits.reserve(static_cast<size_t>(end - start));
		for (auto i = start; i < end; i++) {
			if (values.IsNull(i)) {
				throw IOException(violation("splits"),
				                  "VGI worker '%s' returned a null entry in plan 'splits'", worker_path);
			}
			// The split cell stays zero-copy; only the token and optional key
			// tuple are copied out.
			auto split_batch = DeserializeFromIpcBytesZeroCopy(BinaryCell(batch, values, i));
			if (!split_batch || split_batch->num_rows() == 0) {
				throw IOException(CatalogProtocolViolation("table_function_plan", worker_path).Set(error_key::kField, "splits"),
				                  "VGI worker '%s' returned an unparseable ScanSplit", worker_path);
			}
			generated::ScanSplitWire wire;
			generated::FromBatch(split_batch, 0, wire, worker_path);
			if (wire.token.empty()) {
				throw IOException(
				    CatalogProtocolViolation("table_function_plan", worker_path).Set(error_key::kField, "token"),
				    "VGI worker '%s' returned a ScanSplit with no token; the framework stamps this, "
				    "so an absent token means the worker bypassed it.",
				    worker_path);
			}
			VgiScanSplit split;
			split.token.assign(reinterpret_cast<const char *>(wire.token.data()), wire.token.size());
			split.key_value = std::move(wire.key_value);
			page.splits.push_back(std::move(split));
		}
	}

	auto binary_field = [&](const char *name) -> std::vector<uint8_t> {
		std::vector<uint8_t> out;
		auto arr = batch->GetColumnByName(name);
		if (!arr || arr.IsNull(0)) {
			return out;
		}
		if (!HasType(arr, NANOARROW_TYPE_BINARY)) {
			throw IOException(violation(name), "VGI worker '%s' returned non-binary plan field '%s'",
			                  worker_path, name);
		}
		{
			auto view = arr.GetView(0);
			out.assign(view.begin(), view.end());
		}
		return out;
	};
	auto int_field = [&](const char *name) -> int64_t {
		auto arr = batch->GetColumnByName(name);
		if (!arr || arr.IsNull(0)) {
			return 0;
		}
		if (!HasType(arr, NANOARROW_TYPE_INT64)) {
			throw IOException(violation(name), "VGI worker '%s' returned non-int64 plan field '%s'",
			                  worker_path, name);
		}
		return arr.GetInt(0);
	};

	// next_cursors: every non-empty entry is a disjoint, exhaustive branch.
	if (auto cursors = batch->GetColumnByName("next_cursors")) {
		if (!cursors.IsNull(0)) {
			if (!HasType(cursors, NANOARROW_TYPE_LIST)) {
				throw IOException(violation("next_cursors"),
				                  "VGI worker '%s' returned non-list plan field 'next_cursors'", worker_path);
			}
			int64_t start = 0;
			int64_t end = 0;
			cursors.ListRange(0, start, end);
			auto values = cursors.child(0);
			if (!HasType(values, NANOARROW_TYPE_BINARY)) {
				throw IOException(violation("next_cursors"),
				                  "VGI worker '%s' returned non-binary plan continuation cursors", worker_path);
			}
			for (auto i = start; i < end; ++i) {
				if (values.IsNull(i)) {
					throw IOException(violation("next_cursors"),
					                  "VGI worker '%s' returned a null plan continuation cursor", worker_path);
				}
				auto view = values.GetView(i);
				if (!view.empty()) {
					page.next_cursors.emplace_back(view.begin(), view.end());
				}
			}
		}
	}

	page.max_workers = int_field("max_workers");
	page.catalog_version = int_field("catalog_version");
	page.execution_id = binary_field("execution_id");
	page.init_opaque_data = binary_field("init_opaque_data");

	// DuckDB can map the portable subset to SINGLE_VALUE_PARTITIONS:
	// top-level vgi.identity/1 transforms without parameters. Other transforms
	// remain valid protocol metadata but do not activate this adapter.
	auto key_grouping = batch->GetColumnByName("key_grouping");
	if (key_grouping && !key_grouping.IsNull(0)) {
		if (!HasType(key_grouping, NANOARROW_TYPE_BINARY)) {
			throw IOException(violation("key_grouping"),
			                  "VGI worker '%s' returned non-binary plan field 'key_grouping'", worker_path);
		}
		auto view = key_grouping.GetView(0);
		page.key_grouping = std::vector<uint8_t>(view.begin(), view.end());
		auto grouping = DecodeIpcRecord<generated::KeyGrouping>(*page.key_grouping, worker_path);
		std::vector<std::string> columns;
		bool supported = true;
		for (const auto &key_bytes : grouping.keys) {
			auto transform = DecodeIpcRecord<generated::TransformSpec>(key_bytes, worker_path);
			if (transform.id != "vgi.identity/1" || transform.parameters.has_value() ||
			    transform.inputs.size() != 1) {
				supported = false;
				break;
			}
			auto path = DecodeIpcRecord<generated::FieldPath>(transform.inputs[0], worker_path);
			if (path.components.size() != 1) {
				supported = false;
				break;
			}
			columns.push_back(path.components[0]);
		}
		if (supported && !columns.empty()) {
			page.identity_key_columns = std::move(columns);
		}
	}

	auto scope = batch->GetColumnByName("scope");
	if (!scope || !HasType(scope, NANOARROW_TYPE_STRING) || scope.IsNull(0)) {
		throw IOException(violation("scope"),
		                  "VGI worker '%s' returned a plan without a non-null utf8 'scope' field", worker_path);
	}
	page.scope = scope.GetString(0);

	return page;
}

// Runs on the scheduling thread, so the bounds (see the declaration) turn a worker
// that paginates forever into an error. A breach THROWS: scanning a partial
// enumeration would silently return a subset of the table.
VgiScanPlan InvokeTableFunctionPlan(const CatalogRpcContext &ctx,
                                    const std::vector<uint8_t> &bind_request_bytes,
                                    const std::vector<uint8_t> &bind_opaque_data,
                                    const std::vector<int32_t> &projection_ids,
                                    const std::vector<uint8_t> &pushdown_filters, int64_t min_splits,
                                    int64_t target_split_bytes, const std::string &function_name,
                                    ClientContext &context) {
	auto &worker_path = ctx.params->worker_path();

	// Deliberately generous bounds: they turn a hang into an error, not police
	// split counts. The page cap is a setting so it can be raised for a worker
	// that legitimately paginates a very large table.
	idx_t max_pages = defaults::SPLIT_PLAN_MAX_PAGES;
	Value max_pages_value;
	if (context.TryGetCurrentSetting("vgi_split_plan_max_pages", max_pages_value) &&
	    !max_pages_value.IsNull()) {
		const auto configured = max_pages_value.GetValue<uint64_t>();
		if (configured > 0) {
			max_pages = static_cast<idx_t>(configured);
		}
	}
	constexpr idx_t kMaxSplits = 1u << 20;

	VgiScanPlan plan;
	plan.function_name = function_name;
	std::deque<std::vector<uint8_t>> pending_cursors;
	pending_cursors.emplace_back();
	// Split disjointness across pages is the WORKER's obligation and is not
	// checked (a duplicate split returns duplicate rows). Token comparison is
	// costly near the cap and cannot work uniformly: keyed workers seal tokens
	// under a random nonce. Revisit if a stable split id reaches the wire.
	idx_t pages = 0;
	std::optional<std::vector<uint8_t>> first_key_grouping;
	bool key_grouping_consistent = true;

	while (!pending_cursors.empty()) {
		auto cursor = std::move(pending_cursors.front());
		pending_cursors.pop_front();
		if (context.IsInterrupted()) {
			throw InterruptException();
		}
		if (pages >= max_pages) {
			throw IOException(
			    ErrorInfo(error_subtype::kLimitExceeded)
			        .Rpc("table_function_plan")
			        .Function(function_name, "TABLE")
			        .Worker(worker_path)
			        .Set(error_key::kLimit, static_cast<int64_t>(max_pages))
			        .Set(error_key::kSetting, "vgi_split_plan_max_pages"),
			    "VGI worker '%s' function '%s' exceeded the scan-planning page cap (%llu pages) without "
			    "exhausting its cursor; refusing to scan a partial split enumeration.",
			    worker_path, function_name, static_cast<unsigned long long>(max_pages));
		}

		auto request_batch = BuildTableFunctionPlanRequest(bind_request_bytes, bind_opaque_data, projection_ids,
		                                                   pushdown_filters, min_splits, target_split_bytes, cursor);
		auto request_bytes = SerializeToIpcBytes(request_batch);
		auto params = generated::BuildTableFunctionPlanParams(request_bytes);

		auto response = InvokeRpcMethod(ctx, "table_function_plan", params, context);
		auto result_batch = ExtractAndDeserializeResult(response, "table_function_plan", worker_path);
		if (!result_batch) {
			break;
		}
		pages++;

		auto page = ParseVgiScanPlanPage(result_batch, worker_path);
		// Plan-level facts come from the FIRST page. Key on the page count, not
		// on splits.empty(): leading pages may legally carry zero splits.
		if (pages == 1) {
			plan.max_workers = page.max_workers;
			plan.catalog_version = page.catalog_version;
			plan.scope = page.scope;
			plan.execution_id = page.execution_id;
			plan.init_opaque_data = page.init_opaque_data;
			first_key_grouping = page.key_grouping;
			plan.identity_key_columns = page.identity_key_columns;
		} else if (page.key_grouping != first_key_grouping) {
			key_grouping_consistent = false;
		}
		for (auto &split : page.splits) {
			plan.splits.push_back(std::move(split));
		}

		if (plan.splits.size() > kMaxSplits) {
			throw IOException(ErrorInfo(error_subtype::kLimitExceeded)
			                      .Rpc("table_function_plan")
			                      .Function(function_name, "TABLE")
			                      .Worker(worker_path)
			                      .Set(error_key::kLimit, static_cast<int64_t>(kMaxSplits)),
			                  "VGI worker '%s' returned more than %llu splits for one scan; refusing to "
			                  "buffer an unbounded split vector.",
			                  worker_path, static_cast<unsigned long long>(kMaxSplits));
		}
		plan.pages = pages;

		for (auto &next : page.next_cursors) {
			pending_cursors.push_back(std::move(next));
		}
	}
	if (!key_grouping_consistent) {
		plan.identity_key_columns.reset();
	}

	return plan;
}

// Column statistics

// BaseStatistics::FromConstant(min) merged with FromConstant(max), so DuckDB
// handles every stats type without a hand-written type dispatch.
static unique_ptr<BaseStatistics> BuildColumnStatistics(
    const LogicalType &duck_type, const Value &min_val, const Value &max_val,
    bool has_null, bool has_not_null, int64_t distinct_count,
    bool has_contains_unicode, bool contains_unicode,
    bool has_max_string_length, uint64_t max_string_length) {

	if (min_val.IsNull() && max_val.IsNull()) {
		return make_uniq<BaseStatistics>(BaseStatistics::CreateUnknown(duck_type));
	}

	// Cast to the column type so FromConstant picks the right stats type (e.g.
	// GEOMETRY_STATS); with no registered cast but the same physical type (BLOB
	// -> GEOMETRY), reinterpret instead.
	auto cast_value = [&](const Value &val) -> Value {
		if (val.IsNull()) {
			return val;
		}
		if (val.type() == duck_type) {
			return val;
		}
		try {
			return val.DefaultCastAs(duck_type);
		} catch (...) {
			if (val.type().InternalType() == duck_type.InternalType()) {
				return val.WithType(duck_type);
			}
			return val;
		}
	};

	BaseStatistics result = !min_val.IsNull()
	    ? BaseStatistics::FromConstant(cast_value(min_val))
	    : BaseStatistics::FromConstant(cast_value(max_val));

	if (!min_val.IsNull() && !max_val.IsNull()) {
		result.Merge(BaseStatistics::FromConstant(cast_value(max_val)));
	}

	// FromConstant of a non-null value sets CANNOT_HAVE_NULL; widen as reported.
	if (has_null) {
		result.SetHasNull();
	}
	if (has_not_null) {
		result.SetHasNoNull();
	}
	// FromConstant sets distinct_count = 1; DuckDB treats 0 as unknown.
	result.SetDistinctCount(distinct_count >= 0 ? static_cast<idx_t>(distinct_count) : 0);

	// StringStats on another stats type reads the wrong union member (UB).
	if (result.GetStatsType() == StatisticsType::STRING_STATS) {
		if (has_contains_unicode && contains_unicode) {
			StringStats::SetContainsUnicode(result);
		}
		if (has_max_string_length) {
			StringStats::SetMaxStringLength(result, static_cast<uint32_t>(max_string_length));
		}
	}

	return result.ToUnique();
}

// Wire shape: column_name, min [union], max [union], has_null, has_not_null,
// distinct_count, contains_unicode?, max_string_length?. log_source_key/value
// identify the source in logs (e.g. {"table", "public.users"}).
std::unordered_map<std::string, unique_ptr<BaseStatistics>> ParseColumnStatisticsBatch(
    const VgiBatchPtr &result_batch,
    const std::vector<LogicalType> &column_types,
    const std::vector<std::string> &column_names,
    const std::string &worker_path, const std::string &log_source_key,
    const std::string &log_source_value, ClientContext &context) {
	std::unordered_map<std::string, unique_ptr<BaseStatistics>> out;
	if (!result_batch || result_batch->num_rows() == 0) {
		return out;
	}

	// DuckDB's Arrow scanner converts every type, including the min/max unions.
	ArrowSchemaWrapper schema_root;
	ExportSchema(result_batch->schema(), schema_root);

	vector<LogicalType> all_types;
	ArrowTableSchema arrow_table;
	std::unordered_map<std::string, idx_t> name_indexes;

	for (idx_t col_idx = 0; col_idx < static_cast<idx_t>(schema_root.arrow_schema.n_children); col_idx++) {
		auto &schema_item = *schema_root.arrow_schema.children[col_idx];
		auto arrow_type = ArrowType::GetArrowLogicalType(context, schema_item);
		all_types.push_back(arrow_type->GetDuckType());
		arrow_table.AddColumn(col_idx, std::move(arrow_type), schema_item.name);
		name_indexes[schema_item.name] = col_idx;
	}

	auto current_chunk = make_uniq<ArrowArrayWrapper>();
	ExportRecordBatch(result_batch, *current_chunk);

	DataChunk stats_chunk;
	stats_chunk.Initialize(Allocator::Get(context), all_types,
	                        static_cast<idx_t>(current_chunk->arrow_array.length));
	stats_chunk.SetCardinality(static_cast<idx_t>(current_chunk->arrow_array.length));

	ArrowScanLocalState fake_local_state(std::move(current_chunk), context);
	ArrowTableFunction::ArrowToDuckDB(fake_local_state, arrow_table.GetColumns(), stats_chunk, false);
	stats_chunk.Verify();

	for (const auto &field : {"column_name", "min", "max", "has_null", "has_not_null", "distinct_count"}) {
		if (name_indexes.find(field) == name_indexes.end()) {
			VGI_LOG(context, "column_statistics.missing_fields",
			        {{"worker_path", worker_path},
			         {log_source_key, log_source_value},
			         {"missing_field", field}});
			return out;
		}
	}

	std::unordered_map<std::string, LogicalType> col_type_map;
	for (size_t i = 0; i < column_names.size(); i++) {
		col_type_map[column_names[i]] = column_types[i];
	}

	bool has_contains_unicode_col = name_indexes.count("contains_unicode") > 0;
	bool has_max_string_length_col = name_indexes.count("max_string_length") > 0;

	for (idx_t i = 0; i < stats_chunk.size(); i++) {
		try {
			auto column_name = stats_chunk.data[name_indexes["column_name"]].GetValue(i).GetValue<string>();

			auto type_it = col_type_map.find(column_name);
			if (type_it == col_type_map.end()) {
				continue;
			}
			auto &duck_type = type_it->second;

			auto min_union_val = stats_chunk.data[name_indexes["min"]].GetValue(i);
			auto max_union_val = stats_chunk.data[name_indexes["max"]].GetValue(i);

			Value min_val, max_val;
			if (!min_union_val.IsNull()) {
				min_val = UnionValue::GetValue(min_union_val);
			}
			if (!max_union_val.IsNull()) {
				max_val = UnionValue::GetValue(max_union_val);
			}

			bool has_null = stats_chunk.data[name_indexes["has_null"]].GetValue(i).GetValue<bool>();
			bool has_not_null = stats_chunk.data[name_indexes["has_not_null"]].GetValue(i).GetValue<bool>();
			auto dc_val = stats_chunk.data[name_indexes["distinct_count"]].GetValue(i);
			int64_t distinct_count = dc_val.IsNull() ? -1 : dc_val.GetValue<int64_t>();

			bool has_unicode_flag = false;
			bool unicode_val = false;
			if (has_contains_unicode_col) {
				auto cu_val = stats_chunk.data[name_indexes["contains_unicode"]].GetValue(i);
				if (!cu_val.IsNull()) {
					has_unicode_flag = true;
					unicode_val = cu_val.GetValue<bool>();
				}
			}
			bool has_max_str_len = false;
			uint64_t max_str_len = 0;
			if (has_max_string_length_col) {
				auto msl_val = stats_chunk.data[name_indexes["max_string_length"]].GetValue(i);
				if (!msl_val.IsNull()) {
					has_max_str_len = true;
					max_str_len = msl_val.GetValue<uint64_t>();
				}
			}

			auto stats = BuildColumnStatistics(duck_type, min_val, max_val, has_null, has_not_null,
			                                    distinct_count, has_unicode_flag, unicode_val,
			                                    has_max_str_len, max_str_len);
			if (stats) {
				out[column_name] = std::move(stats);
			}
		} catch (const std::exception &e) {
			VGI_LOG(context, "column_statistics.parse_error",
			        {{"worker_path", worker_path},
			         {log_source_key, log_source_value},
			         {"row", std::to_string(i)},
			         {"error", e.what()}});
		}
	}

	return out;
}

ColumnStatisticsRpcResult InvokeCatalogTableColumnStatisticsGet(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::vector<LogicalType> &column_types,
    const std::vector<std::string> &column_names,
    ClientContext &context) {
	auto &worker_path = ctx.params->worker_path();

	ColumnStatisticsRpcResult rpc_result;

	auto params = generated::BuildCatalogTableColumnStatisticsGetParams(
	    ctx.attach_opaque_data, SingleSchemaPath(schema_name), table_name, OptTxn(ctx));
	const char *method_name = "catalog_table_column_statistics_get";
	auto response = InvokeRpcMethod(ctx, method_name, params, context);

	// Not ExtractAndDeserializeResult: the inner batch's custom_metadata carries
	// cache_max_age_seconds. ValidateResponseSchema is a no-op here (the method
	// is registered dynamic) but kept for symmetry.
	DeserializedBatch deserialized;
	if (response.batch && response.batch->num_rows() != 0) {
		deserialized.batch =
		    UnwrapResultEnvelope(response.batch, method_name, CatalogProtocolViolation(method_name, worker_path),
		                         /*null_result_ok=*/true, WorkerBatchValidation::FULL, &deserialized.custom_metadata);
	}
	if (!deserialized.batch) {
		ValidateResponseSchema(nullptr, method_name, worker_path);
		return rpc_result;
	}
	ValidateResponseSchema(deserialized.batch, method_name, worker_path);

	if (deserialized.custom_metadata) {
		int key_idx = deserialized.custom_metadata->FindKey("cache_max_age_seconds");
		if (key_idx >= 0) {
			try {
				rpc_result.cache_max_age_seconds = std::stoll(deserialized.custom_metadata->value(key_idx));
			} catch (const std::exception &e) {
				VGI_LOG(context, "column_statistics.invalid_cache_ttl",
				        {{"worker_path", worker_path},
				         {"value", deserialized.custom_metadata->value(key_idx)},
				         {"error", e.what()}});
			}
		}
	}

	rpc_result.stats = ParseColumnStatisticsBatch(deserialized.batch, column_types, column_names,
	                                               worker_path, "table", schema_name + "." + table_name, context);
	return rpc_result;
}

std::unordered_map<std::string, unique_ptr<BaseStatistics>> InvokeTableFunctionStatistics(
    const CatalogRpcContext &ctx, const std::vector<uint8_t> &bind_request_bytes,
    const std::vector<uint8_t> &bind_opaque_data,
    const std::vector<LogicalType> &column_types,
    const std::vector<std::string> &column_names,
    const std::string &function_name, ClientContext &context) {
	auto &worker_path = ctx.params->worker_path();

	auto request_batch = BuildTableFunctionStatisticsRequest(bind_request_bytes, bind_opaque_data);
	auto request_bytes = SerializeToIpcBytes(request_batch);
	auto params = generated::BuildTableFunctionStatisticsParams(request_bytes);

	const char *method_name = "table_function_statistics";
	auto response = InvokeRpcMethod(ctx, method_name, params, context);

	// Null result: no stats. The payload shape is function-specific.
	auto result_batch = ExtractAndDeserializeResult(response, method_name, worker_path);
	if (!result_batch) {
		return {};
	}

	return ParseColumnStatisticsBatch(result_batch, column_types, column_names,
	                                   worker_path, "function_name", function_name, context);
}

// Table function dynamic_to_string

InsertionOrderPreservingMap<std::string> InvokeTableFunctionDynamicToString(
    const CatalogRpcContext &ctx, const std::vector<uint8_t> &bind_request_bytes,
    const std::vector<uint8_t> &bind_opaque_data,
    const std::vector<uint8_t> &global_execution_id,
    ClientContext &context) {
	auto &worker_path = ctx.params->worker_path();

	try {
		auto request_batch = BuildTableFunctionDynamicToStringRequest(
		    bind_request_bytes, bind_opaque_data, global_execution_id);
		auto request_bytes = SerializeToIpcBytes(request_batch);
		auto params = generated::BuildTableFunctionDynamicToStringParams(request_bytes);

		auto response = InvokeRpcMethod(ctx, "table_function_dynamic_to_string", params, context);
		auto result_batch = ExtractAndDeserializeResult(response, "table_function_dynamic_to_string", worker_path);
		if (!result_batch) {
			return {};
		}
		return ParseTableFunctionDynamicToStringResult(result_batch, worker_path);
	} catch (const std::exception &e) {
		// EXPLAIN ANALYZE must never fail because of a profiler-only RPC; the
		// caller still appends the keys it knows from the global state.
		VGI_LOG(context, "table_function.dynamic_to_string_error",
		        {{"worker_path", worker_path}, {"error", e.what()}});
		return {};
	}
}

// Enum parsing

// An unrecognized wire enum (typo, protocol skew, newer variant) is a protocol
// error, never silently defaulted to behavior the worker did not declare.
template <typename T>
static T RequireKnownEnum(std::optional<T> parsed, const std::string &raw, const char *field_name,
                          const std::string &worker_path, const std::string &fn_name) {
	if (!parsed) {
		throw IOException(ErrorInfo(error_subtype::kProtocolViolation)
		                      .Worker(worker_path)
		                      .Function(fn_name)
		                      .Set(error_key::kField, field_name)
		                      .Set(error_key::kActual, raw),
		                  "VGI worker '%s' returned unknown %s '%s' for function '%s'", worker_path, field_name, raw,
		                  fn_name);
	}
	return *parsed;
}

std::string VgiFunctionTypeToString(VgiFunctionType type) {
	switch (type) {
	case VgiFunctionType::Scalar:
		return "scalar";
	case VgiFunctionType::Table:
		return "table";
	case VgiFunctionType::TableBuffering:
		return "table_buffering";
	case VgiFunctionType::Aggregate:
		return "aggregate";
	default:
		return "unknown";
	}
}

std::optional<VgiArgumentMonotonicity> ParseVgiArgumentMonotonicity(const std::string &value) {
	if (value == "UNKNOWN") {
		return VgiArgumentMonotonicity::Unknown;
	} else if (value == "CONSTANT") {
		return VgiArgumentMonotonicity::Constant;
	} else if (value == "NON_DECREASING") {
		return VgiArgumentMonotonicity::NonDecreasing;
	} else if (value == "STRICTLY_INCREASING") {
		return VgiArgumentMonotonicity::StrictlyIncreasing;
	} else if (value == "NON_INCREASING") {
		return VgiArgumentMonotonicity::NonIncreasing;
	} else if (value == "STRICTLY_DECREASING") {
		return VgiArgumentMonotonicity::StrictlyDecreasing;
	}
	return std::nullopt;
}

// Protocol enums (generated, wire order) to the extension's / DuckDB's enums.
static VgiFunctionType FromWire(generated::FunctionType v) {
	switch (v) {
	case generated::FunctionType::Scalar:
		return VgiFunctionType::Scalar;
	case generated::FunctionType::Table:
		return VgiFunctionType::Table;
	case generated::FunctionType::TableBuffering:
		return VgiFunctionType::TableBuffering;
	case generated::FunctionType::Aggregate:
		return VgiFunctionType::Aggregate;
	}
	throw InternalException("unhandled FunctionType");
}

static FunctionStability FromWire(generated::FunctionStability v) {
	switch (v) {
	case generated::FunctionStability::Consistent:
		return FunctionStability::CONSISTENT;
	case generated::FunctionStability::Volatile:
		return FunctionStability::VOLATILE;
	case generated::FunctionStability::ConsistentWithinQuery:
		return FunctionStability::CONSISTENT_WITHIN_QUERY;
	}
	throw InternalException("unhandled FunctionStability");
}

static FunctionNullHandling FromWire(generated::FunctionNullHandling v) {
	switch (v) {
	case generated::FunctionNullHandling::Default:
		return FunctionNullHandling::DEFAULT_NULL_HANDLING;
	case generated::FunctionNullHandling::Special:
		return FunctionNullHandling::SPECIAL_HANDLING;
	}
	throw InternalException("unhandled FunctionNullHandling");
}

static VgiOrderPreservation FromWire(generated::VgiOrderPreservation v) {
	switch (v) {
	case generated::VgiOrderPreservation::PreservesOrder:
		return VgiOrderPreservation::PreservesOrder;
	case generated::VgiOrderPreservation::NoOrderGuarantee:
		return VgiOrderPreservation::NoOrderGuarantee;
	case generated::VgiOrderPreservation::FixedOrder:
		return VgiOrderPreservation::FixedOrder;
	}
	throw InternalException("unhandled VgiOrderPreservation");
}

static VgiPartitionKind FromWire(generated::VgiPartitionKind v) {
	switch (v) {
	case generated::VgiPartitionKind::NotPartitioned:
		return VgiPartitionKind::NotPartitioned;
	case generated::VgiPartitionKind::SingleValuePartitions:
		return VgiPartitionKind::SingleValuePartitions;
	case generated::VgiPartitionKind::OverlappingPartitions:
		return VgiPartitionKind::OverlappingPartitions;
	case generated::VgiPartitionKind::DisjointPartitions:
		return VgiPartitionKind::DisjointPartitions;
	}
	throw InternalException("unhandled VgiPartitionKind");
}

static AggregateOrderDependent FromWire(generated::AggregateOrderDependent v) {
	return v == generated::AggregateOrderDependent::OrderDependent ? AggregateOrderDependent::ORDER_DEPENDENT
	                                                               : AggregateOrderDependent::NOT_ORDER_DEPENDENT;
}

static AggregateDistinctDependent FromWire(generated::AggregateDistinctDependent v) {
	return v == generated::AggregateDistinctDependent::DistinctDependent
	           ? AggregateDistinctDependent::DISTINCT_DEPENDENT
	           : AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT;
}

// Result parsing

vector<Value> BatchRowToValues(ClientContext &context, const VgiBatchPtr &batch, int64_t row) {
	vector<Value> result;
	if (!batch || batch->num_columns() == 0 || row < 0 || row >= batch->num_rows()) {
		return result;
	}

	// DuckDB's Arrow scanner converts, so every type DuckDB reads from Arrow works.
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
	vector<LogicalType> types;
	vector<string> names;
	ArrowSchemaToDuckDBTypes(context, batch->schema(), c_schema, arrow_table, types, names);

	// One row, zero-copy; the exported array keeps the batch's buffers alive.
	auto one_row = batch->num_rows() == 1 ? batch : batch->Slice(row, 1);
	auto current_chunk = make_uniq<ArrowArrayWrapper>();
	one_row->ExportTo(*current_chunk);

	DataChunk chunk;
	chunk.Initialize(Allocator::Get(context), types, 1);
	chunk.SetCardinality(1);
	ArrowScanLocalState local_state(std::move(current_chunk), context);
	ArrowTableFunction::ArrowToDuckDB(local_state, arrow_table.GetColumns(), chunk, false);

	result.reserve(types.size());
	for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
		result.push_back(chunk.GetValue(col_idx, 0));
	}
	return result;
}

// The DuckDB type of the single field of a serialized Arrow schema (a setting's or
// attach option's `type`).
static LogicalType SingleFieldType(const std::vector<uint8_t> &schema_bytes, ClientContext &context,
                                   const char *what, const std::string &name, const std::string &worker_path) {
	auto schema = DeserializeSchema(schema_bytes);
	if (!schema || schema->num_fields() == 0) {
		throw IOException(ErrorInfo(error_subtype::kProtocolViolation).Worker(worker_path).Set(error_key::kField, "type"),
		                  "%s '%s' from worker '%s' has an empty type schema", what, name, worker_path);
	}
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
	vector<LogicalType> types;
	vector<string> names;
	ArrowSchemaToDuckDBTypes(context, schema, c_schema, arrow_table, types, names);
	return types[0];
}

// A setting's / attach option's default: the first cell of a serialized
// one-column batch, or NULL of `type` when absent or empty.
static Value DefaultValueOf(ClientContext &context, const std::optional<std::vector<uint8_t>> &bytes,
                            const LogicalType &type) {
	if (bytes && !bytes->empty()) {
		auto batch = DeserializeFromIpcBytes(*bytes);
		if (batch && batch->num_rows() > 0 && batch->num_columns() > 0) {
			auto values = BatchRowToValues(context, batch->SelectColumns({0}), 0);
			// NULL keeps the declared type.
			if (!values.empty() && !values[0].IsNull()) {
				return std::move(values[0]);
			}
		}
	}
	return Value(type);
}

VgiSetting ParseVgiSetting(const std::vector<uint8_t> &bytes, const std::string &worker_path,
                           ClientContext &context) {
	auto spec = DecodeIpcRecord<generated::SettingSpec>(bytes, worker_path);
	VgiSetting setting;
	setting.name = std::move(spec.name);
	setting.description = std::move(spec.description);
	setting.type = SingleFieldType(spec.type, context, "Setting", setting.name, worker_path);
	setting.default_value = DefaultValueOf(context, spec.default_value, setting.type);
	return setting;
}

VgiAttachOptionSpec ParseAttachOptionSpec(const std::vector<uint8_t> &bytes, const std::string &worker_path,
                                          ClientContext &context) {
	auto wire = DecodeIpcRecord<generated::AttachOptionSpec>(bytes, worker_path);
	VgiAttachOptionSpec spec;
	spec.name = std::move(wire.name);
	spec.description = std::move(wire.description);
	spec.type = SingleFieldType(wire.type, context, "Attach option", spec.name, worker_path);
	spec.default_value = DefaultValueOf(context, wire.default_value, spec.type);
	spec.required = wire.required;
	spec.secret = wire.secret;
	return spec;
}

VgiSecretType ParseVgiSecretType(const std::vector<uint8_t> &bytes, const std::string &worker_path,
                                  ClientContext &context) {
	auto wire = DecodeIpcRecord<generated::SecretTypeSpec>(bytes, worker_path);
	VgiSecretType secret_type;
	secret_type.name = std::move(wire.name);
	secret_type.description = std::move(wire.description);

	auto params_schema = DeserializeSchema(wire.parameters_schema);
	if (!params_schema) {
		return secret_type;
	}
	secret_type.doc_url = params_schema->metadata().Get(VGI_DOC_URL_METADATA_KEY).value_or("");
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
	vector<LogicalType> types;
	vector<string> names;
	ArrowSchemaToDuckDBTypes(context, params_schema, c_schema, arrow_table, types, names);
	for (int i = 0; i < params_schema->num_fields(); i++) {
		VgiSecretTypeParam param;
		param.name = names[i];
		param.type = types[i];
		auto field_metadata = params_schema->field_metadata(i);
		param.redact = field_metadata.Get("redact").value_or("") == "true";
		param.required = field_metadata.Get(VGI_REQUIRED_METADATA_KEY).value_or("") == "true";
		param.description = field_metadata.Get(VGI_DOC_METADATA_KEY).value_or("");
		secret_type.parameters.push_back(std::move(param));
	}
	return secret_type;
}

// One serialized AttachCatalogInfo (companion catalog).
static VgiAttachCatalogInfo ParseVgiAttachCatalog(const std::vector<uint8_t> &bytes, const std::string &worker_path) {
	auto wire = DecodeIpcRecord<generated::AttachCatalogInfo>(bytes, worker_path);
	VgiAttachCatalogInfo info;
	info.alias = std::move(wire.alias);
	info.target = std::move(wire.target);
	info.db_type = std::move(wire.db_type);
	info.options = std::move(wire.options);
	info.hidden = wire.hidden;
	info.required = wire.required;
	info.secret_ref = std::move(wire.secret_ref);
	return info;
}

CatalogAttachResult ParseCatalogAttachResult(const VgiBatchPtr &batch,
                                             const std::string &worker_path, ClientContext &context, bool query_offered) {
	auto wire = generated::Decode<generated::CatalogAttachResult>(batch, 0, worker_path);
	CatalogAttachResult result;
	result.attach_opaque_data = std::move(wire.attach_opaque_data);
	result.supports_transactions = wire.supports_transactions;
	result.supports_time_travel = wire.supports_time_travel;
	result.catalog_version_frozen = wire.catalog_version_frozen;
	result.catalog_version = wire.catalog_version;
	result.attach_opaque_data_required = wire.attach_opaque_data_required;
	if (wire.default_schema && (wire.default_schema->empty() || wire.default_schema->find('\0') != std::string::npos)) {
		throw IOException(CatalogProtocolViolation("catalog_attach", worker_path),
		                  "CatalogAttachResult.default_schema must be NULL or a nonempty raw identifier without NUL");
	}
	result.default_schema = std::move(wire.default_schema);
	for (const auto &setting_bytes : wire.settings) {
		result.settings.push_back(ParseVgiSetting(setting_bytes, worker_path, context));
	}
	for (const auto &st_bytes : wire.secret_types) {
		result.secret_types.push_back(ParseVgiSecretType(st_bytes, worker_path, context));
	}
	for (const auto &ac_bytes : wire.attach_catalogs) {
		result.attach_catalogs.push_back(ParseVgiAttachCatalog(ac_bytes, worker_path));
	}
	result.comment = wire.comment.value_or("");
	result.tags = std::move(wire.tags);
	result.supports_column_statistics = wire.supports_column_statistics;
	// Globally-published functions: each a serialized FunctionInfo.
	for (const auto &gf_bytes : wire.global_functions) {
		result.global_functions.push_back(ParseFunctionInfo(DeserializeFromIpcBytes(gf_bytes), 0, worker_path));
	}
	result.global_function_prefix = std::move(wire.global_function_prefix);
	result.resolved_data_version = wire.resolved_data_version.value_or("");
	result.resolved_implementation_version = wire.resolved_implementation_version.value_or("");
	result.supports_catalog_contents = wire.supports_catalog_contents;
	ParseNativeConstraintCapabilities(result, wire.selected_capabilities.value_or(generated::Bytes {}));
	result.query_pushdown = ParseVgiQueryCapabilities(batch->GetColumnByName("query_pushdown"), query_offered, worker_path);
	if (result.query_pushdown && (!result.attach_opaque_data_required || result.attach_opaque_data.empty() ||
	                             result.attach_opaque_data.size() > VGI_QUERY_MAX_TOKEN_BYTES)) {
		throw IOException("VGI query support requires a nonempty required attachment identity [worker: %s]", worker_path);
	}
	return result;
}

static VgiSchemaInfo SchemaInfoFromWire(generated::SchemaInfo wire) {
	VgiSchemaInfo info;
	info.path = std::move(wire.path);
	info.name = SchemaNameFromPath(info.path, "SchemaInfo.path");
	info.comment = std::move(wire.comment);
	info.tags = std::move(wire.tags);
	// Nullable map: downstream defaults missing keys to 1.
	info.estimated_object_count = wire.estimated_object_count.value_or(std::map<std::string, int64_t> {});
	return info;
}

VgiSchemaInfo ParseSchemaInfo(const VgiBatchPtr &batch, const std::string &worker_path) {
	return SchemaInfoFromWire(generated::Decode<generated::SchemaInfo>(batch, 0, worker_path));
}

VgiTableInfo ParseTableInfo(ClientContext &context, const VgiBatchPtr &batch,
                            int64_t row_idx, const std::string &worker_path) {
	auto wire = generated::Decode<generated::TableInfo>(batch, row_idx, worker_path);
	VgiTableInfo info;
	info.name = std::move(wire.name);
	info.schema_name = SchemaNameFromPath(wire.schema_path, "TableInfo.schema_path");
	info.comment = wire.comment.value_or("");
	info.tags = std::move(wire.tags);

	// `columns` is a serialized Arrow schema; at most one field may carry is_row_id.
	info.arrow_schema = DeserializeSchema(wire.columns);
	ValidateNativeColumnMetadata(info.arrow_schema);
	for (int i = 0; info.arrow_schema && i < info.arrow_schema->num_fields(); i++) {
		if (info.arrow_schema->field_metadata(i).FindKey(VGI_ROW_ID_METADATA_KEY) >= 0) {
			if (info.row_id_column >= 0) {
				throw InvalidInputException(ErrorInfo(error_subtype::kProtocolViolation).Worker(worker_path).Set(error_key::kTable, info.name).Set(error_key::kField, "columns"),
		                            StringUtil::Format("Table '%s' has multiple is_row_id columns — at most one is allowed", info.name));
			}
			info.row_id_column = i;
		}
	}

	ValidateNativeConstraintList(batch->GetColumnByName("constraints"), row_idx);
	info.constraints = std::move(wire.constraints);
	info.definition_revision = wire.definition_revision.value_or(generated::Bytes {});
	if (wire.definition_revision && info.definition_revision.empty()) {
		throw IOException("TableInfo.definition_revision must be NULL or a nonempty bounded token");
	}
	DecodeNativeConstraints(info);

	for (auto &entry : wire.write_result_modes) {
		const auto &operation = entry.first;
		const auto &mode = entry.second;
		if (operation != "insert" && operation != "update" && operation != "delete") {
			throw InvalidInputException(ErrorInfo(error_subtype::kProtocolViolation).Worker(worker_path).Set(error_key::kTable, info.name).Set(error_key::kField, "write_result_modes").Set(error_key::kOperation, operation),
		                            StringUtil::Format("Table '%s' declares unknown write operation '%s'", info.name, operation));
		}
		if (mode != "count" && mode != "rows" && mode != "changes") {
			throw InvalidInputException(ErrorInfo(error_subtype::kProtocolViolation).Worker(worker_path).Set(error_key::kTable, info.name).Set(error_key::kField, "write_result_modes").Set(error_key::kOperation, operation),
		                            StringUtil::Format("Table '%s' declares unknown write result mode '%s' for %s", info.name, mode,
			                            operation));
		}
	}
	info.write_result_modes = std::move(wire.write_result_modes);
	info.supports_column_statistics = wire.supports_column_statistics;

	// Inlined function-discovery results: when present the extension skips the
	// corresponding catalog_table_*_function_get RPC.
	auto decode_inlined = [&](const std::optional<generated::Bytes> &bytes) -> std::optional<VgiScanFunctionResult> {
		if (!bytes || bytes->empty()) {
			return std::nullopt;
		}
		return ParseScanFunctionResult(context, DeserializeFromIpcBytes(*bytes), worker_path);
	};
	info.scan_function = decode_inlined(wire.scan_function);
	// Reject unknown scan_planning, or a filtered table that inlines its scan
	// redirect, at catalog load so the error names the catalog, not a later query.
	info.scan_planning = wire.scan_planning;
	if (info.scan_planning != "bind" && info.scan_planning != "filtered") {
		throw IOException(ErrorInfo(error_subtype::kProtocolViolation).Worker(worker_path).Set(error_key::kField, "scan_planning"),
		                  StringUtil::Format("VGI table '%s' declares unknown scan_planning '%s' (expected 'bind' or "
		                                     "'filtered') [worker: %s]",
		                                     info.name, info.scan_planning, worker_path));
	}
	if (info.IsFilteredScanPlanning() && info.scan_function.has_value()) {
		throw IOException(ErrorInfo(error_subtype::kProtocolViolation).Worker(worker_path).Set(error_key::kField, "scan_function"),
		                  StringUtil::Format("VGI table '%s' declares scan_planning='filtered' but also inlines "
		                                     "scan_function; a filtered table's scan redirect must be resolved per "
		                                     "query [worker: %s]",
		                                     info.name, worker_path));
	}
	info.insert_function = decode_inlined(wire.insert_function);
	info.update_function = decode_inlined(wire.update_function);
	info.delete_function = decode_inlined(wire.delete_function);

	// Inlined cardinality replaces the per-bind table_function_cardinality RPC.
	info.cardinality_estimate = wire.cardinality_estimate;
	info.cardinality_max = wire.cardinality_max;

	// Inlined column statistics (deserialized lazily by VgiTableEntry::GetStatistics,
	// which has a ClientContext) and inlined bind result (fed through
	// BuildBindResultFromInlinedBytes); empty bytes mean absent.
	if (wire.column_statistics && !wire.column_statistics->empty()) {
		info.column_statistics = std::move(wire.column_statistics);
	}
	if (wire.bind_result && !wire.bind_result->empty()) {
		info.bind_result = std::move(wire.bind_result);
	}


	// required_filters is mandatory (an explicit empty outer list is required).
	// Reject malformed values; a value_or() fallback would silently disable
	// enforcement.
	auto fail_required = [&](const std::string &reason) {
		throw InvalidInputException(
		    ErrorInfo(error_subtype::kProtocolViolation).Worker(worker_path)
		        .Set(error_key::kTable, info.name).Set(error_key::kField, "required_filters"),
		    StringUtil::Format("Invalid required_filters for table '%s': %s", info.name, reason));
	};
	auto outer = batch->GetColumnByName("required_filters");
	if (!HasType(outer, NANOARROW_TYPE_LIST) || outer.IsNull(row_idx))
		fail_required("expected non-null list<list<list<utf8>>>");
	auto groups = outer.child(0);
	if (!HasType(groups, NANOARROW_TYPE_LIST)) fail_required("expected groups of paths");
	auto paths = groups.child(0);
	if (!HasType(paths, NANOARROW_TYPE_LIST)) fail_required("expected lists of path components");
	auto components = paths.child(0);
	if (!HasType(components, NANOARROW_TYPE_STRING)) fail_required("expected UTF-8 path components");
	int64_t first_group = 0, last_group = 0;
	outer.ListRange(row_idx, first_group, last_group);
	for (int64_t i = first_group; i < last_group; ++i) {
		int64_t first_path = 0, last_path = 0;
		groups.ListRange(i, first_path, last_path);
		if (groups.IsNull(i) || first_path == last_path) fail_required("groups must be non-null and nonempty");
		std::vector<std::vector<std::string>> group;
		for (int64_t j = first_path; j < last_path; ++j) {
			int64_t first = 0, last = 0;
			paths.ListRange(j, first, last);
			if (paths.IsNull(j) || first == last) fail_required("paths must be non-null and nonempty");
			std::vector<std::string> path;
			auto parent = &info.arrow_schema->c_schema();
			for (int64_t k = first; k < last; ++k) {
				if (components.IsNull(k)) fail_required("components must be non-null");
				auto component = components.GetString(k);
				if (component.empty() || component.find('\0') != std::string::npos)
					fail_required("components must be nonempty and contain no NUL");
				const ArrowSchema *resolved = nullptr;
				for (int64_t child = 0; child < parent->n_children; ++child) {
					if (component == parent->children[child]->name) {
						if (resolved) fail_required("ambiguous component: " + component);
						resolved = parent->children[child];
					}
				}
				if (!resolved) fail_required("unknown component: " + component);
				if (k + 1 < last) {
					auto type = DescribeType(*resolved);
					if (type.type != NANOARROW_TYPE_STRUCT || !type.extension_name.empty())
						fail_required("non-struct parent: " + component);
					parent = resolved;
				}
				path.push_back(std::move(component));
			}
			if (std::find(group.begin(), group.end(), path) != group.end()) fail_required("duplicate path in group");
			group.push_back(std::move(path));
		}
		info.required_filters.push_back(std::move(group));
	}

	// UPDATE/DELETE require a row ID column.
	if ((info.write_result_modes.count("update") || info.write_result_modes.count("delete")) &&
	    info.row_id_column < 0) {
		throw InvalidInputException(
		    ErrorInfo(error_subtype::kProtocolViolation)
		        .Worker(worker_path)
		        .Set(error_key::kTable, info.name)
		        .Set(error_key::kField, "write_result_modes"),
		    StringUtil::Format("Table '%s' declares update/delete support but has no row ID column "
		                       "(mark a column with is_row_id metadata)",
		                       info.name));
	}

	return info;
}

VgiTableInfo ParseTableInfo(ClientContext &context, const VgiBatchPtr &batch,
                            const std::string &worker_path) {
	return ParseTableInfo(context, batch, 0, worker_path);
}

std::vector<VgiSchemaInfo> ParseSchemaList(const VgiBatchPtr &batch,
                                           const std::string &worker_path) {
	std::vector<VgiSchemaInfo> schemas;
	for (int64_t i = 0; batch && i < batch->num_rows(); i++) {
		schemas.push_back(SchemaInfoFromWire(generated::Decode<generated::SchemaInfo>(batch, i, worker_path)));
	}
	return schemas;
}

VgiViewInfo ParseViewInfo(const VgiBatchPtr &batch, const std::string &worker_path) {
	auto wire = generated::Decode<generated::ViewInfo>(batch, 0, worker_path);
	VgiViewInfo info;
	info.name = std::move(wire.name);
	info.schema_name = SchemaNameFromPath(wire.schema_path, "ViewInfo.schema_path");
	info.security = std::move(wire.security);
	if (info.security == "barrier") {
		throw NotImplementedException("DuckDB 1.5 cannot enforce ViewInfo.security=barrier");
	}
	if (info.security != "regular") {
		throw IOException(CatalogProtocolViolation("ViewInfo", worker_path), "Unknown ViewInfo.security mode: %s", info.security);
	}
	info.definition = std::move(wire.definition);
	for (const auto &alias : wire.aliases) {
		if (alias.empty() || alias.find('\0') != std::string::npos) {
			throw IOException(CatalogProtocolViolation("ViewInfo", worker_path), "ViewInfo.aliases contains an empty or NUL identifier");
		}
	}
	info.aliases = std::move(wire.aliases);
	info.comment = std::move(wire.comment);
	info.tags = std::move(wire.tags);
	info.column_comments = std::move(wire.column_comments);
	return info;
}

VgiCopyFromFormatInfo ParseCopyFromFormatInfo(const VgiBatchPtr &batch,
                                              const std::string &worker_path) {
	auto wire = generated::Decode<generated::CopyFromFormatInfo>(batch, 0, worker_path);
	VgiCopyFromFormatInfo info;
	info.format_name = std::move(wire.format_name);
	info.handler = std::move(wire.handler);
	info.direction = std::move(wire.direction);
	info.ordered = wire.ordered;
	info.description = std::move(wire.description);
	if (wire.comment) {
		info.comment = std::move(*wire.comment);
	}
	info.tags = std::move(wire.tags);
	// options: the handler's argument schema; may be an empty (zero-field) schema.
	if (!wire.options.empty()) {
		info.options_schema = DeserializeSchema(wire.options);
	}
	return info;
}

// Decode a nested-IPC argument batch (ScanFunctionResult / ScanBranch
// `arguments`): field `arg_<N>` goes to positional slot N, anything else is named.
static void DecodeScanArguments(ClientContext &context,
                                const std::vector<uint8_t> &arguments_bytes,
                                duckdb::vector<Value> &positional_out,
                                std::map<std::string, Value> &named_out) {
	if (arguments_bytes.empty()) {
		return;
	}
	auto arguments_batch = DeserializeFromIpcBytes(arguments_bytes);
	if (!arguments_batch || arguments_batch->num_rows() == 0) {
		return;
	}
	auto values = BatchRowToValues(context, arguments_batch, 0);
	auto &schema = arguments_batch->schema();
	for (int i = 0; i < schema->num_fields(); i++) {
		const auto &field_name = schema->field_names()[static_cast<size_t>(i)];
		auto &duck_value = values[i];
		if (field_name.rfind("arg_", 0) == 0) {
			// The worker-supplied index sizes the positional vector, so cap it
			// (e.g. "arg_9999999999" would force a multi-GB allocation).
			constexpr size_t kMaxPositionalArgs = 1000;
			size_t idx;
			bool is_positional = false;
			try {
				idx = std::stoul(field_name.substr(4));
				is_positional = true;
			} catch (const std::exception &) {
				// Not a numeric suffix: a named argument.
				named_out[field_name] = duck_value;
			}
			if (is_positional) {
				if (idx >= kMaxPositionalArgs) {
					throw IOException("VGI: positional argument index %llu exceeds maximum of %llu",
					                  (unsigned long long)idx, (unsigned long long)kMaxPositionalArgs);
				}
				if (idx >= positional_out.size()) {
					positional_out.resize(idx + 1);
				}
				positional_out[idx] = duck_value;
			}
		} else {
			named_out[field_name] = duck_value;
		}
	}
}

VgiScanFunctionResult ParseScanFunctionResult(ClientContext &context, const VgiBatchPtr &batch,
                                               const std::string &worker_path) {
	auto wire = generated::Decode<generated::ScanFunctionResult>(batch, 0, worker_path);
	VgiScanFunctionResult result;
	result.function_name = std::move(wire.function_name);
	result.required_extensions = std::move(wire.required_extensions);
	DecodeScanArguments(context, wire.arguments, result.positional_arguments, result.named_arguments);
	result.schema_name = OptionalSchemaNameFromPath(wire.schema_path, "ScanFunctionResult.schema_path");
	return result;
}

// Each branch is a serialized 1-row ScanBranch whose `arguments` is a nested IPC
// batch. branch_filter is parsed here but NOT bound: binding happens in the
// rewriter, which has a Binder and the branch's columns.
VgiScanBranchesResult ParseScanBranchesResult(ClientContext &context,
                                               const VgiBatchPtr &batch,
                                               const std::string &worker_path) {
	VgiScanBranchesResult result;

	if (!batch || batch->num_rows() == 0) {
		throw IOException(ErrorInfo(error_subtype::kProtocolViolation).Worker(worker_path),
		                  "Empty response from table_scan_branches_get [worker: " + worker_path + "]");
	}

	auto wire = generated::Decode<generated::ScanBranchesResult>(batch, 0, worker_path);
	// The union across branches.
	result.required_extensions = std::move(wire.required_extensions);
	const auto &branch_blobs = wire.branches;
	// Zero branches is the legal "no rows" answer for a scan_planning='filtered'
	// table; VgiTableEntry rejects it for 'bind' tables.

	result.branches.reserve(branch_blobs.size());
	std::vector<size_t> writable_ordinals;
	for (size_t i = 0; i < branch_blobs.size(); i++) {
		auto branch_wire = DecodeIpcRecord<generated::ScanBranch>(branch_blobs[i], worker_path);

		VgiScanBranch branch;
		branch.function_name = std::move(branch_wire.function_name);
		DecodeScanArguments(context, branch_wire.arguments, branch.positional_arguments, branch.named_arguments);

		// Absent / NULL means unconstrained.
		branch.branch_filter = branch_wire.branch_filter.value_or("");
		if (!branch.branch_filter.empty()) {
			// Validate outside the try so these BinderExceptions are not
			// re-wrapped as a "parse error".
			duckdb::vector<duckdb::unique_ptr<ParsedExpression>> exprs;
			try {
				exprs = Parser(ParserOptions::Builtin()).ParseExpressionList(branch.branch_filter);
			} catch (const std::exception &e) {
				throw BinderException(ErrorInfo(error_subtype::kProtocolViolation)
				                          .Worker(worker_path)
				                          .Set(error_key::kBranchIndex, static_cast<int64_t>(i))
				                          .Set(error_key::kField, "branch_filter")
				                          .Merge(ExtraInfoOf(e)),
				                      "VGI branch_filter parse error [worker: " + worker_path +
				                          ", filter: " + branch.branch_filter + "]: " + RawMessageOf(e));
			}
			if (exprs.empty()) {
				throw BinderException(ErrorInfo(error_subtype::kProtocolViolation)
				                          .Worker(worker_path)
				                          .Set(error_key::kBranchIndex, static_cast<int64_t>(i))
				                          .Set(error_key::kField, "branch_filter"),
				                      "VGI branch_filter parsed to no expressions [worker: " + worker_path +
				                          ", filter: " + branch.branch_filter + "]");
			}
			// Exactly one expression: keeping only exprs[0] of "a < 5, b > 7"
			// would return rows outside the branch's declared scope.
			if (exprs.size() > 1) {
				throw BinderException(ErrorInfo(error_subtype::kProtocolViolation)
				                          .Worker(worker_path)
				                          .Set(error_key::kBranchIndex, static_cast<int64_t>(i))
				                          .Set(error_key::kField, "branch_filter"),
				                      StringUtil::Format("VGI branch_filter must be a single expression; it parsed to "
				                                         "%d expressions (combine conditions with AND) [worker: %s, "
				                                         "filter: %s]",
				                                         static_cast<int>(exprs.size()), worker_path,
				                                         branch.branch_filter));
			}
			branch.parsed_branch_filter = std::move(exprs[0]);
		}

		// Branch statistics.
		if (branch_wire.row_count.has_value() && *branch_wire.row_count < 0) {
			throw BinderException(ErrorInfo(error_subtype::kProtocolViolation)
			                          .Worker(worker_path)
			                          .Set(error_key::kBranchIndex, static_cast<int64_t>(i))
			                          .Set(error_key::kField, "row_count"),
			                      StringUtil::Format("VGI scan branch %d has negative row_count [worker: %s]",
			                                         static_cast<int>(i), worker_path));
		}
		branch.row_count = branch_wire.row_count;
		if (branch_wire.column_statistics.has_value()) {
			branch.column_statistics = std::move(*branch_wire.column_statistics);
		}

		branch.writable = branch_wire.writable;
		if (branch.writable) {
			writable_ordinals.push_back(i);
		}

		// A non-empty source_table makes a catalog-table branch: it scans
		// source_catalog.source_schema.source_table in a companion catalog.
		branch.source_catalog = branch_wire.source_catalog.value_or("");
		branch.source_schema = OptionalSchemaNameFromPath(branch_wire.source_schema_path, "ScanBranch.source_schema_path");
		branch.source_table = branch_wire.source_table.value_or("");

		// Function branch only: the schema function_name is registered in.
		branch.schema_name = OptionalSchemaNameFromPath(branch_wire.schema_path, "ScanBranch.schema_path");

		// A non-empty format_name makes a format branch: read format_locations
		// as that format.
		branch.format_name = branch_wire.format_name.value_or("");
		branch.format_locations.assign(branch_wire.format_locations.begin(), branch_wire.format_locations.end());
		{
			// Reader options: same shape as `arguments` (column names are option
			// names). The rewriter ASSIGNS them as the reader's named arguments.
			const auto &opts_bytes = branch_wire.format_options;
			if (!opts_bytes.empty()) {
				duckdb::vector<Value> unused_positional;
				DecodeScanArguments(context, opts_bytes, unused_positional, branch.format_options);
				if (!unused_positional.empty()) {
					throw BinderException(
					    ErrorInfo(error_subtype::kProtocolViolation).Worker(worker_path).Set(error_key::kBranchIndex, static_cast<int64_t>(i)).Set(error_key::kField, "format_options"),
					    StringUtil::Format("VGI scan branch %d supplied POSITIONAL format_options; reader options are "
					                       "named arguments and must be keyed by name [worker: %s]",
					                       static_cast<int>(result.branches.size()), worker_path));
				}
			}
		}

		// Exactly one branch kind, enforced at catalog load so the error blames
		// the catalog, not a later query.
		{
			const int kinds = static_cast<int>(branch.IsFunctionBranch()) +
			                  static_cast<int>(!branch.source_table.empty()) +
			                  static_cast<int>(!branch.format_name.empty());
			if (kinds == 0) {
				throw BinderException(
				    ErrorInfo(error_subtype::kProtocolViolation).Worker(worker_path).Set(error_key::kBranchIndex, static_cast<int64_t>(i)),
				    StringUtil::Format("VGI scan branch %d declares none of function_name / source_table / format_name; "
				                       "a branch must name exactly one source [worker: %s]",
				                       static_cast<int>(result.branches.size()), worker_path));
			}
			if (kinds > 1) {
				throw BinderException(
				    ErrorInfo(error_subtype::kProtocolViolation).Worker(worker_path).Set(error_key::kBranchIndex, static_cast<int64_t>(i)),
				    StringUtil::Format("VGI scan branch %d declares more than one of function_name / source_table / "
				                       "format_name; these are mutually exclusive branch kinds [worker: %s]",
				                       static_cast<int>(result.branches.size()), worker_path));
			}
			if (branch.IsFormatBranch() && branch.format_locations.empty()) {
				throw BinderException(
				    ErrorInfo(error_subtype::kProtocolViolation).Worker(worker_path).Set(error_key::kBranchIndex, static_cast<int64_t>(i)).Set(error_key::kField, "format_locations"),
				    StringUtil::Format("VGI scan branch %d is a format branch ('%s') but names no locations to read "
				                       "[worker: %s]",
				                       static_cast<int>(result.branches.size()), branch.format_name, worker_path));
			}
		}

		result.branches.push_back(std::move(branch));
	}

	// At most one writable branch: more makes the INSERT target ambiguous (and
	// would break DuckDB's one-writable-catalog-per-transaction rule,
	// MetaTransaction::ModifyDatabase, for UPDATE/DELETE).
	if (writable_ordinals.size() > 1) {
		std::string ordinals_csv;
		for (size_t i = 0; i < writable_ordinals.size(); i++) {
			if (i > 0) {
				ordinals_csv += ", ";
			}
			ordinals_csv += std::to_string(writable_ordinals[i]);
		}
		throw BinderException(
		    ErrorInfo(error_subtype::kProtocolViolation).Worker(worker_path).Set(error_key::kField, "writable"),
		    StringUtil::Format("VGI multi-branch table declared %d writable branches (ordinals: %s); "
		                       "exactly zero or one is allowed [worker: %s]",
		                       static_cast<int>(writable_ordinals.size()), ordinals_csv, worker_path));
	}

	return result;
}

// DuckDB type conversion

// required_filters (CNF: an AND of OR-groups) as JSON arrays of raw paths, e.g.
// [[["accession_number"]],[["ticker"],["cik"]]], for the `vgi_required_filters`
// tag, so callers can discover the requirement via duckdb_tables().tags.
static std::string RenderRequiredFiltersJson(const std::vector<std::vector<std::vector<std::string>>> &groups) {
	yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
	yyjson_mut_val *outer = yyjson_mut_arr(doc);
	for (const auto &group : groups) {
		yyjson_mut_val *inner = yyjson_mut_arr(doc);
		for (const auto &path : group) {
			yyjson_mut_val *parts = yyjson_mut_arr(doc);
			for (const auto &part : path) {
				yyjson_mut_arr_append(parts, yyjson_mut_strncpy(doc, part.c_str(), part.size()));
			}
			yyjson_mut_arr_append(inner, parts);
		}
		yyjson_mut_arr_append(outer, inner);
	}
	char *json_str = yyjson_mut_val_write(outer, 0, nullptr);
	std::string out = json_str ? json_str : "[]";
	if (json_str) {
		free(json_str);
	}
	yyjson_mut_doc_free(doc);
	return out;
}

CreateTableInfo CreateTableInfoFromVgiTable(ClientContext &context, VgiTableInfo &table_info,
                                            const std::string &schema_name) {
	CreateTableInfo create_info;
	create_info.SetQualifiedName(QualifiedName(Identifier(), Identifier(schema_name), Identifier(table_info.name)));
	if (!table_info.comment.empty()) {
		create_info.comment = Value(table_info.comment);
	}
	for (auto &[key, val] : table_info.tags) {
		create_info.tags[key] = val;
	}
	if (!table_info.required_filters.empty()) {
		create_info.tags["vgi_required_filters"] = RenderRequiredFiltersJson(table_info.required_filters);
	}

	if (table_info.arrow_schema) {
		auto type_schema = NativeColumnTypeSchema(table_info.arrow_schema, schema_name);
		if (table_info.row_id_column >= 0) {
			const auto &rowid_field = table_info.arrow_schema->field(table_info.row_id_column);
			if (ReadNativeColumnDefault(rowid_field, schema_name) ||
			    ReadNativeColumnGenerated(rowid_field, schema_name)) {
				throw InvalidInputException(
				    "VGI column expression: a row ID cannot declare a default or generated expression");
			}
			auto rowid_schema = type_schema->Select({table_info.row_id_column});
			ArrowSchemaWrapper c_schema;
			ArrowTableSchema arrow_table;
			vector<LogicalType> types;
			vector<string> names;
			ArrowSchemaToDuckDBTypes(context, rowid_schema, c_schema, arrow_table, types, names);
			table_info.rowid_type = types[0];

			// The row_id field is not a user column.
			auto filtered_schema = type_schema->RemoveField(table_info.row_id_column);
			ArrowSchemaToColumnList(context, filtered_schema, create_info.columns);
		} else {
			ArrowSchemaToColumnList(context, type_schema, create_info.columns);
		}
	}

	// Constraint indexes are shifted down past the removed row_id column.
	for (auto idx : table_info.not_null_constraints) {
		int adjusted = idx;
		if (table_info.row_id_column >= 0) {
			if (idx == table_info.row_id_column) {
				continue;
			}
			if (idx > table_info.row_id_column) {
				adjusted--;
			}
		}
		create_info.constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(adjusted)));
	}

	for (auto &cols : table_info.unique_constraints) {
		vector<duckdb::Identifier> col_names;
		for (auto idx : cols) {
			int adjusted = idx;
			if (table_info.row_id_column >= 0) {
				if (idx == table_info.row_id_column) {
					continue;
				}
				if (idx > table_info.row_id_column) {
					adjusted--;
				}
			}
			if (adjusted < 0 || (idx_t)adjusted >= create_info.columns.LogicalColumnCount()) {
				throw InvalidInputException(
				    "VGI: UNIQUE constraint references column index %d out of range (table '%s' has %llu columns)",
				    adjusted, create_info.GetTableName(), (unsigned long long)create_info.columns.LogicalColumnCount());
			}
			col_names.push_back(create_info.columns.GetColumn(LogicalIndex(adjusted)).Name());
		}
		// A key made only of the (skipped) row_id column has nothing left to constrain.
		if (!col_names.empty()) {
			create_info.constraints.push_back(make_uniq<UniqueConstraint>(std::move(col_names), false));
		}
	}

	for (auto &cols : table_info.primary_key_constraints) {
		vector<duckdb::Identifier> col_names;
		for (auto idx : cols) {
			int adjusted = idx;
			if (table_info.row_id_column >= 0) {
				if (idx == table_info.row_id_column) {
					continue;
				}
				if (idx > table_info.row_id_column) {
					adjusted--;
				}
			}
			if (adjusted < 0 || (idx_t)adjusted >= create_info.columns.LogicalColumnCount()) {
				throw InvalidInputException(
				    "VGI: PRIMARY KEY constraint references column index %d out of range (table '%s' has %llu columns)",
				    adjusted, create_info.GetTableName(), (unsigned long long)create_info.columns.LogicalColumnCount());
			}
			col_names.push_back(create_info.columns.GetColumn(LogicalIndex(adjusted)).Name());
		}
		if (!col_names.empty()) {
			create_info.constraints.push_back(make_uniq<UniqueConstraint>(std::move(col_names), true));
		}
	}

	for (auto &fk : table_info.foreign_key_constraints) {
		ForeignKeyInfo fk_info;
		fk_info.type = ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE;
		fk_info.schema = duckdb::Identifier(fk.referenced_schema);
		fk_info.table = duckdb::Identifier(fk.referenced_table);
		// The negotiated authority enforces this validated key on every write.
		// Worker physical indices are intentionally never imported into DuckDB.
		auto pk_cols = StringsToIdentifiers(vector<string>(fk.pk_columns.begin(), fk.pk_columns.end()));
		auto fk_cols = StringsToIdentifiers(vector<string>(fk.fk_columns.begin(), fk.fk_columns.end()));
		create_info.constraints.push_back(
		    make_uniq<ForeignKeyConstraint>(std::move(pk_cols), std::move(fk_cols), std::move(fk_info)));
	}

	// Construct generated columns with their category before AddColumn assigns
	// physical storage indexes. Bind the raw canonical SQL before adding the
	// engine's output cast, so a hostile result-type mismatch cannot be hidden.
	if (table_info.arrow_schema) {
		ColumnList rebuilt;
		idx_t logical = 0;
		for (int i = 0; i < table_info.arrow_schema->num_fields(); ++i) {
			if (i == table_info.row_id_column)
				continue;
			const auto &field = table_info.arrow_schema->field(i);
			auto column = create_info.columns.GetColumn(LogicalIndex(logical++)).Copy();
			auto generated = ReadNativeColumnGenerated(field, schema_name);
			if (generated) {
				ColumnDefinition definition(column.Name(), column.Type(), std::move(generated),
				                            TableColumnType::GENERATED);
				definition.SetComment(column.Comment());
				rebuilt.AddColumn(std::move(definition));
				continue;
			}
			auto canonical_default = BindNativeColumnDefault(context, field, schema_name, column.Type());
			if (canonical_default)
				column.SetDefaultValue(std::move(canonical_default));
			else if (field.metadata && VgiMetadata::FromC(field.metadata).FindKey(VGI_COLUMN_DEFAULT) < 0) {
				auto metadata = VgiMetadata::FromC(field.metadata);
				auto index = metadata.FindKey("default");
				if (index >= 0) {
					auto expressions = Parser(ParserOptions::Builtin()).ParseExpressionList(metadata.value(index));
					if (!expressions.empty())
						column.SetDefaultValue(std::move(expressions[0]));
				}
			}
			rebuilt.AddColumn(std::move(column));
		}
		ValidateNativeGeneratedColumns(context, rebuilt);
		for (idx_t index = 0; index < rebuilt.LogicalColumnCount(); ++index) {
			auto &column = rebuilt.GetColumnMutable(LogicalIndex(index));
			if (!column.Generated())
				continue;
			auto expression = column.GeneratedExpression().Copy();
			// Avoid duplicating a canonical CREATE's explicit engine cast.
			if (expression->GetExpressionClass() != ExpressionClass::CAST ||
			    UnboundType::TryDefaultBind(expression->Cast<CastExpression>().TargetType()) != column.Type()) {
				column.SetGeneratedExpression(std::move(expression));
			}
		}
		create_info.columns = std::move(rebuilt);
	}

	// Bind after column metadata is applied so generated columns cannot be
	// mistaken for the physical numeric operands qualified by this read profile.
	AppendNativeCheckConstraints(context, table_info, create_info);
	return create_info;
}

// Type equality: the field's own name, nullability and metadata are not part of
// its type; child fields compare by name, type and nullability.
static bool DataTypesEqual(const ArrowSchema &a, const ArrowSchema &b) {
	if (std::string(a.format ? a.format : "") != std::string(b.format ? b.format : "") ||
	    a.n_children != b.n_children) {
		return false;
	}
	constexpr int64_t kTypeFlags = ARROW_FLAG_DICTIONARY_ORDERED | ARROW_FLAG_MAP_KEYS_SORTED;
	if ((a.flags & kTypeFlags) != (b.flags & kTypeFlags)) {
		return false;
	}
	for (int64_t i = 0; i < a.n_children; i++) {
		if (!TypesEqual(*a.children[i], *b.children[i], /*check_names=*/true, /*check_metadata=*/false)) {
			return false;
		}
	}
	if ((a.dictionary == nullptr) != (b.dictionary == nullptr)) {
		return false;
	}
	return !a.dictionary || DataTypesEqual(*a.dictionary, *b.dictionary);
}

// extra_info for a FunctionInfo the worker sent that breaks the protocol.
static ErrorInfo FunctionInfoViolation(const std::string &worker_path, const std::string &function_name,
                                       const char *field) {
	return std::move(ErrorInfo(error_subtype::kProtocolViolation)
	                     .Worker(worker_path)
	                     .Function(function_name)
	                     .Set(error_key::kField, field));
}

VgiFunctionInfo ParseFunctionInfo(const VgiBatchPtr &batch, int64_t row_idx,
                                  const std::string &worker_path) {
	auto wire = generated::Decode<generated::FunctionInfo>(batch, row_idx, worker_path);
	VgiFunctionInfo info;
	info.name = std::move(wire.name);
	info.schema_name = SchemaNameFromPath(wire.schema_path, "FunctionInfo.schema_path");
	info.tags = std::move(wire.tags);
	info.function_type = FromWire(wire.function_type);
	// "public" (or absent) or "table_only"; anything else fails loudly, since a
	// guess could expose a function the worker meant to hide, or vice versa.
	if (wire.visibility && *wire.visibility != "public") {
		if (*wire.visibility == "table_only") {
			info.visibility = VgiFunctionVisibility::TableOnly;
		} else {
			throw IOException(FunctionInfoViolation(worker_path, info.name, "visibility"),
			                  "VGI worker '%s' lists function '%s' with invalid visibility '%s' (expected 'public' "
			                  "or 'table_only')",
			                  worker_path, info.name, *wire.visibility);
		}
	}
	info.description = std::move(wire.description);
	// Nullable on the wire; empty == unset, the convention tables/views use.
	info.comment = wire.comment.value_or("");
	// Nullable enums: absent leaves the default.
	if (wire.stability) {
		info.stability = FromWire(*wire.stability);
	}
	if (wire.null_handling) {
		info.null_handling = FromWire(*wire.null_handling);
	}
	if (wire.order_preservation) {
		info.order_preservation = FromWire(*wire.order_preservation);
	}

	// DuckDB keeps only the example SQL; the full examples go in the
	// vgi.example_queries tag unless the worker set it.
	const bool synthesize_examples = !wire.examples.empty() && info.tags.find("vgi.example_queries") == info.tags.end();
	std::unique_ptr<yyjson_mut_doc, decltype(&yyjson_mut_doc_free)> examples_doc(
	    synthesize_examples ? yyjson_mut_doc_new(nullptr) : nullptr, &yyjson_mut_doc_free);
	if (synthesize_examples && !examples_doc) {
		throw OutOfMemoryException("Could not allocate example metadata for function '%s'", info.name);
	}
	auto *queries = examples_doc ? yyjson_mut_arr(examples_doc.get()) : nullptr;
	for (auto &example : wire.examples) {
		if (examples_doc) {
			auto *entry = yyjson_mut_obj(examples_doc.get());
			yyjson_mut_obj_add_strncpy(examples_doc.get(), entry, "sql", example.sql.data(), example.sql.size());
			const auto &description = example.description;
			yyjson_mut_obj_add_strncpy(examples_doc.get(), entry, "description", description.data(), description.size());
			if (example.expected_output) {
				yyjson_mut_obj_add_strncpy(examples_doc.get(), entry, "expected_output", example.expected_output->data(),
				                           example.expected_output->size());
			}
			yyjson_mut_arr_append(queries, entry);
		}
		info.examples.push_back(std::move(example.sql));
	}
	if (examples_doc) {
		std::unique_ptr<char, decltype(&std::free)> json(yyjson_mut_val_write(queries, 0, nullptr), &std::free);
		if (!json) {
			throw OutOfMemoryException("Could not serialize example metadata for function '%s'", info.name);
		}
		info.tags.emplace("vgi.example_queries", json.get());
	}
	info.categories = std::move(wire.categories);

	// `arguments` / `output_schema` are serialized Arrow schemas.
	if (!wire.arguments.empty()) {
		info.arguments_schema = DeserializeSchema(wire.arguments);
	}

	const auto &argument_monotonicity = wire.argument_monotonicity;
	if (argument_monotonicity) {
		if (info.function_type != VgiFunctionType::Scalar) {
			throw IOException(FunctionInfoViolation(worker_path, info.name, "argument_monotonicity"), "Function '%s' declares argument_monotonicity but is not scalar", info.name);
		}
		if (!info.arguments_schema) {
			throw IOException(FunctionInfoViolation(worker_path, info.name, "argument_monotonicity"), "Function '%s' has argument_monotonicity without an arguments schema", info.name);
		}
		if (argument_monotonicity->size() != static_cast<idx_t>(info.arguments_schema->num_fields())) {
			throw IOException(FunctionInfoViolation(worker_path, info.name, "argument_monotonicity"), "Function '%s' argument_monotonicity has %llu entries, expected %d", info.name,
			                  argument_monotonicity->size(), info.arguments_schema->num_fields());
		}
		std::vector<VgiArgumentMonotonicity> parsed;
		parsed.reserve(argument_monotonicity->size());
		for (const auto &value : *argument_monotonicity) {
			parsed.push_back(RequireKnownEnum(ParseVgiArgumentMonotonicity(value), value, "argument_monotonicity",
			                                  worker_path, info.name));
		}
		info.argument_monotonicity = std::move(parsed);
	}

	if (!wire.output_schema.empty()) {
		info.output_schema = DeserializeSchema(wire.output_schema);
	}

	// Typed parameter defaults: a one-row IPC batch, validated here and kept as
	// bytes (DuckDB's scalar/aggregate registration cannot install defaults).
	auto &parameter_defaults = wire.parameter_default_values;
	if (parameter_defaults) {
		auto defaults_batch = DeserializeFromIpcBytes(*parameter_defaults);
		if (!defaults_batch || defaults_batch->num_rows() != 1) {
			throw IOException(FunctionInfoViolation(worker_path, info.name, "parameter_default_values"), "Function '%s' parameter_default_values must contain exactly one row", info.name);
		}
		if (!info.arguments_schema) {
			throw IOException(FunctionInfoViolation(worker_path, info.name, "parameter_default_values"), "Function '%s' has parameter_default_values without an arguments schema", info.name);
		}

		const auto &defaults_schema = defaults_batch->schema();
		int previous_argument_index = -1;
		for (int default_field_index = 0; default_field_index < defaults_schema->num_fields(); default_field_index++) {
			const auto &default_name = defaults_schema->field_names()[static_cast<size_t>(default_field_index)];
			const auto &default_field = defaults_schema->field(default_field_index);
			const auto argument_index = info.arguments_schema->GetFieldIndex(default_name);
			if (argument_index < 0) {
				throw IOException(FunctionInfoViolation(worker_path, info.name, "parameter_default_values"), "Function '%s' has a default for unknown parameter '%s'", info.name,
				                  default_name);
			}
			if (argument_index <= previous_argument_index) {
				throw IOException(FunctionInfoViolation(worker_path, info.name, "parameter_default_values"), "Function '%s' parameter defaults are not in signature order", info.name);
			}
			const auto &argument_field = info.arguments_schema->field(argument_index);
			if (!DataTypesEqual(argument_field, default_field)) {
				throw IOException(FunctionInfoViolation(worker_path, info.name, "parameter_default_values"), "Function '%s' default for parameter '%s' has type %s, expected %s", info.name,
				                  default_name, TypeToString(default_field),
				                  TypeToString(argument_field));
			}
			previous_argument_index = argument_index;
		}

		if (info.function_type == VgiFunctionType::Scalar || info.function_type == VgiFunctionType::Aggregate) {
			const auto default_count = defaults_batch->num_columns();
			int fixed_parameter_count = info.arguments_schema->num_fields();
			if (fixed_parameter_count > 0) {
				const auto metadata = info.arguments_schema->field_metadata(fixed_parameter_count - 1);
				const auto varargs_index = metadata.FindKey(VGI_VARARGS_METADATA_KEY);
				if (varargs_index >= 0 && metadata.value(varargs_index) == VGI_VARARGS_TRUE_VALUE) {
					fixed_parameter_count--;
				}
			}
			if (default_count > fixed_parameter_count) {
				throw IOException(FunctionInfoViolation(worker_path, info.name, "parameter_default_values"), "Function '%s' has more defaults than parameters", info.name);
			}
			for (int default_index = 0; default_index < default_count; default_index++) {
				const auto expected_parameter = fixed_parameter_count - default_count + default_index;
				if (defaults_schema->field_names()[static_cast<size_t>(default_index)] !=
				    info.arguments_schema->field_names()[static_cast<size_t>(expected_parameter)]) {
					throw IOException(FunctionInfoViolation(worker_path, info.name, "parameter_default_values"), "Function '%s' defaults must form a trailing parameter sequence", info.name);
				}
			}
		}
		info.parameter_default_values_bytes = std::move(*parameter_defaults);
	}

	// late_materialization nullopt means "not capable" (VgiTableEntry's gate).
	info.projection_pushdown = wire.projection_pushdown;
	info.filter_pushdown = wire.filter_pushdown;
	info.sampling_pushdown = wire.sampling_pushdown;
	info.late_materialization = wire.late_materialization;
	info.filter_semantic_profiles = std::move(wire.filter_semantic_profiles);
	for (auto &fn : wire.additional_filter_functions) {
		info.additional_filter_functions.push_back({std::move(fn.namespace_), std::move(fn.name), fn.version});
	}

	info.max_workers = wire.max_workers;
	// supports_batch_index: batches carry vgi_batch_index metadata, surfaced via
	// get_partition_data so ordered sinks reassemble parallel output (and the
	// FIXED_ORDER MaxThreads=1 clamp is skipped).
	info.supports_batch_index = wire.supports_batch_index;
	info.supports_splits = wire.supports_splits;
	info.filters_exactly_applied = wire.filters_exactly_applied;
	info.supports_positions = wire.supports_positions;
	// nullopt means unbounded, NOT "expires immediately".
	info.split_token_ttl_seconds = wire.split_token_ttl_seconds;
	// partition_kind mirrors DuckDB's TablePartitionInfo; only SINGLE_VALUE_PARTITIONS
	// materially affects planning today (vgi_table_function_set.cpp).
	info.partition_kind = FromWire(wire.partition_kind);
	info.order_dependent = FromWire(wire.order_dependent);
	info.distinct_dependent = FromWire(wire.distinct_dependent);
	info.supports_window = wire.supports_window;
	// streaming_partitioned: the aggregate_streaming_open/_chunk/_close protocol, which
	// the optimizer may use for eligible LogicalWindow nodes.
	info.streaming_partitioned = wire.streaming_partitioned;
	// has_finalize=false omits in_out_function_final, so the function can run under
	// LATERAL with correlated input.
	info.has_finalize = wire.has_finalize;
	// TableBuffering only: ParallelSource / ParallelSink / RequiredPartitionInfo.
	info.source_order_dependent = wire.source_order_dependent;
	info.sink_order_dependent = wire.sink_order_dependent;
	info.requires_input_batch_index = wire.requires_input_batch_index;
	// A blended ("UNNEST-style") table-in-out whose positional args ARE its input.
	info.input_from_args = wire.input_from_args;
	info.input_rows_per_call = wire.input_rows_per_call;

	info.required_settings = std::move(wire.required_settings);
	for (auto &secret : wire.required_secrets) {
		if (secret.secret_type.empty()) {
			continue;
		}
		VgiSecretRequirement req;
		req.secret_type = std::move(secret.secret_type);
		req.name = secret.secret_name.value_or("");
		req.scope = secret.scope.value_or("");
		info.required_secrets.push_back(std::move(req));
	}

	return info;
}

// Secret extraction from DuckDB's SecretManager

// Copies out; call while the secret reference is still valid.
static std::map<std::string, Value> ExtractSecretKeyValues(const KeyValueSecret &kv_secret) {
	std::map<std::string, Value> kv_pairs;
	kv_pairs["type"] = Value(kv_secret.GetType());
	kv_pairs["provider"] = Value(kv_secret.GetProvider());
	kv_pairs["name"] = Value(kv_secret.GetName());
	for (const auto &[k, v] : kv_secret.secret_map) {
		kv_pairs[k.GetIdentifierName()] = v;
	}
	// Scope prefixes, newline-joined, written AFTER secret_map so a "scope" key
	// there cannot override it: workers match paths against it (e.g. per S3
	// bucket). Empty for an unscoped secret.
	const auto &scope = kv_secret.GetScope();
	std::string joined_scope;
	for (idx_t i = 0; i < scope.size(); i++) {
		if (i > 0) {
			joined_scope += "\n";
		}
		joined_scope += scope[i];
	}
	kv_pairs["scope"] = Value(joined_scope);
	return kv_pairs;
}

// Keyed by secret name, so several secrets of one type (e.g. S3 buckets) all
// survive and the same secret found via several scopes dedups. An empty name
// (not expected) falls back to the type.
static void InsertResolvedSecret(std::map<std::string, std::map<std::string, Value>> &result,
                                 const std::string &secret_type, const KeyValueSecret &kv_secret) {
	std::string key = kv_secret.GetName().GetIdentifierName();
	if (key.empty()) {
		key = secret_type;
	}
	result[key] = ExtractSecretKeyValues(kv_secret);
}

std::map<std::string, std::map<std::string, Value>> ExtractVgiSecrets(
    ClientContext &context, const std::vector<VgiSecretRequirement> &requirements) {
	std::map<std::string, std::map<std::string, Value>> result;

	if (requirements.empty()) {
		return result;
	}

	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);

	for (const auto &req : requirements) {
		if (!req.name.empty()) {
			// By name; the type must also match.
			auto secret_entry = secret_manager.GetSecretByName(transaction, req.name);
			if (secret_entry) {
				auto &base_secret = secret_entry->secret;
				if (base_secret->GetType() == req.secret_type) {
					auto *kv_secret = dynamic_cast<const KeyValueSecret *>(base_secret.get());
					if (kv_secret) {
						InsertResolvedSecret(result, req.secret_type, *kv_secret);
					}
				}
			}
		} else {
			// By scope (unscoped when empty) and type.
			auto match = secret_manager.LookupSecret(transaction, req.scope, req.secret_type);
			if (match.HasMatch()) {
				auto *kv_secret = dynamic_cast<const KeyValueSecret *>(&match.GetSecret());
				if (kv_secret) {
					InsertResolvedSecret(result, req.secret_type, *kv_secret);
				}
			}
		}
	}

	return result;
}

} // namespace vgi
} // namespace duckdb
