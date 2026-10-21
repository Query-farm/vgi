// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_settings_defaults.hpp"
#include "vgi_catalog_api.hpp"

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
#include "vgi_arrow_ipc.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_exception.hpp"
#include "vgi_http_client.hpp"
#include "vgi_http_params.hpp"
#include "vgi_logging.hpp"
#include "vgi_profiling.hpp"
#include "vgi_protocol_constants.hpp"

#include <cstdlib>
#include <memory>
#include <set>
#include <typeinfo>
#include "vgi_rpc_client.hpp"
#include "vgi_rpc_types.hpp"
#include "generated/vgi_request_builders.hpp"
#include "vgi_schema_registry.hpp"
#include "vgi_wire_codec.hpp"
#include "vgi_transport.hpp"
#include "vgi_unary_rpc.hpp"
#include "yyjson.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {
namespace vgi {

namespace {

// Adapt CatalogRpcContext::transaction_opaque_data (`std::vector<uint8_t>`, empty == none)
// to the std::optional<std::vector<uint8_t>> shape the generated request builders
// expect. The empty-as-absent convention predates the generators; rather than
// thread std::optional through every storage-layer ctx construction, we wrap
// here at the catalog-API boundary.
inline std::optional<std::vector<uint8_t>> OptTxn(const CatalogRpcContext &ctx) {
	return ctx.transaction_opaque_data.empty()
	           ? std::nullopt
	           : std::optional<std::vector<uint8_t>>(ctx.transaction_opaque_data);
}

// Adapt empty-string-as-absent strings (e.g. at_unit/at_value time-travel args)
// to std::optional<std::string>. Mirrors the legacy BuildNullableStringScalar
// behaviour the hand-coded builders relied on.
inline std::optional<std::string> OptStrIfNonEmpty(const std::string &s) {
	return s.empty() ? std::nullopt : std::optional<std::string>(s);
}

// Adapt explicit (value, is_null) pairs from the existing Invoke* signatures
// to std::optional<std::string>. Used for COMMENT ON ... and similar where the
// caller carries an out-of-band null flag.
inline std::optional<std::string> OptStrNullable(const std::string &s, bool is_null) {
	return is_null ? std::nullopt : std::optional<std::string>(s);
}

// Wraps every catalog RPC dispatch with an `outcome=ok|error` `catalog.rpc`
// log carrying duration_ms + the entity context callers have populated on
// CatalogRpcContext. Replaces the older start-side `rpc.invoke` log so each
// dispatch produces exactly one event with the answer to "did it succeed and
// how long did it take", which is what timeline analysis actually wants.
//
// The caller calls MarkOk() on the success path; the destructor distinguishes
// normal-return vs. exception-unwind and logs accordingly. Also doubles as a
// ScopedTimer so VGI_PROFILE=1 runs aggregate per-method totals at exit.
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

	// Stash the exception type/message we caught so the destructor can include
	// them. Used by the explicit catch-then-rethrow path; std::current_exception
	// is unreliable to inspect during stack unwind without a catch.
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

// ============================================================================
// VgiAttachParameters — HTTPParams cache accessor
// ============================================================================

// See the comment on GetOrInitHttpParams in vgi_catalog_api.hpp for the
// rationale. Initialization happens once per catalog; subsequent RPCs reuse
// the same HTTPParams, which is how we avoid re-entering the secret manager
// (and its MetaTransaction mutex) from under VgiTransaction::Start on HTTP
// transport.
std::shared_ptr<HTTPParams> VgiAttachParameters::GetOrInitHttpParams(
    ClientContext &context, const std::string &url) const {
	std::lock_guard<std::mutex> lock(http_params_mutex_);
	if (!cached_http_params_) {
		cached_http_params_ = InitHttpParamsWithSecrets(context, url);
		// Mirror the per-call ApplyHttpTimeout that HttpPostArrowIpcInternal
		// used to do. Captured once at cache-init time — vgi_http_timeout_seconds
		// changes won't be picked up without re-ATTACH. See the TODO on the
		// method declaration.
		Value timeout_val;
		if (context.TryGetCurrentSetting("vgi_http_timeout_seconds", timeout_val)) {
			cached_http_params_->timeout = static_cast<uint64_t>(timeout_val.GetValue<int64_t>());
		} else {
			cached_http_params_->timeout = defaults::HTTP_TIMEOUT_SECONDS;
		}
	}
	return cached_http_params_;
}

std::shared_ptr<VgiHttpClientPool> VgiAttachParameters::GetOrInitHttpClientPool() const {
	std::lock_guard<std::mutex> lock(http_params_mutex_);
	if (!http_client_pool_) {
		http_client_pool_ = std::make_shared<VgiHttpClientPool>();
	}
	return http_client_pool_;
}

// ============================================================================
// RPC-based Catalog API Helper
// ============================================================================

// extra_info for a worker that answered a catalog RPC but broke the protocol
// (missing/empty/mis-shaped response).
static ErrorInfo CatalogProtocolViolation(const std::string &method_name, const std::string &worker_path) {
	return std::move(ErrorInfo(error_subtype::kProtocolViolation).Rpc(method_name).Worker(worker_path));
}

// Rethrow the in-flight exception with the catalog RPC's context (method,
// catalog, entity) added to its extra_info. Must be called from inside a catch
// block. Preserves the exception's concrete type and its message byte-for-byte;
// fields the inner exception already carries win over the ones added here.
//   - VgiRpcException is rebuilt as VgiRpcException: callers pattern-match on
//     it (capability fallback, catalog_version probe, vgi_extension.cpp).
//   - InterruptException passes through untouched.
//   - Exception types ThrowTypedException cannot rebuild faithfully (HTTP,
//     INTERNAL, PERMISSION, ...) and non-DuckDB exceptions pass through.
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
	// Non-DuckDB exceptions propagate out of the try above unchanged.
}

// Invoke a unary RPC catalog method and return the raw unary response.
// Delegates to InvokePooledUnaryRpc which handles pool acquire/release, stderr
// draining (critical — without it, long-running catalogs hang when the worker
// stderr pipe buffer fills), and stale-pool retry.
//
// `protocol` selects the routing key (and the URL's protocol segment on HTTP).
// Only `vgi.v2` methods have entries in the schema registry, so request
// validation applies to those alone; other protocols' callers decode by name.
static UnaryResponseResult InvokeRpcMethod(const CatalogRpcContext &ctx, const std::string &method_name,
                                            const std::shared_ptr<arrow::RecordBatch> &params, ClientContext &context,
                                            const VgiProtocolId &protocol = VGI_MAIN_PROTOCOL) {
	CatalogRpcInstrumentation instr(context, ctx, method_name);
	try {
		// Validate the outgoing request against the registered params schema.
		// Catches encoder drift in BuildXxxParams (e.g. flipped nullability,
		// missing fields) at the C++ boundary, before it turns into an opaque
		// failure on the worker side.
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
		opts.tcp_proxy = ctx.params->tcp_proxy();
		opts.iroh = ctx.params->iroh();
		// Cache-hit on the HTTP path: avoids re-entering the secret manager (and
		// thus the MetaTransaction mutex) for RPCs invoked from inside
		// VgiTransaction::Start. No-op for subprocess transport. See the TODO on
		// VgiAttachParameters::GetOrInitHttpParams.
		if (IsHttpTransport(ctx.params->worker_path()) || IsHttpiTransport(ctx.params->worker_path())) {
			if (IsHttpTransport(ctx.params->worker_path())) {
			opts.cached_http_params = ctx.params->GetOrInitHttpParams(context, ctx.params->worker_path());
			}
			// Keep-alive client pool: consecutive catalog RPCs reuse TCP/TLS
			// connections instead of handshaking per call.
			opts.http_client_pool = ctx.params->GetOrInitHttpClientPool();
			// Share the catalog's capability snapshot with the streaming
			// connections: whichever path first sees the server's
			// VGI-Supported-Encodings settles the codec for both.
			opts.server_caps = ctx.params->server_caps();
		}
		// Forward launcher overrides for `launch:` LOCATIONs.  Empty
		// optionals on every other transport — no per-call cost.
		if (ctx.params->launcher_idle_timeout_seconds().has_value()) {
			opts.launcher_idle_timeout =
			    std::chrono::seconds(*ctx.params->launcher_idle_timeout_seconds());
		}
		if (ctx.params->launcher_state_dir().has_value()) {
			opts.launcher_state_dir = *ctx.params->launcher_state_dir();
		}
		opts.protocol = protocol;
		auto result = InvokePooledUnaryRpc(opts, method_name, params);
		instr.MarkOk();
		return result;
	} catch (const std::exception &e) {
		// typeid().name() gives the demangled-or-mangled C++ type — close enough
		// for triage. The full message goes in error_message; consumers that need
		// the DuckDB ExceptionType can re-parse there.
		instr.NoteException(typeid(e).name(), e.what());
		RethrowWithCatalogRpcContext(ctx, method_name);
	}
}

// Extract result binary bytes from a unary RPC response batch,
// then deserialize to a RecordBatch.
static std::shared_ptr<arrow::RecordBatch> ExtractAndDeserializeResult(
    const UnaryResponseResult &response, const std::string &method_name, const std::string &worker_path) {
	// An empty envelope or a NULL result is a void/absent result, not an error.
	std::shared_ptr<arrow::RecordBatch> result;
	if (response.batch && response.batch->num_rows() != 0) {
		result = UnwrapResultEnvelope(response.batch, method_name, CatalogProtocolViolation(method_name, worker_path),
		                              /*null_result_ok=*/true);
	}

	ValidateResponseSchema(result, method_name, worker_path);
	return result;
}

// Invoke a void-response RPC. After dispatch, run the response through the
// schema registry to catch drift: the worker must return an empty envelope
// (or empty inner batch) for methods registered as void. Any payload triggers
// a clear mismatch error instead of being silently discarded.
static void InvokeVoidRpc(const CatalogRpcContext &ctx, const std::string &method_name,
                          const std::shared_ptr<arrow::RecordBatch> &params, ClientContext &context) {
	auto response = InvokeRpcMethod(ctx, method_name, params, context);
	ExtractAndDeserializeResult(response, method_name, ctx.params->worker_path());
}

// ============================================================================
// vgi_rpc.Reflection.v1
// ============================================================================

std::shared_ptr<arrow::RecordBatch> InvokeProtocolUnary(const CatalogRpcContext &ctx, ClientContext &context,
                                                        const VgiProtocolId &protocol, const std::string &method_name,
                                                        const std::shared_ptr<arrow::RecordBatch> &params) {
	auto response = InvokeRpcMethod(ctx, method_name, params, context, protocol);
	const auto &worker_path = ctx.params->worker_path();
	const std::string protocol_name(protocol.name);
	auto reflection_violation = [&]() {
		return ErrorInfo(error_subtype::kProtocolViolation).Rpc(protocol_name + "/" + method_name).Worker(worker_path);
	};
	// The ordinary unary envelope: a structured return rides as one serialized
	// IPC batch in a binary `result` column.
	auto payload = UnwrapResultEnvelope(response.batch, protocol_name + "/" + method_name,
	                                    ErrorInfo().Worker(worker_path));
	if (!payload || payload->num_rows() < 1) {
		throw IOException(reflection_violation(), "%s/%s reply carried no rows [worker: %s]", protocol_name, method_name,
		                  worker_path);
	}
	return payload;
}

std::shared_ptr<arrow::RecordBatch> InvokeReflectionListProtocols(const CatalogRpcContext &ctx,
                                                                  ClientContext &context) {
	// Reflection declares no protocol_version and is exempt from the version
	// gate, so the request carries the routing key and no version.
	static constexpr VgiProtocolId kReflection {REFLECTION_PROTOCOL_NAME, ""};
	return InvokeProtocolUnary(ctx, context, kReflection, "list_protocols", nullptr);
}

// ============================================================================
// Typed Catalog RPC Functions
// ============================================================================

std::vector<VgiCatalogInfo> InvokeCatalogs(const std::string &worker_path, ClientContext &context,
                                            bool worker_debug, bool use_pool,
	                                            const std::shared_ptr<CatalogAuth> &auth,
	                                            std::optional<int64_t> launcher_idle_timeout_seconds,
	                                            std::optional<std::string> launcher_state_dir,
	                                            std::shared_ptr<void> worker_artifact_anchor,
	                                            const std::string &tcp_proxy,
	                                            std::shared_ptr<IrohClientConfig> iroh) {
	// Use the config struct so launcher overrides flow into the temp_params —
	// same rationale as InvokeCatalogAttach below. Without these, this RPC
	// would prime the launcher cache with [defaults]; the user's subsequent
	// ATTACH (carrying overrides) would then trip the cache-pin's
	// BinderException, breaking even one-shot queries with custom launcher
	// options.
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
	                                    ServerCapabilities *discovered_capabilities) {
	// Use the config struct so launcher overrides flow into the temp_params.
	// Without these, the catalog_attach RPC would prime the launcher cache
	// with [defaults]; the subsequent real ATTACH (carrying overrides) would
	// then trip the cache pin's BinderException, breaking even one-shot
	// queries with custom launcher options.
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

	auto request_batch = BuildCatalogAttachRequest(catalog_name, options_ipc_bytes, data_version_spec, implementation_version);
	auto request_bytes = SerializeToIpcBytes(request_batch);
	auto params = generated::BuildCatalogAttachParams(request_bytes);
	auto response = InvokeRpcMethod(ctx, "catalog_attach", params, context);
	auto result_batch = ExtractAndDeserializeResult(response, "catalog_attach", worker_path);
	if (!result_batch) {
		throw IOException(CatalogProtocolViolation("catalog_attach", worker_path), "Empty response from catalog_attach [worker: %s]", worker_path);
	}
	auto result = ParseCatalogAttachResult(result_batch, worker_path, context);
	if (discovered_capabilities) {
		*discovered_capabilities = temp_params->LoadServerCapabilities();
	}
	return result;
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

// Open a category exactly once, retaining slices of the response's buffers.
// Singleton deserializers deliberately read only the first batch and must
// not be used for these streams.
std::vector<std::shared_ptr<arrow::RecordBatch>> ReadContentsStream(
    const arrow::BinaryArray &values, const ContentsCategory &category, const std::string &worker_path) {
	std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
	if (values.IsNull(0)) {
		return batches;
	}
	auto violation = CatalogProtocolViolation("catalog_contents", worker_path).Set(error_key::kField, category.column);
	auto buffer = arrow::SliceBuffer(values.value_data(), values.value_offset(0), values.value_length(0));
	auto input = std::make_shared<arrow::io::BufferReader>(buffer);
	auto opened = arrow::ipc::RecordBatchStreamReader::Open(input);
	if (!opened.ok()) {
		throw IOException(violation, "catalog_contents: invalid '%s' IPC stream: %s", category.column,
		                  opened.status().ToString());
	}
	auto reader = opened.ValueUnsafe();
	const auto *contract = LookupResponseSchema(category.items_method);
	if (!contract || !contract->item_schema) {
		throw InternalException("Missing catalog metadata schema for %s", category.column);
	}
	if (!reader->schema()->Equals(*contract->item_schema, /*check_metadata=*/false)) {
		throw IOException(violation, "catalog_contents: '%s' stream has an incompatible metadata schema", category.column);
	}
	for (;;) {
		std::shared_ptr<arrow::RecordBatch> batch;
		auto status = reader->ReadNext(&batch);
		if (!status.ok()) {
			throw IOException(violation, "catalog_contents: cannot read '%s' batch: %s", category.column, status.ToString());
		}
		if (!batch) {
			break;
		}
		status = batch->ValidateFull();
		if (!status.ok()) {
			throw IOException(violation, "catalog_contents: invalid '%s' batch: %s", category.column, status.ToString());
		}
		for (int column = 0; column < batch->num_columns(); column++) {
			if (batch->schema()->field(column)->nullable()) {
				continue;
			}
			bool has_null = batch->column(column)->null_count() != 0;
			if (auto dictionary = std::dynamic_pointer_cast<arrow::DictionaryArray>(batch->column(column))) {
				for (int64_t row = 0; !has_null && row < batch->num_rows(); row++) {
					has_null = dictionary->dictionary()->IsNull(dictionary->GetValueIndex(row));
				}
			}
			if (has_null) {
				throw IOException(violation, "catalog_contents: required '%s.%s' contains nulls", category.column,
				                  batch->schema()->field(column)->name());
			}
		}
		batches.push_back(std::move(batch));
	}
	const auto position = input->Tell();
	if (!position.ok() || position.ValueUnsafe() != buffer->size()) {
		throw IOException(violation, "catalog_contents: trailing bytes after '%s' IPC stream", category.column);
	}
	return batches;
}

std::vector<std::string> ContentsPath(const std::shared_ptr<arrow::RecordBatch> &batch, int64_t row,
                                       const char *field, const std::string &worker_path) {
	const auto values = std::static_pointer_cast<arrow::ListArray>(batch->GetColumnByName(field));
	const auto parts = std::static_pointer_cast<arrow::StringArray>(values->values());
	std::vector<std::string> path;
	for (int64_t i = values->value_offset(row); i < values->value_offset(row + 1); i++) {
		if (parts->IsNull(i) || parts->value_length(i) == 0) {
			throw IOException(CatalogProtocolViolation("catalog_contents", worker_path).Set(error_key::kField, field),
			                  "catalog_contents: schema path contains a null or empty component");
		}
		path.push_back(parts->GetString(i));
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

VgiCatalogContents ParseCatalogContents(const std::shared_ptr<arrow::RecordBatch> &result_batch,
                                         const std::string &worker_path,
                                         const std::optional<std::string> &if_none_match) {
	ValidateResponseSchema(result_batch, "catalog_contents", worker_path);
	if (!result_batch || result_batch->num_rows() != 1) {
		throw IOException(CatalogProtocolViolation("catalog_contents", worker_path),
		                  "catalog_contents must contain exactly one response row [worker: %s]", worker_path);
	}
	auto validity = result_batch->ValidateFull();
	if (!validity.ok()) {
		throw IOException(CatalogProtocolViolation("catalog_contents", worker_path),
		                  "Invalid catalog_contents response: %s", validity.ToString());
	}
	for (int column = 0; column < result_batch->num_columns(); column++) {
		if (!result_batch->schema()->field(column)->nullable() && result_batch->column(column)->IsNull(0)) {
			throw IOException(CatalogProtocolViolation("catalog_contents", worker_path),
			                  "catalog_contents required field '%s' is null", result_batch->schema()->field(column)->name());
		}
	}

	VgiCatalogContents result;
	auto version_col = std::static_pointer_cast<arrow::Int64Array>(result_batch->GetColumnByName("catalog_version"));
	result.catalog_version = version_col->Value(0);
	auto etag_col = std::static_pointer_cast<arrow::StringArray>(result_batch->GetColumnByName("etag"));
	if (!etag_col->IsNull(0)) {
		result.etag = etag_col->GetString(0);
	}
	auto not_modified_col =
	    std::static_pointer_cast<arrow::BooleanArray>(result_batch->GetColumnByName("not_modified"));
	result.not_modified = !not_modified_col->IsNull(0) && not_modified_col->Value(0);
	if (result.not_modified) {
		// Only meaningful as the answer to a matching if_none_match.
		if (!if_none_match || result.etag != if_none_match) {
			throw IOException(CatalogProtocolViolation("catalog_contents", worker_path).Set(error_key::kField, "not_modified"),
			                  "catalog_contents answered not_modified without a matching if_none_match [worker: %s]",
			                  worker_path);
		}
		for (auto &category : kContentsCategories) {
			if (!result_batch->GetColumnByName(category.column)->IsNull(0)) {
				throw IOException(CatalogProtocolViolation("catalog_contents", worker_path),
				                  "catalog_contents not_modified response contains '%s' metadata", category.column);
			}
		}
		return result;
	}

	std::map<std::vector<std::string>, size_t> schema_index;
	for (auto &category : kContentsCategories) {
		auto values = std::static_pointer_cast<arrow::BinaryArray>(result_batch->GetColumnByName(category.column));
		for (auto &batch : ReadContentsStream(*values, category, worker_path)) {
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
	// catalog_table_get's Protocol schema always includes at_unit/at_value (nullable);
	// the overload without time-travel passes std::nullopt for both.
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

VgiScanFunctionResult InvokeCatalogTableScanFunctionGet(
    const CatalogRpcContext &ctx, const std::string &schema_name,
    const std::string &table_name, ClientContext &context, const std::string &at_unit, const std::string &at_value) {
	auto &worker_path = ctx.params->worker_path();
	auto params = generated::BuildCatalogTableScanFunctionGetParams(
	    ctx.attach_opaque_data, SingleSchemaPath(schema_name), table_name, OptStrIfNonEmpty(at_unit),
	    OptStrIfNonEmpty(at_value), OptTxn(ctx));
	auto response = InvokeRpcMethod(ctx, "catalog_table_scan_function_get", params, context);
	auto result_batch = ExtractAndDeserializeResult(response, "catalog_table_scan_function_get", worker_path);
	if (!result_batch || result_batch->num_rows() == 0) {
		throw IOException(CatalogProtocolViolation("catalog_table_scan_function_get", worker_path), "Empty response from catalog_table_scan_function_get [worker: %s]", worker_path);
	}
	return ParseScanFunctionResult(context, result_batch, worker_path);
}

// Helper: wrap a single-function (legacy) result as a one-branch
// VgiScanBranchesResult. The branch carries no branch_filter (unconstrained)
// and inherits required_extensions from the legacy result.
static VgiScanBranchesResult WrapLegacyAsOneBranch(VgiScanFunctionResult legacy) {
	VgiScanBranchesResult out;
	VgiScanBranch branch;
	branch.function_name = std::move(legacy.function_name);
	branch.positional_arguments = std::move(legacy.positional_arguments);
	branch.named_arguments = std::move(legacy.named_arguments);
	branch.branch_filter.clear();
	// parsed_branch_filter stays null — no filter to parse.
	out.branches.push_back(std::move(branch));
	out.required_extensions = std::move(legacy.required_extensions);
	return out;
}

VgiScanBranchesResult InvokeCatalogTableScanBranchesGet(
    const CatalogRpcContext &ctx, const std::string &schema_name,
    const std::string &table_name, ClientContext &context, const std::string &at_unit, const std::string &at_value) {
	auto &worker_path = ctx.params->worker_path();

	// Capability-detection cache (A8): if a prior call against this attach
	// already determined the worker doesn't implement the new method, skip
	// the doomed RPC + exception entirely and go straight to the legacy
	// fallback. State machine: 0 = unknown (probe), 1 = supported,
	// 2 = not supported. Set by this function after each probe.
	const int cap = ctx.params->LoadBranchesCapability();
	if (cap == 2) {
		auto legacy = InvokeCatalogTableScanFunctionGet(ctx, schema_name, table_name, context, at_unit, at_value);
		return WrapLegacyAsOneBranch(std::move(legacy));
	}

	// Try the new method (either because cap == 1 / known-supported, or
	// cap == 0 / unknown — probe). If the worker doesn't implement it, the
	// Python-side dispatcher raises MethodNotImplementedError, which gets
	// serialised to an EXCEPTION batch with metadata
	// vgi_rpc.error_kind=method_not_implemented (see vgi-rpc commit adding
	// the typed marker). The C++ side surfaces that as VgiRpcException,
	// which we catch narrowly here.
	auto params = generated::BuildCatalogTableScanBranchesGetParams(
	    ctx.attach_opaque_data, SingleSchemaPath(schema_name), table_name, OptStrIfNonEmpty(at_unit),
	    OptStrIfNonEmpty(at_value), OptTxn(ctx));
	try {
		auto response = InvokeRpcMethod(ctx, "catalog_table_scan_branches_get", params, context);
		auto result_batch = ExtractAndDeserializeResult(response, "catalog_table_scan_branches_get", worker_path);
		if (!result_batch || result_batch->num_rows() == 0) {
			throw IOException(CatalogProtocolViolation("catalog_table_scan_branches_get", worker_path), "Empty response from catalog_table_scan_branches_get [worker: %s]", worker_path);
		}
		auto parsed = ParseScanBranchesResult(context, result_batch, worker_path);
		// Probe succeeded — pin the cache so future calls skip the legacy
		// fallback path entirely. Idempotent on warm-cache hit (cap == 1).
		ctx.params->StoreBranchesCapability(true);
		return parsed;
	} catch (const VgiRpcException &e) {
		if (e.GetErrorKind() == error_kind::kMethodNotImplemented) {
			// Old worker: fall back to the legacy single-function RPC and
			// synthesise a one-branch result. Pin the cache so the next
			// catalog read skips the doomed-RPC round-trip — saves the
			// IPC round-trip + EXCEPTION-batch parsing on every subsequent
			// scan against this attach.
			ctx.params->StoreBranchesCapability(false);
			VGI_LOG(context, "catalog.rpc.scan_branches.fallback",
			    {{"worker_path", worker_path},
			     {"schema_name", schema_name},
			     {"table_name", table_name}});
			auto legacy = InvokeCatalogTableScanFunctionGet(ctx, schema_name, table_name, context, at_unit, at_value);
			return WrapLegacyAsOneBranch(std::move(legacy));
		}
		throw;
	}
}

// ============================================================================
// Table Write Function Get
// ============================================================================

static VgiWriteFunctionResult InvokeCatalogTableWriteFunctionGet(
    const CatalogRpcContext &ctx, const std::string &rpc_method,
    const std::string &schema_name, const std::string &table_name, ClientContext &context,
    const std::optional<std::string> &writable_branch_function_name = std::nullopt) {
	auto &worker_path = ctx.params->worker_path();
	std::shared_ptr<arrow::RecordBatch> params;
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

// ============================================================================
// Transaction Lifecycle
// ============================================================================

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
		// Only a worker that does not implement catalog_version maps to 0
		// ("unknown version": the caller clears its cache as a safe fallback).
		// This used to be catch (...), which also swallowed interrupts, auth
		// failures, transport errors and worker crashes, turning each into a
		// silent cache clear. The narrowing is safe because the probe uses the
		// same rule as the catalog_table_scan_branches_get fallback and the
		// COPY-format discovery in vgi_extension.cpp: match the typed
		// error_kind=method_not_implemented marker only. A worker too old to
		// send that marker also fails the exact major.minor protocol_version
		// check at dispatch, so it cannot get this far anyway. Every other
		// failure now reaches the user.
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

// ============================================================================
// DDL Operations
// ============================================================================

void InvokeCatalogTableCreate(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::shared_ptr<arrow::Schema> &columns_schema,
    const std::string &on_conflict,
    const std::vector<int> &not_null_constraints,
    const std::vector<std::vector<int>> &unique_constraints,
    const std::vector<std::string> &check_constraints,
    const std::vector<std::vector<int>> &primary_key_constraints,
    const std::vector<std::vector<uint8_t>> &foreign_key_constraints,
    ClientContext &context) {
	auto request_batch =
	    BuildTableCreateRequest(ctx.attach_opaque_data, SingleSchemaPath(schema_name), table_name, columns_schema,
	                            on_conflict, not_null_constraints, unique_constraints, check_constraints,
	                            primary_key_constraints, foreign_key_constraints, ctx.transaction_opaque_data);
	auto request_bytes = SerializeToIpcBytes(request_batch);
	auto params = generated::BuildCatalogTableCreateParams(request_bytes);
	InvokeVoidRpc(ctx, "catalog_table_create", params, context);
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
    const std::shared_ptr<arrow::Schema> &column_definition,
    bool if_column_not_exists,
    ClientContext &context) {
	auto column_bytes = SerializeSchemaToIpcBytes(column_definition);
	auto params = generated::BuildCatalogTableColumnAddParams(
	    ctx.attach_opaque_data, SingleSchemaPath(schema_name), table_name, column_bytes,
	    /*ignore_not_found=*/false, if_column_not_exists, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_table_column_add", params, context);
}

void InvokeCatalogTableColumnDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name, bool if_column_exists, bool cascade,
    ClientContext &context) {
	auto params = generated::BuildCatalogTableColumnDropParams(
	    ctx.attach_opaque_data, SingleSchemaPath(schema_name), table_name, column_name,
	    /*ignore_not_found=*/false, if_column_exists, cascade, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_table_column_drop", params, context);
}

void InvokeCatalogTableColumnRename(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &old_column_name, const std::string &new_column_name,
    ClientContext &context) {
	auto params = generated::BuildCatalogTableColumnRenameParams(ctx.attach_opaque_data, SingleSchemaPath(schema_name),
	                                                             table_name, old_column_name, new_column_name,
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
	    ctx.attach_opaque_data, SingleSchemaPath(schema_name), table_name, column_name,
	    OptStrNullable(comment, comment_is_null), ignore_not_found, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_table_column_comment_set", params, context);
}

void InvokeCatalogTableColumnTypeChange(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::shared_ptr<arrow::Schema> &column_definition,
    const std::string &expression,
    ClientContext &context) {
	auto column_bytes = SerializeSchemaToIpcBytes(column_definition);
	auto params = generated::BuildCatalogTableColumnTypeChangeParams(
	    ctx.attach_opaque_data, SingleSchemaPath(schema_name), table_name, column_bytes, OptStrIfNonEmpty(expression),
	    /*ignore_not_found=*/false, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_table_column_type_change", params, context);
}

void InvokeCatalogTableColumnDefaultSet(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name, const std::string &expression,
    ClientContext &context) {
	auto params = generated::BuildCatalogTableColumnDefaultSetParams(
	    ctx.attach_opaque_data, SingleSchemaPath(schema_name), table_name, column_name, expression,
	    /*ignore_not_found=*/false, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_table_column_default_set", params, context);
}

void InvokeCatalogTableColumnDefaultDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name,
    ClientContext &context) {
	auto params = generated::BuildCatalogTableColumnDefaultDropParams(
	    ctx.attach_opaque_data, SingleSchemaPath(schema_name), table_name, column_name,
	    /*ignore_not_found=*/false, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_table_column_default_drop", params, context);
}

void InvokeCatalogTableNotNullSet(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name,
    ClientContext &context) {
	auto params = generated::BuildCatalogTableNotNullSetParams(ctx.attach_opaque_data, SingleSchemaPath(schema_name),
	                                                           table_name, column_name,
	                                                           /*ignore_not_found=*/false, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_table_not_null_set", params, context);
}

void InvokeCatalogTableNotNullDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name,
    ClientContext &context) {
	auto params = generated::BuildCatalogTableNotNullDropParams(ctx.attach_opaque_data, SingleSchemaPath(schema_name),
	                                                            table_name, column_name,
	                                                            /*ignore_not_found=*/false, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_table_not_null_drop", params, context);
}

// ============================================================================
// View DDL Operations
// ============================================================================

void InvokeCatalogViewCreate(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &view_name,
    const std::string &definition, const std::string &on_conflict,
    ClientContext &context) {
	auto params = generated::BuildCatalogViewCreateParams(ctx.attach_opaque_data, SingleSchemaPath(schema_name),
	                                                      view_name, definition, on_conflict, OptTxn(ctx));
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
    ClientContext &context) {
	// CREATE SCHEMA ... TAGS (...) is not yet exposed by the C++ extension; we
	// always send std::nullopt for tags. catalog_schema_create's wire shape requires
	// the field, but the value is always absent until we plumb it through DDL parsing.
	auto params =
	    generated::BuildCatalogSchemaCreateParams(ctx.attach_opaque_data, SingleSchemaPath(schema_name), on_conflict,
	                                              /*comment=*/std::nullopt, /*tags=*/std::nullopt, OptTxn(ctx));
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

// ============================================================================
// Table Function Cardinality
// ============================================================================

TableFunctionCardinalityResult InvokeTableFunctionCardinality(
    const CatalogRpcContext &ctx, const std::vector<uint8_t> &bind_request_bytes,
    const std::vector<uint8_t> &bind_opaque_data, ClientContext &context) {
	auto &worker_path = ctx.params->worker_path();

	// Build the TableFunctionCardinalityRequest batch and serialize to IPC bytes
	auto request_batch = BuildTableFunctionCardinalityRequest(bind_request_bytes, bind_opaque_data);
	auto request_bytes = SerializeToIpcBytes(request_batch);

	// Wrap in {request: binary} (the protocol's TableFunctionCardinalityParams shape)
	auto params = generated::BuildTableFunctionCardinalityParams(request_bytes);

	auto response = InvokeRpcMethod(ctx, "table_function_cardinality", params, context);
	auto result_batch = ExtractAndDeserializeResult(response, "table_function_cardinality", worker_path);
	if (!result_batch) {
		return {};  // Unknown cardinality
	}
	return ParseTableFunctionCardinalityResult(result_batch, worker_path);
}

// ============================================================================
// Scan Planning (splits)
// ============================================================================

//! Parse one ``PlanResponse`` batch into a page.
//!
//! Everything is optional except ``splits`` itself, and an EMPTY splits list is
//! legal — a fully-pruned scan plans zero splits, and that must produce an empty
//! result rather than an error.
VgiScanPlanPage ParseVgiScanPlanPage(const std::shared_ptr<arrow::RecordBatch> &batch,
                                     const std::string &worker_path) {
	VgiScanPlanPage page;
	if (!batch || batch->num_rows() == 0) {
		return page;
	}

	// splits: list<binary>, one serialized ScanSplit per entry.
	if (auto col = batch->GetColumnByName("splits")) {
		if (auto list = std::dynamic_pointer_cast<arrow::ListArray>(col)) {
			if (!list->IsNull(0)) {
				auto values = std::dynamic_pointer_cast<arrow::BinaryArray>(list->values());
				if (!values) {
					throw IOException(CatalogProtocolViolation("table_function_plan", worker_path).Set(error_key::kField, "splits"),
					                  "VGI worker '%s' returned a plan whose 'splits' entries are not binary",
					                  worker_path);
				}
				const auto start = list->value_offset(0);
				const auto end = list->value_offset(1);
				page.splits.reserve(static_cast<size_t>(end - start));
				for (auto i = start; i < end; i++) {
					// Each entry is a serialized ScanSplit. What gets redeemed is its
					// ``token`` — the framework-stamped envelope — NOT the whole
					// record and not the worker's raw payload, which is inside the
					// envelope and unverifiable on its own.
					//
					// Sliced zero-copy rather than copied out: the blob carries the
					// full ScanSplit record (estimates, partition bounds, statistics
					// — everything a bin-packing engine reads), while this client
					// needs only the token. Materializing it copied ~1.8 KB twice per
					// split for ~100 bytes of payload, and that runs single-threaded
					// at the head of every scan, so on a large plan it was hundreds of
					// milliseconds of dead time before the first reader started.
					auto split_batch = DeserializeFromIpcBytesZeroCopy(*values, i);
					if (!split_batch || split_batch->num_rows() == 0) {
						throw IOException(CatalogProtocolViolation("table_function_plan", worker_path).Set(error_key::kField, "splits"),
						                  "VGI worker '%s' returned an unparseable ScanSplit", worker_path);
					}
					auto token_col = split_batch->GetColumnByName("token");
					auto token_arr = std::dynamic_pointer_cast<arrow::BinaryArray>(token_col);
					if (!token_arr || token_arr->IsNull(0)) {
						throw IOException(
						    CatalogProtocolViolation("table_function_plan", worker_path).Set(error_key::kField, "token"),
						    "VGI worker '%s' returned a ScanSplit with no token; the framework stamps this, "
						    "so an absent token means the worker bypassed it.",
						    worker_path);
					}
					auto tok = token_arr->GetView(0);
					page.splits.emplace_back(tok.data(), tok.size());
				}
			}
		}
	}

	auto binary_field = [&](const char *name) -> std::vector<uint8_t> {
		std::vector<uint8_t> out;
		if (auto col = batch->GetColumnByName(name)) {
			if (auto arr = std::dynamic_pointer_cast<arrow::BinaryArray>(col)) {
				if (!arr->IsNull(0)) {
					auto view = arr->GetView(0);
					out.assign(view.begin(), view.end());
				}
			}
		}
		return out;
	};
	auto int_field = [&](const char *name) -> int64_t {
		if (auto col = batch->GetColumnByName(name)) {
			if (auto arr = std::dynamic_pointer_cast<arrow::Int64Array>(col)) {
				if (!arr->IsNull(0)) {
					return arr->Value(0);
				}
			}
		}
		return 0;
	};

	// next_cursors: list<binary>. More than one means the worker wants parallel
	// enumeration, which is only sound if the cursors partition the remainder
	// disjointly. DuckDB paginates serially, so it follows the first and ignores
	// the rest rather than pretending to fan out.
	if (auto col = batch->GetColumnByName("next_cursors")) {
		if (auto list = std::dynamic_pointer_cast<arrow::ListArray>(col)) {
			if (!list->IsNull(0) && list->value_length(0) > 0) {
				auto values = std::dynamic_pointer_cast<arrow::BinaryArray>(list->values());
				if (values) {
					auto view = values->GetView(list->value_offset(0));
					page.next_cursor.assign(view.begin(), view.end());
				}
			}
		}
	}

	page.max_workers = int_field("max_workers");
	page.catalog_version = int_field("catalog_version");
	page.execution_id = binary_field("execution_id");
	page.init_opaque_data = binary_field("init_opaque_data");

	if (auto col = batch->GetColumnByName("scope")) {
		if (auto arr = std::dynamic_pointer_cast<arrow::StringArray>(col)) {
			if (!arr->IsNull(0)) {
				page.scope = arr->GetString(0);
			}
		}
	}

	return page;
}

//! Plan a table-function scan into named, independently redeemable splits.
//!
//! Bounded on three axes, because this runs on the scheduling thread and a worker
//! that paginates forever would otherwise hang the query with no diagnosis:
//! a page cap, a total split cap, and a cancellation check every page.
//!
//! A breach THROWS rather than proceeding with what arrived. Scanning a partial
//! enumeration would silently return a subset of the table — wrong results wearing
//! the costume of a successful query — so truncate-and-proceed is never correct here.
VgiScanPlan InvokeTableFunctionPlan(const CatalogRpcContext &ctx,
                                    const std::vector<uint8_t> &bind_request_bytes,
                                    const std::vector<uint8_t> &bind_opaque_data,
                                    const std::vector<int32_t> &projection_ids,
                                    const std::vector<uint8_t> &pushdown_filters, int64_t min_splits,
                                    int64_t target_split_bytes, const std::string &function_name,
                                    ClientContext &context) {
	auto &worker_path = ctx.params->worker_path();

	// Bounds. Deliberately generous: they exist to turn a hang into a legible
	// error, not to second-guess a worker's split count.
	// Read from the session so an operator can raise it for a worker that
	// legitimately paginates a very large table — this bounds PAGES, not splits,
	// so the reachable split count is this times the worker's page size, which
	// may sit far below the split cap below.
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
	std::vector<uint8_t> cursor;
	// Split disjointness is the WORKER's obligation and is not policed here.
	//
	// A paginated enumeration must not hand out the same split twice; violating
	// that returns duplicate rows. The client used to dedup by token, then to
	// refuse on a repeat, and neither earned its keep:
	//
	//   It cost every scan to guard an unused one. Detection needs a set holding a
	//   COPY of every token beside the vector already holding them — on a plan near
	//   the cap that is hundreds of MB of duplicated bytes plus the hashing, paid by
	//   every user of every split scan, to police parallel cursor fan-out that no
	//   client emits.
	//
	//   It could not hold uniformly. Detection compares token bytes; a keyless
	//   worker stamps deterministically and a keyed one seals under a fresh random
	//   nonce, so the check ran on the development transport and never on the one a
	//   distributed engine uses.
	//
	//   It could not repair anything. The most a client can do with a duplicate is
	//   refuse the query — swapping one worker bug for a different error message.
	//
	// So it is a contract, like the worker's obligation to keep a SINGLE_VALUE
	// split single-valued before reporting partitioning, or to keep splits
	// replayable, or to keep redemption state cross-process. None of those are
	// checked here either. Should a stable split identity ever land on the wire
	// (deterministic sealing, or a plaintext split_id), enforcing this becomes
	// cheap and uniform and is worth revisiting; until then it is the worker's.
	idx_t pages = 0;

	while (true) {
		if (context.interrupted) {
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
		// Plan-level facts come from the FIRST page, keyed on the page count
		// rather than on `splits.empty()`. A leading page may legally carry zero
		// splits and a cursor (a worker still enumerating), and keying on
		// emptiness let each such page overwrite these — so what survived was the
		// LAST empty page's values, not the first page's.
		if (pages == 1) {
			plan.max_workers = page.max_workers;
			plan.catalog_version = page.catalog_version;
			plan.scope = page.scope;
			plan.execution_id = page.execution_id;
			plan.init_opaque_data = page.init_opaque_data;
		}
		for (auto &token : page.splits) {
			plan.splits.push_back(std::move(token));
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

		if (page.next_cursor.empty()) {
			break;
		}
		cursor = page.next_cursor;
	}

	return plan;
}

// ============================================================================
// Column Statistics
// ============================================================================

// Build DuckDB BaseStatistics from min/max Values and metadata.
// Uses DuckDB's BaseStatistics::FromConstantType + Merge to handle all types
// (numeric, string, decimal, list, struct, etc.) without manual type dispatch.
static unique_ptr<BaseStatistics> BuildColumnStatistics(
    const LogicalType &duck_type, const Value &min_val, const Value &max_val,
    bool has_null, bool has_not_null, int64_t distinct_count,
    bool has_contains_unicode, bool contains_unicode,
    bool has_max_string_length, uint64_t max_string_length) {

	// Use DuckDB's FromConstant to build stats from min, then Merge with max to
	// expand the range. This delegates all type-specific logic (NumericStats,
	// StringStats, ListStats, etc.) to DuckDB internals — no manual type dispatch.
	if (min_val.IsNull() && max_val.IsNull()) {
		return make_uniq<BaseStatistics>(BaseStatistics::CreateUnknown(duck_type));
	}

	// Cast values to the target type so FromConstant dispatches to the correct
	// stats type (e.g., GEOMETRY_STATS for geometry columns). For types that
	// share the same physical representation but have no registered cast
	// (e.g., BLOB → GEOMETRY), reinterpret the value with the target type.
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
			// Cast not available — if the physical types match, reinterpret
			// the value with the target logical type (e.g., BLOB → GEOMETRY)
			if (val.type().InternalType() == duck_type.InternalType()) {
				auto reinterpreted = val;
				reinterpreted.Reinterpret(duck_type);
				return reinterpreted;
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

	// Override null/valid flags (FromConstantType sets CANNOT_HAVE_NULL by default)
	if (has_null) {
		result.SetHasNull();
	}
	if (has_not_null) {
		result.SetHasNoNull();
	}
	if (distinct_count >= 0) {
		result.SetDistinctCount(static_cast<idx_t>(distinct_count));
	}

	// String-specific fields — must guard with type check to avoid accessing
	// the wrong union member (UB if called on NUMERIC_STATS etc.)
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

// Convert a deserialized ColumnStatistics RecordBatch into a name → BaseStatistics map.
// Shared by `catalog_table_column_statistics_get` and `table_function_statistics`, which
// use the same wire shape (columns: column_name, min [union], max [union], has_null,
// has_not_null, distinct_count, contains_unicode?, max_string_length?).
// `log_source_key` / `log_source_value` identify the RPC source in log records
// (e.g. {"table", "public.users"} or {"function_name", "sequence"}).
std::unordered_map<std::string, unique_ptr<BaseStatistics>> ParseColumnStatisticsBatch(
    const std::shared_ptr<arrow::RecordBatch> &result_batch,
    const std::vector<LogicalType> &column_types,
    const std::vector<std::string> &column_names,
    const std::string &worker_path, const std::string &log_source_key,
    const std::string &log_source_value, ClientContext &context) {
	std::unordered_map<std::string, unique_ptr<BaseStatistics>> out;
	if (!result_batch || result_batch->num_rows() == 0) {
		return out;
	}

	// Convert Arrow RecordBatch → DuckDB DataChunk using DuckDB's Arrow scanner.
	// Correctly handles all types (string, decimal, timestamp, union, etc.)
	// without manual per-type conversion.
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

			// Extract min/max from the UNION-typed columns.
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

	// Custom decode path (uses DeserializeFromIpcBytesWithMetadata to preserve
	// IPC batch custom_metadata, where cache_max_age_seconds lives). The outer
	// envelope is validated here with the same rigor as ExtractAndDeserializeResult,
	// then ValidateResponseSchema is a no-op (method is registered dynamic) but we
	// keep it for symmetry with the rest of the codebase.
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

	// Extract cache_max_age_seconds from IPC batch custom_metadata
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

	// Response envelope: {result: binary|null}. Null result → no stats available
	// (method returns Optional[bytes] in vgi-python; shape of the bytes varies
	// per function and is validated by ParseColumnStatisticsBatch downstream).
	auto result_batch = ExtractAndDeserializeResult(response, method_name, worker_path);
	if (!result_batch) {
		return {};
	}

	return ParseColumnStatisticsBatch(result_batch, column_types, column_names,
	                                   worker_path, "function_name", function_name, context);
}

// ============================================================================
// Table Function dynamic_to_string
// ============================================================================

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
		// EXPLAIN ANALYZE must never fail because of a profiler-only RPC.
		// Log and surface an empty map; the caller will append the intrinsic
		// keys it knows from the global state.
		VGI_LOG(context, "table_function.dynamic_to_string_error",
		        {{"worker_path", worker_path}, {"error", e.what()}});
		return {};
	}
}

// ============================================================================
// Enum parsing functions
// ============================================================================

// Enforce that a wire enum string parsed to a known value. The parser helpers
// return std::nullopt for unrecognized strings; this turns that into a loud
// IOException so a worker typo, protocol skew, or newer worker-side enum
// variant surfaces instead of being silently masked as a default (which would
// run with behavior inconsistent with what the worker declared).
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

// ============================================================================
// Result parsing
// ============================================================================

// Helper to extract a single value from an Arrow array at given row index
// Uses DuckDB type from ArrowSchemaToDuckDBTypes and constructs Value appropriately
static Value ExtractArrowValue(const std::shared_ptr<arrow::Array> &array, int64_t row_idx,
                               const LogicalType &duck_type) {
	if (!array || array->IsNull(row_idx)) {
		return Value(duck_type);
	}

	// Extract value based on Arrow type and construct DuckDB Value
	switch (array->type()->id()) {
	case arrow::Type::BOOL: {
		auto typed = std::static_pointer_cast<arrow::BooleanArray>(array);
		return Value::BOOLEAN(typed->Value(row_idx));
	}
	case arrow::Type::INT8: {
		auto typed = std::static_pointer_cast<arrow::Int8Array>(array);
		return Value::TINYINT(typed->Value(row_idx));
	}
	case arrow::Type::INT16: {
		auto typed = std::static_pointer_cast<arrow::Int16Array>(array);
		return Value::SMALLINT(typed->Value(row_idx));
	}
	case arrow::Type::INT32: {
		auto typed = std::static_pointer_cast<arrow::Int32Array>(array);
		return Value::INTEGER(typed->Value(row_idx));
	}
	case arrow::Type::INT64: {
		auto typed = std::static_pointer_cast<arrow::Int64Array>(array);
		return Value::BIGINT(typed->Value(row_idx));
	}
	case arrow::Type::UINT8: {
		auto typed = std::static_pointer_cast<arrow::UInt8Array>(array);
		return Value::UTINYINT(typed->Value(row_idx));
	}
	case arrow::Type::UINT16: {
		auto typed = std::static_pointer_cast<arrow::UInt16Array>(array);
		return Value::USMALLINT(typed->Value(row_idx));
	}
	case arrow::Type::UINT32: {
		auto typed = std::static_pointer_cast<arrow::UInt32Array>(array);
		return Value::UINTEGER(typed->Value(row_idx));
	}
	case arrow::Type::UINT64: {
		auto typed = std::static_pointer_cast<arrow::UInt64Array>(array);
		return Value::UBIGINT(typed->Value(row_idx));
	}
	case arrow::Type::FLOAT: {
		auto typed = std::static_pointer_cast<arrow::FloatArray>(array);
		return Value::FLOAT(typed->Value(row_idx));
	}
	case arrow::Type::DOUBLE: {
		auto typed = std::static_pointer_cast<arrow::DoubleArray>(array);
		return Value::DOUBLE(typed->Value(row_idx));
	}
	case arrow::Type::STRING: {
		auto typed = std::static_pointer_cast<arrow::StringArray>(array);
		return Value(typed->GetString(row_idx));
	}
	case arrow::Type::LARGE_STRING: {
		auto typed = std::static_pointer_cast<arrow::LargeStringArray>(array);
		return Value(typed->GetString(row_idx));
	}
	case arrow::Type::BINARY: {
		auto typed = std::static_pointer_cast<arrow::BinaryArray>(array);
		auto view = typed->GetView(row_idx);
		return Value::BLOB(reinterpret_cast<const_data_ptr_t>(view.data()), view.size());
	}
	case arrow::Type::LARGE_BINARY: {
		auto typed = std::static_pointer_cast<arrow::LargeBinaryArray>(array);
		auto view = typed->GetView(row_idx);
		return Value::BLOB(reinterpret_cast<const_data_ptr_t>(view.data()), view.size());
	}
	default:
		// For complex/unsupported types, try to get string representation
		auto scalar_result = array->GetScalar(row_idx);
		if (scalar_result.ok()) {
			return Value(scalar_result.ValueUnsafe()->ToString());
		}
		return Value(duck_type);
	}
}

// Helper to convert Arrow RecordBatch to vector of DuckDB Values
// Uses DuckDB's ArrowSchemaToDuckDBTypes for proper type mapping
static vector<Value> ArrowBatchToValues(ClientContext &context, const std::shared_ptr<arrow::RecordBatch> &batch) {
	vector<Value> result;
	if (!batch || batch->num_rows() == 0) {
		return result;
	}

	// Use DuckDB's proper Arrow schema conversion to get correct types
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
	vector<LogicalType> types;
	vector<string> names;
	ArrowSchemaToDuckDBTypes(context, batch->schema(), c_schema, arrow_table, types, names);

	// Extract value from each column at row 0
	for (int64_t col_idx = 0; col_idx < batch->num_columns(); col_idx++) {
		auto array = batch->column(col_idx);
		auto &duck_type = types[col_idx];
		result.push_back(ExtractArrowValue(array, 0, duck_type));
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
static Value DefaultValueOf(const std::optional<std::vector<uint8_t>> &bytes, const LogicalType &type) {
	if (bytes && !bytes->empty()) {
		auto batch = DeserializeFromIpcBytes(*bytes);
		if (batch && batch->num_rows() > 0 && batch->num_columns() > 0) {
			return ExtractArrowValue(batch->column(0), 0, type);
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
	setting.default_value = DefaultValueOf(spec.default_value, setting.type);
	return setting;
}

VgiAttachOptionSpec ParseAttachOptionSpec(const std::vector<uint8_t> &bytes, const std::string &worker_path,
                                          ClientContext &context) {
	auto wire = DecodeIpcRecord<generated::AttachOptionSpec>(bytes, worker_path);
	VgiAttachOptionSpec spec;
	spec.name = std::move(wire.name);
	spec.description = std::move(wire.description);
	spec.type = SingleFieldType(wire.type, context, "Attach option", spec.name, worker_path);
	spec.default_value = DefaultValueOf(wire.default_value, spec.type);
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
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
	vector<LogicalType> types;
	vector<string> names;
	ArrowSchemaToDuckDBTypes(context, params_schema, c_schema, arrow_table, types, names);
	for (int i = 0; i < params_schema->num_fields(); i++) {
		VgiSecretTypeParam param;
		param.name = names[i];
		param.type = types[i];
		auto field_metadata = params_schema->field(i)->metadata();
		if (field_metadata) {
			auto redact_idx = field_metadata->FindKey("redact");
			param.redact = redact_idx >= 0 && field_metadata->value(redact_idx) == "true";
		}
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

CatalogAttachResult ParseCatalogAttachResult(const std::shared_ptr<arrow::RecordBatch> &batch,
                                             const std::string &worker_path, ClientContext &context) {
	auto wire = generated::Decode<generated::CatalogAttachResult>(batch, 0, worker_path);
	CatalogAttachResult result;
	result.attach_opaque_data = std::move(wire.attach_opaque_data);
	result.supports_transactions = wire.supports_transactions;
	result.supports_time_travel = wire.supports_time_travel;
	result.catalog_version_frozen = wire.catalog_version_frozen;
	result.catalog_version = wire.catalog_version;
	result.attach_opaque_data_required = wire.attach_opaque_data_required;
	result.default_schema = wire.default_schema.empty() ? "main" : std::move(wire.default_schema);
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
	// Globally-published functions (protocol 1.3.0+): each a serialized FunctionInfo.
	for (const auto &gf_bytes : wire.global_functions) {
		result.global_functions.push_back(ParseFunctionInfo(DeserializeFromIpcBytes(gf_bytes), 0, worker_path));
	}
	result.global_function_prefix = std::move(wire.global_function_prefix);
	result.resolved_data_version = wire.resolved_data_version.value_or("");
	result.resolved_implementation_version = wire.resolved_implementation_version.value_or("");
	result.supports_catalog_contents = wire.supports_catalog_contents;
	return result;
}

static VgiSchemaInfo SchemaInfoFromWire(generated::SchemaInfo wire) {
	VgiSchemaInfo info;
	info.path = std::move(wire.path);
	info.name = SchemaNameFromPath(info.path, "SchemaInfo.path");
	info.comment = wire.comment.value_or("");
	info.tags = std::move(wire.tags);
	// Nullable map: downstream defaults missing keys to 1.
	info.estimated_object_count = wire.estimated_object_count.value_or(std::map<std::string, int64_t> {});
	return info;
}

VgiSchemaInfo ParseSchemaInfo(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &worker_path) {
	return SchemaInfoFromWire(generated::Decode<generated::SchemaInfo>(batch, 0, worker_path));
}

VgiTableInfo ParseTableInfo(ClientContext &context, const std::shared_ptr<arrow::RecordBatch> &batch,
                            int64_t row_idx, const std::string &worker_path) {
	auto wire = generated::Decode<generated::TableInfo>(batch, row_idx, worker_path);
	VgiTableInfo info;
	info.name = std::move(wire.name);
	info.schema_name = SchemaNameFromPath(wire.schema_path, "TableInfo.schema_path");
	info.comment = wire.comment.value_or("");
	info.tags = std::move(wire.tags);

	// `columns` is a serialized Arrow schema; at most one field may carry is_row_id.
	info.arrow_schema = DeserializeSchema(wire.columns);
	for (int i = 0; info.arrow_schema && i < info.arrow_schema->num_fields(); i++) {
		auto &field = info.arrow_schema->field(i);
		if (field->HasMetadata() && field->metadata()->FindKey(VGI_ROW_ID_METADATA_KEY) >= 0) {
			if (info.row_id_column >= 0) {
				throw InvalidInputException(ErrorInfo(error_subtype::kProtocolViolation).Worker(worker_path).Set(error_key::kTable, info.name).Set(error_key::kField, "columns"),
		                            StringUtil::Format("Table '%s' has multiple is_row_id columns — at most one is allowed", info.name));
			}
			info.row_id_column = i;
		}
	}

	info.not_null_constraints.assign(wire.not_null_constraints.begin(), wire.not_null_constraints.end());
	for (const auto &u : wire.unique_constraints) {
		info.unique_constraints.emplace_back(u.begin(), u.end());
	}
	info.check_constraints = std::move(wire.check_constraints);
	for (const auto &p : wire.primary_key_constraints) {
		info.primary_key_constraints.emplace_back(p.begin(), p.end());
	}
	for (const auto &fk_bytes : wire.foreign_key_constraints) {
		auto fk_wire = DecodeIpcRecord<generated::ForeignKeyInfo>(fk_bytes, worker_path);
		VgiTableInfo::ForeignKey fk;
		fk.fk_columns = std::move(fk_wire.fk_columns);
		fk.pk_columns = std::move(fk_wire.pk_columns);
		fk.referenced_table = std::move(fk_wire.referenced_table);
		fk.referenced_schema = SchemaNameFromPath(fk_wire.referenced_schema_path, "ForeignKeyDef.referenced_schema_path");
		info.foreign_key_constraints.push_back(std::move(fk));
	}

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

	// CNF required filters (AND of OR-groups of dotted paths); empty means no enforcement.
	info.required_filters = std::move(wire.required_filters);

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

VgiTableInfo ParseTableInfo(ClientContext &context, const std::shared_ptr<arrow::RecordBatch> &batch,
                            const std::string &worker_path) {
	return ParseTableInfo(context, batch, 0, worker_path);
}

std::vector<VgiSchemaInfo> ParseSchemaList(const std::shared_ptr<arrow::RecordBatch> &batch,
                                           const std::string &worker_path) {
	std::vector<VgiSchemaInfo> schemas;
	for (int64_t i = 0; batch && i < batch->num_rows(); i++) {
		schemas.push_back(SchemaInfoFromWire(generated::Decode<generated::SchemaInfo>(batch, i, worker_path)));
	}
	return schemas;
}

VgiViewInfo ParseViewInfo(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &worker_path) {
	auto wire = generated::Decode<generated::ViewInfo>(batch, 0, worker_path);
	VgiViewInfo info;
	info.name = std::move(wire.name);
	info.schema_name = SchemaNameFromPath(wire.schema_path, "ViewInfo.schema_path");
	info.definition = std::move(wire.definition);
	info.comment = wire.comment.value_or("");
	info.tags = std::move(wire.tags);
	info.column_comments = std::move(wire.column_comments);
	return info;
}

VgiCopyFromFormatInfo ParseCopyFromFormatInfo(const std::shared_ptr<arrow::RecordBatch> &batch,
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

VgiMacroInfo ParseMacroInfo(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &worker_path) {
	auto wire = generated::Decode<generated::MacroInfo>(batch, 0, worker_path);
	VgiMacroInfo info;
	info.name = std::move(wire.name);
	info.schema_name = SchemaNameFromPath(wire.schema_path, "MacroInfo.schema_path");
	info.macro_type = generated::ToWire(wire.macro_type);
	info.definition = std::move(wire.definition);
	info.comment = wire.comment.value_or("");
	info.tags = std::move(wire.tags);
	info.parameters = std::move(wire.parameters);
	if (wire.parameter_default_values && !wire.parameter_default_values->empty()) {
		info.parameter_default_values_bytes = std::move(*wire.parameter_default_values);
	}
	// Per-parameter docs ride as vgi_doc field metadata on this schema.
	if (wire.arguments_schema && !wire.arguments_schema->empty()) {
		info.arguments_schema = DeserializeSchema(*wire.arguments_schema);
	}
	return info;
}

// Shared helper: decode the nested-IPC argument batch (the "arguments" binary
// field on both ScanFunctionResult and ScanBranch) into positional+named Values.
// Field names matching ``arg_<N>`` are routed to positional slot N; everything
// else becomes a named argument.
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
	auto values = ArrowBatchToValues(context, arguments_batch);
	auto &schema = arguments_batch->schema();
	for (int i = 0; i < schema->num_fields(); i++) {
		const auto &field_name = schema->field(i)->name();
		auto &duck_value = values[i];
		if (field_name.rfind("arg_", 0) == 0) {
			// Cap to a sane ceiling: a worker-supplied index is used to size the
			// positional vector, so an unbounded value (e.g. "arg_9999999999")
			// would force a multi-GB allocation. >1000 positional args to a
			// single function is not a real workload — treat it as a protocol
			// error rather than honoring the resize.
			constexpr size_t kMaxPositionalArgs = 1000;
			size_t idx;
			bool is_positional = false;
			try {
				idx = std::stoul(field_name.substr(4));
				is_positional = true;
			} catch (const std::exception &) {
				// Not a numeric arg_<N> suffix — fall through to named handling.
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

VgiScanFunctionResult ParseScanFunctionResult(ClientContext &context, const std::shared_ptr<arrow::RecordBatch> &batch,
                                               const std::string &worker_path) {
	auto wire = generated::Decode<generated::ScanFunctionResult>(batch, 0, worker_path);
	VgiScanFunctionResult result;
	result.function_name = std::move(wire.function_name);
	result.required_extensions = std::move(wire.required_extensions);
	DecodeScanArguments(context, wire.arguments, result.positional_arguments, result.named_arguments);
	result.schema_name = OptionalSchemaNameFromPath(wire.schema_path, "ScanFunctionResult.schema_path");
	return result;
}

// Parse the result of catalog_table_scan_branches_get. The outer batch is a
// 1-row record with a list<binary> column carrying serialized ScanBranch
// 1-row batches; each branch's `arguments` field is itself a nested IPC
// stream of typed scalars. The branch_filter SQL string (when non-empty) is
// parsed eagerly via Parser::ParseExpressionList — the resulting
// ParsedExpression lives on the catalog entry alongside the branches list.
// We do NOT bind the expression here; binding happens in the rewriter where
// a Binder + the branch's column list are in hand. See plan: §C++ wiring
// sketch "branch_filter parse-at-attach, re-bind-per-rewrite, no bound cache".
VgiScanBranchesResult ParseScanBranchesResult(ClientContext &context,
                                               const std::shared_ptr<arrow::RecordBatch> &batch,
                                               const std::string &worker_path) {
	VgiScanBranchesResult result;

	if (!batch || batch->num_rows() == 0) {
		throw IOException(ErrorInfo(error_subtype::kProtocolViolation).Worker(worker_path),
		                  "Empty response from table_scan_branches_get [worker: " + worker_path + "]");
	}

	auto wire = generated::Decode<generated::ScanBranchesResult>(batch, 0, worker_path);
	// Top-level required_extensions: the union across branches.
	result.required_extensions = std::move(wire.required_extensions);
	// Each branch is the IPC bytes of a 1-row ScanBranch batch.
	const auto &branch_blobs = wire.branches;
	if (branch_blobs.empty()) {
		// Loud-fail at parse time — workers must return at least one branch.
		// Catches "worker bug returned empty" before silent zero-row queries.
		throw BinderException(ErrorInfo(error_subtype::kProtocolViolation).Worker(worker_path).Set(error_key::kField, "branches"),
		                      "VGI table returned zero scan branches [worker: " + worker_path + "]");
	}

	result.branches.reserve(branch_blobs.size());
	std::vector<size_t> writable_ordinals;
	for (size_t i = 0; i < branch_blobs.size(); i++) {
		auto branch_wire = DecodeIpcRecord<generated::ScanBranch>(branch_blobs[i], worker_path);

		VgiScanBranch branch;
		branch.function_name = std::move(branch_wire.function_name);
		DecodeScanArguments(context, branch_wire.arguments, branch.positional_arguments, branch.named_arguments);

		// branch_filter is nullable; absent / NULL → empty string == unconstrained.
		branch.branch_filter = branch_wire.branch_filter.value_or("");
		if (!branch.branch_filter.empty()) {
			// Parse inside the try, validate outside it: the empty-result check
			// below throws its own BinderException, which must not be re-wrapped
			// as a "parse error".
			duckdb::vector<duckdb::unique_ptr<ParsedExpression>> exprs;
			try {
				exprs = Parser::ParseExpressionList(branch.branch_filter);
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
			// If the worker supplied multiple comma-separated expressions,
			// AND them together. (Single expression is the typical case.)
			branch.parsed_branch_filter = std::move(exprs[0]);
			// Multi-expression case: leave as a single expression by AND-ing.
			// Skipped here for simplicity — typical worker emits one
			// predicate; multi-AND can be added if it shows up in practice.
		}

		// writable flag is non-nullable on the wire — surface a loud error
		// if the worker emits NULL. (Legacy single-function workers go
		// through WrapLegacyAsOneBranch and never reach this parser, so
		// they don't need a fallback default here.)
		branch.writable = branch_wire.writable;
		if (branch.writable) {
			writable_ordinals.push_back(i);
		}

		// Catalog-table branch fields (nullable; empty ⇒ function branch). A
		// non-empty source_table selects the catalog-table kind — the branch
		// scans source_catalog.source_schema.source_table in a companion catalog.
		branch.source_catalog = branch_wire.source_catalog.value_or("");
		branch.source_schema = OptionalSchemaNameFromPath(branch_wire.source_schema_path, "ScanBranch.source_schema_path");
		branch.source_table = branch_wire.source_table.value_or("");

		// Function branch only: DuckDB 1.5 schema the function_name is
		// registered in. Empty for a catalog-table/format branch or a pre-1.5.0
		// peer — same nullable-string convention as source_catalog/etc above.
		branch.schema_name = OptionalSchemaNameFromPath(branch_wire.schema_path, "ScanBranch.schema_path");

		// Format-branch fields (P4). A non-empty format_name with no function and
		// no source_table selects the format kind: read these locations as this
		// format.
		branch.format_name = branch_wire.format_name.value_or("");
		branch.format_locations.assign(branch_wire.format_locations.begin(), branch_wire.format_locations.end());
		{
			// Reader options, carried as a 1-row IPC batch whose COLUMN NAMES are
			// the option names — the same shape as `arguments`, so the same
			// decoder reads it. Absent / NULL means no options.
			//
			// This column existed on neither side for a while: the rewriter has
			// always consumed `format_options` as the reader's named arguments,
			// but nothing ever parsed it, so it was permanently empty. Because
			// the rewriter ASSIGNS rather than merges, that made it impossible to
			// pass a single reader option to a format branch — `header`, `delim`,
			// `union_by_name` all silently unreachable.
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

		// Three-way discriminator, enforced HERE rather than at bind. A branch that
		// sets two identifying fields is a worker bug; catching it at bind would
		// blame the query instead of the catalog, and the message would arrive far
		// from the thing that produced it.
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

	// At-most-one-writable invariant. Multiple writable arms would violate
	// DuckDB's single-writable-catalog-per-transaction rule
	// (duckdb/src/transaction/meta_transaction.cpp:257-261) if UPDATE/DELETE
	// ever land, and even for INSERT it makes the routing target ambiguous.
	// Reject loudly at catalog-load.
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

// ============================================================================
// DuckDB type conversion
// ============================================================================

// Render required_filters (CNF: an AND of OR-groups) as a compact JSON array of
// arrays of strings, e.g. [["accession_number"],["ticker","cik"]]. Surfaced on
// the extension-injected `vgi_required_filters` table tag so callers can discover
// the requirement via duckdb_tables().tags before hitting the BinderException.
static std::string RenderRequiredFiltersJson(const std::vector<std::vector<std::string>> &groups) {
	yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
	yyjson_mut_val *outer = yyjson_mut_arr(doc);
	for (const auto &group : groups) {
		yyjson_mut_val *inner = yyjson_mut_arr(doc);
		for (const auto &path : group) {
			yyjson_mut_arr_append(inner, yyjson_mut_strncpy(doc, path.c_str(), path.size()));
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
	create_info.table = table_info.name;
	create_info.schema = schema_name;
	if (!table_info.comment.empty()) {
		create_info.comment = Value(table_info.comment);
	}
	for (auto &[key, val] : table_info.tags) {
		create_info.tags[key] = val;
	}
	// Expose the required-filter requirement to SQL callers as a reserved tag
	// (extension-injected, `vgi_`-prefixed) so it's discoverable pre-query.
	if (!table_info.required_filters.empty()) {
		create_info.tags["vgi_required_filters"] = RenderRequiredFiltersJson(table_info.required_filters);
	}

	if (table_info.arrow_schema) {
		if (table_info.row_id_column >= 0) {
			// Convert the row_id field type to DuckDB LogicalType
			auto rowid_field = table_info.arrow_schema->field(table_info.row_id_column);
			auto rowid_schema = arrow::schema({rowid_field});
			ArrowSchemaWrapper c_schema;
			ArrowTableSchema arrow_table;
			vector<LogicalType> types;
			vector<string> names;
			ArrowSchemaToDuckDBTypes(context, rowid_schema, c_schema, arrow_table, types, names);
			table_info.rowid_type = types[0];

			// Build filtered schema excluding the row_id field
			std::vector<std::shared_ptr<arrow::Field>> filtered_fields;
			for (int i = 0; i < table_info.arrow_schema->num_fields(); i++) {
				if (i != table_info.row_id_column) {
					filtered_fields.push_back(table_info.arrow_schema->field(i));
				}
			}
			auto filtered_schema = arrow::schema(filtered_fields);
			ArrowSchemaToColumnList(context, filtered_schema, create_info.columns);
		} else {
			ArrowSchemaToColumnList(context, table_info.arrow_schema, create_info.columns);
		}
	}

	// Apply NOT NULL constraints
	for (auto idx : table_info.not_null_constraints) {
		int adjusted = idx;
		if (table_info.row_id_column >= 0) {
			if (idx == table_info.row_id_column) {
				continue; // Skip row_id column
			}
			if (idx > table_info.row_id_column) {
				adjusted--;
			}
		}
		create_info.constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(adjusted)));
	}

	// Apply UNIQUE constraints
	for (auto &cols : table_info.unique_constraints) {
		vector<string> col_names;
		for (auto idx : cols) {
			int adjusted = idx;
			if (table_info.row_id_column >= 0) {
				if (idx == table_info.row_id_column) {
					continue; // Skip row_id column
				}
				if (idx > table_info.row_id_column) {
					adjusted--;
				}
			}
			if (adjusted < 0 || (idx_t)adjusted >= create_info.columns.LogicalColumnCount()) {
				throw InvalidInputException(
				    "VGI: UNIQUE constraint references column index %d out of range (table '%s' has %llu columns)",
				    adjusted, create_info.table, (unsigned long long)create_info.columns.LogicalColumnCount());
			}
			col_names.push_back(create_info.columns.GetColumn(LogicalIndex(adjusted)).Name());
		}
		// A key made only of the (skipped) row_id column has nothing left to constrain.
		if (!col_names.empty()) {
			create_info.constraints.push_back(make_uniq<UniqueConstraint>(std::move(col_names), false));
		}
	}

	// Apply PRIMARY KEY constraints (UniqueConstraint with is_primary_key=true)
	for (auto &cols : table_info.primary_key_constraints) {
		vector<string> col_names;
		for (auto idx : cols) {
			int adjusted = idx;
			if (table_info.row_id_column >= 0) {
				if (idx == table_info.row_id_column) {
					continue; // Skip row_id column
				}
				if (idx > table_info.row_id_column) {
					adjusted--;
				}
			}
			if (adjusted < 0 || (idx_t)adjusted >= create_info.columns.LogicalColumnCount()) {
				throw InvalidInputException(
				    "VGI: PRIMARY KEY constraint references column index %d out of range (table '%s' has %llu columns)",
				    adjusted, create_info.table, (unsigned long long)create_info.columns.LogicalColumnCount());
			}
			col_names.push_back(create_info.columns.GetColumn(LogicalIndex(adjusted)).Name());
		}
		if (!col_names.empty()) {
			create_info.constraints.push_back(make_uniq<UniqueConstraint>(std::move(col_names), true));
		}
	}

	// Apply CHECK constraints
	for (auto &expr_str : table_info.check_constraints) {
		auto expressions = Parser::ParseExpressionList(expr_str);
		if (!expressions.empty()) {
			create_info.constraints.push_back(make_uniq<CheckConstraint>(expressions[0]->Copy()));
		}
	}

	// Apply FOREIGN KEY constraints
	for (auto &fk : table_info.foreign_key_constraints) {
		ForeignKeyInfo fk_info;
		fk_info.type = ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE;
		fk_info.schema = fk.referenced_schema;
		fk_info.table = fk.referenced_table;
		// Physical indices are not populated — VGI tables are read-only so
		// DML enforcement is not needed. The constraint is metadata-only.
		vector<string> pk_cols(fk.pk_columns.begin(), fk.pk_columns.end());
		vector<string> fk_cols(fk.fk_columns.begin(), fk.fk_columns.end());
		create_info.constraints.push_back(
		    make_uniq<ForeignKeyConstraint>(std::move(pk_cols), std::move(fk_cols), std::move(fk_info)));
	}

	// Apply column metadata (defaults, comments) from Arrow field metadata
	if (table_info.arrow_schema) {
		for (int i = 0; i < table_info.arrow_schema->num_fields(); i++) {
			if (table_info.row_id_column >= 0 && i == table_info.row_id_column) {
				continue;
			}
			auto &arrow_field = table_info.arrow_schema->field(i);
			if (!arrow_field->HasMetadata()) {
				continue;
			}

			int adjusted = i;
			if (table_info.row_id_column >= 0 && i > table_info.row_id_column) {
				adjusted--;
			}
			auto &col = create_info.columns.GetColumnMutable(LogicalIndex(adjusted));

			// Generated expression (mutually exclusive with default)
			auto gen_idx = arrow_field->metadata()->FindKey(VGI_GENERATED_EXPRESSION_METADATA_KEY);
			if (gen_idx >= 0) {
				auto gen_expr_str = arrow_field->metadata()->value(gen_idx);
				auto gen_expressions = Parser::ParseExpressionList(gen_expr_str);
				if (!gen_expressions.empty()) {
					col.SetGeneratedExpression(std::move(gen_expressions[0]));
				}
			} else {
				// Default value (only for non-generated columns)
				auto default_idx = arrow_field->metadata()->FindKey("default");
				if (default_idx >= 0) {
					auto default_expr = arrow_field->metadata()->value(default_idx);
					auto expressions = Parser::ParseExpressionList(default_expr);
					if (!expressions.empty()) {
						col.SetDefaultValue(std::move(expressions[0]));
					}
				}
			}

			// Column comment (applies to both generated and non-generated)
			auto comment_idx = arrow_field->metadata()->FindKey("comment");
			if (comment_idx >= 0) {
				col.SetComment(Value(arrow_field->metadata()->value(comment_idx)));
			}
		}
	}

	return create_info;
}

// extra_info for a FunctionInfo the worker sent that breaks the protocol.
static ErrorInfo FunctionInfoViolation(const std::string &worker_path, const std::string &function_name,
                                       const char *field) {
	return std::move(ErrorInfo(error_subtype::kProtocolViolation)
	                     .Worker(worker_path)
	                     .Function(function_name)
	                     .Set(error_key::kField, field));
}

VgiFunctionInfo ParseFunctionInfo(const std::shared_ptr<arrow::RecordBatch> &batch, int64_t row_idx,
                                  const std::string &worker_path) {
	auto wire = generated::Decode<generated::FunctionInfo>(batch, row_idx, worker_path);
	VgiFunctionInfo info;
	info.name = std::move(wire.name);
	info.schema_name = SchemaNameFromPath(wire.schema_path, "FunctionInfo.schema_path");
	info.tags = std::move(wire.tags);
	info.function_type = FromWire(wire.function_type);
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

	// DuckDB exposes example SQL directly; retain the structured descriptions
	// in its discovery tag so workers need not transmit a duplicate copy.
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

	// Typed parameter defaults ride as an IPC-serialized one-row RecordBatch. Keep
	// the bytes on VgiFunctionInfo so DuckDB 1.5 can preserve the VGI 2.0 contract
	// even though its scalar/aggregate registration API cannot install defaults yet.
	auto &parameter_defaults = wire.parameter_default_values;
	if (parameter_defaults) {
		auto defaults_batch = DeserializeFromIpcBytes(*parameter_defaults);
		if (!defaults_batch || defaults_batch->num_rows() != 1) {
			throw IOException(FunctionInfoViolation(worker_path, info.name, "parameter_default_values"), "Function '%s' parameter_default_values must contain exactly one row", info.name);
		}
		if (!info.arguments_schema) {
			throw IOException(FunctionInfoViolation(worker_path, info.name, "parameter_default_values"), "Function '%s' has parameter_default_values without an arguments schema", info.name);
		}

		int previous_argument_index = -1;
		for (const auto &default_field : defaults_batch->schema()->fields()) {
			const auto argument_index = info.arguments_schema->GetFieldIndex(default_field->name());
			if (argument_index < 0) {
				throw IOException(FunctionInfoViolation(worker_path, info.name, "parameter_default_values"), "Function '%s' has a default for unknown parameter '%s'", info.name,
				                  default_field->name());
			}
			if (argument_index <= previous_argument_index) {
				throw IOException(FunctionInfoViolation(worker_path, info.name, "parameter_default_values"), "Function '%s' parameter defaults are not in signature order", info.name);
			}
			const auto &argument_field = info.arguments_schema->field(argument_index);
			if (!argument_field->type()->Equals(default_field->type())) {
				throw IOException(FunctionInfoViolation(worker_path, info.name, "parameter_default_values"), "Function '%s' default for parameter '%s' has type %s, expected %s", info.name,
				                  default_field->name(), default_field->type()->ToString(),
				                  argument_field->type()->ToString());
			}
			previous_argument_index = argument_index;
		}

		if (info.function_type == VgiFunctionType::Scalar || info.function_type == VgiFunctionType::Aggregate) {
			const auto default_count = defaults_batch->num_columns();
			int fixed_parameter_count = info.arguments_schema->num_fields();
			if (fixed_parameter_count > 0) {
				const auto &last_field = info.arguments_schema->field(fixed_parameter_count - 1);
				if (last_field->HasMetadata()) {
					const auto metadata = last_field->metadata();
					const auto varargs_index = metadata->FindKey(VGI_VARARGS_METADATA_KEY);
					if (varargs_index >= 0 && metadata->value(varargs_index) == VGI_VARARGS_TRUE_VALUE) {
						fixed_parameter_count--;
					}
				}
			}
			if (default_count > fixed_parameter_count) {
				throw IOException(FunctionInfoViolation(worker_path, info.name, "parameter_default_values"), "Function '%s' has more defaults than parameters", info.name);
			}
			for (int default_index = 0; default_index < default_count; default_index++) {
				const auto expected_parameter = fixed_parameter_count - default_count + default_index;
				if (defaults_batch->schema()->field(default_index)->name() !=
				    info.arguments_schema->field(expected_parameter)->name()) {
					throw IOException(FunctionInfoViolation(worker_path, info.name, "parameter_default_values"), "Function '%s' defaults must form a trailing parameter sequence", info.name);
				}
			}
		}
		info.parameter_default_values_bytes = std::move(*parameter_defaults);
	}

	// Table function capabilities. late_materialization is nullopt for workers whose
	// metadata predates it; the gate in vgi_table_entry.cpp treats that as "not capable".
	info.projection_pushdown = wire.projection_pushdown;
	info.filter_pushdown = wire.filter_pushdown;
	info.sampling_pushdown = wire.sampling_pushdown;
	info.late_materialization = wire.late_materialization;
	info.filter_semantic_profiles = std::move(wire.filter_semantic_profiles);
	for (auto &fn : wire.additional_filter_functions) {
		info.additional_filter_functions.push_back({std::move(fn.namespace_), std::move(fn.name), fn.version});
	}

	info.max_workers = wire.max_workers;
	// supports_batch_index: the function emits ``vgi_batch_index`` in each batch's
	// metadata, threaded through ``TableFunction::get_partition_data`` so ordered
	// sinks can reassemble parallel output (and skips the FIXED_ORDER
	// MaxThreads=1 clamp — see vgi_table_function_set.cpp).
	info.supports_batch_index = wire.supports_batch_index;
	// split_token_ttl_seconds: nullopt means unbounded, NOT "expires immediately".
	info.supports_splits = wire.supports_splits;
	info.filters_exactly_applied = wire.filters_exactly_applied;
	info.supports_positions = wire.supports_positions;
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

// ============================================================================
// Secret Extraction from DuckDB SecretManager
// ============================================================================

// Extract key-value pairs from a KeyValueSecret into a map.
// Must be called while the secret reference is still valid.
static std::map<std::string, Value> ExtractSecretKeyValues(const KeyValueSecret &kv_secret) {
	std::map<std::string, Value> kv_pairs;
	// Add standard fields from BaseSecret
	kv_pairs["type"] = Value(kv_secret.GetType());
	kv_pairs["provider"] = Value(kv_secret.GetProvider());
	kv_pairs["name"] = Value(kv_secret.GetName());
	// Add all custom key-value entries
	for (const auto &[k, v] : kv_secret.secret_map) {
		kv_pairs[k] = v;
	}
	// Add the secret's scope prefixes (newline-joined), set AFTER the secret_map
	// loop so it is reliable for worker-side scope matching. Lets a worker that
	// requested several scopes pick the right secret per path (e.g. per S3
	// bucket). Empty for an unscoped secret (matches anything as a fallback).
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

// Insert a resolved secret's fields into `result`, keyed by the secret's unique
// name so multiple secrets of the same type (e.g. several S3 buckets) all
// survive. Requesting the same secret via several scopes dedups naturally (same
// name). The serialized `type`/`scope`/... fields let the worker select per
// path via `Secrets::for_scope`. (No name should be empty, but fall back to the
// type to avoid an empty key colliding.)
static void InsertResolvedSecret(std::map<std::string, std::map<std::string, Value>> &result,
                                 const std::string &secret_type, const KeyValueSecret &kv_secret) {
	std::string key = kv_secret.GetName();
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
			// Name-based lookup (optionally constrained by type)
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
			// Scope-based lookup (unscoped if scope is empty)
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
