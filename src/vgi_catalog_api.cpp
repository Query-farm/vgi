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
#include "vgi_logging.hpp"
#include "vgi_profiling.hpp"
#include "vgi_protocol_constants.hpp"

#include <typeinfo>
#include "vgi_rpc_client.hpp"
#include "vgi_rpc_types.hpp"
#include "generated/vgi_request_builders.hpp"
#include "vgi_schema_registry.hpp"
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
		auto attach_hex = BytesToHex(ctx_.attach_opaque_data);
		if (!attach_hex.empty()) {
			info.emplace_back("attach_opaque_data", attach_hex);
		}
		auto txn_hex = BytesToHex(ctx_.transaction_opaque_data);
		if (!txn_hex.empty()) {
			info.emplace_back("transaction_opaque_data", txn_hex);
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
		auto &db = *context.db;
		auto &http_util = HTTPUtil::Get(db);
		auto owned = http_util.InitializeParameters(context, url);
		cached_http_params_ = std::shared_ptr<HTTPParams>(owned.release());
		// Mirror the per-call ApplyHttpTimeout that HttpPostArrowIpcInternal
		// used to do. Captured once at cache-init time — vgi_http_timeout_seconds
		// changes won't be picked up without re-ATTACH. See the TODO on the
		// method declaration.
		Value timeout_val;
		if (context.TryGetCurrentSetting("vgi_http_timeout_seconds", timeout_val)) {
			cached_http_params_->timeout = static_cast<uint64_t>(timeout_val.GetValue<int64_t>());
		} else {
			cached_http_params_->timeout = 300;
		}
	}
	return cached_http_params_;
}

// ============================================================================
// RPC-based Catalog API Helper
// ============================================================================

// Invoke a unary RPC catalog method and return the raw unary response.
// Delegates to InvokePooledUnaryRpc which handles pool acquire/release, stderr
// draining (critical — without it, long-running catalogs hang when the worker
// stderr pipe buffer fills), and stale-pool retry.
static UnaryResponseResult InvokeRpcMethod(const CatalogRpcContext &ctx, const std::string &method_name,
                                            const std::shared_ptr<arrow::RecordBatch> &params, ClientContext &context) {
	CatalogRpcInstrumentation instr(context, ctx, method_name);
	try {
		// Validate the outgoing request against the registered params schema.
		// Catches encoder drift in BuildXxxParams (e.g. flipped nullability,
		// missing fields) at the C++ boundary, before it turns into an opaque
		// failure on the worker side.
		ValidateRequestSchema(params, method_name, ctx.params->worker_path());

		UnaryRpcOptions opts {context,
		                      ctx.params->worker_path(),
		                      ctx.params->worker_debug(),
		                      ctx.params->use_pool(),
		                      ctx.params->data_version_spec(),
		                      ctx.params->implementation_version(),
		                      "rpc_catalog",
		                      ctx.params->auth(),
		                      ctx.params->cookie_jar()};
		// Cache-hit on the HTTP path: avoids re-entering the secret manager (and
		// thus the MetaTransaction mutex) for RPCs invoked from inside
		// VgiTransaction::Start. No-op for subprocess transport. See the TODO on
		// VgiAttachParameters::GetOrInitHttpParams.
		if (IsHttpTransport(ctx.params->worker_path())) {
			opts.cached_http_params = ctx.params->GetOrInitHttpParams(context, ctx.params->worker_path());
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
		auto result = InvokePooledUnaryRpc(opts, method_name, params);
		instr.MarkOk();
		return result;
	} catch (const std::exception &e) {
		// typeid().name() gives the demangled-or-mangled C++ type — close enough
		// for triage. The full message goes in error_message; consumers that need
		// the DuckDB ExceptionType can re-parse there.
		instr.NoteException(typeid(e).name(), e.what());
		throw;
	}
}

// Extract result binary bytes from a unary RPC response batch,
// then deserialize to a RecordBatch.
static std::shared_ptr<arrow::RecordBatch> ExtractAndDeserializeResult(
    const UnaryResponseResult &response, const std::string &method_name, const std::string &worker_path) {
	std::shared_ptr<arrow::RecordBatch> result;

	if (response.batch && response.batch->num_rows() != 0) {
		auto result_col = response.batch->GetColumnByName("result");
		if (!result_col) {
			throw IOException("Response missing 'result' column from %s [worker: %s]", method_name, worker_path);
		}
		// The outer envelope column must be Binary (not Utf8 or any other type).
		// Use type-id comparison instead of dynamic_pointer_cast<BinaryArray>:
		// arrow::StringArray derives from arrow::BinaryArray, so the cast would
		// silently accept a String-typed column and feed raw UTF-8 bytes into
		// DeserializeFromIpcBytes — an invariant we must enforce here.
		if (result_col->type()->id() != arrow::Type::BINARY) {
			throw IOException(
			    "Response 'result' column from %s has type %s, expected Binary [worker: %s]. "
			    "The vgi-rpc unary envelope must wrap the inner IPC payload in a Binary column.",
			    method_name, result_col->type()->ToString(), worker_path);
		}
		auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(result_col);
		if (!binary_array->IsNull(0)) {
			auto view = binary_array->GetView(0);
			try {
				result =
				    DeserializeFromIpcBytes(reinterpret_cast<const uint8_t *>(view.data()), view.size());
			} catch (const std::exception &e) {
				throw IOException(
				    "Failed to deserialize IPC response for %s from worker [worker: %s]: %s. "
				    "The worker likely returned a malformed or out-of-date response shape for this method.",
				    method_name, worker_path, e.what());
			}
		}
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
// Typed Catalog RPC Functions
// ============================================================================

std::vector<VgiCatalogInfo> InvokeCatalogs(const std::string &worker_path, ClientContext &context,
                                            bool worker_debug, bool use_pool,
                                            const std::shared_ptr<CatalogAuth> &auth) {
	auto temp_params = std::make_shared<VgiAttachParameters>(worker_path, "", worker_debug, use_pool, auth);
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
		RecordBatchSingleRow row(info_batch, 0, "CatalogInfo", worker_path);
		VgiCatalogInfo info;
		info.name = row["name"].value_not_null<std::string>();
		info.implementation_version = row["implementation_version"].value_or(std::string(""));
		info.data_version_spec = row["data_version_spec"].value_or(std::string(""));

		// Parse attach_option_specs (list[bytes] of serialized AttachOptionSpec).
		// Older workers that predate attach-option discovery omit this field — treat as empty.
		auto spec_bytes_list = row["attach_option_specs"].value_or(std::vector<std::vector<uint8_t>> {});
		for (const auto &spec_bytes : spec_bytes_list) {
			info.attach_option_specs.push_back(ParseAttachOptionSpec(spec_bytes, worker_path, context));
		}

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
                                        std::optional<std::string> launcher_state_dir) {
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
	temp_cfg.launcher_idle_timeout_seconds = launcher_idle_timeout_seconds;
	temp_cfg.launcher_state_dir = launcher_state_dir;
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
		throw IOException("Empty response from catalog_attach [worker: %s]", worker_path);
	}
	return ParseCatalogAttachResult(result_batch, worker_path, context);
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

std::vector<VgiTableInfo> InvokeCatalogSchemaContentsTables(const CatalogRpcContext &ctx,
                                                            const std::string &schema_name, ClientContext &context) {
	auto &worker_path = ctx.params->worker_path();
	auto params = generated::BuildCatalogSchemaContentsTablesParams(ctx.attach_opaque_data, schema_name, OptTxn(ctx));
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
	auto params = generated::BuildCatalogSchemaContentsViewsParams(ctx.attach_opaque_data, schema_name, OptTxn(ctx));
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
	auto params = generated::BuildCatalogSchemaContentsFunctionsParams(ctx.attach_opaque_data, schema_name, function_type, OptTxn(ctx));
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
	auto params = generated::BuildCatalogSchemaContentsMacrosParams(ctx.attach_opaque_data, schema_name, macro_type, OptTxn(ctx));
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

std::optional<VgiTableInfo> InvokeCatalogTableGet(const CatalogRpcContext &ctx,
                                                   const std::string &schema_name, const std::string &table_name,
                                                   ClientContext &context) {
	auto &worker_path = ctx.params->worker_path();
	// catalog_table_get's Protocol schema always includes at_unit/at_value (nullable);
	// the overload without time-travel passes std::nullopt for both.
	auto params = generated::BuildCatalogTableGetParams(ctx.attach_opaque_data, schema_name, table_name,
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
	auto params = generated::BuildCatalogTableGetParams(ctx.attach_opaque_data, schema_name, table_name,
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
	auto params = generated::BuildCatalogViewGetParams(ctx.attach_opaque_data, schema_name, view_name, OptTxn(ctx));
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
	auto params = generated::BuildCatalogTableScanFunctionGetParams(ctx.attach_opaque_data, schema_name, table_name,
	                                                                OptStrIfNonEmpty(at_unit), OptStrIfNonEmpty(at_value), OptTxn(ctx));
	auto response = InvokeRpcMethod(ctx, "catalog_table_scan_function_get", params, context);
	auto result_batch = ExtractAndDeserializeResult(response, "catalog_table_scan_function_get", worker_path);
	if (!result_batch || result_batch->num_rows() == 0) {
		throw IOException("Empty response from catalog_table_scan_function_get [worker: %s]", worker_path);
	}
	return ParseScanFunctionResult(context, result_batch, worker_path);
}

// ============================================================================
// Table Write Function Get
// ============================================================================

static VgiWriteFunctionResult InvokeCatalogTableWriteFunctionGet(
    const CatalogRpcContext &ctx, const std::string &rpc_method,
    const std::string &schema_name, const std::string &table_name, ClientContext &context) {
	auto &worker_path = ctx.params->worker_path();
	std::shared_ptr<arrow::RecordBatch> params;
	if (rpc_method == "catalog_table_insert_function_get") {
		params = generated::BuildCatalogTableInsertFunctionGetParams(ctx.attach_opaque_data, schema_name, table_name, OptTxn(ctx));
	} else if (rpc_method == "catalog_table_update_function_get") {
		params = generated::BuildCatalogTableUpdateFunctionGetParams(ctx.attach_opaque_data, schema_name, table_name, OptTxn(ctx));
	} else {
		params = generated::BuildCatalogTableDeleteFunctionGetParams(ctx.attach_opaque_data, schema_name, table_name, OptTxn(ctx));
	}
	auto response = InvokeRpcMethod(ctx, rpc_method, params, context);
	auto result_batch = ExtractAndDeserializeResult(response, rpc_method, worker_path);
	if (!result_batch || result_batch->num_rows() == 0) {
		throw IOException("Empty response from %s [worker: %s]", rpc_method, worker_path);
	}
	return ParseScanFunctionResult(context, result_batch, worker_path);
}

VgiWriteFunctionResult InvokeCatalogTableInsertFunctionGet(
    const CatalogRpcContext &ctx, const std::string &schema_name,
    const std::string &table_name, ClientContext &context) {
	return InvokeCatalogTableWriteFunctionGet(ctx, "catalog_table_insert_function_get",
	                                          schema_name, table_name, context);
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
		RecordBatchSingleRow row(result_batch, 0, "CatalogVersionResponse", worker_path);
		return row["version"].value_not_null<int64_t>();
	} catch (...) {
		// RPC failure (e.g., older worker that doesn't implement catalog_version).
		// Return 0 to signal unknown version — caller will clear cache as a safe fallback.
		return 0;
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

	RecordBatchSingleRow row(result_batch, 0, "TransactionBeginResponse", worker_path);
	return row["transaction_opaque_data"].value_or(std::vector<uint8_t>{});
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
	auto request_batch = BuildTableCreateRequest(ctx.attach_opaque_data, schema_name, table_name, columns_schema, on_conflict,
	                                             not_null_constraints, unique_constraints, check_constraints,
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
	auto params = generated::BuildCatalogTableDropParams(ctx.attach_opaque_data, schema_name, table_name, ignore_not_found, cascade, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_table_drop", params, context);
}

void InvokeCatalogTableRename(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &new_name, bool ignore_not_found,
    ClientContext &context) {
	auto params = generated::BuildCatalogTableRenameParams(ctx.attach_opaque_data, schema_name, table_name, new_name, ignore_not_found, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_table_rename", params, context);
}

void InvokeCatalogTableColumnAdd(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::shared_ptr<arrow::Schema> &column_definition,
    bool if_column_not_exists,
    ClientContext &context) {
	auto column_bytes = SerializeSchemaToIpcBytes(column_definition);
	auto params = generated::BuildCatalogTableColumnAddParams(ctx.attach_opaque_data, schema_name, table_name, column_bytes,
	                                                          /*ignore_not_found=*/false, if_column_not_exists, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_table_column_add", params, context);
}

void InvokeCatalogTableColumnDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name, bool if_column_exists, bool cascade,
    ClientContext &context) {
	auto params = generated::BuildCatalogTableColumnDropParams(ctx.attach_opaque_data, schema_name, table_name, column_name,
	                                                           /*ignore_not_found=*/false, if_column_exists, cascade, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_table_column_drop", params, context);
}

void InvokeCatalogTableColumnRename(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &old_column_name, const std::string &new_column_name,
    ClientContext &context) {
	auto params = generated::BuildCatalogTableColumnRenameParams(ctx.attach_opaque_data, schema_name, table_name, old_column_name, new_column_name,
	                                                             /*ignore_not_found=*/false, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_table_column_rename", params, context);
}

void InvokeCatalogTableCommentSet(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &comment, bool comment_is_null,
    bool ignore_not_found,
    ClientContext &context) {
	auto params = generated::BuildCatalogTableCommentSetParams(ctx.attach_opaque_data, schema_name, table_name,
	                                                           OptStrNullable(comment, comment_is_null), ignore_not_found, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_table_comment_set", params, context);
}

void InvokeCatalogTableColumnCommentSet(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name,
    const std::string &comment, bool comment_is_null,
    bool ignore_not_found,
    ClientContext &context) {
	auto params = generated::BuildCatalogTableColumnCommentSetParams(ctx.attach_opaque_data, schema_name, table_name, column_name,
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
	auto params = generated::BuildCatalogTableColumnTypeChangeParams(ctx.attach_opaque_data, schema_name, table_name, column_bytes,
	                                                                 OptStrIfNonEmpty(expression), /*ignore_not_found=*/false, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_table_column_type_change", params, context);
}

void InvokeCatalogTableColumnDefaultSet(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name, const std::string &expression,
    ClientContext &context) {
	auto params = generated::BuildCatalogTableColumnDefaultSetParams(ctx.attach_opaque_data, schema_name, table_name, column_name,
	                                                                 expression, /*ignore_not_found=*/false, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_table_column_default_set", params, context);
}

void InvokeCatalogTableColumnDefaultDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name,
    ClientContext &context) {
	auto params = generated::BuildCatalogTableColumnDefaultDropParams(ctx.attach_opaque_data, schema_name, table_name, column_name,
	                                                                  /*ignore_not_found=*/false, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_table_column_default_drop", params, context);
}

void InvokeCatalogTableNotNullSet(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name,
    ClientContext &context) {
	auto params = generated::BuildCatalogTableNotNullSetParams(ctx.attach_opaque_data, schema_name, table_name, column_name,
	                                                           /*ignore_not_found=*/false, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_table_not_null_set", params, context);
}

void InvokeCatalogTableNotNullDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name,
    ClientContext &context) {
	auto params = generated::BuildCatalogTableNotNullDropParams(ctx.attach_opaque_data, schema_name, table_name, column_name,
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
	auto params = generated::BuildCatalogViewCreateParams(ctx.attach_opaque_data, schema_name, view_name, definition, on_conflict, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_view_create", params, context);
}

void InvokeCatalogViewDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &view_name,
    bool ignore_not_found, bool cascade,
    ClientContext &context) {
	auto params = generated::BuildCatalogViewDropParams(ctx.attach_opaque_data, schema_name, view_name, ignore_not_found, cascade, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_view_drop", params, context);
}

void InvokeCatalogViewRename(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &view_name,
    const std::string &new_name, bool ignore_not_found,
    ClientContext &context) {
	auto params = generated::BuildCatalogViewRenameParams(ctx.attach_opaque_data, schema_name, view_name, new_name, ignore_not_found, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_view_rename", params, context);
}

void InvokeCatalogViewCommentSet(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &view_name,
    const std::string &comment, bool comment_is_null,
    bool ignore_not_found,
    ClientContext &context) {
	auto params = generated::BuildCatalogViewCommentSetParams(ctx.attach_opaque_data, schema_name, view_name,
	                                                          OptStrNullable(comment, comment_is_null), ignore_not_found, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_view_comment_set", params, context);
}

void InvokeCatalogSchemaCreate(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &on_conflict,
    ClientContext &context) {
	// CREATE SCHEMA ... TAGS (...) is not yet exposed by the C++ extension; we
	// always send std::nullopt for tags. catalog_schema_create's wire shape requires
	// the field, but the value is always absent until we plumb it through DDL parsing.
	auto params = generated::BuildCatalogSchemaCreateParams(ctx.attach_opaque_data, schema_name, on_conflict,
	                                                        /*comment=*/std::nullopt, /*tags=*/std::nullopt, OptTxn(ctx));
	InvokeVoidRpc(ctx, "catalog_schema_create", params, context);
}

void InvokeCatalogSchemaDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, bool ignore_not_found, bool cascade,
    ClientContext &context) {
	auto params = generated::BuildCatalogSchemaDropParams(ctx.attach_opaque_data, schema_name, ignore_not_found, cascade, OptTxn(ctx));
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

	auto params = generated::BuildCatalogTableColumnStatisticsGetParams(ctx.attach_opaque_data, schema_name, table_name, OptTxn(ctx));
	const char *method_name = "catalog_table_column_statistics_get";
	auto response = InvokeRpcMethod(ctx, method_name, params, context);

	// Custom decode path (uses DeserializeFromIpcBytesWithMetadata to preserve
	// IPC batch custom_metadata, where cache_max_age_seconds lives). The outer
	// envelope is validated here with the same rigor as ExtractAndDeserializeResult,
	// then ValidateResponseSchema is a no-op (method is registered dynamic) but we
	// keep it for symmetry with the rest of the codebase.
	if (!response.batch || response.batch->num_rows() == 0) {
		ValidateResponseSchema(nullptr, method_name, worker_path);
		return rpc_result;
	}
	auto result_col = response.batch->GetColumnByName("result");
	if (!result_col) {
		throw IOException("Response missing 'result' column from %s [worker: %s]", method_name, worker_path);
	}
	if (result_col->type()->id() != arrow::Type::BINARY) {
		throw IOException(
		    "Response 'result' column from %s has type %s, expected Binary [worker: %s]",
		    method_name, result_col->type()->ToString(), worker_path);
	}
	auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(result_col);
	if (binary_array->IsNull(0)) {
		ValidateResponseSchema(nullptr, method_name, worker_path);
		return rpc_result;
	}
	auto view = binary_array->GetView(0);
	auto deserialized = DeserializeFromIpcBytesWithMetadata(
	    reinterpret_cast<const uint8_t *>(view.data()), view.size());
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
	if (!response.batch || response.batch->num_rows() == 0) {
		ValidateResponseSchema(nullptr, method_name, worker_path);
		return {};
	}
	auto result_col = response.batch->GetColumnByName("result");
	if (!result_col) {
		throw IOException("Response missing 'result' column from %s [worker: %s]", method_name, worker_path);
	}
	if (result_col->type()->id() != arrow::Type::BINARY) {
		throw IOException(
		    "Response 'result' column from %s has type %s, expected Binary [worker: %s]",
		    method_name, result_col->type()->ToString(), worker_path);
	}
	auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(result_col);
	if (binary_array->IsNull(0)) {
		ValidateResponseSchema(nullptr, method_name, worker_path);
		return {};
	}
	auto view = binary_array->GetView(0);
	auto result_batch = DeserializeFromIpcBytes(
	    reinterpret_cast<const uint8_t *>(view.data()), view.size());
	ValidateResponseSchema(result_batch, method_name, worker_path);

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

std::optional<VgiFunctionType> ParseVgiFunctionType(const std::string &value) {
	if (value == "scalar" || value == "SCALAR") {
		return VgiFunctionType::Scalar;
	} else if (value == "table" || value == "TABLE" || value == "table_in_out") {
		// Both "table" and the legacy "table_in_out" map to streaming Table.
		return VgiFunctionType::Table;
	} else if (value == "table_buffering" || value == "TABLE_BUFFERING") {
		return VgiFunctionType::TableBuffering;
	} else if (value == "aggregate" || value == "AGGREGATE") {
		return VgiFunctionType::Aggregate;
	}
	return std::nullopt;
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

// Parse FunctionStability from wire format (Python enum .name)
// Wire format: "CONSISTENT", "VOLATILE", "CONSISTENT_WITHIN_QUERY"
static std::optional<FunctionStability> ParseFunctionStability(const std::string &value) {
	if (value == "CONSISTENT") {
		return FunctionStability::CONSISTENT;
	} else if (value == "VOLATILE") {
		return FunctionStability::VOLATILE;
	} else if (value == "CONSISTENT_WITHIN_QUERY") {
		return FunctionStability::CONSISTENT_WITHIN_QUERY;
	}
	return std::nullopt;
}

// Parse FunctionNullHandling from wire format (Python enum .name)
// Wire format: "DEFAULT", "SPECIAL"
static std::optional<FunctionNullHandling> ParseFunctionNullHandling(const std::string &value) {
	if (value == "DEFAULT") {
		return FunctionNullHandling::DEFAULT_NULL_HANDLING;
	} else if (value == "SPECIAL") {
		return FunctionNullHandling::SPECIAL_HANDLING;
	}
	return std::nullopt;
}

std::optional<VgiOrderPreservation> ParseVgiOrderPreservation(const std::string &value) {
	if (value == "PRESERVES_ORDER") {
		return VgiOrderPreservation::PreservesOrder;
	} else if (value == "NO_ORDER_GUARANTEE") {
		return VgiOrderPreservation::NoOrderGuarantee;
	} else if (value == "FIXED_ORDER") {
		return VgiOrderPreservation::FixedOrder;
	}
	return std::nullopt;
}

std::optional<VgiPartitionKind> ParseVgiPartitionKind(const std::string &value) {
	if (value == "NOT_PARTITIONED") {
		return VgiPartitionKind::NotPartitioned;
	} else if (value == "SINGLE_VALUE_PARTITIONS") {
		return VgiPartitionKind::SingleValuePartitions;
	} else if (value == "OVERLAPPING_PARTITIONS") {
		return VgiPartitionKind::OverlappingPartitions;
	} else if (value == "DISJOINT_PARTITIONS") {
		return VgiPartitionKind::DisjointPartitions;
	}
	return std::nullopt;
}

// Parse AggregateOrderDependent from wire format (Python enum .name)
// Wire format: "ORDER_DEPENDENT", "NOT_ORDER_DEPENDENT"
static std::optional<AggregateOrderDependent> ParseAggregateOrderDependent(const std::string &value) {
	if (value == "ORDER_DEPENDENT") {
		return AggregateOrderDependent::ORDER_DEPENDENT;
	} else if (value == "NOT_ORDER_DEPENDENT") {
		return AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	}
	return std::nullopt;
}

// Parse AggregateDistinctDependent from wire format (Python enum .name)
// Wire format: "DISTINCT_DEPENDENT", "NOT_DISTINCT_DEPENDENT"
static std::optional<AggregateDistinctDependent> ParseAggregateDistinctDependent(const std::string &value) {
	if (value == "DISTINCT_DEPENDENT") {
		return AggregateDistinctDependent::DISTINCT_DEPENDENT;
	} else if (value == "NOT_DISTINCT_DEPENDENT") {
		return AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT;
	}
	return std::nullopt;
}


// ============================================================================
// Result parsing using RecordBatchSingleRow
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

VgiSetting ParseVgiSetting(const std::vector<uint8_t> &bytes, const std::string &worker_path,
                           ClientContext &context) {
	// Deserialize the Setting RecordBatch
	auto batch = DeserializeFromIpcBytes(bytes);
	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty Setting batch from worker: %s", worker_path);
	}

	RecordBatchSingleRow row(batch, 0, "Setting", worker_path);

	VgiSetting setting;
	setting.name = row["name"].value_not_null<std::string>();
	setting.description = row["description"].value_not_null<std::string>();

	// Parse the type from the serialized schema bytes
	auto type_bytes = row["type"].value_not_null<std::vector<uint8_t>>();
	auto type_buffer = arrow::Buffer::Wrap(type_bytes.data(), type_bytes.size());
	auto type_stream = std::make_shared<arrow::io::BufferReader>(type_buffer);
	arrow::ipc::DictionaryMemo type_dict_memo;
	auto type_schema_result = arrow::ipc::ReadSchema(type_stream.get(), &type_dict_memo);
	if (!type_schema_result.ok()) {
		throw IOException("Failed to read type schema for setting '%s': %s", setting.name,
		                  type_schema_result.status().ToString());
	}
	auto type_schema = type_schema_result.ValueUnsafe();
	auto arrow_type = type_schema->field(0)->type();

	// Convert Arrow type to DuckDB type using the standard conversion pipeline
	{
		ArrowSchemaWrapper c_schema;
		ArrowTableSchema arrow_table;
		vector<LogicalType> types;
		vector<string> names;
		ArrowSchemaToDuckDBTypes(context, type_schema, c_schema, arrow_table, types, names);
		setting.type = types[0];
	}

	// Parse the default value if present
	auto default_bytes_opt = row["default_value"].as<std::vector<uint8_t>>();
	if (default_bytes_opt && !default_bytes_opt->empty()) {
		auto default_batch = DeserializeFromIpcBytes(*default_bytes_opt);
		if (default_batch && default_batch->num_rows() > 0 && default_batch->num_columns() > 0) {
			setting.default_value = ExtractArrowValue(default_batch->column(0), 0, setting.type);
		} else {
			setting.default_value = Value(setting.type);
		}
	} else {
		setting.default_value = Value(setting.type);
	}

	return setting;
}

VgiAttachOptionSpec ParseAttachOptionSpec(const std::vector<uint8_t> &bytes, const std::string &worker_path,
                                          ClientContext &context) {
	auto batch = DeserializeFromIpcBytes(bytes);
	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty AttachOptionSpec batch from worker: %s", worker_path);
	}

	RecordBatchSingleRow row(batch, 0, "AttachOptionSpec", worker_path);

	VgiAttachOptionSpec spec;
	spec.name = row["name"].value_not_null<std::string>();
	spec.description = row["description"].value_not_null<std::string>();

	auto type_bytes = row["type"].value_not_null<std::vector<uint8_t>>();
	auto type_buffer = arrow::Buffer::Wrap(type_bytes.data(), type_bytes.size());
	auto type_stream = std::make_shared<arrow::io::BufferReader>(type_buffer);
	arrow::ipc::DictionaryMemo type_dict_memo;
	auto type_schema_result = arrow::ipc::ReadSchema(type_stream.get(), &type_dict_memo);
	if (!type_schema_result.ok()) {
		throw IOException("Failed to read type schema for attach option '%s': %s", spec.name,
		                  type_schema_result.status().ToString());
	}
	auto type_schema = type_schema_result.ValueUnsafe();

	{
		ArrowSchemaWrapper c_schema;
		ArrowTableSchema arrow_table;
		vector<LogicalType> types;
		vector<string> names;
		ArrowSchemaToDuckDBTypes(context, type_schema, c_schema, arrow_table, types, names);
		spec.type = types[0];
	}

	auto default_bytes_opt = row["default_value"].as<std::vector<uint8_t>>();
	if (default_bytes_opt && !default_bytes_opt->empty()) {
		auto default_batch = DeserializeFromIpcBytes(*default_bytes_opt);
		if (default_batch && default_batch->num_rows() > 0 && default_batch->num_columns() > 0) {
			spec.default_value = ExtractArrowValue(default_batch->column(0), 0, spec.type);
		} else {
			spec.default_value = Value(spec.type);
		}
	} else {
		spec.default_value = Value(spec.type);
	}

	return spec;
}

VgiSecretType ParseVgiSecretType(const std::vector<uint8_t> &bytes, const std::string &worker_path,
                                  ClientContext &context) {
	// Deserialize the SecretTypeSpec RecordBatch
	auto batch = DeserializeFromIpcBytes(bytes);
	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty SecretTypeSpec batch from worker: %s", worker_path);
	}

	RecordBatchSingleRow row(batch, 0, "SecretTypeSpec", worker_path);

	VgiSecretType secret_type;
	secret_type.name = row["name"].value_not_null<std::string>();
	secret_type.description = row["description"].value_not_null<std::string>();

	// Parse the parameters schema from IPC-serialized Arrow schema bytes
	auto schema_bytes = row["parameters_schema"].value_not_null<std::vector<uint8_t>>();
	auto schema_buffer = arrow::Buffer::Wrap(schema_bytes.data(), schema_bytes.size());
	auto schema_stream = std::make_shared<arrow::io::BufferReader>(schema_buffer);
	arrow::ipc::DictionaryMemo dict_memo;
	auto schema_result = arrow::ipc::ReadSchema(schema_stream.get(), &dict_memo);
	if (!schema_result.ok()) {
		throw IOException("Failed to read parameters schema for secret type '%s': %s", secret_type.name,
		                  schema_result.status().ToString());
	}
	auto params_schema = schema_result.ValueUnsafe();

	// Convert Arrow schema to DuckDB types in one pass
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
	vector<LogicalType> types;
	vector<string> names;
	ArrowSchemaToDuckDBTypes(context, params_schema, c_schema, arrow_table, types, names);

	// Build VgiSecretTypeParam for each field
	for (int i = 0; i < params_schema->num_fields(); i++) {
		VgiSecretTypeParam param;
		param.name = names[i];
		param.type = types[i];

		// Check for redact metadata
		auto field_metadata = params_schema->field(i)->metadata();
		if (field_metadata) {
			auto redact_idx = field_metadata->FindKey("redact");
			if (redact_idx >= 0 && field_metadata->value(redact_idx) == "true") {
				param.redact = true;
			}
		}

		secret_type.parameters.push_back(std::move(param));
	}

	return secret_type;
}

CatalogAttachResult ParseCatalogAttachResult(const std::shared_ptr<arrow::RecordBatch> &batch,
                                             const std::string &worker_path, ClientContext &context) {
	CatalogAttachResult result;

	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty response from catalog_attach");
	}

	RecordBatchSingleRow row(batch, 0, "CatalogAttachResult", worker_path);
	result.attach_opaque_data = row["attach_opaque_data"].value_not_null<std::vector<uint8_t>>();
	result.supports_transactions = row["supports_transactions"].value_not_null<bool>();
	result.supports_time_travel = row["supports_time_travel"].value_not_null<bool>();
	result.catalog_version_frozen = row["catalog_version_frozen"].value_not_null<bool>();
	result.catalog_version = row["catalog_version"].value_not_null<int64_t>();
	result.attach_opaque_data_required = row["attach_opaque_data_required"].value_not_null<bool>();
	result.default_schema = row["default_schema"].value_not_null<std::string>();
	if (result.default_schema.empty()) {
		result.default_schema = "main";
	}

	// Parse settings list - each element is a serialized Setting
	auto settings_bytes = row["settings"].value_or(std::vector<std::vector<uint8_t>> {});
	for (const auto &setting_bytes : settings_bytes) {
		result.settings.push_back(ParseVgiSetting(setting_bytes, worker_path, context));
	}

	// Parse secret_types list - each element is a serialized SecretTypeSpec
	auto secret_types_bytes = row["secret_types"].value_or(std::vector<std::vector<uint8_t>> {});
	for (const auto &st_bytes : secret_types_bytes) {
		result.secret_types.push_back(ParseVgiSecretType(st_bytes, worker_path, context));
	}

	// Parse optional comment and tags (backward-compatible with older workers)
	result.comment = row["comment"].value_or(std::string(""));
	result.tags = row["tags"].value_or(std::map<std::string, std::string> {});

	// Parse column statistics capability flag (backward-compatible)
	result.supports_column_statistics = row["supports_column_statistics"].value_or(false);

	// Resolved versions (empty when the worker has no opinion or the request
	// omitted a constraint)
	result.resolved_data_version = row["resolved_data_version"].value_or(std::string(""));
	result.resolved_implementation_version = row["resolved_implementation_version"].value_or(std::string(""));

	return result;
}

VgiSchemaInfo ParseSchemaInfo(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &worker_path) {
	VgiSchemaInfo info;

	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty response from schema_get");
	}

	RecordBatchSingleRow row(batch, 0, "SchemaInfo", worker_path);
	info.name = row["name"].value_not_null<std::string>();
	info.comment = row["comment"].value_or("");
	info.tags = row["tags"].value_not_null<std::map<std::string, std::string>>();
	// nullable map — workers may omit it entirely; downstream defaults missing keys to 1
	info.estimated_object_count = row["estimated_object_count"].value_or(std::map<std::string, int64_t>{});

	return info;
}

VgiTableInfo ParseTableInfo(ClientContext &context, const std::shared_ptr<arrow::RecordBatch> &batch,
                            int64_t row_idx, const std::string &worker_path) {
	VgiTableInfo info;

	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty response from table_get");
	}

	if (row_idx >= batch->num_rows()) {
		throw IOException("Row index %lld out of range (batch has %lld rows)", row_idx, batch->num_rows());
	}

	RecordBatchSingleRow row(batch, row_idx, "TableInfo", worker_path);
	info.name = row["name"].value_not_null<std::string>();
	info.schema_name = row["schema_name"].value_not_null<std::string>();
	info.comment = row["comment"].value_or("");
	info.tags = row["tags"].value_not_null<std::map<std::string, std::string>>();

	// Parse the columns field which contains a serialized Arrow schema
	auto columns_data = row["columns"].value_not_null<std::vector<uint8_t>>();
	info.arrow_schema = DeserializeSchema(columns_data);

	// Detect is_row_id field metadata
	for (int i = 0; i < info.arrow_schema->num_fields(); i++) {
		auto &field = info.arrow_schema->field(i);
		if (field->HasMetadata() && field->metadata()->FindKey(VGI_ROW_ID_METADATA_KEY) >= 0) {
			if (info.row_id_column >= 0) {
				throw InvalidInputException("Table '%s' has multiple is_row_id columns — at most one is allowed",
				                            info.name);
			}
			info.row_id_column = i;
		}
	}

	// Parse constraints (non-nullable arrays per protocol)
	auto not_null = row["not_null_constraints"].value_not_null<std::vector<int32_t>>();
	info.not_null_constraints = std::vector<int>(not_null.begin(), not_null.end());
	auto unique = row["unique_constraints"].value_not_null<std::vector<std::vector<int32_t>>>();
	for (const auto &u : unique) {
		info.unique_constraints.push_back(std::vector<int>(u.begin(), u.end()));
	}
	info.check_constraints = row["check_constraints"].value_not_null<std::vector<std::string>>();

	// Parse primary_key_constraints (optional, backward-compatible)
	auto pk = row["primary_key_constraints"].value_or(std::vector<std::vector<int32_t>>{});
	for (const auto &p : pk) {
		info.primary_key_constraints.push_back(std::vector<int>(p.begin(), p.end()));
	}

	// Parse foreign_key_constraints (optional, backward-compatible)
	// Each element is IPC-serialized bytes containing fk_columns, pk_columns,
	// referenced_table, referenced_schema
	auto fk_bytes_list = row["foreign_key_constraints"].value_or(std::vector<std::vector<uint8_t>>{});
	for (const auto &fk_bytes : fk_bytes_list) {
		auto fk_batch = DeserializeFromIpcBytes(fk_bytes);
		if (!fk_batch || fk_batch->num_rows() == 0) {
			continue;
		}
		RecordBatchSingleRow fk_row(fk_batch, 0, "ForeignKeyInfo", worker_path);

		VgiTableInfo::ForeignKey fk;
		fk.fk_columns = fk_row["fk_columns"].value_not_null<std::vector<std::string>>();
		fk.pk_columns = fk_row["pk_columns"].value_not_null<std::vector<std::string>>();
		fk.referenced_table = fk_row["referenced_table"].value_not_null<std::string>();
		fk.referenced_schema = fk_row["referenced_schema"].value_not_null<std::string>();
		info.foreign_key_constraints.push_back(std::move(fk));
	}

	// Parse write support flags (optional, backward-compatible with old workers)
	info.supports_insert = row["supports_insert"].value_or(false);
	info.supports_update = row["supports_update"].value_or(false);
	info.supports_delete = row["supports_delete"].value_or(false);
	// Workers must opt in: defaults to false so a worker that supports
	// INSERT/UPDATE/DELETE but never wired up RETURNING handling doesn't get
	// surprised by a planner that sends RETURNING through.
	info.supports_returning = row["supports_returning"].value_or(false);

	// Parse column statistics capability flag (backward-compatible)
	info.supports_column_statistics = row["supports_column_statistics"].value_or(false);

	// Parse optional inlined function-discovery results (backward-compatible).
	// When present, the extension uses these directly and skips the
	// corresponding catalog_table_*_function_get RPC.
	auto decode_inlined = [&](const char *field_name) -> std::optional<VgiScanFunctionResult> {
		auto bytes = row[field_name].value_or(std::vector<uint8_t>{});
		if (bytes.empty()) {
			return std::nullopt;
		}
		auto sf_batch = DeserializeFromIpcBytes(bytes);
		if (!sf_batch || sf_batch->num_rows() == 0) {
			return std::nullopt;
		}
		return ParseScanFunctionResult(context, sf_batch, worker_path);
	};
	info.scan_function = decode_inlined("scan_function");
	info.insert_function = decode_inlined("insert_function");
	info.update_function = decode_inlined("update_function");
	info.delete_function = decode_inlined("delete_function");

	// Parse optional inlined cardinality (backward-compatible). When present,
	// the extension uses these values instead of firing the per-bind
	// table_function_cardinality RPC.
	info.cardinality_estimate = row["cardinality_estimate"].as<int64_t>();
	info.cardinality_max = row["cardinality_max"].as<int64_t>();

	// Parse optional inlined column statistics (backward-compatible). The
	// bytes are the IPC payload of `serialize_column_statistics` from the
	// worker — same wire shape as the on-demand catalog_table_column_statistics_get
	// RPC's response. We hold the raw bytes here and let
	// VgiTableEntry::GetStatistics deserialize lazily on first call (it has a
	// ClientContext &; this code path doesn't).
	{
		auto stats_bytes = row["column_statistics"].value_or(std::vector<uint8_t>{});
		info.column_statistics =
		    stats_bytes.empty() ? std::nullopt : std::make_optional(std::move(stats_bytes));
	}

	// Parse optional inlined bind result (backward-compatible). The bytes
	// are the IPC payload of `BindResponse.serialize_to_bytes()` — same wire
	// shape a worker's `bind` RPC returns. PerformVgiTableFunctionBind
	// short-circuits when this is set and feeds the bytes through
	// `BuildBindResultFromInlinedBytes`.
	{
		auto bind_bytes = row["bind_result"].value_or(std::vector<uint8_t>{});
		info.bind_result =
		    bind_bytes.empty() ? std::nullopt : std::make_optional(std::move(bind_bytes));
	}

	// Validate: UPDATE/DELETE require a row ID column
	if ((info.supports_update || info.supports_delete) && info.row_id_column < 0) {
		throw InvalidInputException(
		    "Table '%s' declares update/delete support but has no row ID column "
		    "(mark a column with is_row_id metadata)",
		    info.name);
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

	if (!batch || batch->num_rows() == 0) {
		return schemas;
	}

	for (int64_t i = 0; i < batch->num_rows(); i++) {
		RecordBatchSingleRow row(batch, i, "SchemaInfo", worker_path);
		VgiSchemaInfo info;
		info.name = row["name"].value_not_null<std::string>();
		info.comment = row["comment"].value_or("");
		info.tags = row["tags"].value_not_null<std::map<std::string, std::string>>();
		schemas.push_back(std::move(info));
	}

	return schemas;
}

VgiViewInfo ParseViewInfo(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &worker_path) {
	VgiViewInfo info;

	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty response from view_get");
	}

	RecordBatchSingleRow row(batch, 0, "ViewInfo", worker_path);
	info.name = row["name"].value_not_null<std::string>();
	info.schema_name = row["schema_name"].value_not_null<std::string>();
	info.definition = row["definition"].value_not_null<std::string>();
	info.comment = row["comment"].value_or("");
	info.tags = row["tags"].value_not_null<std::map<std::string, std::string>>();

	return info;
}

VgiMacroInfo ParseMacroInfo(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &worker_path) {
	VgiMacroInfo info;

	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty response from macro_get");
	}

	RecordBatchSingleRow row(batch, 0, "MacroInfo", worker_path);
	info.name = row["name"].value_not_null<std::string>();
	info.schema_name = row["schema_name"].value_not_null<std::string>();
	info.macro_type = row["macro_type"].value_not_null<std::string>();
	info.definition = row["definition"].value_not_null<std::string>();
	info.comment = row["comment"].value_or("");
	info.tags = row["tags"].value_not_null<std::map<std::string, std::string>>();
	info.parameters = row["parameters"].value_not_null<std::vector<std::string>>();

	// parameter_default_values is nullable binary (IPC bytes)
	auto default_bytes = row["parameter_default_values"].as<std::vector<uint8_t>>();
	if (default_bytes && !default_bytes->empty()) {
		info.parameter_default_values_bytes = std::move(*default_bytes);
	}

	return info;
}

VgiScanFunctionResult ParseScanFunctionResult(ClientContext &context, const std::shared_ptr<arrow::RecordBatch> &batch,
                                               const std::string &worker_path) {
	VgiScanFunctionResult result;

	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty response from table_scan_function_get");
	}

	RecordBatchSingleRow row(batch, 0, "ScanFunctionResult", worker_path);

	// Get function_name (required, non-nullable)
	result.function_name = row["function_name"].value_not_null<std::string>();

	// Get required_extensions (required, non-nullable list<string>)
	result.required_extensions = row["required_extensions"].value_or(std::vector<std::string> {});

	// Get arguments as binary and deserialize the nested IPC batch
	auto arguments_bytes = row["arguments"].value_not_null<std::vector<uint8_t>>();
	if (!arguments_bytes.empty()) {
		auto arguments_batch = DeserializeFromIpcBytes(arguments_bytes);
		if (arguments_batch && arguments_batch->num_rows() > 0) {
			// Convert the entire batch to DuckDB Values using proper conversion
			auto values = ArrowBatchToValues(context, arguments_batch);
			auto &schema = arguments_batch->schema();

			// Map values to positional or named arguments based on field names
			for (int i = 0; i < schema->num_fields(); i++) {
				const auto &field_name = schema->field(i)->name();
				auto &duck_value = values[i];

				// Check if this is a positional argument (arg_0, arg_1, etc.)
				if (field_name.rfind("arg_", 0) == 0) {
					// Extract index from arg_N
					try {
						size_t idx = std::stoul(field_name.substr(4));
						// Ensure positional_arguments vector is large enough
						if (idx >= result.positional_arguments.size()) {
							result.positional_arguments.resize(idx + 1);
						}
						result.positional_arguments[idx] = duck_value;
					} catch (const std::exception &) {
						// If parsing fails, treat as named argument
						result.named_arguments[field_name] = duck_value;
					}
				} else {
					// Named argument
					result.named_arguments[field_name] = duck_value;
				}
			}
		}
	}

	return result;
}

// ============================================================================
// DuckDB type conversion
// ============================================================================

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
			col_names.push_back(create_info.columns.GetColumn(LogicalIndex(adjusted)).Name());
		}
		create_info.constraints.push_back(make_uniq<UniqueConstraint>(std::move(col_names), false));
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
			col_names.push_back(create_info.columns.GetColumn(LogicalIndex(adjusted)).Name());
		}
		create_info.constraints.push_back(make_uniq<UniqueConstraint>(std::move(col_names), true));
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

VgiFunctionInfo ParseFunctionInfo(const std::shared_ptr<arrow::RecordBatch> &batch, int64_t row_idx,
                                  const std::string &worker_path) {
	VgiFunctionInfo info;

	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty response from function_get");
	}

	if (row_idx >= batch->num_rows()) {
		throw IOException("Row index %lld out of range (batch has %lld rows)", row_idx, batch->num_rows());
	}

	RecordBatchSingleRow row(batch, row_idx, "FunctionInfo", worker_path);

	// Required fields (non-nullable per protocol)
	info.name = row["name"].value_not_null<std::string>();
	info.schema_name = row["schema_name"].value_not_null<std::string>();
	info.tags = row["tags"].value_not_null<std::map<std::string, std::string>>();

	// Parse function_type as enum (required, non-nullable)
	auto function_type_str = row["function_type"].value_not_null<std::string>();
	auto function_type = ParseVgiFunctionType(function_type_str);
	if (!function_type) {
		throw IOException("VGI worker '%s' returned unknown function_type '%s' for function '%s'", worker_path,
		                  function_type_str, info.name);
	}
	info.function_type = *function_type;

	// Optional string field for description
	info.description = row["description"].value_or("");

	// Parse optional enum fields (nullable per protocol)
	auto stability_str = row["stability"].as<std::string>();
	if (stability_str) {
		info.stability = ParseFunctionStability(*stability_str);
	}

	auto null_handling_str = row["null_handling"].as<std::string>();
	if (null_handling_str) {
		info.null_handling = ParseFunctionNullHandling(*null_handling_str);
	}

	auto order_preservation_str = row["order_preservation"].as<std::string>();
	if (order_preservation_str) {
		info.order_preservation = ParseVgiOrderPreservation(*order_preservation_str);
	}

	// Documentation fields
	// examples is a list of structs with {sql, description, expected_output} - extract sql strings.
	// Some Arrow producers (depending on version / language binding) emit
	// LargeListArray instead of ListArray; handle both so a wire-format
	// drift doesn't silently drop the examples list.
	auto examples_col = batch->GetColumnByName("examples");
	auto extract_examples = [&](auto list_array) {
		if (!list_array || list_array->IsNull(row_idx)) {
			return;
		}
		auto start = list_array->value_offset(row_idx);
		auto end = list_array->value_offset(row_idx + 1);
		auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(list_array->values());
		if (!struct_array) {
			return;
		}
		auto sql_field = struct_array->GetFieldByName("sql");
		auto sql_array = std::dynamic_pointer_cast<arrow::StringArray>(sql_field);
		if (!sql_array) {
			return;
		}
		for (auto i = start; i < end; i++) {
			if (!sql_array->IsNull(i)) {
				info.examples.push_back(sql_array->GetString(i));
			}
		}
	};
	if (examples_col) {
		if (auto list_array = std::dynamic_pointer_cast<arrow::ListArray>(examples_col)) {
			extract_examples(list_array);
		} else if (auto large_list_array =
		               std::dynamic_pointer_cast<arrow::LargeListArray>(examples_col)) {
			extract_examples(large_list_array);
		}
	}
	// categories is a simple list of strings
	info.categories = row["categories"].value_or(std::vector<std::string> {});

	// Parse the arguments field which contains a serialized Arrow schema (non-nullable)
	auto args_data = row["arguments"].value_not_null<std::vector<uint8_t>>();
	if (!args_data.empty()) {
		info.arguments_schema = DeserializeSchema(args_data);
	}

	// Parse the output_schema field which contains a serialized Arrow schema (non-nullable)
	auto output_data = row["output_schema"].value_not_null<std::vector<uint8_t>>();
	if (!output_data.empty()) {
		info.output_schema = DeserializeSchema(output_data);
	}

	// Table function capabilities (nullable booleans, stored as optional)
	info.projection_pushdown = row["projection_pushdown"].as<bool>();
	info.filter_pushdown = row["filter_pushdown"].as<bool>();
	info.sampling_pushdown = row["sampling_pushdown"].as<bool>();
	info.supported_expression_filters = row["supported_expression_filters"].value_or(std::vector<std::string> {});

	// max_workers (nullable int, stored as optional)
	info.max_workers = row["max_workers"].as<int32_t>();

	// supports_batch_index — optional bool (defaults to false for older
	// workers). When true, the function emits ``vgi_batch_index`` in each
	// Arrow batch's KeyValueMetadata; the extension threads the value
	// through ``TableFunction::get_partition_data`` so ordered sinks can
	// reassemble parallel output. Also skips the FIXED_ORDER MaxThreads=1
	// clamp — see vgi_table_function_set.cpp.
	info.supports_batch_index = row["supports_batch_index"].value_or(false);

	// partition_kind — optional string mirroring DuckDB's
	// TablePartitionInfo enum. Defaults to NOT_PARTITIONED for older
	// workers. When non-default, vgi_table_function_set.cpp installs
	// ``TableFunction::get_partition_info`` returning the corresponding
	// value so the planner can pick PhysicalPartitionedAggregate for
	// matching GROUP BY queries. Only ``SINGLE_VALUE_PARTITIONS``
	// materially affects planner behavior today.
	auto partition_kind_str = row["partition_kind"].value_or(std::string {"NOT_PARTITIONED"});
	info.partition_kind =
	    ParseVgiPartitionKind(partition_kind_str).value_or(VgiPartitionKind::NotPartitioned);

	// Aggregate function fields (non-nullable with defaults)
	auto order_dependent_str = row["order_dependent"].value_or(std::string {"NOT_ORDER_DEPENDENT"});
	auto order_dependent = ParseAggregateOrderDependent(order_dependent_str);
	info.order_dependent = order_dependent.value_or(AggregateOrderDependent::NOT_ORDER_DEPENDENT);

	auto distinct_dependent_str = row["distinct_dependent"].value_or(std::string {"NOT_DISTINCT_DEPENDENT"});
	auto distinct_dependent = ParseAggregateDistinctDependent(distinct_dependent_str);
	info.distinct_dependent = distinct_dependent.value_or(AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT);

	// supports_window — optional bool (defaults to false for older workers).
	info.supports_window = row["supports_window"].value_or(false);

	// streaming_partitioned — optional bool (defaults to false for older
	// workers). When true, the function declares the
	// aggregate_streaming_open/_chunk/_close protocol; the extension's
	// optimizer rule may rewrite eligible LogicalWindow nodes to use it.
	info.streaming_partitioned = row["streaming_partitioned"].value_or(false);

	// has_finalize — optional bool (defaults to false for older workers).
	// When false, the table-in-out registration omits in_out_function_final,
	// which lets the function be used under LATERAL with correlated input.
	info.has_finalize = row["has_finalize"].value_or(false);

	// Whether the function uses the buffered Sink+Source path is encoded in
	// ``function_type`` (TableBuffering vs Table) — parsed above. No
	// separate boolean column on the wire.

	// source_order_dependent — optional bool. Only meaningful when
	// function_type == TableBuffering; controls ParallelSource on the buffered op.
	info.source_order_dependent = row["source_order_dependent"].value_or(false);

	// sink_order_dependent — optional bool. Only meaningful when
	// function_type == TableBuffering; controls ParallelSink (single-thread ingest).
	info.sink_order_dependent = row["sink_order_dependent"].value_or(false);

	// requires_input_batch_index — optional bool. Only meaningful when
	// function_type == TableBuffering; advertises RequiredPartitionInfo()=BatchIndex()
	// so DuckDB threads source-position metadata to every Sink call.
	info.requires_input_batch_index = row["requires_input_batch_index"].value_or(false);

	// Required settings for this function (list of strings)
	info.required_settings = row["required_settings"].value_or(std::vector<std::string> {});

	// Required secrets for this function (list of struct<secret_type, secret_name, scope>)
	// Parse from the Arrow list<struct> column
	auto secrets_col = batch->GetColumnByName("required_secrets");
	if (secrets_col && !secrets_col->IsNull(row_idx)) {
		auto list_array = std::dynamic_pointer_cast<arrow::ListArray>(secrets_col);
		if (list_array) {
			auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(list_array->values());
			if (struct_array) {
				int64_t start = list_array->value_offset(row_idx);
				int64_t end = list_array->value_offset(row_idx + 1);

				auto type_col = std::dynamic_pointer_cast<arrow::StringArray>(struct_array->GetFieldByName("secret_type"));
				auto name_col = std::dynamic_pointer_cast<arrow::StringArray>(struct_array->GetFieldByName("secret_name"));
				auto scope_col = std::dynamic_pointer_cast<arrow::StringArray>(struct_array->GetFieldByName("scope"));

				for (int64_t i = start; i < end; i++) {
					VgiSecretRequirement req;
					if (type_col && !type_col->IsNull(i)) {
						req.secret_type = type_col->GetString(i);
					}
					if (name_col && !name_col->IsNull(i)) {
						req.name = name_col->GetString(i);
					}
					if (scope_col && !scope_col->IsNull(i)) {
						req.scope = scope_col->GetString(i);
					}
					if (!req.secret_type.empty()) {
						info.required_secrets.push_back(std::move(req));
					}
				}
			}
		}
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
	return kv_pairs;
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
						result[req.secret_type] = ExtractSecretKeyValues(*kv_secret);
					}
				}
			}
		} else {
			// Scope-based lookup (unscoped if scope is empty)
			auto match = secret_manager.LookupSecret(transaction, req.scope, req.secret_type);
			if (match.HasMatch()) {
				auto *kv_secret = dynamic_cast<const KeyValueSecret *>(&match.GetSecret());
				if (kv_secret) {
					result[req.secret_type] = ExtractSecretKeyValues(*kv_secret);
				}
			}
		}
	}

	return result;
}

} // namespace vgi
} // namespace duckdb
