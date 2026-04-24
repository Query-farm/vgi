#define DUCKDB_EXTENSION_MAIN

#include "vgi_extension.hpp"

#include <cerrno>
#include <mutex>
#include <set>
#include <signal.h>

#include "duckdb.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/common/arrow/arrow_type_extension.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "query_farm_telemetry.hpp"
#include "storage/vgi_catalog.hpp"
#include "storage/vgi_transaction.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_catalogs.hpp"
#include "vgi_cookie_jar.hpp"
#include "vgi_logging.hpp"
#include "vgi_oauth.hpp"
#include "vgi_profiling.hpp"
#ifndef __EMSCRIPTEN__
#include "vgi_subprocess.hpp"
#endif
#include "vgi_table_function.hpp"
#include "vgi_table_function_impl.hpp"
#include "vgi_transport.hpp"
#include "vgi_worker_pool.hpp"
#include "vgi_table_statistics_function.hpp"
#include "vgi_clear_cache.hpp"
#include "vgi_worker_pool_functions.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/table_function.hpp"

#define VGI_EXTENSION_VERSION "2026011201"

namespace duckdb {

// Forward declarations — functions defined below after VgiStorageExtension class
static unique_ptr<BaseSecret> VgiCreateSecret(ClientContext &context, CreateSecretInput &input);
static unique_ptr<Catalog> VgiCatalogAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                            AttachedDatabase &db, const string &name, AttachInfo &info,
                                            AttachOptions &options);
static unique_ptr<TransactionManager> CreateVgiTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                  AttachedDatabase &db, Catalog &catalog);

// ============================================================================
// VgiJoinOptimizer — auto-raise InFilter threshold for queries with VGI scans
// ============================================================================
// When DuckDB joins a local table against a VGI remote table, the hash join
// can push an InFilter containing the build-side's distinct key values to the
// probe-side scan. DuckDB's default threshold (dynamic_or_filter_threshold=50)
// is too low for remote scans where network savings justify sending more keys.
// This optimizer raises the threshold to vgi_join_keys_limit when a VGI scan
// is detected in the plan.

class VgiJoinOptimizer : public OptimizerExtension {
public:
	VgiJoinOptimizer() {
		pre_optimize_function = Optimize;
	}

private:
	static bool IsVgiScan(LogicalOperator &op) {
		if (op.type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = op.Cast<LogicalGet>();
			return get.function.function == vgi::VgiTableFunctionScan;
		}
		return false;
	}

	static bool SubtreeContainsVgiScan(LogicalOperator &op) {
		if (IsVgiScan(op)) {
			return true;
		}
		for (auto &child : op.children) {
			if (SubtreeContainsVgiScan(*child)) {
				return true;
			}
		}
		return false;
	}

	//! Check if the plan has a comparison join where any child subtree contains a VGI scan.
	//! Only raises the threshold when there's an actual join involving VGI — a plain
	//! SELECT * FROM vgi_table doesn't trigger it.
	static bool HasJoinWithVgiScan(LogicalOperator &op) {
		if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
		    op.type == LogicalOperatorType::LOGICAL_ANY_JOIN) {
			for (auto &child : op.children) {
				if (SubtreeContainsVgiScan(*child)) {
					return true;
				}
			}
		}
		for (auto &child : op.children) {
			if (HasJoinWithVgiScan(*child)) {
				return true;
			}
		}
		return false;
	}

	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
		if (!HasJoinWithVgiScan(*plan)) {
			return;
		}

		Value limit_val;
		if (!input.context.TryGetCurrentSetting("vgi_join_keys_limit", limit_val) || limit_val.IsNull()) {
			return;
		}
		auto vgi_limit = limit_val.GetValue<idx_t>();
		if (vgi_limit == 0) {
			return; // disabled
		}

		// Only raise, never lower a user-set threshold
		auto current = Settings::Get<DynamicOrFilterThresholdSetting>(input.context);
		if (current < vgi_limit) {
			auto &client_config = ClientConfig::GetConfig(input.context);
			client_config.user_settings.SetUserSetting(DynamicOrFilterThresholdSetting::SettingIndex,
			                                           Value::UBIGINT(vgi_limit));
		}
	}
};

// Storage extension for VGI — also holds per-secret-type redact keys
class VgiStorageExtension : public StorageExtension {
public:
	VgiStorageExtension() {
		attach = VgiCatalogAttach;
		create_transaction_manager = CreateVgiTransactionManager;
	}

	// Thread-safe registry of redact keys per secret type name.
	// Populated during ATTACH after successful secret type registration.
	void SetRedactKeys(const std::string &secret_type_name, case_insensitive_set_t keys) {
		std::lock_guard<std::mutex> lock(redact_mutex_);
		redact_keys_[secret_type_name] = std::move(keys);
	}

	case_insensitive_set_t GetRedactKeys(const std::string &secret_type_name) const {
		std::lock_guard<std::mutex> lock(redact_mutex_);
		auto it = redact_keys_.find(secret_type_name);
		if (it != redact_keys_.end()) {
			return it->second;
		}
		return {};
	}

private:
	mutable std::mutex redact_mutex_;
	std::unordered_map<std::string, case_insensitive_set_t> redact_keys_;
};

// Create function for VGI secrets — builds a KeyValueSecret from config options.
// All named parameters from CREATE SECRET are copied into the secret's key-value map.
static unique_ptr<BaseSecret> VgiCreateSecret(ClientContext &context, CreateSecretInput &input) {
	auto scope = input.scope;
	auto secret = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);

	// Look up redact keys from VgiStorageExtension (per-DatabaseInstance, not global)
	auto &config = DBConfig::GetConfig(context);
	auto vgi_ext = StorageExtension::Find(config, "vgi");
	if (vgi_ext) {
		auto &vgi_storage = static_cast<VgiStorageExtension &>(*vgi_ext);
		secret->redact_keys = vgi_storage.GetRedactKeys(input.type);
	}

	// Copy all user-provided options into the secret map
	for (const auto &entry : input.options) {
		secret->secret_map[entry.first] = entry.second;
	}

	return secret;
}

// ATTACH handler for VGI catalogs
static unique_ptr<Catalog> VgiCatalogAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                            AttachedDatabase &db, const string &name, AttachInfo &info,
                                            AttachOptions &options) {
	string worker_path;
	string catalog_name = info.path; // The first argument to ATTACH is the catalog name
	bool worker_debug = false;
	bool use_pool = true;
	int64_t pool_max_override = -1;      // -1 = not set
	int64_t pool_timeout_override = -1;  // -1 = not set
	string oauth_refresh_token;
	string bearer_token;
	string data_version_spec;
	string implementation_version;

	// Extract options
	for (auto &entry : info.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "type") {
			continue;
		} else if (lower_name == "location" || lower_name == "path") {
			worker_path = entry.second.ToString();
		} else if (lower_name == "worker_debug") {
			worker_debug = entry.second.GetValue<bool>();
		} else if (lower_name == "pool") {
			use_pool = entry.second.GetValue<bool>();
		} else if (lower_name == "pool_max") {
			pool_max_override = entry.second.GetValue<int64_t>();
		} else if (lower_name == "pool_timeout") {
			pool_timeout_override = entry.second.GetValue<int64_t>();
		} else if (lower_name == "oauth_refresh_token") {
			oauth_refresh_token = entry.second.ToString();
		} else if (lower_name == "bearer_token") {
			bearer_token = entry.second.ToString();
		} else if (lower_name == "data_version_spec") {
			data_version_spec = entry.second.ToString();
		} else if (lower_name == "implementation_version") {
			implementation_version = entry.second.ToString();
		} else {
			throw BinderException("Unrecognized option for VGI ATTACH: %s", entry.first);
		}
	}

	// Validate mutual exclusivity of auth options
	if (!bearer_token.empty() && !oauth_refresh_token.empty()) {
		throw BinderException("Cannot specify both bearer_token and oauth_refresh_token");
	}

	if (worker_path.empty()) {
		throw BinderException("VGI ATTACH requires LOCATION option specifying the worker path");
	}

#ifdef __EMSCRIPTEN__
	// WASM: only HTTP transport is supported (no subprocess/fork)
	if (!vgi::IsHttpTransport(worker_path)) {
		throw BinderException("VGI in WASM only supports HTTP transport. "
		                      "LOCATION must be an HTTP/HTTPS URL.");
	}
	use_pool = false;
	// HTTP in WASM goes through duckdb-wasm's XHR layer, not httpfs
#else
	// HTTP transport: require httpfs for POST support, disable subprocess pooling
	if (vgi::IsHttpTransport(worker_path)) {
		try {
			ExtensionHelper::TryAutoLoadExtension(db.GetDatabase(), "httpfs");
		} catch (...) {
			// ignore auto-load errors, check below
		}
		if (!db.GetDatabase().ExtensionIsLoaded("httpfs")) {
			throw BinderException("VGI HTTP transport requires the httpfs extension. "
			                      "Install it with: INSTALL httpfs; LOAD httpfs;");
		}
		use_pool = false; // HTTP is stateless, no subprocess pooling
	}
#endif

	// Create per-catalog auth state
	std::shared_ptr<vgi::CatalogAuth> auth;
	if (!bearer_token.empty()) {
		if (!vgi::IsHttpTransport(worker_path)) {
			throw BinderException("bearer_token is only valid for HTTP transport "
			                      "(LOCATION must be an HTTP/HTTPS URL)");
		}
		auth = std::make_shared<vgi::BearerTokenCatalogAuth>(bearer_token);
	} else {
		auto oauth_auth = std::make_shared<vgi::OAuthCatalogAuth>();
		if (!oauth_refresh_token.empty()) {
			if (!vgi::IsHttpTransport(worker_path)) {
				throw BinderException("oauth_refresh_token is only valid for HTTP transport "
				                      "(LOCATION must be an HTTP/HTTPS URL)");
			}
			oauth_auth->SeedRefreshToken(oauth_refresh_token);
		}
		auth = std::move(oauth_auth);
	}

	// HTTP cookie jar — carries proxy-issued Set-Cookie / Cookie headers for
	// sticky version-aware routing. Subprocess transport doesn't use this.
	std::shared_ptr<vgi::SessionCookieJar> cookie_jar;
	if (vgi::IsHttpTransport(worker_path)) {
		cookie_jar = std::make_shared<vgi::SessionCookieJar>();
	}

	// Call catalog_attach via RPC. The worker validates data_version_spec and
	// implementation_version and throws with a human-readable message on
	// unsatisfiable requests; that surfaces as the ATTACH failure.
	auto attach_result = vgi::InvokeCatalogAttach(worker_path, catalog_name, context, worker_debug, use_pool, auth,
	                                              data_version_spec, implementation_version, cookie_jar);

	// Register extension options for settings exposed by this catalog
	// Check for type conflicts with existing settings
	auto &config = DBConfig::GetConfig(db.GetDatabase());

	for (const auto &setting : attach_result.settings) {
		if (config.HasExtensionOption(setting.name)) {
			// Setting already exists - verify types match
			ExtensionOption existing_option;
			if (!config.TryGetExtensionOption(setting.name, existing_option)) {
				throw BinderException("Failed to retrieve existing VGI setting '%s'", setting.name);
			}
			if (existing_option.type != setting.type) {
				throw BinderException("VGI setting '%s' already exists with type %s, but catalog '%s' requires type %s",
				                      setting.name, existing_option.type.ToString(), catalog_name,
				                      setting.type.ToString());
			}
			// Types match - setting is already registered, no need to add again
		} else {
			// New setting - register it
			config.AddExtensionOption(setting.name, setting.description, setting.type, setting.default_value);
		}
	}

	// Register secret types exposed by this catalog
	auto &secret_manager = SecretManager::Get(context);
	auto vgi_ext = StorageExtension::Find(config, "vgi");
	for (const auto &st : attach_result.secret_types) {
		// Build redact keys set from parameter metadata
		case_insensitive_set_t redact_set;
		for (const auto &param : st.parameters) {
			if (param.redact) {
				redact_set.insert(param.name);
			}
		}

		// Register the secret type with DuckDB
		SecretType secret_type;
		secret_type.name = st.name;
		secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
		secret_type.default_provider = "config";
		secret_type.extension = "vgi";
		try {
			secret_manager.RegisterSecretType(secret_type);
		} catch (const InternalException &) {
			// Already registered by another VGI catalog — skip
			// Don't overwrite redact keys from the first (winning) registration
			redact_set = {};
		}

		// Store redact keys on VgiStorageExtension (only for newly registered types)
		if (vgi_ext && !redact_set.empty()) {
			static_cast<VgiStorageExtension &>(*vgi_ext).SetRedactKeys(st.name, std::move(redact_set));
		}

		// Register "config" provider create function with named parameters
		CreateSecretFunction create_func;
		create_func.secret_type = st.name;
		create_func.provider = "config";
		create_func.function = VgiCreateSecret;
		for (const auto &param : st.parameters) {
			create_func.named_parameters[param.name] = param.type;
		}
		secret_manager.RegisterSecretFunction(create_func, OnCreateConflict::IGNORE_ON_CONFLICT);
	}

	// Build per-path pool config and register with pool
	vgi::PoolSettings pool_settings;
	if (use_pool) {
		// Default per-path limit from vgi_worker_pool_max setting
		Value max_val;
		if (context.TryGetCurrentSetting("vgi_worker_pool_max", max_val)) {
			pool_settings.max_pool_size = static_cast<size_t>(max_val.GetValue<int64_t>());
		}
		Value idle_val;
		if (context.TryGetCurrentSetting("vgi_worker_pool_idle_limit_seconds", idle_val)) {
			pool_settings.idle_timeout_seconds = static_cast<size_t>(idle_val.GetValue<int64_t>());
		}
		// ATTACH options override the defaults
		if (pool_max_override >= 0) {
			pool_settings.max_pool_size = static_cast<size_t>(pool_max_override);
		}
		if (pool_timeout_override >= 0) {
			pool_settings.idle_timeout_seconds = static_cast<size_t>(pool_timeout_override);
		}
	} else {
		pool_settings.max_pool_size = 0; // disabled
	}
	vgi::VgiWorkerPool::Instance().ConfigurePath(worker_path, pool_settings);

	// Set database-level comment and tags from worker metadata
	if (!attach_result.comment.empty()) {
		db.comment = Value(attach_result.comment);
	}
	for (auto &[key, val] : attach_result.tags) {
		db.tags[key] = val;
	}

	// Surface resolved versions via duckdb_databases().tags so users can verify
	// what the worker actually picked. Namespaced with vgi_ to avoid clashing
	// with any worker-declared tags.
	if (!attach_result.resolved_data_version.empty()) {
		db.tags["vgi_resolved_data_version"] = attach_result.resolved_data_version;
	}
	if (!attach_result.resolved_implementation_version.empty()) {
		db.tags["vgi_resolved_implementation_version"] = attach_result.resolved_implementation_version;
	}

	// Create attach parameters. Resolved versions are what the worker picked;
	// they're used by the pool key so catalogs attached at different versions
	// never share a subprocess worker.
	auto attach_params = std::make_shared<vgi::VgiAttachParameters>(
	    worker_path, catalog_name, worker_debug, use_pool, auth, attach_result.resolved_data_version,
	    attach_result.resolved_implementation_version, cookie_jar);

	// Prime the HTTPParams cache while we're outside any VGI catalog
	// transaction. HTTPFSUtil::InitializeParameters pulls settings/secrets
	// through KeyValueSecretReader, which takes the MetaTransaction mutex —
	// doing it lazily from inside VgiTransaction::Start would self-deadlock
	// (the surrounding MetaTransaction::GetTransaction call already holds that
	// mutex). Doing it here, during ATTACH handling and before any VGI
	// transaction exists, avoids the reentrancy. TODO(#22258): remove once
	// https://github.com/duckdb/duckdb/issues/22258 is fixed.
	if (vgi::IsHttpTransport(worker_path)) {
		attach_params->GetOrInitHttpParams(context, worker_path);
	}

	auto attach_result_ptr = std::make_shared<vgi::CatalogAttachResult>(std::move(attach_result));

	return make_uniq<VgiCatalog>(db, name, options.access_mode, std::move(attach_params), std::move(attach_result_ptr));
}

// Create transaction manager for VGI catalog
static unique_ptr<TransactionManager> CreateVgiTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                  AttachedDatabase &db, Catalog &catalog) {
	auto &vgi_catalog = catalog.Cast<VgiCatalog>();
	return make_uniq<VgiTransactionManager>(db, vgi_catalog);
}

//===--------------------------------------------------------------------===//
// vgi_oauth_logout() table function
//===--------------------------------------------------------------------===//

struct OAuthLogoutBindData : public TableFunctionData {
	std::string target_catalog; // empty = all catalogs
	bool logout_all = false;
};

struct OAuthLogoutState : public GlobalTableFunctionState {
	struct LogoutRow {
		std::string catalog_name;
		std::string status;
	};
	std::vector<LogoutRow> rows;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> OAuthLogoutBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<OAuthLogoutBindData>();
	if (input.inputs.size() > 1) {
		throw BinderException("vgi_oauth_logout expects 0 or 1 arguments (catalog name), got %d", input.inputs.size());
	}
	if (!input.inputs.empty()) {
		bind_data->target_catalog = input.inputs[0].GetValue<string>();
	} else {
		bind_data->logout_all = true;
	}
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("catalog_name");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("status");
	return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> OAuthLogoutInit(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<OAuthLogoutState>();
	auto &bind_data = input.bind_data->Cast<OAuthLogoutBindData>();

	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db : databases) {
		auto &catalog = db->GetCatalog();
		if (catalog.GetCatalogType() != "vgi") {
			continue;
		}
		auto catalog_name = catalog.GetName();
		if (!bind_data.logout_all && catalog_name != bind_data.target_catalog) {
			continue;
		}

		auto &vgi_catalog = catalog.Cast<VgiCatalog>();
		const auto &params = vgi_catalog.attach_parameters();
		if (params && params->auth()) {
			params->auth()->ClearTokens();
			state->rows.push_back({catalog_name, "logged_out"});
		}
	}

	return std::move(state);
}

static void OAuthLogoutFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<OAuthLogoutState>();
	idx_t count = 0;
	while (state.offset < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = state.rows[state.offset];
		output.SetValue(0, count, Value(row.catalog_name));
		output.SetValue(1, count, Value(row.status));
		state.offset++;
		count++;
	}
	output.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// vgi_oauth_tokens() table function
//===--------------------------------------------------------------------===//

struct OAuthTokensState : public GlobalTableFunctionState {
	struct TokenRow {
		std::string catalog_name;
		std::string origin;
		std::string status;
		bool has_expires;
		int64_t expires_in_seconds;
		bool has_refresh_token;
	};
	std::vector<TokenRow> rows;
	idx_t offset = 0;
};

static LogicalType OAuthStatusEnumType() {
	Vector values(LogicalType::VARCHAR, 3);
	values.SetValue(0, Value("active"));
	values.SetValue(1, Value("expired"));
	values.SetValue(2, Value("none"));
	return LogicalType::ENUM("vgi_oauth_status", values, 3);
}

static unique_ptr<FunctionData> OAuthTokensBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("catalog_name");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("origin");
	return_types.push_back(OAuthStatusEnumType());
	names.push_back("status");
	return_types.push_back(LogicalType::INTERVAL);
	names.push_back("expires_in");
	return_types.push_back(LogicalType::BOOLEAN);
	names.push_back("has_refresh_token");
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> OAuthTokensInit(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<OAuthTokensState>();

	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db : databases) {
		auto &catalog = db->GetCatalog();
		if (catalog.GetCatalogType() != "vgi") {
			continue;
		}
		auto &vgi_catalog = catalog.Cast<VgiCatalog>();
		const auto &params = vgi_catalog.attach_parameters();
		if (!params) {
			continue;
		}

		OAuthTokensState::TokenRow row;
		row.catalog_name = catalog.GetName();
		row.origin = vgi::ExtractOrigin(params->worker_path());

		auto &auth = params->auth();
		if (!auth) {
			row.status = "none";
			row.has_expires = false;
			row.expires_in_seconds = 0;
			row.has_refresh_token = false;
		} else {
			auto bearer = auth->GetToken();
			if (!bearer.empty()) {
				row.status = "active";
			} else {
				row.status = "expired";
			}

			vgi::CatalogAuth::TokenInfo token_info;
			if (auth->GetTokenInfo(token_info)) {
				row.has_refresh_token = token_info.has_refresh_token;
				row.has_expires = token_info.has_expires;
				row.expires_in_seconds = token_info.expires_in_seconds;
			} else {
				row.has_refresh_token = false;
				row.has_expires = false;
				row.expires_in_seconds = 0;
			}
		}

		state->rows.push_back(std::move(row));
	}

	return std::move(state);
}

static void OAuthTokensFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<OAuthTokensState>();
	idx_t count = 0;
	while (state.offset < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = state.rows[state.offset];
		output.SetValue(0, count, Value(row.catalog_name));
		output.SetValue(1, count, Value(row.origin));
		output.SetValue(2, count, Value(row.status));
		if (row.status == "active" && row.has_expires) {
			output.SetValue(3, count, Value::INTERVAL(Interval::FromMicro(row.expires_in_seconds * Interval::MICROS_PER_SEC)));
		} else {
			output.SetValue(3, count, Value());
		}
		output.SetValue(4, count, Value::BOOLEAN(row.has_refresh_token));
		state.offset++;
		count++;
	}
	output.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// vgi_catalog_identity() table function
//===--------------------------------------------------------------------===//
//
// Emits one row per attached VGI catalog showing the OIDC identity parsed
// from the OAuth id_token (if any). Catalogs without OAuth state appear
// with authenticated=false and NULL identity fields.

struct CatalogIdentityState : public GlobalTableFunctionState {
	struct IdentityRow {
		std::string catalog_name;
		std::string origin;
		bool authenticated = false;
		bool has_identity = false;
		std::string sub;
		std::string email;
		std::string name;
		std::string issuer;
		std::string claims_json;  // empty when no id_token was parsed
	};
	std::vector<IdentityRow> rows;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> CatalogIdentityBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("catalog_name");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("origin");
	return_types.push_back(LogicalType::BOOLEAN);
	names.push_back("authenticated");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("sub");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("email");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("name");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("issuer");
	// Raw decoded JWT payload — provider-specific claims (Entra preferred_username/oid/tid,
	// group/role arrays, custom attributes) reachable via JSON path expressions.
	return_types.push_back(LogicalType::JSON());
	names.push_back("claims");
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> CatalogIdentityInit(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<CatalogIdentityState>();

	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db : databases) {
		auto &catalog = db->GetCatalog();
		if (catalog.GetCatalogType() != "vgi") {
			continue;
		}
		auto &vgi_catalog = catalog.Cast<VgiCatalog>();
		const auto &params = vgi_catalog.attach_parameters();
		if (!params) {
			continue;
		}

		CatalogIdentityState::IdentityRow row;
		row.catalog_name = catalog.GetName();
		row.origin = vgi::ExtractOrigin(params->worker_path());

		// Get identity from per-catalog auth state
		auto &auth = params->auth();
		if (auth) {
			vgi::OAuthTokenSet token_copy;
			if (auth->GetTokenSetCopy(token_copy)) {
				row.authenticated = token_copy.IsValid();
				if (token_copy.identity.present) {
					row.has_identity = true;
					row.sub = token_copy.identity.sub;
					row.email = token_copy.identity.email;
					row.name = token_copy.identity.name;
					row.issuer = token_copy.identity.issuer;
					row.claims_json = token_copy.identity.claims_json;
				}
			}
		}

		state->rows.push_back(std::move(row));
	}

	return std::move(state);
}

static void CatalogIdentityFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<CatalogIdentityState>();
	idx_t count = 0;
	while (state.offset < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = state.rows[state.offset];
		output.SetValue(0, count, Value(row.catalog_name));
		output.SetValue(1, count, Value(row.origin));
		output.SetValue(2, count, Value::BOOLEAN(row.authenticated));
		output.SetValue(3, count, row.has_identity && !row.sub.empty() ? Value(row.sub) : Value());
		output.SetValue(4, count, row.has_identity && !row.email.empty() ? Value(row.email) : Value());
		output.SetValue(5, count, row.has_identity && !row.name.empty() ? Value(row.name) : Value());
		output.SetValue(6, count, row.has_identity && !row.issuer.empty() ? Value(row.issuer) : Value());
		// The claims column is typed JSON — DuckDB stores JSON as VARCHAR with an alias,
		// so a plain Value(string) lands correctly and returns as JSON on read.
		output.SetValue(7, count,
		                row.has_identity && !row.claims_json.empty() ? Value(row.claims_json) : Value());
		state.offset++;
		count++;
	}
	output.SetCardinality(count);
}

static void LoadInternal(ExtensionLoader &loader) {
	// Ignore SIGPIPE - we handle broken pipes via EPIPE error from write()
	// This prevents the process from being killed when a worker dies unexpectedly
	struct sigaction sa;
	sa.sa_handler = SIG_IGN;
	sigemptyset(&sa.sa_mask);
	sa.sa_flags = 0;
	if (sigaction(SIGPIPE, &sa, nullptr) == -1) {
		VGI_STDERR_DEBUG("[VGI] extension.load sigpipe_ignore_failed errno=%d\n", errno);
	}

	// Register profiling atexit handler if VGI_PROFILE is enabled
	vgi::VgiProfileRegisterAtExit();

	// Register VGI log type
	auto &log_manager = loader.GetDatabaseInstance().GetLogManager();
	log_manager.RegisterLogType(make_uniq<VgiLogType>());

	QueryFarmSendTelemetry(loader, "vgi", VGI_EXTENSION_VERSION);

	// Register VGI storage extension
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	StorageExtension::Register(config, "vgi", make_shared_ptr<VgiStorageExtension>());

	// -------------------------------------------------------------------------
	// Register native GeoArrow Arrow extension types
	// -------------------------------------------------------------------------
	// Enable round-tripping GEOMETRY through Arrow IPC as native struct/list
	// layouts instead of opaque WKB blobs.  Each entry maps a GeoArrow
	// extension name to a DuckDB struct type and a GeometryStorageType used
	// by the spatial extension's Geometry::ToVectorizedFormat /
	// FromVectorizedFormat for conversion.
	//
	// ArrowToDuck (worker → client): struct vector → GEOMETRY via cast
	// DuckToArrow (client → worker): GEOMETRY → struct vector via ToVectorizedFormat
	{
		auto xy = LogicalType::STRUCT({{"x", LogicalType::DOUBLE}, {"y", LogicalType::DOUBLE}});

		struct NativeGeoArrow {
			static void ArrowToDuck(ClientContext &context, Vector &source, Vector &result, idx_t count) {
				auto &cast_set = CastFunctionSet::Get(context);
				GetCastFunctionInput cast_input(context);
				auto info = cast_set.GetCastFunction(source.GetType(), result.GetType(), cast_input);
				CastParameters params(info.cast_data.get(), false, nullptr, nullptr);
				info.function(source, result, count, params);
			}
		};

		// DuckToArrow closures per geometry storage type
		// Each lambda captures the storage type for ToVectorizedFormat
#define VGI_GEOARROW_DUCK_TO_ARROW(STORAGE_TYPE)                                                                       \
	[](ClientContext &, Vector &source, Vector &result, idx_t count) {                                                 \
		Geometry::ToVectorizedFormat(source, result, count, STORAGE_TYPE);                                              \
	}

		struct GeoArrowEntry {
			const char *name;
			LogicalType arrow_type;
			cast_duck_arrow_t duck_to_arrow;
		};

		GeoArrowEntry entries[] = {
		    {"geoarrow.point", xy,
		     VGI_GEOARROW_DUCK_TO_ARROW(GeometryStorageType::POINT_XY)},
		    {"geoarrow.linestring", LogicalType::LIST(xy),
		     VGI_GEOARROW_DUCK_TO_ARROW(GeometryStorageType::LINESTRING_XY)},
		    {"geoarrow.polygon", LogicalType::LIST(LogicalType::LIST(xy)),
		     VGI_GEOARROW_DUCK_TO_ARROW(GeometryStorageType::POLYGON_XY)},
		    {"geoarrow.multipoint", LogicalType::LIST(xy),
		     VGI_GEOARROW_DUCK_TO_ARROW(GeometryStorageType::MULTIPOINT_XY)},
		    {"geoarrow.multilinestring", LogicalType::LIST(LogicalType::LIST(xy)),
		     VGI_GEOARROW_DUCK_TO_ARROW(GeometryStorageType::MULTILINESTRING_XY)},
		    {"geoarrow.multipolygon", LogicalType::LIST(LogicalType::LIST(LogicalType::LIST(xy))),
		     VGI_GEOARROW_DUCK_TO_ARROW(GeometryStorageType::MULTIPOLYGON_XY)},
		};

#undef VGI_GEOARROW_DUCK_TO_ARROW

		for (auto &entry : entries) {
			try {
				config.RegisterArrowExtension(
				    {entry.name, nullptr, nullptr,
				     make_shared_ptr<ArrowTypeExtensionData>(LogicalType::GEOMETRY(), entry.arrow_type,
				                                             NativeGeoArrow::ArrowToDuck, entry.duck_to_arrow)});
			} catch (...) {
				// Extension may already be registered (e.g., by spatial extension in a future version)
			}
		}
	}

	// Register catalog timeout setting (used for subprocess transport)
	config.AddExtensionOption("vgi_catalog_timeout_seconds",
	                          "Timeout in seconds for VGI subprocess catalog operations (list schemas, functions, etc.)",
	                          LogicalType::BIGINT, Value::BIGINT(vgi::CATALOG_OPERATION_TIMEOUT_SECONDS));

	// Register HTTP timeout setting
	config.AddExtensionOption("vgi_http_timeout_seconds",
	                          "Timeout in seconds for VGI HTTP requests (catalog, init, and exchange operations)",
	                          LogicalType::BIGINT, Value::BIGINT(300));

	// Register OAuth settings
	config.AddExtensionOption("vgi_oauth_timeout_seconds",
	                          "Timeout in seconds for OAuth browser authentication flow",
	                          LogicalType::BIGINT, Value::BIGINT(120));
	config.AddExtensionOption("vgi_oauth_enabled",
	                          "Enable OAuth PKCE authentication on HTTP 401 (set false to fail fast)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));
	config.AddExtensionOption("vgi_oauth_flow",
	                          "OAuth flow type: auto (default), device_code, or pkce",
	                          LogicalType::VARCHAR, Value("auto"));
	config.AddExtensionOption("vgi_oauth_prompt",
	                          "OAuth prompt behavior: none (default), login, select_account, or consent",
	                          LogicalType::VARCHAR, Value("none"));

	// Register async prefetch setting. Default is off: async prefetch returns
	// SourceResultType::BLOCKED from table-scan sources, which DuckDB's
	// PositionalTableScanner operator does not handle (it throws
	// NotImplementedException). Other join forms (CROSS / NATURAL / ASOF /
	// LATERAL / INNER / HASH) route through PipelineExecutor and suspend
	// correctly on BLOCKED, so users running queries without POSITIONAL JOIN
	// can opt in via ``SET vgi_async_prefetch=true;`` to reclaim the
	// subprocess-RPC latency hiding.
	config.AddExtensionOption("vgi_async_prefetch",
	                          "Enable async I/O prefetch for VGI table function scans (default off: "
	                          "DuckDB's POSITIONAL JOIN operator does not handle BLOCKED sources)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));

	// Register join key pushdown settings + optimizer
	config.AddExtensionOption("vgi_join_keys_limit",
	                          "Max distinct join key values to push to VGI workers (0 = disabled)",
	                          LogicalType::UBIGINT, Value::UBIGINT(100000));
	config.AddExtensionOption("vgi_join_keys_max_bytes",
	                          "Max estimated byte size for join keys batch (skip pushdown if exceeded)",
	                          LogicalType::UBIGINT, Value::UBIGINT(67108864)); // 64MB
	OptimizerExtension::Register(config, VgiJoinOptimizer());

	// Register worker pool settings
	config.AddExtensionOption("vgi_worker_pool_idle_limit_seconds",
	                          "Maximum idle time in seconds before pooled workers are removed", LogicalType::BIGINT,
	                          Value::BIGINT(5));
	config.AddExtensionOption("vgi_worker_pool_max", "Default per-path pool limit for VGI workers (0 = disabled)",
	                          LogicalType::BIGINT, Value::BIGINT(256));

	// Set default pool settings for paths without explicit per-path config
	// (e.g., direct vgi_table_function() calls that don't go through ATTACH)
	vgi::VgiWorkerPool::Instance().SetDefaultSettings({256, 5});

	// Register VGI table functions
	RegisterVgiCatalogsFunction(loader);
	RegisterVgiTableFunction(loader);

	// Register worker pool diagnostic functions
	vgi::RegisterVgiWorkerPoolFunction(loader);
	vgi::RegisterVgiWorkerPoolStatsFunction(loader);
	vgi::RegisterVgiWorkerPoolFlushFunction(loader);

	// Register table statistics diagnostic function
	vgi::RegisterVgiTableStatisticsFunction(loader);

	// Register cache management function
	vgi::RegisterVgiClearCacheFunction(loader);

	// Register OAuth diagnostic/management functions
	{
		// vgi_oauth_logout([origin])
		TableFunction logout_func("vgi_oauth_logout", {}, OAuthLogoutFunction, OAuthLogoutBind, OAuthLogoutInit);
		logout_func.varargs = LogicalType::VARCHAR;
		loader.RegisterFunction(logout_func);

		// vgi_oauth_tokens()
		TableFunction tokens_func("vgi_oauth_tokens", {}, OAuthTokensFunction, OAuthTokensBind, OAuthTokensInit);
		loader.RegisterFunction(tokens_func);

		// vgi_catalog_identity() — surfaces OIDC identity claims per attached VGI catalog
		TableFunction identity_func("vgi_catalog_identity", {}, CatalogIdentityFunction, CatalogIdentityBind,
		                            CatalogIdentityInit);
		loader.RegisterFunction(identity_func);
	}
}

void VgiExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string VgiExtension::Name() {
	return "vgi";
}

std::string VgiExtension::Version() const {
	return VGI_EXTENSION_VERSION;
}
} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(vgi, loader) {
	duckdb::LoadInternal(loader);
}
}
