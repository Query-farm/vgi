#define DUCKDB_EXTENSION_MAIN

#include "vgi_extension.hpp"

#include <cerrno>
#include <mutex>
#include <set>
#include <signal.h>

#include "duckdb.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "query_farm_telemetry.hpp"
#include "storage/vgi_catalog.hpp"
#include "storage/vgi_transaction.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_catalogs.hpp"
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
		} else {
			throw BinderException("Unrecognized option for VGI ATTACH: %s", entry.first);
		}
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

	// Seed OAuth refresh token if provided
	if (!oauth_refresh_token.empty()) {
		if (!vgi::IsHttpTransport(worker_path)) {
			throw BinderException("oauth_refresh_token is only valid for HTTP transport "
			                      "(LOCATION must be an HTTP/HTTPS URL)");
		}
		auto origin = vgi::VgiTokenManager::ExtractOrigin(worker_path);
		vgi::VgiTokenManager::Instance().SeedRefreshToken(origin, oauth_refresh_token);
	}

	// Call catalog_attach via RPC
	auto attach_result = vgi::InvokeCatalogAttach(worker_path, catalog_name, context, worker_debug, use_pool);

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

	// Create attach parameters
	auto attach_params = std::make_shared<vgi::VgiAttachParameters>(worker_path, catalog_name, worker_debug, use_pool);
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
	std::string target_origin; // empty = all origins
	bool logout_all = false;
};

struct OAuthLogoutState : public GlobalTableFunctionState {
	std::vector<std::string> logged_out_origins;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> OAuthLogoutBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<OAuthLogoutBindData>();
	if (input.inputs.size() > 1) {
		throw BinderException("vgi_oauth_logout expects 0 or 1 arguments, got %d", input.inputs.size());
	}
	if (!input.inputs.empty()) {
		bind_data->target_origin = input.inputs[0].GetValue<string>();
	} else {
		bind_data->logout_all = true;
	}
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("origin");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("status");
	return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> OAuthLogoutInit(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<OAuthLogoutState>();
	auto &bind_data = input.bind_data->Cast<OAuthLogoutBindData>();
	auto &token_mgr = vgi::VgiTokenManager::Instance();

	if (bind_data.logout_all) {
		auto origins = token_mgr.GetAllOrigins();
		// Also check persisted secrets for origins not in memory
		try {
			auto &secret_manager = SecretManager::Get(context);
			auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
			auto all_secrets = secret_manager.AllSecrets(transaction);
			for (auto &entry : all_secrets) {
				if (entry.secret->GetType() == "vgi_oauth_refresh_token") {
					auto &kv = dynamic_cast<const KeyValueSecret &>(*entry.secret);
					auto origin_val = kv.TryGetValue("origin");
					if (!origin_val.IsNull()) {
						auto o = origin_val.ToString();
						bool found = false;
						for (const auto &existing : origins) {
							if (existing == o) { found = true; break; }
						}
						if (!found) {
							origins.push_back(o);
						}
					}
				}
			}
		} catch (...) {}

		for (const auto &o : origins) {
			token_mgr.ClearTokens(context, o);
			state->logged_out_origins.push_back(o);
		}
	} else {
		token_mgr.ClearTokens(context, bind_data.target_origin);
		state->logged_out_origins.push_back(bind_data.target_origin);
	}

	return std::move(state);
}

static void OAuthLogoutFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<OAuthLogoutState>();
	idx_t count = 0;
	while (state.offset < state.logged_out_origins.size() && count < STANDARD_VECTOR_SIZE) {
		output.SetValue(0, count, Value(state.logged_out_origins[state.offset]));
		output.SetValue(1, count, Value("logged_out"));
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
		std::string origin;
		std::string status;
		bool has_expires;
		int64_t expires_in_seconds;
		bool has_refresh_token;
		std::string persisted;
		std::string scope;
	};
	std::vector<TokenRow> rows;
	idx_t offset = 0;
};

static LogicalType OAuthStatusEnumType() {
	Vector values(LogicalType::VARCHAR, 3);
	values.SetValue(0, Value("active"));
	values.SetValue(1, Value("expired"));
	values.SetValue(2, Value("persisted_only"));
	return LogicalType::ENUM("vgi_oauth_status", values, 3);
}

static LogicalType OAuthPersistedEnumType() {
	Vector values(LogicalType::VARCHAR, 3);
	values.SetValue(0, Value("persistent"));
	values.SetValue(1, Value("temporary"));
	values.SetValue(2, Value("none"));
	return LogicalType::ENUM("vgi_oauth_persisted", values, 3);
}

static unique_ptr<FunctionData> OAuthTokensBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("origin");
	return_types.push_back(OAuthStatusEnumType());
	names.push_back("status");
	return_types.push_back(LogicalType::INTERVAL);
	names.push_back("expires_in");
	return_types.push_back(LogicalType::BOOLEAN);
	names.push_back("has_refresh_token");
	return_types.push_back(OAuthPersistedEnumType());
	names.push_back("persisted");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("scope");
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> OAuthTokensInit(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<OAuthTokensState>();
	auto &token_mgr = vgi::VgiTokenManager::Instance();
	auto origins = token_mgr.GetAllOrigins();

	// Track which origins we've seen (for merging persisted-only entries)
	std::set<std::string> seen_origins;

	// Get in-memory token states
	for (const auto &origin : origins) {
		seen_origins.insert(origin);
		OAuthTokensState::TokenRow row;
		row.origin = origin;

		auto bearer = token_mgr.GetToken(origin);
		if (!bearer.empty()) {
			row.status = "active";
		} else {
			row.status = "expired";
		}

		// Get expiry and refresh token info from in-memory state
		vgi::VgiTokenManager::TokenInfo token_info;
		if (token_mgr.GetTokenInfo(origin, token_info)) {
			row.has_refresh_token = token_info.has_refresh_token;
			row.has_expires = token_info.has_expires;
			row.expires_in_seconds = token_info.expires_in_seconds;
		} else {
			row.has_refresh_token = false;
			row.has_expires = false;
			row.expires_in_seconds = 0;
		}
		row.persisted = "none";
		row.scope = "";

		// Check persisted secret for this origin
		try {
			auto &secret_manager = SecretManager::Get(context);
			auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
			auto secret_name = vgi::VgiTokenManager::SecretNameForOrigin(origin);
			auto entry = secret_manager.GetSecretByName(transaction, secret_name);
			if (entry) {
				auto &kv = dynamic_cast<const KeyValueSecret &>(*entry->secret);
				auto rt = kv.TryGetValue("refresh_token");
				row.has_refresh_token = !rt.IsNull() && !rt.ToString().empty();
				auto sc = kv.TryGetValue("scope");
				if (!sc.IsNull()) row.scope = sc.ToString();

				if (entry->persist_type == SecretPersistType::PERSISTENT) {
					row.persisted = "persistent";
				} else {
					row.persisted = "temporary";
				}
			}
		} catch (...) {}

		state->rows.push_back(std::move(row));
	}

	// Check for persisted-only entries (origins with secrets but no in-memory state)
	try {
		auto &secret_manager = SecretManager::Get(context);
		auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
		auto all_secrets = secret_manager.AllSecrets(transaction);
		for (auto &entry : all_secrets) {
			if (entry.secret->GetType() == "vgi_oauth_refresh_token") {
				auto &kv = dynamic_cast<const KeyValueSecret &>(*entry.secret);
				auto origin_val = kv.TryGetValue("origin");
				if (origin_val.IsNull()) continue;
				auto origin = origin_val.ToString();
				if (seen_origins.count(origin)) continue;

				OAuthTokensState::TokenRow row;
				row.origin = origin;
				row.status = "persisted_only";
				row.has_expires = false;
				row.expires_in_seconds = 0;

				auto rt = kv.TryGetValue("refresh_token");
				row.has_refresh_token = !rt.IsNull() && !rt.ToString().empty();
				auto sc = kv.TryGetValue("scope");
				row.scope = sc.IsNull() ? "" : sc.ToString();

				if (entry.persist_type == SecretPersistType::PERSISTENT) {
					row.persisted = "persistent";
				} else {
					row.persisted = "temporary";
				}
				state->rows.push_back(std::move(row));
			}
		}
	} catch (...) {}

	return std::move(state);
}

static void OAuthTokensFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<OAuthTokensState>();
	idx_t count = 0;
	while (state.offset < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = state.rows[state.offset];
		output.SetValue(0, count, Value(row.origin));
		output.SetValue(1, count, Value(row.status));
		if (row.status == "active" && row.has_expires) {
			output.SetValue(2, count, Value::INTERVAL(Interval::FromMicro(row.expires_in_seconds * Interval::MICROS_PER_SEC)));
		} else {
			output.SetValue(2, count, Value());
		}
		output.SetValue(3, count, Value::BOOLEAN(row.has_refresh_token));
		output.SetValue(4, count, Value(row.persisted));
		output.SetValue(5, count, Value(row.scope));
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
	config.AddExtensionOption("vgi_oauth_persist",
	                          "Persist OAuth refresh tokens to disk via DuckDB SecretManager",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));
	config.AddExtensionOption("vgi_oauth_prompt",
	                          "OAuth prompt behavior: none (default), login, select_account, or consent",
	                          LogicalType::VARCHAR, Value("none"));

	// Register vgi_oauth_refresh_token secret type
	{
		auto &secret_manager = SecretManager::Get(loader.GetDatabaseInstance());
		SecretType secret_type;
		secret_type.name = "vgi_oauth_refresh_token";
		secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
		secret_type.default_provider = "config";
		secret_type.extension = "vgi";
		try {
			secret_manager.RegisterSecretType(secret_type);
		} catch (const InternalException &) {
			// Already registered — idempotent
		}

		// Register create function
		CreateSecretFunction create_func;
		create_func.secret_type = "vgi_oauth_refresh_token";
		create_func.provider = "config";
		create_func.function = VgiCreateSecret;
		create_func.named_parameters["refresh_token"] = LogicalType::VARCHAR;
		create_func.named_parameters["token_endpoint"] = LogicalType::VARCHAR;
		create_func.named_parameters["client_id"] = LogicalType::VARCHAR;
		create_func.named_parameters["client_secret"] = LogicalType::VARCHAR;
		create_func.named_parameters["scope"] = LogicalType::VARCHAR;
		create_func.named_parameters["use_id_token"] = LogicalType::VARCHAR;
		create_func.named_parameters["resource_metadata_url"] = LogicalType::VARCHAR;
		create_func.named_parameters["origin"] = LogicalType::VARCHAR;
		secret_manager.RegisterSecretFunction(create_func, OnCreateConflict::IGNORE_ON_CONFLICT);

		// Store redact keys
		auto vgi_ext = StorageExtension::Find(config, "vgi");
		if (vgi_ext) {
			case_insensitive_set_t redact_keys;
			redact_keys.insert("refresh_token");
			redact_keys.insert("client_secret");
			static_cast<VgiStorageExtension &>(*vgi_ext).SetRedactKeys("vgi_oauth_refresh_token", std::move(redact_keys));
		}
	}

	// Register async prefetch setting
	config.AddExtensionOption("vgi_async_prefetch",
	                          "Enable async I/O prefetch for VGI table function scans",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));

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

	// Register OAuth diagnostic/management functions
	{
		// vgi_oauth_logout([origin])
		TableFunction logout_func("vgi_oauth_logout", {}, OAuthLogoutFunction, OAuthLogoutBind, OAuthLogoutInit);
		logout_func.varargs = LogicalType::VARCHAR;
		loader.RegisterFunction(logout_func);

		// vgi_oauth_tokens()
		TableFunction tokens_func("vgi_oauth_tokens", {}, OAuthTokensFunction, OAuthTokensBind, OAuthTokensInit);
		loader.RegisterFunction(tokens_func);
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
