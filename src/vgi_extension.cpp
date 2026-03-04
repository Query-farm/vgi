#define DUCKDB_EXTENSION_MAIN

#include "vgi_extension.hpp"

#include <cerrno>
#include <mutex>
#include <signal.h>

#include "duckdb.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "query_farm_telemetry.hpp"
#include "storage/vgi_catalog.hpp"
#include "storage/vgi_transaction.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_catalogs.hpp"
#include "vgi_logging.hpp"
#include "vgi_profiling.hpp"
#include "vgi_subprocess.hpp"
#include "vgi_table_function.hpp"
#include "vgi_transport.hpp"
#include "vgi_worker_pool.hpp"
#include "vgi_worker_pool_functions.hpp"

#define VGI_EXTENSION_VERSION "2026011201"

namespace duckdb {

// Forward declarations — functions defined below after VgiStorageExtension class
static unique_ptr<BaseSecret> VgiCreateSecret(ClientContext &context, CreateSecretInput &input);
static unique_ptr<Catalog> VgiCatalogAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                            AttachedDatabase &db, const string &name, AttachInfo &info,
                                            AttachOptions &options);
static unique_ptr<TransactionManager> CreateVgiTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                  AttachedDatabase &db, Catalog &catalog);

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
		} else {
			throw BinderException("Unrecognized option for VGI ATTACH: %s", entry.first);
		}
	}

	if (worker_path.empty()) {
		throw BinderException("VGI ATTACH requires LOCATION option specifying the worker path");
	}

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
