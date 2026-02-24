#define DUCKDB_EXTENSION_MAIN

#include "vgi_extension.hpp"

#include <cerrno>
#include <signal.h>

#include "duckdb.hpp"
#include "duckdb/main/config.hpp"
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
#include "vgi_worker_pool.hpp"
#include "vgi_worker_pool_functions.hpp"

#define VGI_EXTENSION_VERSION "2026011201"

namespace duckdb {

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

// Storage extension for VGI
class VgiStorageExtension : public StorageExtension {
public:
	VgiStorageExtension() {
		attach = VgiCatalogAttach;
		create_transaction_manager = CreateVgiTransactionManager;
	}
};

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

	// Register catalog timeout setting
	config.AddExtensionOption("vgi_catalog_timeout_seconds",
	                          "Timeout in seconds for VGI catalog operations (list schemas, functions, etc.)",
	                          LogicalType::BIGINT, Value::BIGINT(vgi::CATALOG_OPERATION_TIMEOUT_SECONDS));

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
