#define DUCKDB_EXTENSION_MAIN

#include "vgi_extension.hpp"
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
#include "vgi_protocol.hpp"
#include "vgi_table_function.hpp"

#define VGI_EXTENSION_VERSION "2026011201"

namespace duckdb {

// ATTACH handler for VGI catalogs
static unique_ptr<Catalog> VgiCatalogAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                            AttachedDatabase &db, const string &name, AttachInfo &info,
                                            AttachOptions &options) {
	string worker_path;
	string catalog_name = info.path; // The first argument to ATTACH is the catalog name
	bool worker_debug = false;

	// Extract options
	for (auto &entry : info.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "type") {
			continue;
		} else if (lower_name == "location" || lower_name == "path") {
			worker_path = entry.second.ToString();
		} else if (lower_name == "worker_debug") {
			worker_debug = entry.second.GetValue<bool>();
		} else {
			throw BinderException("Unrecognized option for VGI ATTACH: %s", entry.first);
		}
	}

	if (worker_path.empty()) {
		throw BinderException("VGI ATTACH requires LOCATION option specifying the worker path");
	}

	// Call catalog_attach via worker
	auto args = vgi::CreateCatalogAttachArgs(catalog_name);
	auto result_batch = vgi::InvokeCatalogMethod(worker_path, vgi::CatalogMethod::CatalogAttach, args, context, worker_debug);
	auto attach_result = vgi::ParseCatalogAttachResult(result_batch, worker_path);

	// Create attach parameters
	auto attach_params = std::make_shared<vgi::VgiAttachParameters>(worker_path, catalog_name, worker_debug);
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

	// Register VGI log type
	auto &log_manager = loader.GetDatabaseInstance().GetLogManager();
	log_manager.RegisterLogType(make_uniq<VgiLogType>());

	QueryFarmSendTelemetry(loader, "vgi", VGI_EXTENSION_VERSION);

	// Register VGI storage extension
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.storage_extensions["vgi"] = make_uniq<VgiStorageExtension>();

	// Register VGI table functions
	RegisterVgiCatalogsFunction(loader);
	RegisterVgiTableFunction(loader);
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
