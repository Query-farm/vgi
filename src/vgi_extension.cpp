#define DUCKDB_EXTENSION_MAIN

#include "vgi_extension.hpp"
#include "duckdb.hpp"
#include "query_farm_telemetry.hpp"
#include "vgi_catalogs.hpp"
#include "vgi_logging.hpp"

#define VGI_EXTENSION_VERSION "2026011201"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {

	// Register VGI log type
	auto &log_manager = loader.GetDatabaseInstance().GetLogManager();
	log_manager.RegisterLogType(make_uniq<VgiLogType>());

	QueryFarmSendTelemetry(loader, "vgi", VGI_EXTENSION_VERSION);

	// Register VGI table functions
	RegisterVgiCatalogsFunction(loader);
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
