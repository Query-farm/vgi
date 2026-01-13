#define DUCKDB_EXTENSION_MAIN

#include "vgi_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "query_farm_telemetry.hpp"

#define VGI_EXTENSION_VERSION "2026011201"

namespace duckdb {

inline void vgiScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "vgi " + name.GetString() + " 🐥");
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto vgi_scalar_function = ScalarFunction("vgi", {LogicalType::VARCHAR}, LogicalType::VARCHAR, vgiScalarFun);
	loader.RegisterFunction(vgi_scalar_function);

	QueryFarmSendTelemetry(loader, "vgi", VGI_EXTENSION_VERSION);
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
