// © Copyright 2026 Query Farm LLC - https://query.farm
//
// Minimal stubs so the SAB runtime-e2e test can link WebWorkerFunctionConnection
// (and vgi_bind_protocol) WITHOUT pulling the full catalog subtree
// (vgi_catalog_api.cpp) into the lightweight vgi_unit_tests binary. The count_to
// table function under test declares no secrets, so "no secrets" is the correct
// behavior here. NOT compiled into the shipped extension.
#include "duckdb.hpp"

#include "vgi_catalog_rpc.hpp" // ExtractVgiSecrets declaration + VgiSecretRequirement

namespace duckdb {
namespace vgi {

std::map<std::string, std::map<std::string, Value>>
ExtractVgiSecrets(ClientContext &, const std::vector<VgiSecretRequirement> &) {
	return {};
}

} // namespace vgi
} // namespace duckdb
