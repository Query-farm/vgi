// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once
#include <string>
namespace duckdb {
namespace vgi {
// These operations can allocate or mutate before their response is lost.
// Keep the subprocess and HTTP transport policy aligned until durable replay
// identities are negotiated by the protocol.
inline bool RequiresSingleAttemptUnary(const std::string &method) {
	return method == "catalog_query_prepare" || method == "catalog_table_create" ||
	       method == "catalog_table_constraints_apply" || method == "catalog_index_create" ||
	       method == "catalog_index_drop";
}
} // namespace vgi
} // namespace duckdb
