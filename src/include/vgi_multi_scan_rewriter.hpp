// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// VgiMultiScanRewriter: an OptimizerExtension that rewrites a LogicalGet whose
// bind_data is VgiMultiBranchMarkerBindData into LogicalSetOperation(UNION_ALL,
// [LogicalProjection(LogicalFilter(branch_filter, LogicalGet(branch_fn))), ...]).
// Tables that bind directly to one scan function carry no marker. It runs as a
// pre_optimize_function (unlike the post-pushdown VgiTableBufferingRewriter):
// the result is standard DuckDB operators, so filter pushdown afterwards
// distributes parent filters into each arm. See docs/multi_branch.md.

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "duckdb/common/types.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

#include "vgi_catalog_metadata.hpp"

namespace duckdb {

class VgiTableEntry;

namespace vgi {

// Bind data of the placeholder TableFunction that VgiTableEntry::
// GetScanFunctionImpl returns for multi-branch tables; the rewriter replaces
// the LogicalGet carrying it. All fields are owned, so it lives as long as the
// plan / prepared statement.
struct VgiMultiBranchMarkerBindData : public TableFunctionData {
	// One per physical source. Usually more than one; a single branch uses the
	// marker only when it has what a plain scan cannot express (branch_filter,
	// writable, catalog-table or format source). Empty at bind when `deferred`.
	std::vector<VgiScanBranch> branches;

	// Union of extensions required across all branches; auto-loaded by the
	// rewriter before binding each branch.
	std::vector<std::string> required_extensions;

	// The table's declared columns: the union's output schema. Each branch's
	// output is matched to it by NAME (not position), NULL-filling missing columns.
	std::vector<std::string> canonical_column_names;
	std::vector<LogicalType> canonical_column_types;

	// Catalog context for branch-function resolution (vgi::ResolveVgiTableFunction).
	std::string table_catalog_name;
	std::string table_schema_name;
	//! The table's own name (diagnostics: an unresolved branch function names it).
	std::string table_name;
	std::string default_schema;

	// Protocol 3.2 filtered scan planning (docs/multi_branch.md).
	// When `deferred` is set the table declared scan_planning='filtered':
	// `branches` is empty at bind time and the rewriter calls `fetch_branches`
	// with the query's predicates (a filter v2 snapshot, or nullopt) to get the
	// branches, which may legitimately be empty ("no rows"). `has_at_clause`
	// records an AT (...) on the scan; the rewriter refuses it when more than
	// one branch remains.
	bool deferred = false;
	bool has_at_clause = false;
	std::function<VgiScanBranchesResult(ClientContext &, const std::optional<std::vector<uint8_t>> &)> fetch_branches;
};

// The placeholder TableFunction: no bind (bind_data comes from
// VgiTableEntry::GetScanFunctionImpl) and no init. Reaching its execute means
// the rewriter did not fire; it throws InvalidInputException (not
// InternalException, which would invalidate the database).
TableFunction MakeMultiBranchMarkerFunction();

// The multi-branch and native-delegation placeholders are only valid if one of
// VGI's optimizer extensions replaces them before execution. DuckDB skips every
// optimizer extension when the optimizer is off (PRAGMA disable_optimizer) or
// when 'extension' is in disabled_optimizers — both settable by any user. Call
// before returning a placeholder: throws BinderException naming `what` and the
// setting to change, instead of letting the placeholder reach execution.
void RequireVgiOptimizerExtensions(ClientContext &context, const std::string &what);

void RegisterVgiMultiScanRewriter(DBConfig &config);

} // namespace vgi
} // namespace duckdb
