// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// =============================================================================
// VgiMultiScanRewriter — OptimizerExtension that rewrites multi-branch VGI
// table scans into LogicalSetOperation(UNION_ALL, [LogicalProjection(
// LogicalFilter(branch_filter, LogicalGet(branch_fn))), ...]).
//
// Detection: matches LogicalGet whose bind_data is a
// VgiMultiBranchMarkerBindData. Single-branch tables don't carry the marker
// (they bind directly to the underlying scan function), so the rewriter
// no-ops for them.
//
// Phase ordering: pre_optimize_function. Different from
// VgiTableBufferingRewriter (post-pushdown) because we rewrite into standard
// DuckDB operators that benefit from filter pushdown — pushdown runs after
// us and distributes parent filters into each arm. See the phase-split
// comment in vgi_extension.cpp.
//
// Plan reference: §C++ wiring sketch in
// ~/.claude/plans/right-now-vgi-and-partitioned-nebula.md
// =============================================================================

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

// Bind data carried by the placeholder TableFunction that VgiTableEntry::
// GetScanFunctionImpl returns for multi-branch tables. The rewriter detects
// the marker via dynamic_cast<VgiMultiBranchMarkerBindData *>(get.bind_data.get())
// and replaces the LogicalGet with the union.
//
// All fields are owned (moved-in) so the bind data outlives any per-call
// scope. Lifetime is the prepared statement / query plan.
struct VgiMultiBranchMarkerBindData : public TableFunctionData {
	// One per physical source. Always size > 1 — single-branch tables use the
	// legacy path. Each VgiScanBranch carries its function_name, args, and an
	// optional parsed branch_filter expression.
	std::vector<VgiScanBranch> branches;

	// Union of extensions required across all branches; auto-loaded by the
	// rewriter before binding each branch.
	std::vector<std::string> required_extensions;

	// Canonical column list — defines the union's output schema. The rewriter
	// reorders each branch's raw output to match this order (by NAME, not
	// position) and NULL-fills any missing canonical columns. Sourced from
	// the VgiTableEntry's declared ColumnList at marker-construction time.
	std::vector<std::string> canonical_column_names;
	std::vector<LogicalType> canonical_column_types;

	// Catalog context for branch-function resolution
	// (vgi::ResolveVgiTableFunction): this catalog's visible entries in the
	// table's schema and default_schema, then their hidden (table_only) sets,
	// then the system catalog (for native readers like read_parquet /
	// iceberg_scan).
	std::string table_catalog_name;
	std::string table_schema_name;
	//! The table's own name (diagnostics: an unresolved branch function names it).
	std::string table_name;
	std::string default_schema;

	// Protocol 3.2 filtered scan planning (docs/protocol/scan-planning.md).
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

// Construct the placeholder TableFunction. Its execute callback throws
// InternalException — if execution reaches it, the rewriter failed to fire
// (either disabled by vgi_multi_branch_scans=false or registered wrong).
//
// No bind callback; bind_data is supplied by VgiTableEntry::GetScanFunctionImpl.
// No init_global / init_local; same reason — the marker should never be
// executed.
TableFunction MakeMultiBranchMarkerFunction();

// The multi-branch and native-delegation placeholders are only valid if one of
// VGI's optimizer extensions replaces them before execution. DuckDB skips every
// optimizer extension when the optimizer is off (PRAGMA disable_optimizer) or
// when 'extension' is in disabled_optimizers — both settable by any user. Call
// before returning a placeholder: throws BinderException naming `what` and the
// setting to change, instead of letting the placeholder reach execution.
void RequireVgiOptimizerExtensions(ClientContext &context, const std::string &what);

// Register the optimizer extension. Called from VgiExtension::Load().
void RegisterVgiMultiScanRewriter(DBConfig &config);

} // namespace vgi
} // namespace duckdb
