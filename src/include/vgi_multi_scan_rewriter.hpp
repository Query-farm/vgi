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

	// Catalog context for branch-function resolution (Catalog::GetEntry).
	// The rewriter looks up function_name in this catalog's schema first,
	// then default_schema, then the system catalog (for native readers like
	// read_parquet / iceberg_scan).
	std::string table_catalog_name;
	std::string table_schema_name;
	std::string default_schema;
};

// Construct the placeholder TableFunction. Its execute callback throws
// InternalException — if execution reaches it, the rewriter failed to fire
// (either disabled by vgi_multi_branch_scans=false or registered wrong).
//
// No bind callback; bind_data is supplied by VgiTableEntry::GetScanFunctionImpl.
// No init_global / init_local; same reason — the marker should never be
// executed.
TableFunction MakeMultiBranchMarkerFunction();

// Register the optimizer extension. Called from VgiExtension::Load().
void RegisterVgiMultiScanRewriter(DBConfig &config);

} // namespace vgi
} // namespace duckdb
