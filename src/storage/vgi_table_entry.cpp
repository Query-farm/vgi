// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "storage/vgi_table_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "duckdb/storage/table_storage_info.hpp"

#include "storage/vgi_catalog.hpp"
#include "storage/vgi_transaction.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_logging.hpp"
#include "vgi_multi_scan_rewriter.hpp"
#include "vgi_table_function_impl.hpp"
#include "vgi_worker_pool.hpp"

namespace duckdb {

VgiTableEntry::VgiTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                             const vgi::VgiTableInfo &table_info)
    : TableCatalogEntry(catalog, schema, info), table_info_(table_info), catalog_(catalog) {
	if (table_info_.row_id_column >= 0) {
		rowid_type_ = table_info_.rowid_type;
	}
}

VgiTableEntry::~VgiTableEntry() = default;

unique_ptr<BaseStatistics> VgiTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	if (column_id == COLUMN_IDENTIFIER_ROW_ID || column_id == COLUMN_IDENTIFIER_EMPTY) {
		return nullptr;
	}

	// Check catalog-level capability flag
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_result = vgi_catalog.attach_result();
	if (!attach_result || !attach_result->supports_column_statistics) {
		return nullptr;
	}

	// Check table-level capability flag
	if (!table_info_.supports_column_statistics) {
		return nullptr;
	}

	// Map column_id to column name before acquiring lock
	auto &columns = GetColumns();
	if (column_id >= columns.PhysicalColumnCount()) {
		return nullptr;
	}
	auto &col_def = columns.GetColumn(PhysicalIndex(column_id));
	auto column_name = col_def.GetName();

	// Two-phase: decide whether *this* call has to fetch, drop the mutex,
	// run the RPC, then re-acquire to publish + look up. Holding the mutex
	// across the RPC would self-deadlock on any path that re-enters
	// GetStatistics on the same table from inside the RPC (logging,
	// optimizer fanout, secret-manager reentry, etc.).
	bool must_fetch = false;
	bool concurrent_wait = false;
	double wait_ms = 0.0;
	auto qualifier = ParentSchema().name + "." + name;
	{
		std::unique_lock<std::mutex> lk(stats_cache_.mutex);
		// Wait out a concurrent fetch. The waiter then sees the freshly
		// populated cache and skips fetching.
		if (stats_cache_.loading) {
			concurrent_wait = true;
			auto wait_start = std::chrono::steady_clock::now();
			stats_cache_.cv.wait(lk, [this] { return !stats_cache_.loading; });
			auto wait_end = std::chrono::steady_clock::now();
			wait_ms = std::chrono::duration<double, std::milli>(wait_end - wait_start).count();
		}
		if (stats_cache_.IsStale()) {
			stats_cache_.loading = true;
			must_fetch = true;
		}
	}

	double fetch_ms = 0.0;
	if (must_fetch) {
		// Both populate paths handle their own publish + cv notify. They
		// must clear stats_cache_.loading on every exit path (success,
		// thrown exception, or caught error). Prefer the inline-blob path
		// when the worker pre-shipped stats — no RPC needed.
		auto fetch_start = std::chrono::steady_clock::now();
		if (table_info_.column_statistics.has_value()) {
			PopulateStatsCacheFromInline(context);
		} else {
			FetchColumnStatistics(context);
		}
		auto fetch_end = std::chrono::steady_clock::now();
		fetch_ms = std::chrono::duration<double, std::milli>(fetch_end - fetch_start).count();
	}

	std::string outcome = must_fetch ? "fetched" : (concurrent_wait ? "concurrent_wait" : "fresh_hit");
	vector<pair<string, string>> info{
	    {"qualifier", qualifier},
	    {"column", column_name},
	    {"outcome", outcome},
	};
	if (concurrent_wait) {
		info.emplace_back("wait_ms", std::to_string(wait_ms));
	}
	if (must_fetch) {
		info.emplace_back("fetch_ms", std::to_string(fetch_ms));
	}
	VGI_LOG(context, "catalog.stats_cache", info);

	std::lock_guard<std::mutex> lk(stats_cache_.mutex);
	auto it = stats_cache_.entries.find(column_name);
	if (it != stats_cache_.entries.end() && it->second) {
		return it->second->ToUnique();
	}
	return nullptr;
}

bool VgiTableEntry::StatsCache::IsStale() const {
	if (!fetched) {
		return true;
	}
	if (max_age_seconds < 0) {
		return false; // Never expires
	}
	if (max_age_seconds == 0) {
		return true; // Always re-fetch
	}
	auto elapsed = std::chrono::steady_clock::now() - fetched_at;
	return elapsed > std::chrono::seconds(max_age_seconds);
}

// Caller must have set stats_cache_.loading = true under the mutex; this
// method runs the worker RPC with the mutex *released* and is responsible
// for clearing the loading flag (and notifying waiters) on every exit.
void VgiTableEntry::FetchColumnStatistics(ClientContext &context) const {
	std::unordered_map<std::string, unique_ptr<BaseStatistics>> fetched_entries;
	int64_t fetched_max_age = -1;
	bool ok = false;

	try {
		auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
		auto &attach_params = vgi_catalog.attach_parameters();
		auto &attach_result_data = vgi_catalog.attach_result();

		// Build column name/type vectors from the table's schema
		auto &columns = GetColumns();
		std::vector<LogicalType> col_types;
		std::vector<std::string> col_names;
		for (idx_t i = 0; i < columns.PhysicalColumnCount(); i++) {
			auto &col = columns.GetColumn(PhysicalIndex(i));
			col_types.push_back(col.GetType());
			col_names.push_back(col.GetName());
		}

		auto &vgi_tx = VgiTransaction::Get(context, catalog_);
		vgi::CatalogRpcContext rpc_ctx{attach_params, attach_result_data->attach_opaque_data,
		                                vgi_tx.GetTransactionOpaqueData()};
		rpc_ctx.entity_kind = "table";
		rpc_ctx.entity_qualifier = ParentSchema().name + "." + name;

		auto rpc_result = vgi::InvokeCatalogTableColumnStatisticsGet(
		    rpc_ctx, ParentSchema().name, name, col_types, col_names, context);

		fetched_entries = std::move(rpc_result.stats);
		fetched_max_age = rpc_result.cache_max_age_seconds;
		ok = true;
	} catch (const std::exception &e) {
		// Best-effort logging; never propagate. We must always reach the
		// publish-and-clear block below or other callers wedge forever
		// waiting on stats_cache_.cv.
		try {
			auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
			VGI_LOG(context, "column_statistics.fetch_error",
			        {{"worker_path", vgi_catalog.attach_parameters()->worker_path()},
			         {"table", ParentSchema().name + "." + name},
			         {"error", e.what()}});
		} catch (...) {
			// swallow
		}
	} catch (...) {
		// unknown — likewise, must reach publish-and-clear below
	}

	// Publish results under the mutex. Even on failure we mark the cache as
	// "fetched" so subsequent callers don't pile up retries; max_age=-1
	// keeps the empty cache effectively pinned, which matches the previous
	// behaviour. The next CheckAndInvalidateCache / explicit clear will
	// reset.
	{
		std::lock_guard<std::mutex> lk(stats_cache_.mutex);
		if (ok) {
			stats_cache_.entries = std::move(fetched_entries);
			stats_cache_.max_age_seconds = fetched_max_age;
		} else {
			stats_cache_.entries.clear();
		}
		stats_cache_.fetched = true;
		stats_cache_.fetched_at = std::chrono::steady_clock::now();
		stats_cache_.loading = false;
	}
	stats_cache_.cv.notify_all();
}

// Caller must have set stats_cache_.loading = true under the mutex. This
// runs the deserialize *outside* the mutex (matching FetchColumnStatistics)
// because Arrow→DuckDB conversion can re-enter logging / catalog paths,
// and is responsible for clearing the loading flag and notifying waiters
// on every exit path.
void VgiTableEntry::PopulateStatsCacheFromInline(ClientContext &context) const {
	std::unordered_map<std::string, unique_ptr<BaseStatistics>> parsed_entries;
	bool ok = false;

	try {
		auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
		(void)vgi_catalog; // (referenced below in error path)

		// Build column name/type vectors from the table's schema — same
		// derivation as the on-demand fetch path so the two routes produce
		// identical BaseStatistics shapes.
		auto &columns = GetColumns();
		std::vector<LogicalType> col_types;
		std::vector<std::string> col_names;
		for (idx_t i = 0; i < columns.PhysicalColumnCount(); i++) {
			auto &col = columns.GetColumn(PhysicalIndex(i));
			col_types.push_back(col.GetType());
			col_names.push_back(col.GetName());
		}

		const auto &bytes = *table_info_.column_statistics;
		auto batch = vgi::DeserializeFromIpcBytes(bytes.data(), bytes.size());
		if (batch && batch->num_rows() > 0) {
			parsed_entries = vgi::ParseColumnStatisticsBatch(
			    batch, col_types, col_names,
			    vgi_catalog.attach_parameters()->worker_path(),
			    "table", ParentSchema().name + "." + name, context);
		}
		ok = true;
	} catch (const std::exception &e) {
		try {
			auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
			VGI_LOG(context, "column_statistics.inline_parse_error",
			        {{"worker_path", vgi_catalog.attach_parameters()->worker_path()},
			         {"table", ParentSchema().name + "." + name},
			         {"error", e.what()}});
		} catch (...) {
			// swallow
		}
	} catch (...) {
		// unknown — likewise, must reach publish-and-clear below
	}

	// Publish results. Mark `fetched=true` even on parse failure so callers
	// don't pile up retries; max_age_seconds = -1 means "never expires within
	// this catalog_version" — cache invalidation evicts on version bump.
	// This is the right TTL for inlined stats: the worker built them at some
	// past instant we don't know, but the version-bump path is what reliably
	// signals freshness, not steady_clock since attach.
	{
		std::lock_guard<std::mutex> lk(stats_cache_.mutex);
		if (ok) {
			stats_cache_.entries = std::move(parsed_entries);
		} else {
			stats_cache_.entries.clear();
		}
		stats_cache_.max_age_seconds = -1;
		stats_cache_.fetched = true;
		stats_cache_.fetched_at = std::chrono::steady_clock::now();
		stats_cache_.loading = false;
	}
	stats_cache_.cv.notify_all();
}

TableFunction VgiTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	return GetScanFunctionImpl(context, bind_data, "", "");
}

TableFunction VgiTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data, const EntryLookupInfo &lookup) {
	string at_unit;
	string at_value;
	auto at = lookup.GetAtClause();
	if (at) {
		at_unit = at->Unit();
		at_value = at->GetValue().ToString();
	}
	return GetScanFunctionImpl(context, bind_data, at_unit, at_value);
}

bool VgiTableEntry::IsKnownSingleBranchNoAT() const {
	// Inlined scan_function on table_info_ means the worker pre-shipped
	// the legacy single-function path — definitely single-branch, no
	// RPC ever needed for this table.
	if (table_info_.scan_function.has_value()) {
		return true;
	}
	// Otherwise consult the atomic hint populated by a prior
	// FetchScanBranches call.
	return multi_branch_hint_no_at_.load(std::memory_order_acquire) == 0;
}

void VgiTableEntry::RecordMultiBranchHintNoAT(bool is_multi_branch) const {
	int target = is_multi_branch ? 1 : 0;
	int expected = -1;
	// First-writer-wins. Subsequent calls leave the existing value alone.
	multi_branch_hint_no_at_.compare_exchange_strong(expected, target, std::memory_order_acq_rel);
}

vgi::VgiScanBranchesResult VgiTableEntry::FetchScanBranches(ClientContext &context, const std::string &at_unit,
                                                              const std::string &at_value) {
	// Fresh RPC per call. branches[] can vary with AT(...) — e.g.,
	// versioned_data tables return different function args per version.
	// Capability cache on VgiAttachParameters (A8) avoids the doomed-RPC
	// round-trip on workers that don't implement the new method.
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();
	auto &vgi_tx_scan = VgiTransaction::Get(context, catalog_);
	vgi::CatalogRpcContext rpc_ctx{attach_params, attach_result->attach_opaque_data,
	                                vgi_tx_scan.GetTransactionOpaqueData()};
	rpc_ctx.entity_kind = "table";
	rpc_ctx.entity_qualifier = ParentSchema().name + "." + name;
	auto result = vgi::InvokeCatalogTableScanBranchesGet(rpc_ctx, ParentSchema().name, name, context, at_unit, at_value);
	// Populate the multi-branch hint for the no-AT case so subsequent
	// write-refusal helpers can short-circuit without an RPC. AT-carrying
	// fetches don't populate the hint because the count COULD differ
	// per version in principle (writes always use empty AT, so the no-AT
	// snapshot is authoritative for them).
	if (at_unit.empty() && at_value.empty()) {
		RecordMultiBranchHintNoAT(result.branches.size() > 1);
	}
	return result;
}

TableFunction VgiTableEntry::GetScanFunctionImpl(ClientContext &context, unique_ptr<FunctionData> &bind_data,
                                                  const string &at_unit, const string &at_value) {
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();

	// Resolve the scan-function shape. Three paths in priority order:
	//   1. Inlined ``table_info_.scan_function`` (legacy single-branch
	//      pre-shipped path) — saves the per-bind RPC on HTTP transports.
	//   2. Already-loaded ``branches_`` cache from a prior call (lazy load).
	//   3. RPC: InvokeCatalogTableScanBranchesGet (probes the new multi-
	//      branch method, transparently falls back to the legacy
	//      single-function RPC for old workers).
	//
	// Multi-branch tables (branches.size() > 1) require the optimizer
	// rewriter to stitch arms together via LogicalSetOperation(UNION_ALL,
	// ...). That rewriter ships in Phase C — until then, multi-branch
	// returns surface a NotImplementedException loud-fail so the design
	// is observable without the rewriter shipping.
	vgi::VgiScanFunctionResult scan_result;
	bool used_inline = false;
	if (table_info_.scan_function.has_value()) {
		scan_result = *table_info_.scan_function;
		used_inline = true;
		VGI_LOG(context, "vgi.scan_function.inlined",
		        {{"schema", ParentSchema().name}, {"table", name}, {"function", scan_result.function_name}});
	} else {
		// Fetch branches fresh — branches[] varies with AT (e.g. versioned_data
		// returns different args per VERSION). Capability cache on the attach
		// params avoids the doomed-RPC round-trip on workers without the new
		// method.
		auto branches_result = FetchScanBranches(context, at_unit, at_value);

		// B2 (Phase B): refuse AT (...) on multi-branch tables with a
		// BinderException at bind time, so the rewriter never sees an AT
		// clause it can't honour. Single-branch tables still honour AT
		// (the branches.size() == 1 path threads at_unit/at_value into the
		// branch's scan-function call exactly as the legacy code did).
		if (!at_unit.empty() && branches_result.branches.size() > 1) {
			throw BinderException(
			    "AT (...) clauses are not supported on multi-branch VGI tables. "
			    "Query a specific branch's underlying function directly. "
			    "[table: %s.%s, branches: %d]",
			    ParentSchema().name, name, static_cast<int>(branches_result.branches.size()));
		}

		if (branches_result.branches.size() > 1) {
			// Multi-branch path. Up-front check of the rollback knob — if
			// the rewriter is disabled, refuse here with a clear
			// BinderException rather than returning the marker and reaching
			// the internal-error safety net at execute time.
			Value mb_enabled;
			if (!context.TryGetCurrentSetting("vgi_multi_branch_scans", mb_enabled) ||
			    mb_enabled.IsNull() || !mb_enabled.GetValue<bool>()) {
				throw BinderException(
				    "Multi-branch VGI table scan disabled via vgi_multi_branch_scans=false. "
				    "Re-enable the setting or query a specific branch directly. "
				    "[table: %s.%s, branches: %d]",
				    ParentSchema().name, name,
				    static_cast<int>(branches_result.branches.size()));
			}

			// Return the marker placeholder TableFunction. VgiMultiScanRewriter
			// (pre_optimize_function in vgi_extension.cpp) detects the marker
			// bind_data and rewrites the LogicalGet into a LogicalSetOperation
			// (UNION_ALL, ...) with one arm per branch. The marker's execute
			// callback throws InternalException — if execution reaches it,
			// the rewriter failed to fire despite being enabled, which IS a bug.
			auto marker_bd = make_uniq<vgi::VgiMultiBranchMarkerBindData>();
			marker_bd->branches = std::move(branches_result.branches);
			marker_bd->required_extensions = std::move(branches_result.required_extensions);
			marker_bd->table_catalog_name = catalog_.GetName();
			marker_bd->table_schema_name = ParentSchema().name;
			marker_bd->default_schema = attach_result->default_schema;
			// Canonical column list comes from the table's declared columns
			// (GetColumns() inherited from TableCatalogEntry). The rewriter's
			// per-arm projection layer reorders each branch's raw output to
			// match this order (by name) and NULL-fills missing canonicals.
			const auto &cols = GetColumns();
			marker_bd->canonical_column_names.reserve(cols.LogicalColumnCount());
			marker_bd->canonical_column_types.reserve(cols.LogicalColumnCount());
			for (auto &col : cols.Logical()) {
				marker_bd->canonical_column_names.push_back(col.Name());
				marker_bd->canonical_column_types.push_back(col.Type());
			}

			VGI_LOG(context, "vgi.multi_scan.marker_returned",
			        {{"schema", ParentSchema().name},
			         {"table", name},
			         {"branches", std::to_string(marker_bd->branches.size())}});

			bind_data = std::move(marker_bd);
			return vgi::MakeMultiBranchMarkerFunction();
		}

		// Single-branch path: project branches[0] back into the legacy
		// ScanFunctionResult shape so the rest of this function (extension
		// auto-load, function-catalog lookup, pushdown capability discovery,
		// bind-data construction) keeps working unchanged.
		auto &branch = branches_result.branches[0];
		scan_result.function_name = std::move(branch.function_name);
		scan_result.positional_arguments = std::move(branch.positional_arguments);
		scan_result.named_arguments = std::move(branch.named_arguments);
		// required_extensions hoisted to the top-level ScanBranchesResult;
		// thread it back into scan_result so the auto-load loop below works.
		scan_result.required_extensions = std::move(branches_result.required_extensions);
	}
	(void)used_inline; // tag for future per-path metrics

	// Load any required extensions before scanning
	for (auto &ext : scan_result.required_extensions) {
		ExtensionHelper::TryAutoLoadExtension(context, ext);
	}

	// Convert scan function arguments to Arrow
	vector<std::pair<string, Value>> named_args_vec;
	for (auto &[k, v] : scan_result.named_arguments) {
		named_args_vec.emplace_back(k, v);
	}

	// Look up the scan function's catalog entry to get its capabilities
	// (e.g., filter_pushdown, projection_pushdown)
	bool has_projection_pushdown = false;
	bool has_filter_pushdown = false;
	bool has_sampling_pushdown = false;
	bool has_late_materialization = false;
	// Default to INSERTION_ORDER (same as DuckDB's TableFunction default).
	// Overridden below to the resolved function's declared preservation when
	// at least one overload of the bound function pins it — required so the
	// virtual-table form (e.g. ``SELECT … FROM k.topics.<topic>``) honours
	// ``preserves_order = NO_ORDER_GUARANTEE`` declared by the worker.
	// Without this the planner kept seeing INSERTION_ORDER and forced
	// CTAS / parallel-friendly insertion paths to serialise.
	OrderPreservationType resolved_order_preservation = OrderPreservationType::INSERTION_ORDER;
	bool order_preservation_set = false;
	std::vector<std::string> scan_supported_expression_filters;
	std::vector<vgi::VgiSecretRequirement> scan_required_secrets;
	auto func_entry = catalog_.GetEntry<TableFunctionCatalogEntry>(
	    context, ParentSchema().name, scan_result.function_name, OnEntryNotFound::RETURN_NULL);
	if (!func_entry) {
		// Function may be in a different schema (e.g., main) than the table's schema (e.g., data)
		auto &default_schema = vgi_catalog.attach_result()->default_schema;
		if (default_schema != ParentSchema().name) {
			func_entry = catalog_.GetEntry<TableFunctionCatalogEntry>(
			    context, default_schema, scan_result.function_name, OnEntryNotFound::RETURN_NULL);
		}
	}
	if (func_entry) {
		for (auto &tf : func_entry->functions.functions) {
			has_projection_pushdown = has_projection_pushdown || tf.projection_pushdown;
			has_filter_pushdown = has_filter_pushdown || tf.filter_pushdown;
			has_sampling_pushdown = has_sampling_pushdown || tf.sampling_pushdown;
			// First overload's order_preservation_type wins. Different overloads
			// of one function name share the same row-ordering semantics in
			// practice — the field is per-FunctionInfo, not per-overload.
			if (!order_preservation_set) {
				resolved_order_preservation = tf.order_preservation_type;
				order_preservation_set = true;
			}
			// Extract required_secrets from the VgiTableFunctionInfo
			if (tf.function_info) {
				auto &vgi_tf_info = tf.function_info->Cast<vgi::VgiTableFunctionInfo>();
				scan_required_secrets = vgi_tf_info.function_info().required_secrets;
				scan_supported_expression_filters = vgi_tf_info.function_info().supported_expression_filters;
				// late_materialization is advertised on the parsed worker metadata
				// (VgiFunctionInfo), not on DuckDB's TableFunction — the synthetic
				// vgi_table_scan below is constructed fresh, so read the capability
				// here and gate func.late_materialization on it.
				has_late_materialization =
				    has_late_materialization || vgi_tf_info.function_info().late_materialization.value_or(false);
			}
		}
	}

	// Build shared bind data
	auto scan_bind_data = make_uniq<vgi::VgiTableFunctionBindData>();
	scan_bind_data->attach_params = attach_params;
	scan_bind_data->attach_opaque_data = attach_result->attach_opaque_data;
	auto &vgi_tx = VgiTransaction::Get(context, catalog_);
	scan_bind_data->transaction_opaque_data = vgi_tx.GetTransactionOpaqueData();
	scan_bind_data->function_name = scan_result.function_name;
	scan_bind_data->arguments = vgi::BuildArgumentsFromValues(context, scan_result.positional_arguments, named_args_vec);
	scan_bind_data->projection_pushdown = has_projection_pushdown;
	scan_bind_data->supported_expression_filters = scan_supported_expression_filters;
	scan_bind_data->required_secrets = scan_required_secrets;

	// Store table entry reference for get_bind_info callback
	scan_bind_data->table_entry = this;

	// Retain the AT (...) clause (empty for the no-AT case) so the
	// vgi_table_scan (de)serialize callbacks can rebuild the same branch
	// resolution after a logical-plan deep copy.
	scan_bind_data->at_unit = at_unit;
	scan_bind_data->at_value = at_value;

	// Pass row_id info to bind data
	if (table_info_.row_id_column >= 0) {
		scan_bind_data->rowid_worker_col_index = table_info_.row_id_column;
		scan_bind_data->rowid_type = table_info_.rowid_type;
	}

	// Perform bind handshake with the worker (discovers output schema)
	vector<LogicalType> return_types;
	vector<string> names;
	vgi::PerformVgiTableFunctionBind(context, *scan_bind_data, return_types, names);

	// Use inlined cardinality from TableInfo when present, skipping the
	// per-bind table_function_cardinality RPC. Saves 50–80ms RTT on remote
	// HTTP transports for read-only / slow-changing tables. Workers opt in
	// by setting Table.cardinality_estimate / cardinality_max.
	//
	// MUST run AFTER PerformVgiTableFunctionBind, which resets
	// ``cardinality_estimate = -1`` on the bind data unconditionally.
	if (table_info_.cardinality_estimate.has_value() || table_info_.cardinality_max.has_value()) {
		if (table_info_.cardinality_estimate.has_value()) {
			scan_bind_data->cardinality_estimate = *table_info_.cardinality_estimate;
		}
		if (table_info_.cardinality_max.has_value()) {
			scan_bind_data->cardinality_max = *table_info_.cardinality_max;
		}
		// Mark fetched so VgiTableFunctionCardinality skips the RPC.
		scan_bind_data->cardinality_fetched = true;
		VGI_LOG(context, "vgi.cardinality.inlined",
		        {{"schema", ParentSchema().name},
		         {"table", name},
		         {"estimate", std::to_string(scan_bind_data->cardinality_estimate)},
		         {"max", std::to_string(scan_bind_data->cardinality_max)}});
	}

	bind_data = std::move(scan_bind_data);

	// Wire up the shared table function implementation
	TableFunction func("vgi_table_scan", {}, vgi::VgiTableFunctionScan, nullptr,
	                   vgi::VgiTableFunctionInitGlobal, vgi::VgiTableFunctionInitLocal);
	func.projection_pushdown = has_projection_pushdown;
	func.filter_pushdown = has_filter_pushdown;
	func.sampling_pushdown = has_sampling_pushdown;
	// Propagate the worker's declared order preservation to the synthetic
	// TableFunction so DuckDB's planner sees the right SourceOrder() for
	// virtual-table scans (e.g. ``k.topics.<topic>``). Without this the
	// dynamic-catalog path always advertised INSERTION_ORDER and CTAS over
	// a virtual table forcibly serialised even when the underlying function
	// is NO_ORDER_GUARANTEE.
	func.order_preservation_type = resolved_order_preservation;
	if (!scan_supported_expression_filters.empty()) {
		func.pushdown_expression = vgi::VgiPushdownExpression;
	}
	func.cardinality = vgi::VgiTableFunctionCardinality;
	func.table_scan_progress = vgi::VgiTableFunctionProgress;
	func.to_string = vgi::VgiTableFunctionToString;
	func.dynamic_to_string = vgi::VgiTableFunctionDynamicToString;
	func.get_bind_info = vgi::VgiTableScanGetBindInfo;
	func.set_scan_order = vgi::VgiSetScanOrder;
	func.statistics = vgi::VgiTableFunctionStatistics;
	// Make the synthetic scan function survive LogicalOperator::Copy()'s
	// serialize/deserialize round-trip (WindowSelfJoin et al.). See the
	// callback definitions below and the catalog registration in
	// vgi_extension.cpp (RegisterVgiTableScanFunction).
	func.serialize = VgiTableScanSerialize;
	func.deserialize = VgiTableScanDeserialize;
	// INITIALIZE_ON_SCHEDULE would move init_global into
	// Executor::ScheduleEvents' eager-init loop, collapsing N sequential
	// metadata RTTs into one parallel batch — but it fires init_global
	// before any sibling build pipeline has populated
	// PhysicalTableScan::dynamic_filters via JoinFilterPushdownInfo::PushInFilter.
	// A VGI catalog table on the probe side of a hash join would never
	// see the build-side InFilter, breaking join key pushdown for the
	// `SELECT … FROM catalog.schema.table` form. The same fix already lives
	// in vgi_table_function_set.cpp for the function-call form; mirror it
	// here. See that file's comment block for the full rationale and the
	// TODO about a deferred-filter-capture refactor that could restore the
	// eager flag without sacrificing join pushdown.

	// Set virtual column callbacks for row_id support
	if (table_info_.row_id_column >= 0) {
		func.get_virtual_columns = vgi::VgiTableScanGetVirtualColumns;
		func.get_row_id_columns = vgi::VgiTableScanGetRowIdColumns;

		// Opt into DuckDB's late-materialization optimizer only when the worker
		// advertises the capability AND the surviving rowids can be pushed back
		// to the wide re-scan (filter_pushdown) over a pruned projection
		// (projection_pushdown). Without filter_pushdown the RHS scan returns the
		// whole table — a net loss (extra scan + semi-join, no pruning). The
		// rowid virtual column required by the optimizer is registered just
		// above (row_id_column >= 0), so this branch is the only place the flag
		// can be safely set; the direct vgi_table_function() path registers no
		// rowid callbacks and must stay false.
		func.late_materialization =
		    has_late_materialization && has_filter_pushdown && has_projection_pushdown;
	}

	return func;
}

void VgiTableEntry::VgiTableScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                                          const TableFunction &function) {
	// The scan function carries no callable arguments; everything needed to
	// rebuild it lives in the bind data. Persist only the table identity (and
	// AT clause), mirroring DuckDB's own TableScanSerialize — the heavy bind
	// state (Arrow schema, worker connection params, pushdown capabilities) is
	// re-derived from the catalog entry on deserialize.
	auto &bind_data = bind_data_p->Cast<vgi::VgiTableFunctionBindData>();
	if (!bind_data.table_entry) {
		// Only the catalog-scan path installs these callbacks, and it always
		// sets table_entry. A null here means the direct vgi_table_function()
		// bind data reached this callback — a wiring bug, not a user error.
		throw SerializationException(
		    "vgi_table_scan cannot be serialized without an originating table entry");
	}
	auto &table = *bind_data.table_entry;
	serializer.WriteProperty(100, "catalog", table.ParentCatalog().GetName());
	serializer.WriteProperty(101, "schema", table.ParentSchema().name);
	serializer.WriteProperty(102, "table", table.name);
	serializer.WriteProperty(103, "at_unit", bind_data.at_unit);
	serializer.WriteProperty(104, "at_value", bind_data.at_value);
}

unique_ptr<FunctionData> VgiTableEntry::VgiTableScanDeserialize(Deserializer &deserializer, TableFunction &function) {
	auto catalog = deserializer.ReadProperty<string>(100, "catalog");
	auto schema = deserializer.ReadProperty<string>(101, "schema");
	auto table = deserializer.ReadProperty<string>(102, "table");
	auto at_unit = deserializer.ReadProperty<string>(103, "at_unit");
	auto at_value = deserializer.ReadProperty<string>(104, "at_value");
	auto &context = deserializer.Get<ClientContext &>();

	auto &catalog_entry = Catalog::GetEntry<TableCatalogEntry>(context, catalog, schema, table);
	auto &vgi_entry = catalog_entry.Cast<VgiTableEntry>();

	// Re-run the bind to produce a fully-configured TableFunction (pushdown
	// flags, order preservation, virtual columns, cardinality, statistics —
	// all recomputed for *this* table) plus its bind data. Overwriting
	// `function` is intentional and supported: LogicalGet::Deserialize reads
	// `function.get_virtual_columns` from the reassigned function after this
	// callback returns, so the copy ends up identical to a fresh bind.
	unique_ptr<FunctionData> bind_data;
	function = vgi_entry.GetScanFunctionImpl(context, bind_data, at_unit, at_value);
	return bind_data;
}

TableStorageInfo VgiTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo info;
	info.cardinality = 0; // Unknown

	// Expose PK and UNIQUE constraints as virtual index_info entries.
	//
	// DuckDB's binder requires matching index_info when validating ON CONFLICT clauses
	// (see bind_insert.cpp — it iterates storage_info.index_info looking for UNIQUE indexes
	// that match the conflict target columns). Without these entries, the binder rejects
	// ON CONFLICT with "no UNIQUE/PRIMARY KEY constraints that refer to this table".
	//
	// These are NOT real indexes — VGI tables have no local storage or ART indexes.
	// They exist solely to satisfy the binder's validation. Actual conflict detection
	// is delegated to the worker, which owns the data and its constraints.
	//
	// Constraint indices from the worker are in Arrow schema space (which includes
	// the row_id column). DuckDB's physical column space excludes virtual columns
	// like row_id. Adjust by shifting indices down when they're after row_id.
	auto adjust_col = [&](int col) -> column_t {
		auto adjusted = col;
		if (table_info_.row_id_column >= 0 && col > table_info_.row_id_column) {
			adjusted--;
		} else if (table_info_.row_id_column >= 0 && col == table_info_.row_id_column) {
			// row_id itself should never be a constraint column
			throw InternalException("row_id column cannot be part of a constraint");
		}
		return NumericCast<column_t>(adjusted);
	};

	for (auto &pk : table_info_.primary_key_constraints) {
		IndexInfo idx_info;
		idx_info.is_unique = true;
		idx_info.is_primary = true;
		idx_info.is_foreign = false;
		for (auto col : pk) {
			idx_info.column_set.insert(adjust_col(col));
		}
		info.index_info.push_back(std::move(idx_info));
	}
	for (auto &unique : table_info_.unique_constraints) {
		IndexInfo idx_info;
		idx_info.is_unique = true;
		idx_info.is_primary = false;
		idx_info.is_foreign = false;
		for (auto col : unique) {
			idx_info.column_set.insert(adjust_col(col));
		}
		info.index_info.push_back(std::move(idx_info));
	}

	return info;
}

virtual_column_map_t VgiTableEntry::GetVirtualColumns() const {
	if (rowid_type_ != LogicalType::INVALID) {
		virtual_column_map_t result;
		result.insert({COLUMN_IDENTIFIER_ROW_ID, TableColumn("rowid", rowid_type_)});
		return result;
	}
	return {};
}

vector<column_t> VgiTableEntry::GetRowIdColumns() const {
	if (rowid_type_ != LogicalType::INVALID) {
		return {COLUMN_IDENTIFIER_ROW_ID};
	}
	return {};
}

} // namespace duckdb
