// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "storage/vgi_table_entry.hpp"
#include "storage/vgi_table_function_resolver.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "duckdb/storage/table_storage_info.hpp"

#include "storage/vgi_catalog.hpp"
#include "storage/vgi_transaction.hpp"
#include "vgi_engine_compat.hpp"
#include "vgi_catalog_rpc.hpp"
#include "vgi_column_expression.hpp"
#include "vgi_projection_pushdown.hpp"
#include "vgi_exception.hpp"
#include "vgi_logging.hpp"
#include "vgi_multi_scan_rewriter.hpp"
#include "vgi_table_function_impl.hpp"
#include "vgi_worker_pool.hpp"

#include <algorithm>

namespace duckdb {

namespace {

// Backstop only (bind refuses via RequireVgiOptimizerExtensions); an ordinary
// error because an InternalException would invalidate the database.
void NativeDelegationMarkerExecute(ClientContext &, TableFunctionInput &, DataChunk &) {
	throw InvalidInputException("VgiRequiredFiltersOptimizer did not fire — native-delegation placeholder "
	                            "reached execution. Check that the optimizer is enabled and that 'extension' "
	                            "is not in disabled_optimizers.");
}

} // namespace

TableFunction MakeNativeDelegationMarkerFunction() {
	TableFunction fn("vgi_native_delegation_marker", {}, NativeDelegationMarkerExecute);
	// No bind/init: bind_data comes from GetScanFunctionImpl and it never runs.
	// filter_pushdown so FilterPushdown fills table_filters, which the rewriter
	// hands to the native function.
	fn.filter_pushdown = true;
	VgiDisableRuntimeFilterPushdown(fn);
	fn.projection_pushdown = true;
	return fn;
}

VgiTableEntry::VgiTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                             const vgi::VgiTableInfo &table_info)
    : TableCatalogEntry(catalog, schema, info), columns_(info.columns.Copy()), table_info_(table_info),
      catalog_(catalog) {
	if (table_info_.row_id_column >= 0) {
		rowid_type_ = table_info_.rowid_type;
	}
}

VgiTableEntry::~VgiTableEntry() = default;

unique_ptr<BaseStatistics> VgiTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	if (column_id == COLUMN_IDENTIFIER_ROW_ID || column_id == COLUMN_IDENTIFIER_EMPTY ||
	    GetColumns().PhysicalColumnCount() != GetColumns().LogicalColumnCount()) {
		return nullptr;
	}

	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_result = vgi_catalog.attach_result();
	if (!attach_result || !attach_result->supports_column_statistics) {
		return nullptr;
	}

	if (!table_info_.supports_column_statistics) {
		return nullptr;
	}

	auto &columns = GetColumns();
	if (column_id >= columns.PhysicalColumnCount()) {
		return nullptr;
	}
	auto &col_def = columns.GetColumn(PhysicalIndex(column_id));
	auto column_name = col_def.GetName().GetIdentifierName();

	// Decide under the mutex whether this call fetches; the fetch itself runs
	// unlocked (see StatsCache).
	bool must_fetch = false;
	bool concurrent_wait = false;
	double wait_ms = 0.0;
	auto qualifier = ParentSchema().name.GetIdentifierName() + "." + name.GetIdentifierName();
	{
		std::unique_lock<std::mutex> lk(stats_cache_.mutex);
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
		// Both loaders publish, clear `loading` and notify on every exit path.
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
	vector<pair<string, string>> info {
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

// Caller set stats_cache_.loading under the mutex; this runs the RPC unlocked
// and must clear `loading` and notify waiters on every exit.
void VgiTableEntry::FetchColumnStatistics(ClientContext &context) const {
	std::unordered_map<std::string, unique_ptr<BaseStatistics>> fetched_entries;
	int64_t fetched_max_age = -1;
	bool ok = false;

	try {
		auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
		auto &attach_params = vgi_catalog.attach_parameters();
		auto &attach_result_data = vgi_catalog.attach_result();

		auto &columns = GetColumns();
		std::vector<LogicalType> col_types;
		std::vector<std::string> col_names;
		for (idx_t i = 0; i < columns.PhysicalColumnCount(); i++) {
			auto &col = columns.GetColumn(PhysicalIndex(i));
			col_types.push_back(col.GetType());
			col_names.push_back(col.GetName().GetIdentifierName());
		}

		auto &vgi_tx = VgiTransaction::Get(context, catalog_);
		vgi::CatalogRpcContext rpc_ctx {attach_params, attach_result_data->attach_opaque_data,
		                                vgi_tx.GetTransactionOpaqueData()};
		rpc_ctx.entity_kind = "table";
		rpc_ctx.entity_qualifier = ParentSchema().name.GetIdentifierName() + "." + name.GetIdentifierName();

		auto rpc_result = vgi::InvokeCatalogTableColumnStatisticsGet(
		    rpc_ctx, ParentSchema().name.GetIdentifierName(), name.GetIdentifierName(), col_types, col_names, context);

		fetched_entries = std::move(rpc_result.stats);
		fetched_max_age = rpc_result.cache_max_age_seconds;
		ok = true;
	} catch (const std::exception &e) {
		// Never propagate: the publish-and-clear below must run or waiters on
		// stats_cache_.cv hang forever.
		try {
			auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
			VGI_LOG(context, "column_statistics.fetch_error",
			        {{"worker_path", vgi_catalog.attach_parameters()->worker_path()},
			         {"table", ParentSchema().name.GetIdentifierName() + "." + name.GetIdentifierName()},
			         {"error", e.what()}});
		} catch (...) {
			// swallow
		}
	} catch (...) {
		// unknown — likewise, must reach publish-and-clear below
	}

	// Even on failure mark the cache fetched so callers don't pile up retries;
	// the empty cache stays until the entry is rebuilt (version bump / clear).
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

// Same contract as FetchColumnStatistics; the decode runs unlocked because
// Arrow-to-DuckDB conversion can re-enter logging / catalog paths.
void VgiTableEntry::PopulateStatsCacheFromInline(ClientContext &context) const {
	std::unordered_map<std::string, unique_ptr<BaseStatistics>> parsed_entries;
	bool ok = false;

	try {
		auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
		(void)vgi_catalog;

		// Same derivation as FetchColumnStatistics, so both yield identical shapes.
		auto &columns = GetColumns();
		std::vector<LogicalType> col_types;
		std::vector<std::string> col_names;
		for (idx_t i = 0; i < columns.PhysicalColumnCount(); i++) {
			auto &col = columns.GetColumn(PhysicalIndex(i));
			col_types.push_back(col.GetType());
			col_names.push_back(col.GetName().GetIdentifierName());
		}

		const auto &bytes = *table_info_.column_statistics;
		auto batch = vgi::DeserializeFromIpcBytes(bytes.data(), bytes.size());
		if (batch && batch->num_rows() > 0) {
			parsed_entries = vgi::ParseColumnStatisticsBatch(
			    batch, col_types, col_names, vgi_catalog.attach_parameters()->worker_path(), "table",
			    ParentSchema().name.GetIdentifierName() + "." + name.GetIdentifierName(), context);
		}
		ok = true;
	} catch (const std::exception &e) {
		try {
			auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
			VGI_LOG(context, "column_statistics.inline_parse_error",
			        {{"worker_path", vgi_catalog.attach_parameters()->worker_path()},
			         {"table", ParentSchema().name.GetIdentifierName() + "." + name.GetIdentifierName()},
			         {"error", e.what()}});
		} catch (...) {
			// swallow
		}
	} catch (...) {
		// unknown — likewise, must reach publish-and-clear below
	}

	// Fetched even on parse failure (no retry pile-up). Inlined stats never
	// expire: their age is unknown, and a catalog version bump rebuilds the entry.
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

TableFunction VgiTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
                                             const EntryLookupInfo &lookup) {
	string at_unit;
	string at_value;
	auto at = lookup.GetAtClause();
	if (at) {
		at_unit = at->Unit().GetIdentifierName();
		at_value = at->GetValue().ToString();
	}
	return GetScanFunctionImpl(context, bind_data, at_unit, at_value);
}

bool VgiTableEntry::IsKnownSingleBranchNoAT() const {
	// An inlined scan_function is always single-branch.
	if (table_info_.scan_function.has_value()) {
		return true;
	}
	return multi_branch_hint_no_at_.load(std::memory_order_acquire) == 0;
}

void VgiTableEntry::RecordMultiBranchHintNoAT(bool is_multi_branch) const {
	int target = is_multi_branch ? 1 : 0;
	int expected = -1;
	// First writer wins.
	multi_branch_hint_no_at_.compare_exchange_strong(expected, target, std::memory_order_acq_rel);
}

vgi::VgiScanBranchesResult VgiTableEntry::FetchScanBranches(ClientContext &context, const std::string &at_unit,
                                                            const std::string &at_value,
                                                            const std::optional<std::vector<uint8_t>> &pushdown_filters) {
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();
	auto &vgi_tx_scan = VgiTransaction::Get(context, catalog_);
	vgi::CatalogRpcContext rpc_ctx {attach_params, attach_result->attach_opaque_data,
	                                vgi_tx_scan.GetTransactionOpaqueData()};
	rpc_ctx.entity_kind = "table";
	rpc_ctx.entity_qualifier = ParentSchema().name.GetIdentifierName() + "." + name.GetIdentifierName();
	auto result = vgi::InvokeCatalogTableScanBranchesGet(rpc_ctx, ParentSchema().name.GetIdentifierName(),
	                                                     name.GetIdentifierName(), context, at_unit, at_value,
	                                                     pushdown_filters);
	// Only no-AT fetches set the hint: the branch count may differ per version,
	// and writes always use the no-AT view.
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
	const bool generated_columns = GetColumns().PhysicalColumnCount() != GetColumns().LogicalColumnCount();
	// The worker scan contains only physical fields, including any hidden rowid.
	auto worker_column = [&](int declared_index) {
		int physical = 0;
		for (int i = 0; i < declared_index; ++i) {
			const auto &field = table_info_.arrow_schema->field(i);
			if (!vgi::VgiMetadata::FromC(field.metadata).Contains(vgi::VGI_COLUMN_GENERATED))
				physical++;
		}
		return physical;
	};

	// Scan shape, in priority order: filtered scan planning (deferred marker),
	// the inlined ``table_info_.scan_function`` (saves the per-bind RPC), else
	// catalog_table_scan_branches_get. Multi-branch results become a marker
	// that VgiMultiScanRewriter expands into a UNION ALL of arms.
	//
	// Filtered scan planning: no RPC at bind time. The deferred marker lets the
	// pre-optimizer rewrite fetch branches with the query's predicates, prune,
	// and emit the arms (or an empty result).
	if (table_info_.IsFilteredScanPlanning()) {
		if (generated_columns) {
			throw NotImplementedException("VGI generated columns require a single worker function scan");
		}
		Value mb_enabled;
		if (!context.TryGetCurrentSetting("vgi_multi_branch_scans", mb_enabled) || mb_enabled.IsNull() ||
		    !mb_enabled.GetValue<bool>()) {
			throw BinderException(
			    vgi::ErrorInfo(vgi::error_subtype::kUnsupported)
			        .Set(vgi::error_key::kTable, ParentSchema().name.GetIdentifierName() + "." + name.GetIdentifierName())
			        .Set(vgi::error_key::kOperation, "SCAN")
			        .Set(vgi::error_key::kSetting, "vgi_multi_branch_scans"),
			    StringUtil::Format("VGI table %s.%s uses filtered scan planning, which needs the multi-branch scan "
			                       "rewriter; it is disabled via vgi_multi_branch_scans=false.",
			                       ParentSchema().name.GetIdentifierName(), name.GetIdentifierName()));
		}
		vgi::RequireVgiOptimizerExtensions(context, StringUtil::Format("Filtered-planning VGI table %s.%s",
		                                                               ParentSchema().name.GetIdentifierName(),
		                                                               name.GetIdentifierName()));
		auto marker_bd = make_uniq<vgi::VgiMultiBranchMarkerBindData>();
		marker_bd->deferred = true;
		marker_bd->has_at_clause = !at_unit.empty();
		marker_bd->table_catalog_name = catalog_.GetName().GetIdentifierName();
		marker_bd->table_schema_name = ParentSchema().name.GetIdentifierName();
		marker_bd->table_name = name.GetIdentifierName();
		marker_bd->default_schema = attach_result->default_schema.value_or("");
		const auto &cols = GetColumns();
		for (auto &col : cols.Logical()) {
			marker_bd->canonical_column_names.push_back(col.Name().GetIdentifierName());
			marker_bd->canonical_column_types.push_back(col.Type());
		}
		// Safe to capture `this`: the entry outlives planning, where the rewrite runs.
		marker_bd->fetch_branches = [this, at_unit, at_value](ClientContext &ctx,
		                                                      const std::optional<std::vector<uint8_t>> &filters) {
			return FetchScanBranches(ctx, at_unit, at_value, filters);
		};
		VGI_LOG(context, "vgi.scan_planning.deferred",
		        {{"schema", ParentSchema().name.GetIdentifierName()}, {"table", name.GetIdentifierName()}});
		bind_data = std::move(marker_bd);
		return vgi::MakeMultiBranchMarkerFunction();
	}

	vgi::VgiScanFunctionResult scan_result;
	bool used_inline = false;
	if (table_info_.scan_function.has_value()) {
		scan_result = *table_info_.scan_function;
		used_inline = true;
		VGI_LOG(context, "vgi.scan_function.inlined",
		        {{"schema", ParentSchema().name.GetIdentifierName()},
		         {"table", name.GetIdentifierName()},
		         {"function", scan_result.function_name}});
	} else {
		auto branches_result = FetchScanBranches(context, at_unit, at_value);
		// Zero branches is legal only for scan_planning='filtered' tables, which
		// never reach here; for a 'bind' table fail rather than return no rows.
		if (branches_result.branches.empty()) {
			throw BinderException(vgi::ErrorInfo(vgi::error_subtype::kProtocolViolation)
			                          .Worker(attach_params->worker_path())
			                          .Set(vgi::error_key::kField, "branches"),
			                      "VGI table returned zero scan branches [worker: " + attach_params->worker_path() +
			                          "]");
		}

		// Refuse AT (...) at bind for anything that needs the rewriter, which
		// cannot honour it; a plain single-branch scan threads AT through.
		if (!at_unit.empty() &&
		    (branches_result.branches.size() > 1 || !branches_result.branches[0].branch_filter.empty() ||
		     branches_result.branches[0].IsCatalogTable() || branches_result.branches[0].IsFormatBranch())) {
			throw BinderException(vgi::ErrorInfo(vgi::error_subtype::kUnsupported)
			                          .Set(vgi::error_key::kTable,
			                               ParentSchema().name.GetIdentifierName() + "." + name.GetIdentifierName())
			                          .Set(vgi::error_key::kOperation, "AT"),
			                      StringUtil::Format("AT (...) clauses are not supported on multi-branch VGI tables. "
			                                         "Query a specific branch's underlying function directly. "
			                                         "[table: %s.%s, branches: %d]",
			                                         ParentSchema().name.GetIdentifierName(), name.GetIdentifierName(),
			                                         static_cast<int>(branches_result.branches.size())));
		}

		// A single branch still needs the rewriter when it carries what the
		// ScanFunctionResult shape below cannot express (branch_filter, writable,
		// catalog-table or format source); collapsing it would silently drop
		// them, and a dropped branch_filter returns wrong rows.
		const bool single_branch_needs_rewriter =
		    branches_result.branches.size() == 1 &&
		    (!branches_result.branches[0].branch_filter.empty() || branches_result.branches[0].writable ||
		     branches_result.branches[0].IsCatalogTable() || branches_result.branches[0].IsFormatBranch());

		if (branches_result.branches.size() > 1 || single_branch_needs_rewriter) {
			if (generated_columns)
				throw NotImplementedException("VGI generated columns require a single worker function scan");
			// Refuse here when the rewriter is disabled rather than reach the
			// marker's execute-time backstop.
			Value mb_enabled;
			if (!context.TryGetCurrentSetting("vgi_multi_branch_scans", mb_enabled) || mb_enabled.IsNull() ||
			    !mb_enabled.GetValue<bool>()) {
				throw BinderException(
				    vgi::ErrorInfo(vgi::error_subtype::kUnsupported)
				        .Set(vgi::error_key::kTable,
				             ParentSchema().name.GetIdentifierName() + "." + name.GetIdentifierName())
				        .Set(vgi::error_key::kOperation, "SCAN")
				        .Set(vgi::error_key::kSetting, "vgi_multi_branch_scans"),
				    StringUtil::Format("Multi-branch VGI table scan disabled via vgi_multi_branch_scans=false. "
				                       "Re-enable the setting or query a specific branch directly. "
				                       "[table: %s.%s, branches: %d]",
				                       ParentSchema().name.GetIdentifierName(), name.GetIdentifierName(),
				                       static_cast<int>(branches_result.branches.size())));
			}

			vgi::RequireVgiOptimizerExtensions(context, StringUtil::Format("Multi-branch VGI table %s.%s",
			                                                               ParentSchema().name.GetIdentifierName(),
			                                                               name.GetIdentifierName()));

			// VgiMultiScanRewriter (a pre-optimize extension) rewrites the
			// marker's LogicalGet into a UNION ALL with one arm per branch.
			auto marker_bd = make_uniq<vgi::VgiMultiBranchMarkerBindData>();
			marker_bd->branches = std::move(branches_result.branches);
			marker_bd->required_extensions = std::move(branches_result.required_extensions);
			marker_bd->table_catalog_name = catalog_.GetName().GetIdentifierName();
			marker_bd->table_schema_name = ParentSchema().name.GetIdentifierName();
			marker_bd->table_name = name.GetIdentifierName();
			marker_bd->default_schema = attach_result->default_schema.value_or("");
			// Canonical columns: the rewriter reorders each arm's output to them
			// by name and NULL-fills missing ones.
			const auto &cols = GetColumns();
			marker_bd->canonical_column_names.reserve(cols.LogicalColumnCount());
			marker_bd->canonical_column_types.reserve(cols.LogicalColumnCount());
			for (auto &col : cols.Logical()) {
				marker_bd->canonical_column_names.push_back(col.Name().GetIdentifierName());
				marker_bd->canonical_column_types.push_back(col.Type());
			}

			VGI_LOG(context, "vgi.multi_scan.marker_returned",
			        {{"schema", ParentSchema().name.GetIdentifierName()},
			         {"table", name.GetIdentifierName()},
			         {"branches", std::to_string(marker_bd->branches.size())}});

			bind_data = std::move(marker_bd);
			return vgi::MakeMultiBranchMarkerFunction();
		}

		// Single branch: project branches[0] into the ScanFunctionResult shape.
		auto &branch = branches_result.branches[0];
		scan_result.function_name = std::move(branch.function_name);
		scan_result.positional_arguments = std::move(branch.positional_arguments);
		scan_result.named_arguments = std::move(branch.named_arguments);
		// required_extensions lives on the ScanBranchesResult, not the branch.
		scan_result.required_extensions = std::move(branches_result.required_extensions);
	}
	(void)used_inline;

	for (auto &ext : scan_result.required_extensions) {
		ExtensionHelper::TryAutoLoadExtension(context, ext);
	}

	vector<std::pair<string, Value>> named_args_vec;
	for (auto &[k, v] : scan_result.named_arguments) {
		named_args_vec.emplace_back(k, v);
	}

	// Capabilities of the resolved scan function's catalog entry.
	bool has_projection_pushdown = false;
	bool has_filter_pushdown = false;
	bool has_sampling_pushdown = false;
	bool has_late_materialization = false;
	// Read off worker metadata, like late_materialization below.
	auto scan_partition_kind = vgi::VgiPartitionKind::NotPartitioned;
	// DuckDB's default, replaced below by the resolved function's declared
	// preservation, so a worker's NO_ORDER_GUARANTEE reaches table scans.
	OrderPreservationType resolved_order_preservation = OrderPreservationType::INSERTION_ORDER;
	bool order_preservation_set = false;
	std::vector<std::string> scan_filter_semantic_profiles;
	std::vector<vgi::VgiFilterFunctionCapability> scan_additional_filter_functions;
	std::vector<vgi::VgiSecretRequirement> scan_required_secrets;
	// Schema the scan function resolved in; the bind request must name it, as
	// one function name may exist in several schemas.
	std::string scan_function_schema;
	const auto default_schema = vgi_catalog.attach_result()->default_schema.value_or("");
	// Resolution order and rules: ResolveVgiTableFunction. allow_system_catalog
	// enables native delegation (read_parquet, iceberg_scan).
	vgi::VgiTableFunctionRequest scan_request;
	scan_request.catalog = &catalog_;
	scan_request.table_schema = ParentSchema().name.GetIdentifierName();
	scan_request.table_name = name.GetIdentifierName();
	scan_request.role = "scan";
	scan_request.function_name = scan_result.function_name;
	scan_request.worker_schema = scan_result.schema_name;
	scan_request.default_schema = default_schema;
	scan_request.allow_system_catalog = true;
	auto resolved_scan = vgi::ResolveVgiTableFunction(context, scan_request);
	optional_ptr<TableFunctionCatalogEntry> func_entry = resolved_scan.entry;
	scan_function_schema = resolved_scan.schema_name;
	const bool from_system_catalog = resolved_scan.from_system_catalog;

	if (from_system_catalog && func_entry) {
		if (generated_columns)
			throw NotImplementedException("VGI generated columns require a worker function scan");
		vgi::RequireVgiOptimizerExtensions(context,
		                                   StringUtil::Format("VGI table %s.%s (native delegation to '%s')",
		                                                      ParentSchema().name.GetIdentifierName(),
		                                                      name.GetIdentifierName(), scan_result.function_name));
		// Native delegation: bind the system function eagerly and return the
		// marker (VgiNativeDelegationMarkerBindData). Same binding as
		// VgiMultiScanRewriter's BindBranchArm, for the single-branch path.
		vector<unique_ptr<Expression>> positional_arguments;
		for (const auto &value : scan_result.positional_arguments)
			positional_arguments.push_back(make_uniq<BoundConstantExpression>(value));
		vector<pair<Identifier, unique_ptr<Expression>>> named_arguments;
		for (const auto &item : scan_result.named_arguments)
			named_arguments.emplace_back(Identifier(item.first), make_uniq<BoundConstantExpression>(item.second));
		vector<Value> parameters;
		named_argument_map_t named_parameters;
		FunctionBinder binder(context);
		ErrorData error;
		auto overload = binder.BindFunction(func_entry->name, func_entry->functions, positional_arguments,
		                                    named_arguments, parameters, named_parameters, error);
		if (!overload.IsValid())
			error.Throw();
		BoundTableFunction native_tf(func_entry->functions.GetFunctionByOffset(overload.GetIndex()));
		native_tf.GetSignature().FillNamedDefaults(context, named_parameters);
		native_tf.SetCallArguments(parameters, named_parameters);
		vector<LogicalType> input_table_types;
		vector<Identifier> input_table_names;
		TableFunctionRef ref;
		TableFunctionBindInput bind_input(parameters, named_parameters, input_table_types, input_table_names,
		                                  native_tf.function_info.get(), nullptr, native_tf, ref);
		vector<LogicalType> return_types;
		vector<Identifier> return_names;
		if (!native_tf.bind)
			throw NotImplementedException("VGI native scan delegation requires a table-function bind callback");
		auto native_bind = native_tf.bind(context, bind_input, return_types, return_names);
		virtual_column_map_t native_virtual_columns;
		if (native_tf.get_virtual_columns) {
			native_virtual_columns = native_tf.get_virtual_columns(context, native_bind.get());
		}

		// The declared columns must match the native bind's output by position
		// and name: the LogicalGet's column_ids / table_filters keys come from
		// the catalog columns and would misroute after the rewrite. (Typical
		// cause: the worker's schema omits Hive-partition columns read_parquet
		// appends.)
		{
			const auto &decl_columns = GetColumns();
			const auto decl_count = decl_columns.LogicalColumnCount();
			if (decl_count != return_names.size()) {
				throw BinderException(
				    vgi::ErrorInfo(vgi::error_subtype::kSchemaMismatch)
				        .Set(vgi::error_key::kCatalog, catalog_.GetName().GetIdentifierName())
				        .Set(vgi::error_key::kTable,
				             ParentSchema().name.GetIdentifierName() + "." + name.GetIdentifierName())
				        .Set(vgi::error_key::kFunctionName, scan_result.function_name)
				        .Worker(attach_params->worker_path())
				        .ExpectedActual(std::to_string(decl_count), std::to_string(return_names.size())),
				    StringUtil::Format("VGI native delegation for '%s.%s.%s' (function '%s'): catalog declares "
				                       "%llu column(s) but the native bind returned %llu. The catalog's columns "
				                       "must match exactly what the native function emits at scan time (positions "
				                       "+ names). Common cause: Hive-partition columns that read_parquet appends "
				                       "but the worker's schema source omitted.",
				                       catalog_.GetName().GetIdentifierName(), ParentSchema().name.GetIdentifierName(),
				                       name.GetIdentifierName(), scan_result.function_name,
				                       static_cast<unsigned long long>(decl_count),
				                       static_cast<unsigned long long>(return_names.size())));
			}
			for (idx_t i = 0; i < decl_count; ++i) {
				const auto &decl_name = decl_columns.GetColumn(LogicalIndex(i)).Name().GetIdentifierName();
				if (decl_name != return_names[i].GetIdentifierName()) {
					throw BinderException(
					    vgi::ErrorInfo(vgi::error_subtype::kSchemaMismatch)
					        .Set(vgi::error_key::kCatalog, catalog_.GetName().GetIdentifierName())
					        .Set(vgi::error_key::kTable,
					             ParentSchema().name.GetIdentifierName() + "." + name.GetIdentifierName())
					        .Set(vgi::error_key::kFunctionName, scan_result.function_name)
					        .Worker(attach_params->worker_path())
					        .Set(vgi::error_key::kColumnIndex, static_cast<int64_t>(i))
					        .ExpectedActual(decl_name, return_names[i].GetIdentifierName()),
					    StringUtil::Format("VGI native delegation for '%s.%s.%s' (function '%s'): catalog "
					                       "declared column %llu as '%s' but the native bind returned '%s'. "
					                       "Names must match by position.",
					                       catalog_.GetName().GetIdentifierName(),
					                       ParentSchema().name.GetIdentifierName(), name.GetIdentifierName(),
					                       scan_result.function_name, static_cast<unsigned long long>(i), decl_name,
					                       return_names[i]));
				}
			}
		}

		auto function_name_log = scan_result.function_name;
		bind_data = make_uniq<VgiNativeDelegationMarkerBindData>(
		    *this, std::move(native_tf), std::move(native_bind), std::move(return_types), std::move(return_names),
		    std::move(native_virtual_columns), std::move(scan_result), attach_params->worker_path());

		VGI_LOG(context, "vgi.scan_function.native_delegation_marker",
		        {{"schema", ParentSchema().name.GetIdentifierName()},
		         {"table", name.GetIdentifierName()},
		         {"function", function_name_log}});
		return MakeNativeDelegationMarkerFunction();
	}
	if (func_entry) {
		for (auto &tf : func_entry->functions.functions) {
			has_projection_pushdown = has_projection_pushdown || tf->projection_pushdown;
			has_filter_pushdown = has_filter_pushdown || tf->filter_pushdown;
			has_sampling_pushdown = has_sampling_pushdown || tf->sampling_pushdown;
			// First overload wins: order preservation is per-FunctionInfo.
			if (!order_preservation_set) {
				resolved_order_preservation = tf->order_preservation_type;
				order_preservation_set = true;
			}
			if (tf->function_info) {
				auto &vgi_tf_info = tf->function_info->Cast<vgi::VgiTableFunctionInfo>();
				scan_required_secrets = vgi_tf_info.function_info().required_secrets;
				scan_filter_semantic_profiles = vgi_tf_info.function_info().filter_semantic_profiles;
				scan_additional_filter_functions = vgi_tf_info.function_info().additional_filter_functions;
				// Worker metadata only (not on DuckDB's TableFunction): the
				// synthetic vgi_table_scan below is built fresh.
				has_late_materialization =
				    has_late_materialization || vgi_tf_info.function_info().late_materialization.value_or(false);
				if (vgi_tf_info.function_info().partition_kind != vgi::VgiPartitionKind::NotPartitioned) {
					scan_partition_kind = vgi_tf_info.function_info().partition_kind;
				}
			}
		}
	}

	auto scan_bind_data = make_uniq<vgi::VgiTableFunctionBindData>();
	scan_bind_data->attach_params = attach_params;
	scan_bind_data->attach_opaque_data = attach_result->attach_opaque_data;
	auto &vgi_tx = VgiTransaction::Get(context, catalog_);
	scan_bind_data->transaction_opaque_data = vgi_tx.GetTransactionOpaqueData();
	scan_bind_data->function_name = scan_result.function_name;
	// Empty when the scan resolved to a built-in DuckDB function from the
	// system catalog — those never reach a VGI worker.
	scan_bind_data->schema_name = scan_function_schema;
	scan_bind_data->arguments =
	    vgi::BuildArgumentsFromValues(context, scan_result.positional_arguments, named_args_vec);
	scan_bind_data->projection_pushdown = has_projection_pushdown;
	scan_bind_data->filter_semantic_profiles = scan_filter_semantic_profiles;
	scan_bind_data->additional_filter_functions = scan_additional_filter_functions;
	scan_bind_data->required_secrets = scan_required_secrets;

	// For get_bind_info and the (de)serialize callbacks.
	scan_bind_data->table_entry = this;

	// Kept so (de)serialize rebuilds the same branch resolution.
	scan_bind_data->at_unit = at_unit;
	scan_bind_data->at_value = at_value;

	// PartitionColumns: the kind and column indices read by VgiGetPartitionInfo
	// and VgiGetPartitionData, as VgiCatalogTableFunctionBind sets them for the
	// function-call path. Indices come from the table's own arrow schema and are
	// worker-schema positions (rowid included), the space VgiGetPartitionData uses.
	if (generated_columns && scan_partition_kind != vgi::VgiPartitionKind::NotPartitioned) {
		throw NotImplementedException("VGI partitioned generated-column scans require a qualified adapter");
	}
	if (scan_partition_kind != vgi::VgiPartitionKind::NotPartitioned && table_info_.arrow_schema) {
		for (int i = 0; i < table_info_.arrow_schema->num_fields(); ++i) {
			auto meta = table_info_.arrow_schema->field_metadata(i);
			auto idx = meta.FindKey("vgi.partition_column");
			if (idx >= 0 && meta.value(idx) == "true") {
				scan_bind_data->partition_column_indices.push_back(static_cast<idx_t>(i));
			}
		}
		// A declared kind with no annotated field is a worker bug; degrade to
		// NOT_PARTITIONED rather than letting the planner pick a partitioned
		// aggregate we cannot answer for.
		if (!scan_bind_data->partition_column_indices.empty()) {
			scan_bind_data->partition_kind = scan_partition_kind;
		} else {
			scan_partition_kind = vgi::VgiPartitionKind::NotPartitioned;
		}
	}

	if (table_info_.row_id_column >= 0) {
		scan_bind_data->rowid_worker_col_index = worker_column(table_info_.row_id_column);
		scan_bind_data->rowid_type = table_info_.rowid_type;
	}

	vector<LogicalType> return_types;
	vector<Identifier> names;
	vgi::PerformVgiTableFunctionBind(context, *scan_bind_data, return_types, names);

	// DuckDB plans this scan from GetColumns(), not the bind result, and
	// ArrowToDuckDB indexes worker batch children by those column ids: a bind
	// with *fewer* physical columns than the catalog would read past the batch's
	// children (a crash on worker data). Only that floor is enforced: names may
	// differ (virtual tables rename), extra bound columns go unused, generated
	// columns are excluded (PhysicalColumnCount), and the rowid is in neither
	// list (ApplyBindResultToBindData erases it from `names`).
	{
		const auto scannable_count = GetColumns().PhysicalColumnCount();
		if (names.size() < scannable_count) {
			throw BinderException(
			    vgi::ErrorInfo(vgi::error_subtype::kSchemaMismatch)
			        .Set(vgi::error_key::kCatalog, catalog_.GetName().GetIdentifierName())
			        .Set(vgi::error_key::kTable,
			             ParentSchema().name.GetIdentifierName() + "." + name.GetIdentifierName())
			        .Set(vgi::error_key::kFunctionName, scan_result.function_name)
			        .Worker(attach_params->worker_path())
			        .ExpectedActual(std::to_string(scannable_count), std::to_string(names.size())),
			    StringUtil::Format("VGI worker returned a bind output_schema with %llu column(s) but the catalog "
			                       "advertises %llu for '%s.%s.%s' (function '%s'). The bind output schema must "
			                       "cover every column the table exposes. A mismatch usually means the worker's table "
			                       "listing and its scan bind disagree — e.g. a proxy narrowed one but not the other.",
			                       static_cast<unsigned long long>(names.size()),
			                       static_cast<unsigned long long>(scannable_count),
			                       catalog_.GetName().GetIdentifierName(), ParentSchema().name.GetIdentifierName(),
			                       name.GetIdentifierName(), scan_result.function_name));
		}
	}

	// Inlined Table.cardinality_estimate / cardinality_max skip the per-bind
	// table_function_cardinality RPC. Must run after PerformVgiTableFunctionBind,
	// which resets cardinality_estimate to -1.
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
		        {{"schema", ParentSchema().name.GetIdentifierName()},
		         {"table", name.GetIdentifierName()},
		         {"estimate", std::to_string(scan_bind_data->cardinality_estimate)},
		         {"max", std::to_string(scan_bind_data->cardinality_max)}});
	}

	bind_data = std::move(scan_bind_data);

	TableFunction func("vgi_table_scan", {}, vgi::VgiTableFunctionScan, nullptr, vgi::VgiTableFunctionInitGlobal,
	                   vgi::VgiTableFunctionInitLocal);
	// DuckDB must project away generated fields even when the worker cannot.
	// The shared init callbacks translate logical IDs to compact physical IDs.
	func.projection_pushdown = generated_columns || has_projection_pushdown;
	const auto supports_filter_v2 =
	    std::find(scan_filter_semantic_profiles.begin(), scan_filter_semantic_profiles.end(),
	              "vgi.duckdb.standard.v1") != scan_filter_semantic_profiles.end();
	// Generated-table optimizer callbacks still use logical coordinates; keep
	// those filters/order/statistics local until their remapping is qualified.
	func.filter_pushdown = !generated_columns && has_filter_pushdown && supports_filter_v2;
	VgiDisableRuntimeFilterPushdown(func);
	func.sampling_pushdown = has_sampling_pushdown;
	func.order_preservation_type = resolved_order_preservation;
	if (func.filter_pushdown) {
		func.pushdown_expression = vgi::VgiPushdownExpression;
	}
	func.cardinality = vgi::VgiTableFunctionCardinality;
	func.table_scan_progress = vgi::VgiTableFunctionProgress;
	func.to_string = vgi::VgiTableFunctionToString;
	func.get_metrics = vgi::VgiTableFunctionGetMetrics;
	func.get_bind_info = vgi::VgiTableScanGetBindInfo;
	func.get_table_entry = vgi::VgiTableScanGetTableEntry;
	if (!generated_columns) {
		func.set_scan_order = vgi::VgiSetScanOrder;
		func.statistics = vgi::VgiTableFunctionStatistics;
	}
	// See VgiTableScanSerialize (header).
	func.serialize = VgiTableScanSerialize;
	func.deserialize = VgiTableScanDeserialize;
	// Deliberately not INITIALIZE_ON_SCHEDULE: it would run init_global before a
	// hash join's build side fills dynamic_filters, losing join-key pushdown
	// (same rule as VgiTableFunctionSet).

	// Generated tables expose an empty virtual for COUNT/constant scans so the
	// engine never selects an unexpanded generated field merely to count rows.
	if (generated_columns || table_info_.row_id_column >= 0) {
		func.get_virtual_columns = vgi::VgiTableScanGetVirtualColumns;
	}
	if (table_info_.row_id_column >= 0) {
		func.get_row_id_columns = vgi::VgiTableScanGetRowIdColumns;

		// Late materialization needs the capability plus filter and projection
		// pushdown (else the re-scan reads the whole table), and the rowid
		// callbacks registered just above: never set it without them.
		func.late_materialization =
		    !generated_columns && has_late_materialization && has_filter_pushdown && has_projection_pushdown;
	}

	// Without get_partition_info the planner never considers a partitioned
	// aggregate; get_partition_data supplies the per-column (min, max).
	if (scan_partition_kind != vgi::VgiPartitionKind::NotPartitioned) {
		func.get_partition_info = vgi::VgiGetPartitionInfo;
		func.get_partition_data = vgi::VgiGetPartitionData;
	}

	return func;
}

void VgiTableEntry::VgiTableScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                                          const BoundTableFunction &function) {
	// Catalog identity and AT clause; accepted projections and query-scoped
	// snapshots also keep their exact bind state, so a copy cannot silently
	// switch provider state.
	auto &bind_data = bind_data_p->Cast<vgi::VgiTableFunctionBindData>();
	if (!bind_data.table_entry) {
		// Only the catalog-scan path installs these callbacks and it always
		// sets table_entry: a wiring bug.
		throw SerializationException("vgi_table_scan cannot be serialized without an originating table entry");
	}
	auto &table = *bind_data.table_entry;
	serializer.WriteProperty(100, "catalog", table.ParentCatalog().GetName().GetIdentifierName());
	serializer.WriteProperty(101, "schema", table.ParentSchema().name.GetIdentifierName());
	serializer.WriteProperty(102, "table", table.name.GetIdentifierName());
	serializer.WriteProperty(103, "at_unit", bind_data.at_unit);
	serializer.WriteProperty(104, "at_value", bind_data.at_value);
	serializer.WritePropertyWithDefault(
	    105, "projection_request",
	    vector<uint8_t>(bind_data.projection_request_bytes.begin(), bind_data.projection_request_bytes.end()));
	serializer.WritePropertyWithDefault(
	    106, "projection_response",
	    vector<uint8_t>(bind_data.projection_response_bytes.begin(), bind_data.projection_response_bytes.end()));
	const auto stable = bind_data.bind_result.repeatability == vgi::BindRepeatability::STABLE_WITHIN_QUERY;
	if (stable &&
	    (bind_data.bind_result.bind_request_bytes.empty() || bind_data.bind_result.bind_response_bytes.empty())) {
		throw SerializationException("Repeatable VGI scan is missing its original bind state");
	}
	serializer.WritePropertyWithDefault(107, "repeatable_bind_request",
	                                    stable ? vector<uint8_t>(bind_data.bind_result.bind_request_bytes.begin(),
	                                                             bind_data.bind_result.bind_request_bytes.end())
	                                           : vector<uint8_t>());
	serializer.WritePropertyWithDefault(108, "repeatable_bind_result",
	                                    stable ? vector<uint8_t>(bind_data.bind_result.bind_response_bytes.begin(),
	                                                             bind_data.bind_result.bind_response_bytes.end())
	                                           : vector<uint8_t>());
}

unique_ptr<FunctionData> VgiTableEntry::VgiTableScanDeserialize(Deserializer &deserializer,
                                                                BoundTableFunction &function) {
	auto catalog = deserializer.ReadProperty<string>(100, "catalog");
	auto schema = deserializer.ReadProperty<string>(101, "schema");
	auto table = deserializer.ReadProperty<string>(102, "table");
	auto at_unit = deserializer.ReadProperty<string>(103, "at_unit");
	auto at_value = deserializer.ReadProperty<string>(104, "at_value");
	auto projection_request = deserializer.ReadPropertyWithDefault<vector<uint8_t>>(105, "projection_request");
	auto projection_response = deserializer.ReadPropertyWithDefault<vector<uint8_t>>(106, "projection_response");
	auto repeatable_request = deserializer.ReadPropertyWithDefault<vector<uint8_t>>(107, "repeatable_bind_request");
	auto repeatable_result = deserializer.ReadPropertyWithDefault<vector<uint8_t>>(108, "repeatable_bind_result");
	auto &context = deserializer.Get<ClientContext &>();

	auto &catalog_entry =
	    Catalog::GetEntry<TableCatalogEntry>(context, Identifier(catalog), Identifier(schema), Identifier(table));
	auto &vgi_entry = catalog_entry.Cast<VgiTableEntry>();

	// Re-bind and overwrite `function`: LogicalGet::Deserialize reads
	// get_virtual_columns from the reassigned function afterwards, so the copy
	// matches a fresh bind.
	unique_ptr<FunctionData> bind_data;
	function = BoundTableFunction(vgi_entry.GetScanFunctionImpl(context, bind_data, at_unit, at_value));
	if (!projection_request.empty() || !projection_response.empty()) {
		vgi::RestoreProjectionAcceptance(context, bind_data->Cast<vgi::VgiTableFunctionBindData>(), function,
		                                 projection_request, projection_response);
	}
	if (!repeatable_request.empty() || !repeatable_result.empty()) {
		auto &restored = bind_data->Cast<vgi::VgiTableFunctionBindData>().bind_result;
		// This engine's table-plan copy rebinds. Do not silently replace the
		// original snapshot, including when a projection has replaced its state.
		if (repeatable_request.empty() || repeatable_result.empty() ||
		    restored.repeatability != vgi::BindRepeatability::STABLE_WITHIN_QUERY ||
		    !std::equal(repeatable_request.begin(), repeatable_request.end(), restored.bind_request_bytes.begin(),
		                restored.bind_request_bytes.end()) ||
		    !std::equal(repeatable_result.begin(), repeatable_result.end(), restored.bind_response_bytes.begin(),
		                restored.bind_response_bytes.end())) {
			throw SerializationException("Repeatable VGI source bind state changed; bind a fresh query");
		}
	}
	return bind_data;
}

TableStorageInfo VgiTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo info;
	info.cardinality = 0; // Unknown

	// PK / UNIQUE constraints as index_info entries: the ON CONFLICT binder
	// requires a matching UNIQUE index. They are not real indexes; the worker
	// does conflict detection.
	//
	// Worker indices are Arrow-schema positions (rowid included); shift down
	// past the rowid. A rowid inside a key is bad worker input: skip it (and
	// drop an emptied key) rather than throw, since duckdb_tables() calls this
	// for every table.
	auto add_index = [&](const std::vector<int> &cols, bool is_primary) {
		IndexInfo idx_info;
		idx_info.is_unique = true;
		idx_info.is_primary = is_primary;
		idx_info.is_foreign = false;
		for (auto col : cols) {
			if (table_info_.row_id_column >= 0 && col == table_info_.row_id_column) {
				continue;
			}
			auto adjusted = (table_info_.row_id_column >= 0 && col > table_info_.row_id_column) ? col - 1 : col;
			if (adjusted < 0) {
				throw InvalidInputException("VGI: %s constraint on table '%s' references column index %d",
				                            is_primary ? "PRIMARY KEY" : "UNIQUE", name.GetIdentifierName(), col);
			}
			idx_info.column_set.insert(NumericCast<column_t>(adjusted));
		}
		if (!idx_info.column_set.empty()) {
			info.index_info.push_back(std::move(idx_info));
		}
	};
	for (auto &pk : table_info_.primary_key_constraints) {
		add_index(pk, true);
	}
	for (auto &unique : table_info_.unique_constraints) {
		add_index(unique, false);
	}

	return info;
}

virtual_column_map_t VgiTableEntry::GetVirtualColumns() const {
	virtual_column_map_t result;
	if (GetColumns().PhysicalColumnCount() != GetColumns().LogicalColumnCount()) {
		result.insert({COLUMN_IDENTIFIER_EMPTY, TableColumn("", GetColumns().GetColumn(PhysicalIndex(0)).Type())});
	}
	if (rowid_type_ != LogicalType::INVALID) {
		result.insert({COLUMN_IDENTIFIER_ROW_ID, TableColumn("rowid", rowid_type_)});
	}
	return result;
}

#if VGI_ENGINE_HAS_DELETE_RETURNING_VIRTUAL_COLUMNS
virtual_column_map_t VgiTableEntry::GetDeleteReturningVirtualColumns() const {
	// DELETE RETURNING rows carry only user columns: the rowid is absent and
	// response order does not map back to the input identifiers.
	return {};
}
#endif

vector<column_t> VgiTableEntry::GetRowIdColumns() const {
	if (rowid_type_ != LogicalType::INVALID) {
		return {COLUMN_IDENTIFIER_ROW_ID};
	}
	return {};
}

} // namespace duckdb
