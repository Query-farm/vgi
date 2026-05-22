// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#define DUCKDB_EXTENSION_MAIN

#include "vgi_extension.hpp"

#include "vgi_platform.hpp"

#include <cerrno>
#include <mutex>
#include <set>
#if VGI_POSIX_TRANSPORT
#include <signal.h>
#endif

#include "duckdb.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/common/arrow/arrow_type_extension.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "query_farm_telemetry.hpp"
#include "vgi_multi_scan_rewriter.hpp"
#ifdef __EMSCRIPTEN__
#include "vgi_wasm_async_pool.hpp"
#endif
#include "storage/vgi_catalog.hpp"
#include "storage/vgi_transaction.hpp"
#include "vgi_cancel_dispatcher.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_catalogs.hpp"
#include "vgi_cookie_jar.hpp"
#include "vgi_logging.hpp"
#include "vgi_aggregate_function_impl.hpp"
#include "vgi_oauth.hpp"
#include "vgi_profiling.hpp"
#include "vgi_streaming_window_operator.hpp"
#include "vgi_table_buffering_impl.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#ifndef __EMSCRIPTEN__
#include "vgi_subprocess.hpp"
#endif
#include "vgi_table_function.hpp"
#include "vgi_table_function_impl.hpp"
#include "vgi_transport.hpp"
#include "vgi_worker_pool.hpp"
#include "vgi_table_statistics_function.hpp"
#include "vgi_table_branches_function.hpp"
#include "vgi_clear_cache.hpp"
#include "vgi_worker_pool_functions.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/table_function.hpp"

#define VGI_EXTENSION_VERSION "2026011201"

namespace duckdb {

// Forward declarations — functions defined below after VgiStorageExtension class
static unique_ptr<BaseSecret> VgiCreateSecret(ClientContext &context, CreateSecretInput &input);
static unique_ptr<Catalog> VgiCatalogAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                            AttachedDatabase &db, const string &name, AttachInfo &info,
                                            AttachOptions &options);
static unique_ptr<TransactionManager> CreateVgiTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                  AttachedDatabase &db, Catalog &catalog);

// ============================================================================
// VgiJoinOptimizer — auto-raise InFilter threshold for queries with VGI scans
// ============================================================================
// When DuckDB joins a local table against a VGI remote table, the hash join
// can push an InFilter containing the build-side's distinct key values to the
// probe-side scan. DuckDB's default threshold (dynamic_or_filter_threshold=50)
// is too low for remote scans where network savings justify sending more keys.
// This optimizer raises the threshold to vgi_join_keys_limit when a VGI scan
// is detected in the plan.

class VgiJoinOptimizer : public OptimizerExtension {
public:
	VgiJoinOptimizer() {
		pre_optimize_function = Optimize;
	}

private:
	static bool IsVgiScan(LogicalOperator &op) {
		if (op.type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = op.Cast<LogicalGet>();
			return get.function.function == vgi::VgiTableFunctionScan;
		}
		return false;
	}

	static bool SubtreeContainsVgiScan(LogicalOperator &op) {
		if (IsVgiScan(op)) {
			return true;
		}
		for (auto &child : op.children) {
			if (SubtreeContainsVgiScan(*child)) {
				return true;
			}
		}
		return false;
	}

	//! Check if the plan has a comparison join where any child subtree contains a VGI scan.
	//! Only raises the threshold when there's an actual join involving VGI — a plain
	//! SELECT * FROM vgi_table doesn't trigger it.
	static bool HasJoinWithVgiScan(LogicalOperator &op) {
		if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
		    op.type == LogicalOperatorType::LOGICAL_ANY_JOIN) {
			for (auto &child : op.children) {
				if (SubtreeContainsVgiScan(*child)) {
					return true;
				}
			}
		}
		for (auto &child : op.children) {
			if (HasJoinWithVgiScan(*child)) {
				return true;
			}
		}
		return false;
	}

	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
		if (!HasJoinWithVgiScan(*plan)) {
			return;
		}

		Value limit_val;
		if (!input.context.TryGetCurrentSetting("vgi_join_keys_limit", limit_val) || limit_val.IsNull()) {
			return;
		}
		auto vgi_limit = limit_val.GetValue<idx_t>();
		if (vgi_limit == 0) {
			return; // disabled
		}

		// Only raise, never lower a user-set threshold
		auto current = Settings::Get<DynamicOrFilterThresholdSetting>(input.context);
		if (current < vgi_limit) {
			auto &client_config = ClientConfig::GetConfig(input.context);
			client_config.user_settings.SetUserSetting(DynamicOrFilterThresholdSetting::SettingIndex,
			                                           Value::UBIGINT(vgi_limit));
		}
	}
};

// ============================================================================
// VgiStreamingWindowOptimizer — rewrite eligible LogicalWindow → streaming op
// ============================================================================
// Walks the plan post-optimize. For each LogicalWindow whose every window
// expression is a VGI aggregate with streaming_partitioned=true and a
// cumulative frame (UNBOUNDED PRECEDING -> CURRENT ROW), replace with our
// custom LogicalVgiStreamingWindow. Otherwise leave the LogicalWindow alone
// (it falls through to PhysicalWindow as today).
//
// All-or-nothing in v1: if any expression is ineligible, the whole node
// stays a regular LogicalWindow. Mixed-eligibility splitting is a v2 task.

class VgiStreamingWindowOptimizer : public OptimizerExtension {
public:
	VgiStreamingWindowOptimizer() {
		optimize_function = Optimize;
	}

private:
	static bool IsCumulativeFrame(const BoundWindowExpression &wexpr) {
		if (wexpr.start != WindowBoundary::UNBOUNDED_PRECEDING) {
			return false;
		}
		switch (wexpr.end) {
		case WindowBoundary::CURRENT_ROW_RANGE:
		case WindowBoundary::CURRENT_ROW_ROWS:
		case WindowBoundary::CURRENT_ROW_GROUPS:
			return true;
		default:
			return false;
		}
	}

	static bool IsStreamingEligible(const Expression &expr) {
		if (expr.GetExpressionClass() != ExpressionClass::BOUND_WINDOW) {
			return false;
		}
		auto &wexpr = expr.Cast<BoundWindowExpression>();
		if (!wexpr.aggregate) {
			return false;
		}
		auto info = wexpr.aggregate->function_info.get();
		if (!info) {
			return false;
		}
		auto vgi_info = dynamic_cast<vgi::VgiAggregateFunctionInfo *>(info);
		if (!vgi_info || !vgi_info->streaming_partitioned) {
			return false;
		}
		// Streaming-open currently ships an empty Arguments envelope; any
		// const positional would be silently dropped on the wire. Punt
		// these to PhysicalWindow until the const-arg pass-through is
		// wired (see comment in BuildStreamingOpenRequest).
		for (bool is_const : vgi_info->positional_is_const) {
			if (is_const) {
				return false;
			}
		}
		if (!IsCumulativeFrame(wexpr)) {
			return false;
		}
		// EXCLUDE clauses produce multiple subframes per row — punt for v1.
		if (wexpr.exclude_clause != WindowExcludeMode::NO_OTHER) {
			return false;
		}
		// DISTINCT, FILTER, ORDER BY in args — punt for v1, fall through to
		// PhysicalWindow which handles them.
		if (wexpr.distinct || wexpr.filter_expr || !wexpr.arg_orders.empty()) {
			return false;
		}
		return true;
	}

	static void Rewrite(unique_ptr<LogicalOperator> &op) {
		// Recurse first so children are rewritten before we look at this node.
		for (auto &child : op->children) {
			Rewrite(child);
		}
		if (op->type != LogicalOperatorType::LOGICAL_WINDOW) {
			return;
		}
		auto &window = op->Cast<LogicalWindow>();
		if (window.expressions.empty()) {
			return;
		}
		for (auto &expr : window.expressions) {
			if (!IsStreamingEligible(*expr)) {
				return; // leave the LogicalWindow alone
			}
		}

		// All expressions eligible — swap in the streaming operator.
		auto streaming_op = make_uniq<vgi::LogicalVgiStreamingWindow>(window.window_index);
		streaming_op->expressions = std::move(window.expressions);
		streaming_op->children = std::move(window.children);
		streaming_op->ResolveOperatorTypes();

		VGI_STDERR_DEBUG("[VGI] LogicalWindow rewritten -> LogicalVgiStreamingWindow "
		                 "(window_index=%llu, %zu expression(s))\n",
		                 static_cast<unsigned long long>(window.window_index),
		                 streaming_op->expressions.size());

		op = std::move(streaming_op);
	}

	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
		Value enabled_val;
		if (!input.context.TryGetCurrentSetting("vgi_streaming_window", enabled_val) ||
		    enabled_val.IsNull() || !enabled_val.GetValue<bool>()) {
			return;
		}
		Rewrite(plan);
	}
};

// ============================================================================
// VgiTableBufferingRewriter — rewrite LogicalGet → LogicalVgiTableBufferingFunction
// ============================================================================
// Walks the plan post-built-in-optimizers. For each LogicalGet whose bind_data
// is a VgiTableInOutBindData with table_buffering=true, replaces it with a
// LogicalVgiTableBufferingFunction carrying the same return types/names and
// the bind_data ownership. The LATERAL decorrelation pass has already run by
// this point, so any correlated subqueries are already DELIM_JOINs and our
// LogicalGet appears as a plain (non-projected_input) node.

class VgiTableBufferingRewriter : public OptimizerExtension {
public:
	VgiTableBufferingRewriter() {
		optimize_function = Optimize;
	}

private:
	static bool IsBufferedTableInOutGet(LogicalGet &get) {
		if (!get.bind_data) {
			return false;
		}
		auto *bd = dynamic_cast<vgi::VgiTableInOutBindData *>(get.bind_data.get());
		return bd && bd->table_buffering;
	}

	static void Rewrite(unique_ptr<LogicalOperator> &op, ClientContext &context) {
		// Recurse first so leaf-level rewrites finish before we look at this node.
		for (auto &child : op->children) {
			Rewrite(child, context);
		}
		if (op->type != LogicalOperatorType::LOGICAL_GET) {
			return;
		}
		auto &get = op->Cast<LogicalGet>();
		if (!IsBufferedTableInOutGet(get)) {
			return;
		}
		// The LogicalGet's child carries the table-input subquery for an
		// in-out function. Move it into our new extension op.
		D_ASSERT(get.children.size() <= 1);

		// LogicalGet::ResolveTypes() (duckdb/src/planner/operator/logical_get.cpp:162-177)
		// has already narrowed `get.types` to match column_ids/projection_ids
		// under projection pushdown. Pass the narrowed types as our op's
		// return_types so upstream LogicalProjection/LogicalFilter bindings
		// resolve correctly. The full worker schema lives separately in
		// all_column_types/all_column_names for filter serialization and
		// for telling the worker what the projected output_schema should be.
		auto post_proj_types = get.types;
		auto post_proj_names = vector<string>();
		const auto &all_col_ids = get.GetColumnIds();
		if (!get.projection_ids.empty()) {
			post_proj_names.reserve(get.projection_ids.size());
			for (auto proj_id : get.projection_ids) {
				const auto &idx = all_col_ids[proj_id];
				post_proj_names.push_back(idx.IsVirtualColumn() ? string("__virtual__")
				                                                : get.names[idx.GetPrimaryIndex()]);
			}
		} else if (!all_col_ids.empty()) {
			post_proj_names.reserve(all_col_ids.size());
			for (const auto &idx : all_col_ids) {
				post_proj_names.push_back(idx.IsVirtualColumn() ? string("__virtual__")
				                                                : get.names[idx.GetPrimaryIndex()]);
			}
		} else {
			post_proj_names = get.names;
		}
		D_ASSERT(post_proj_types.size() == post_proj_names.size());

		auto rewritten = make_uniq<vgi::LogicalVgiTableBufferingFunction>(
		    get.table_index, std::move(post_proj_types), std::move(post_proj_names),
		    std::move(get.bind_data));
		rewritten->children = std::move(get.children);

		// Capture pushdown state. The streaming pure-table path captures the
		// same data at InitGlobal time via TableFunctionInitInput; the buffered
		// path goes through a custom PhysicalOperator with no InitInput, so the
		// rewriter is the analogous capture point.
		rewritten->all_column_types = get.returned_types;
		rewritten->all_column_names = get.names;

		// Read pushdown capabilities from bind_data (echoed from Meta at
		// catalog-load time). The catalog set's HasTableInput branch already
		// gated `table_func.projection_pushdown=true` on this same flag, so
		// `column_ids` only contains real column indices (rather than a
		// virtual GetAnyColumn placeholder) when this is true.
		const auto *bd_for_caps =
		    static_cast<const vgi::VgiTableInOutBindData *>(rewritten->bind_data.get());
		const bool worker_supports_projection =
		    bd_for_caps && bd_for_caps->projection_pushdown;

		// projection_ids: worker-schema indices of the columns the parent op
		// actually wants. Match the streaming pure-table convention
		// (vgi_table_function_impl.cpp:1023-1042): when the function declares
		// projection_pushdown=true, use ``get.GetColumnIds()`` as the
		// projection (NOT ``get.projection_ids`` — that's a secondary
		// projection on top of column_ids; rare in practice). The streaming
		// path also handles get.projection_ids for completeness.
		if (worker_supports_projection && !all_col_ids.empty()) {
			rewritten->projection_pushdown = true;
			if (get.projection_ids.empty()) {
				rewritten->projection_ids.reserve(all_col_ids.size());
				for (const auto &idx : all_col_ids) {
					rewritten->projection_ids.push_back(static_cast<int32_t>(
					    idx.IsVirtualColumn() ? COLUMN_IDENTIFIER_ROW_ID
					                          : idx.GetPrimaryIndex()));
				}
			} else {
				rewritten->projection_ids.reserve(get.projection_ids.size());
				for (auto proj_id : get.projection_ids) {
					const auto &idx = all_col_ids[proj_id];
					rewritten->projection_ids.push_back(static_cast<int32_t>(
					    idx.IsVirtualColumn() ? COLUMN_IDENTIFIER_ROW_ID
					                          : idx.GetPrimaryIndex()));
				}
			}
		}
		// column_ids: what TableFilter col indices reference. Empty when no
		// filters were pushed. VgiSerializeFilters indexes into
		// all_column_names through this list.
		rewritten->column_ids.reserve(all_col_ids.size());
		for (const auto &idx : all_col_ids) {
			rewritten->column_ids.push_back(static_cast<int32_t>(
			    idx.IsVirtualColumn() ? COLUMN_IDENTIFIER_ROW_ID : idx.GetPrimaryIndex()));
		}

		// Serialize filters via the shared utility (declared in
		// vgi_table_function_impl.hpp). Catches and swallows
		// InvalidInputException — same convention as the streaming path:
		// unsupported filter types skip pushdown and DuckDB will filter
		// locally above us.
		if (!get.table_filters.filters.empty()) {
			try {
				auto bd = rewritten->bind_data.get();
				auto bd_typed = static_cast<vgi::VgiTableInOutBindData *>(bd);
				auto worker_path = bd_typed && bd_typed->attach_params
				                       ? bd_typed->attach_params->worker_path()
				                       : std::string{};
				// VgiSerializeFilters takes column_ids as the DuckDB-side post-
				// projection list — what filter col_idx indexes into. Use the
				// raw column_t form via GetColumnIds() which mirrors what the
				// streaming path's InitGlobal uses (input.column_ids).
				vector<column_t> col_ids_raw;
				col_ids_raw.reserve(all_col_ids.size());
				for (const auto &idx : all_col_ids) {
					col_ids_raw.push_back(idx.IsVirtualColumn() ? COLUMN_IDENTIFIER_ROW_ID
					                                            : idx.GetPrimaryIndex());
				}
				auto serialized = vgi::VgiSerializeFilters(
				    context, col_ids_raw, &get.table_filters,
				    rewritten->all_column_names, worker_path);
				rewritten->pushdown_filters = std::move(serialized.filter_bytes);
				rewritten->join_keys_buffers = std::move(serialized.join_keys_buffers);
			} catch (const InvalidInputException &) {
				// Unsupported filter — leave pushdown_filters null; DuckDB
				// will filter locally above the operator.
			}
		}

		// Pre-build the EXPLAIN summary while we still have the raw
		// TableFilter objects in scope (the wire-serialized form is opaque
		// to the C++ side). Reused later by PhysicalVgiTableBufferingFunction
		// ::ParamsToString.
		if (rewritten->projection_pushdown || !get.table_filters.filters.empty()) {
			std::string summary;
			if (rewritten->projection_pushdown) {
				summary += "Projections: [";
				for (size_t i = 0; i < rewritten->projection_ids.size(); ++i) {
					if (i > 0) {
						summary += ", ";
					}
					auto col_idx = rewritten->projection_ids[i];
					if (col_idx >= 0 && static_cast<size_t>(col_idx) < rewritten->all_column_names.size()) {
						summary += rewritten->all_column_names[col_idx];
					} else {
						summary += "<unknown>";
					}
				}
				summary += "]";
			}
			if (!get.table_filters.filters.empty()) {
				if (!summary.empty()) {
					summary += " | ";
				}
				summary += "Filters: ";
				bool first = true;
				// table_filters.filters keys are indices into column_ids (the
				// post-projection list), not direct worker-schema indices.
				// Resolve through column_ids → worker-schema → all_column_names.
				for (auto &kv : get.table_filters.filters) {
					if (!first) {
						summary += " AND ";
					}
					first = false;
					std::string name = "<unknown>";
					if (kv.first < all_col_ids.size()) {
						const auto &idx = all_col_ids[kv.first];
						if (!idx.IsVirtualColumn() &&
						    idx.GetPrimaryIndex() < rewritten->all_column_names.size()) {
							name = rewritten->all_column_names[idx.GetPrimaryIndex()];
						}
					}
					summary += kv.second->ToString(name);
				}
			}
			rewritten->explain_summary = std::move(summary);
		}

		rewritten->ResolveOperatorTypes();

		VGI_STDERR_DEBUG(
		    "[VGI] LogicalGet rewritten -> LogicalVgiTableBufferingFunction "
		    "(table_index=%llu, %zu output column(s), projection_pushdown=%d, "
		    "filter_bytes=%s)\n",
		    static_cast<unsigned long long>(get.table_index), rewritten->return_types.size(),
		    rewritten->projection_pushdown ? 1 : 0,
		    rewritten->pushdown_filters ? "yes" : "no");

		op = std::move(rewritten);
	}

	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
		// Gated on a session setting so an emergency-rollback path exists if
		// the rewriter regresses against a real workload. When disabled, the
		// loud-failure asserts in VgiTableInOutFunction/VgiTableInOutFinalize
		// will catch any table_buffering function call so users see a clear
		// error rather than the pre-fix UNION-ALL corruption.
		Value enabled_val;
		if (!input.context.TryGetCurrentSetting("vgi_table_buffering", enabled_val) ||
		    enabled_val.IsNull() || !enabled_val.GetValue<bool>()) {
			return;
		}
		Rewrite(plan, input.context);
	}
};

// Storage extension for VGI — also holds per-secret-type redact keys
class VgiStorageExtension : public StorageExtension {
public:
	explicit VgiStorageExtension(DatabaseInstance &db) : dispatcher_(db) {
		attach = VgiCatalogAttach;
		create_transaction_manager = CreateVgiTransactionManager;
	}

	vgi::VgiCancelDispatcher &GetCancelDispatcher() {
		return dispatcher_;
	}

	// Convenience: locate the VGI extension on a DatabaseInstance and
	// return its dispatcher. nullptr if the extension isn't registered.
	static vgi::VgiCancelDispatcher *FindCancelDispatcher(DatabaseInstance &db) {
		auto &config = DBConfig::GetConfig(db);
		auto ext = StorageExtension::Find(config, "vgi");
		if (!ext) {
			return nullptr;
		}
		return &static_cast<VgiStorageExtension &>(*ext).dispatcher_;
	}

	// Thread-safe registry of redact keys per secret type name.
	// Populated during ATTACH after successful secret type registration.
	void SetRedactKeys(const std::string &secret_type_name, case_insensitive_set_t keys) {
		std::lock_guard<std::mutex> lock(redact_mutex_);
		redact_keys_[secret_type_name] = std::move(keys);
	}

	case_insensitive_set_t GetRedactKeys(const std::string &secret_type_name) const {
		std::lock_guard<std::mutex> lock(redact_mutex_);
		auto it = redact_keys_.find(secret_type_name);
		if (it != redact_keys_.end()) {
			return it->second;
		}
		return {};
	}

private:
	mutable std::mutex redact_mutex_;
	std::unordered_map<std::string, case_insensitive_set_t> redact_keys_;
	vgi::VgiCancelDispatcher dispatcher_;
};

// Create function for VGI secrets — builds a KeyValueSecret from config options.
// All named parameters from CREATE SECRET are copied into the secret's key-value map.
static unique_ptr<BaseSecret> VgiCreateSecret(ClientContext &context, CreateSecretInput &input) {
	auto scope = input.scope;
	auto secret = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);

	// Look up redact keys from VgiStorageExtension (per-DatabaseInstance, not global)
	auto &config = DBConfig::GetConfig(context);
	auto vgi_ext = StorageExtension::Find(config, "vgi");
	if (vgi_ext) {
		auto &vgi_storage = static_cast<VgiStorageExtension &>(*vgi_ext);
		secret->redact_keys = vgi_storage.GetRedactKeys(input.type);
	}

	// Copy all user-provided options into the secret map
	for (const auto &entry : input.options) {
		secret->secret_map[entry.first] = entry.second;
	}

	return secret;
}

// ATTACH handler for VGI catalogs
static unique_ptr<Catalog> VgiCatalogAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                            AttachedDatabase &db, const string &name, AttachInfo &info,
                                            AttachOptions &options) {
	string worker_path;
	string catalog_name = info.path; // The first argument to ATTACH is the catalog name
	bool worker_debug = false;
	bool use_pool = true;
	int64_t pool_max_override = -1;      // -1 = not set
	int64_t pool_timeout_override = -1;  // -1 = not set
	string oauth_refresh_token;
	string bearer_token;
	string data_version_spec;
	string implementation_version;
	// Per-LOCATION launcher overrides — only valid with ``launch:`` LOCATIONs;
	// rejected at parse time for any other transport.  -1 sentinel for "unset"
	// on the integer; std::nullopt-equivalent on the string is empty.
	int64_t launcher_idle_timeout_seconds = -1;
	string launcher_state_dir;
	// Unknown-to-the-extension options are forwarded to the worker as
	// attach-time options after validation against the catalog's declared
	// AttachOptionSpec list (fetched via InvokeCatalogs when present).
	std::map<std::string, Value> attach_options;

	// Per-option handler, shared between the connection-string query (below) and
	// the explicit (TYPE vgi, ...) options clause so both honour the same names.
	auto apply_option = [&](const string &lower_name, const Value &value) {
		if (lower_name == "type") {
			return;
		} else if (lower_name == "location" || lower_name == "path") {
			worker_path = value.ToString();
		} else if (lower_name == "worker_debug") {
			// DefaultCastAs (not GetValue<bool>) so VARCHAR values from the
			// connection-string query ("true"/"false") cast semantically rather
			// than as a raw int8; typed BOOLEAN values from the clause are a no-op.
			worker_debug = value.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>();
		} else if (lower_name == "pool") {
			use_pool = value.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>();
		} else if (lower_name == "pool_max") {
			pool_max_override = value.DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>();
		} else if (lower_name == "pool_timeout") {
			pool_timeout_override = value.DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>();
		} else if (lower_name == "oauth_refresh_token") {
			oauth_refresh_token = value.ToString();
		} else if (lower_name == "bearer_token") {
			bearer_token = value.ToString();
		} else if (lower_name == "data_version_spec") {
			data_version_spec = value.ToString();
		} else if (lower_name == "implementation_version") {
			implementation_version = value.ToString();
		} else if (lower_name == "launcher_idle_timeout") {
			launcher_idle_timeout_seconds = value.DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>();
			if (launcher_idle_timeout_seconds < 0) {
				throw BinderException(
				    "launcher_idle_timeout must be >= 0 (got %lld); use 0 for no timeout",
				    static_cast<long long>(launcher_idle_timeout_seconds));
			}
		} else if (lower_name == "launcher_state_dir") {
			launcher_state_dir = value.ToString();
			if (launcher_state_dir.empty()) {
				throw BinderException("launcher_state_dir, if set, must not be empty");
			}
		} else {
			// Collect for validation + forwarding to the worker.
			attach_options.emplace(lower_name, value);
		}
	};

	// Connection-string form (mirrors the airport extension): the worker location
	// and ATTACH options may be encoded in the ATTACH path as
	//   '<catalog>?location=<worker>&<opt>=<val>&...'
	// so a catalog is reachable without an explicit options clause — e.g. from the
	// DuckDB/haybarn CLI:  haybarn-cli "vgi:example?location=https://host/vgi/".
	// The base (before '?') stays the catalog name; the query string supplies
	// options, applied first so an explicit (TYPE vgi, ...) clause still overrides.
	bool secret_in_path = false;
	{
		auto qpos = catalog_name.find('?');
		if (qpos != string::npos) {
			string query = catalog_name.substr(qpos + 1);
			catalog_name = catalog_name.substr(0, qpos);
			size_t start = 0;
			while (start <= query.size()) {
				auto amp = query.find('&', start);
				string token =
				    query.substr(start, amp == string::npos ? string::npos : amp - start);
				if (!token.empty()) {
					auto eq = token.find('=');
					string key = (eq == string::npos) ? token : token.substr(0, eq);
					string val = (eq == string::npos) ? string() : token.substr(eq + 1);
					auto lower_key = StringUtil::Lower(key);
					if (lower_key == "oauth_refresh_token" || lower_key == "bearer_token") {
						secret_in_path = true;
					}
					apply_option(lower_key, Value(val));
				}
				if (amp == string::npos) {
					break;
				}
				start = amp + 1;
			}
			// A token embedded in the ATTACH path is just as introspection-visible
			// as one in the options clause — strip the whole query from info.path.
			// Recommend CREATE SECRET / the options clause for non-ephemeral tokens.
			if (secret_in_path) {
				info.path = catalog_name + "?<redacted>";
			}
		}
	}

	// Explicit (TYPE vgi, ...) options clause. Applied after the query string so it
	// wins on conflicts; tokens are wiped from the parsed AttachInfo so they can't
	// be echoed by DuckDB introspection (catalog logging, prepared-statement dumps,
	// telemetry). The CatalogAuth holds the live token from here on.
	for (auto &entry : info.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		apply_option(lower_name, entry.second);
		if (lower_name == "oauth_refresh_token" || lower_name == "bearer_token") {
			entry.second = Value("<redacted>");
		}
	}

	// Validate mutual exclusivity of auth options
	if (!bearer_token.empty() && !oauth_refresh_token.empty()) {
		throw BinderException("Cannot specify both bearer_token and oauth_refresh_token");
	}

	if (worker_path.empty()) {
		throw BinderException("VGI ATTACH requires LOCATION option specifying the worker path");
	}

	// Launcher overrides are only meaningful for ``launch:`` LOCATIONs.
	// ``unix://`` connects to an externally-managed worker (we don't spawn,
	// don't manage state); HTTP and bare-subprocess transports don't go
	// through the launcher at all.  Reject loudly so users catch typos
	// or wrong-transport mistakes before they hit the worker.
	if ((launcher_idle_timeout_seconds >= 0 || !launcher_state_dir.empty()) &&
	    !vgi::IsLaunchLocation(worker_path)) {
		throw BinderException(
		    "launcher_idle_timeout / launcher_state_dir are only valid for `launch:` "
		    "LOCATIONs (got LOCATION=%s)",
		    worker_path);
	}

#ifdef __EMSCRIPTEN__
	// WASM: only HTTP transport is supported (no subprocess/fork)
	if (!vgi::IsHttpTransport(worker_path)) {
		throw BinderException("VGI in WASM only supports HTTP transport. "
		                      "LOCATION must be an HTTP/HTTPS URL.");
	}
	use_pool = false;
	// HTTP in WASM goes through duckdb-wasm's XHR layer, not httpfs
#else
	// HTTP transport: require httpfs for POST support, disable subprocess pooling
	if (vgi::IsHttpTransport(worker_path)) {
		try {
			ExtensionHelper::TryAutoLoadExtension(db.GetDatabase(), "httpfs");
		} catch (...) {
			// ignore auto-load errors, check below
		}
		if (!db.GetDatabase().ExtensionIsLoaded("httpfs")) {
			throw BinderException("VGI HTTP transport requires the httpfs extension. "
			                      "Install it with: INSTALL httpfs; LOAD httpfs;");
		}
		use_pool = false; // HTTP is stateless, no subprocess pooling
	}
#endif

	// Create per-catalog auth state
	std::shared_ptr<vgi::CatalogAuth> auth;
	if (!bearer_token.empty()) {
		if (!vgi::IsHttpTransport(worker_path)) {
			throw BinderException("bearer_token is only valid for HTTP transport "
			                      "(LOCATION must be an HTTP/HTTPS URL)");
		}
		auth = std::make_shared<vgi::BearerTokenCatalogAuth>(bearer_token);
	} else {
		auto oauth_auth = std::make_shared<vgi::OAuthCatalogAuth>();
		if (!oauth_refresh_token.empty()) {
			if (!vgi::IsHttpTransport(worker_path)) {
				throw BinderException("oauth_refresh_token is only valid for HTTP transport "
				                      "(LOCATION must be an HTTP/HTTPS URL)");
			}
			oauth_auth->SeedRefreshToken(oauth_refresh_token);
		}
		auth = std::move(oauth_auth);
	}

	// HTTP cookie jar — carries proxy-issued Set-Cookie / Cookie headers for
	// sticky version-aware routing. Subprocess transport doesn't use this.
	std::shared_ptr<vgi::SessionCookieJar> cookie_jar;
	if (vgi::IsHttpTransport(worker_path)) {
		cookie_jar = std::make_shared<vgi::SessionCookieJar>();
	}

	// If the user passed attach-time options, validate them against the
	// catalog's declared AttachOptionSpec list. Skip the extra RPC when
	// attach_options is empty — catalogs without options pay no overhead.
	if (!attach_options.empty()) {
		auto catalogs = vgi::InvokeCatalogs(worker_path, context, worker_debug, use_pool, auth);
		const vgi::VgiCatalogInfo *matching_info = nullptr;
		for (const auto &info_entry : catalogs) {
			if (info_entry.name == catalog_name) {
				matching_info = &info_entry;
				break;
			}
		}
		if (!matching_info) {
			throw BinderException("Worker at '%s' exposes no catalog named '%s'", worker_path, catalog_name);
		}

		auto build_accepted_list = [&]() {
			std::string joined;
			for (const auto &spec : matching_info->attach_option_specs) {
				if (!joined.empty()) {
					joined += ", ";
				}
				joined += spec.name;
			}
			return joined.empty() ? std::string("(none)") : joined;
		};

		std::map<std::string, Value> validated_options;
		for (const auto &opt : attach_options) {
			const vgi::VgiAttachOptionSpec *matching_spec = nullptr;
			for (const auto &spec : matching_info->attach_option_specs) {
				if (StringUtil::Lower(spec.name) == opt.first) {
					matching_spec = &spec;
					break;
				}
			}
			if (!matching_spec) {
				throw BinderException("Unknown ATTACH option '%s' for catalog '%s'. Accepted options: %s",
				                      opt.first, catalog_name, build_accepted_list());
			}

			Value casted;
			std::string cast_error;
			if (!opt.second.DefaultTryCastAs(matching_spec->type, casted, &cast_error)) {
				throw BinderException("Cannot cast ATTACH option '%s' to declared type %s: %s", matching_spec->name,
				                      matching_spec->type.ToString(), cast_error);
			}
			validated_options.emplace(matching_spec->name, std::move(casted));
		}
		attach_options = std::move(validated_options);
	}

	// Call catalog_attach via RPC. The worker validates data_version_spec and
	// implementation_version and throws with a human-readable message on
	// unsatisfiable requests; that surfaces as the ATTACH failure.
	std::optional<int64_t> launcher_idle_for_attach;
	std::optional<std::string> launcher_state_dir_for_attach;
	if (launcher_idle_timeout_seconds >= 0) {
		launcher_idle_for_attach = launcher_idle_timeout_seconds;
	}
	if (!launcher_state_dir.empty()) {
		launcher_state_dir_for_attach = launcher_state_dir;
	}
	auto attach_result = vgi::InvokeCatalogAttach(worker_path, catalog_name, context, worker_debug, use_pool, auth,
	                                              data_version_spec, implementation_version, cookie_jar,
	                                              attach_options, launcher_idle_for_attach,
	                                              launcher_state_dir_for_attach);

	// Register extension options for settings exposed by this catalog
	// Check for type conflicts with existing settings
	auto &config = DBConfig::GetConfig(db.GetDatabase());

	for (const auto &setting : attach_result.settings) {
		if (config.HasExtensionOption(setting.name)) {
			// Setting already exists - verify types match
			ExtensionOption existing_option;
			if (!config.TryGetExtensionOption(setting.name, existing_option)) {
				throw BinderException("Failed to retrieve existing VGI setting '%s'", setting.name);
			}
			if (existing_option.type != setting.type) {
				throw BinderException("VGI setting '%s' already exists with type %s, but catalog '%s' requires type %s",
				                      setting.name, existing_option.type.ToString(), catalog_name,
				                      setting.type.ToString());
			}
			// Types match - setting is already registered, no need to add again
		} else {
			// New setting - register it
			config.AddExtensionOption(setting.name, setting.description, setting.type, setting.default_value);
		}
	}

	// Register secret types exposed by this catalog
	auto &secret_manager = SecretManager::Get(context);
	auto vgi_ext = StorageExtension::Find(config, "vgi");
	for (const auto &st : attach_result.secret_types) {
		// Build redact keys set from parameter metadata
		case_insensitive_set_t redact_set;
		for (const auto &param : st.parameters) {
			if (param.redact) {
				redact_set.insert(param.name);
			}
		}

		// Register the secret type with DuckDB
		SecretType secret_type;
		secret_type.name = st.name;
		secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
		secret_type.default_provider = "config";
		secret_type.extension = "vgi";
		try {
			secret_manager.RegisterSecretType(secret_type);
		} catch (const InternalException &) {
			// Already registered by another VGI catalog — skip
			// Don't overwrite redact keys from the first (winning) registration
			redact_set = {};
		}

		// Store redact keys on VgiStorageExtension (only for newly registered types)
		if (vgi_ext && !redact_set.empty()) {
			static_cast<VgiStorageExtension &>(*vgi_ext).SetRedactKeys(st.name, std::move(redact_set));
		}

		// Register "config" provider create function with named parameters
		CreateSecretFunction create_func;
		create_func.secret_type = st.name;
		create_func.provider = "config";
		create_func.function = VgiCreateSecret;
		for (const auto &param : st.parameters) {
			create_func.named_parameters[param.name] = param.type;
		}
		secret_manager.RegisterSecretFunction(create_func, OnCreateConflict::IGNORE_ON_CONFLICT);
	}

	// Build per-path pool config and register with pool
	vgi::PoolSettings pool_settings;
	if (use_pool) {
		// Default per-path limit from vgi_worker_pool_max setting
		Value max_val;
		if (context.TryGetCurrentSetting("vgi_worker_pool_max", max_val)) {
			pool_settings.max_pool_size = static_cast<size_t>(max_val.GetValue<int64_t>());
		}
		Value idle_val;
		if (context.TryGetCurrentSetting("vgi_worker_pool_idle_limit_seconds", idle_val)) {
			pool_settings.idle_timeout_seconds = static_cast<size_t>(idle_val.GetValue<int64_t>());
		}
		// ATTACH options override the defaults
		if (pool_max_override >= 0) {
			pool_settings.max_pool_size = static_cast<size_t>(pool_max_override);
		}
		if (pool_timeout_override >= 0) {
			pool_settings.idle_timeout_seconds = static_cast<size_t>(pool_timeout_override);
		}
	} else {
		pool_settings.max_pool_size = 0; // disabled
	}
	vgi::VgiWorkerPool::Instance().ConfigurePath(worker_path, pool_settings);

	// Set database-level comment and tags from worker metadata
	if (!attach_result.comment.empty()) {
		db.comment = Value(attach_result.comment);
	}
	for (auto &[key, val] : attach_result.tags) {
		db.tags[key] = val;
	}

	// Surface resolved versions via duckdb_databases().tags so users can verify
	// what the worker actually picked. Namespaced with vgi_ to avoid clashing
	// with any worker-declared tags.
	if (!attach_result.resolved_data_version.empty()) {
		db.tags["vgi_resolved_data_version"] = attach_result.resolved_data_version;
	}
	if (!attach_result.resolved_implementation_version.empty()) {
		db.tags["vgi_resolved_implementation_version"] = attach_result.resolved_implementation_version;
	}

	// Create attach parameters. Resolved versions are what the worker picked;
	// they're used by the pool key so catalogs attached at different versions
	// never share a subprocess worker.  Launcher overrides flow through to
	// the cache layer where they're pinned to the worker's lifetime.
	vgi::VgiAttachParametersConfig attach_cfg;
	attach_cfg.worker_path = worker_path;
	attach_cfg.catalog_name = catalog_name;
	attach_cfg.worker_debug = worker_debug;
	attach_cfg.use_pool = use_pool;
	attach_cfg.auth = auth;
	attach_cfg.data_version_spec = attach_result.resolved_data_version;
	attach_cfg.implementation_version = attach_result.resolved_implementation_version;
	attach_cfg.cookie_jar = cookie_jar;
	if (launcher_idle_timeout_seconds >= 0) {
		attach_cfg.launcher_idle_timeout_seconds = launcher_idle_timeout_seconds;
	}
	if (!launcher_state_dir.empty()) {
		attach_cfg.launcher_state_dir = launcher_state_dir;
	}
	auto attach_params = std::make_shared<vgi::VgiAttachParameters>(std::move(attach_cfg));

	// Prime the HTTPParams cache while we're outside any VGI catalog
	// transaction. HTTPFSUtil::InitializeParameters pulls settings/secrets
	// through KeyValueSecretReader, which takes the MetaTransaction mutex —
	// doing it lazily from inside VgiTransaction::Start would self-deadlock
	// (the surrounding MetaTransaction::GetTransaction call already holds that
	// mutex). Doing it here, during ATTACH handling and before any VGI
	// transaction exists, avoids the reentrancy. TODO(#22258): remove once
	// https://github.com/duckdb/duckdb/issues/22258 is fixed.
	if (vgi::IsHttpTransport(worker_path)) {
		attach_params->GetOrInitHttpParams(context, worker_path);
	}

	auto attach_result_ptr = std::make_shared<vgi::CatalogAttachResult>(std::move(attach_result));

	// Snapshot eager-load thresholds at attach time. The runtime form is a
	// fixed struct of int64s; we never re-parse the map on the hot path.
	VgiObjectCounts eager_thresholds;
	{
		Value threshold_val;
		std::map<std::string, int64_t> threshold_map;
		if (context.TryGetCurrentSetting("vgi_eager_load_threshold", threshold_val) && !threshold_val.IsNull()) {
			auto &child_values = MapValue::GetChildren(threshold_val);
			for (auto &kv : child_values) {
				auto &kv_struct = StructValue::GetChildren(kv);
				if (kv_struct.size() != 2 || kv_struct[0].IsNull() || kv_struct[1].IsNull()) {
					continue;
				}
				threshold_map[kv_struct[0].GetValue<std::string>()] = kv_struct[1].GetValue<int64_t>();
			}
		}
		// Default for missing keys is 1000 (per setting docs); ObjectCountsFromMap fills in.
		eager_thresholds = ObjectCountsFromMap(threshold_map, 1000);
	}

	return make_uniq<VgiCatalog>(db, name, options.access_mode, std::move(attach_params),
	                              std::move(attach_result_ptr), eager_thresholds);
}

// Create transaction manager for VGI catalog
static unique_ptr<TransactionManager> CreateVgiTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                  AttachedDatabase &db, Catalog &catalog) {
	auto &vgi_catalog = catalog.Cast<VgiCatalog>();
	return make_uniq<VgiTransactionManager>(db, vgi_catalog);
}

//===--------------------------------------------------------------------===//
// vgi_oauth_logout() table function
//===--------------------------------------------------------------------===//

struct OAuthLogoutBindData : public TableFunctionData {
	std::string target_catalog; // empty = all catalogs
	bool logout_all = false;
};

struct OAuthLogoutState : public GlobalTableFunctionState {
	struct LogoutRow {
		std::string catalog_name;
		std::string status;
	};
	std::vector<LogoutRow> rows;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> OAuthLogoutBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<OAuthLogoutBindData>();
	if (input.inputs.size() > 1) {
		throw BinderException("vgi_oauth_logout expects 0 or 1 arguments (catalog name), got %d", input.inputs.size());
	}
	if (!input.inputs.empty()) {
		bind_data->target_catalog = input.inputs[0].GetValue<string>();
	} else {
		bind_data->logout_all = true;
	}
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("catalog_name");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("status");
	return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> OAuthLogoutInit(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<OAuthLogoutState>();
	auto &bind_data = input.bind_data->Cast<OAuthLogoutBindData>();

	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db : databases) {
		auto &catalog = db->GetCatalog();
		if (catalog.GetCatalogType() != "vgi") {
			continue;
		}
		auto catalog_name = catalog.GetName();
		if (!bind_data.logout_all && catalog_name != bind_data.target_catalog) {
			continue;
		}

		auto &vgi_catalog = catalog.Cast<VgiCatalog>();
		const auto &params = vgi_catalog.attach_parameters();
		if (params && params->auth()) {
			params->auth()->ClearTokens();
			state->rows.push_back({catalog_name, "logged_out"});
		}
	}

	return std::move(state);
}

static void OAuthLogoutFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<OAuthLogoutState>();
	idx_t count = 0;
	while (state.offset < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = state.rows[state.offset];
		output.SetValue(0, count, Value(row.catalog_name));
		output.SetValue(1, count, Value(row.status));
		state.offset++;
		count++;
	}
	output.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// vgi_oauth_tokens() table function
//===--------------------------------------------------------------------===//

struct OAuthTokensState : public GlobalTableFunctionState {
	struct TokenRow {
		std::string catalog_name;
		std::string origin;
		std::string status;
		bool has_expires;
		int64_t expires_in_seconds;
		bool has_refresh_token;
	};
	std::vector<TokenRow> rows;
	idx_t offset = 0;
};

static LogicalType OAuthStatusEnumType() {
	Vector values(LogicalType::VARCHAR, 3);
	values.SetValue(0, Value("active"));
	values.SetValue(1, Value("expired"));
	values.SetValue(2, Value("none"));
	return LogicalType::ENUM("vgi_oauth_status", values, 3);
}

static unique_ptr<FunctionData> OAuthTokensBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("catalog_name");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("origin");
	return_types.push_back(OAuthStatusEnumType());
	names.push_back("status");
	return_types.push_back(LogicalType::INTERVAL);
	names.push_back("expires_in");
	return_types.push_back(LogicalType::BOOLEAN);
	names.push_back("has_refresh_token");
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> OAuthTokensInit(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<OAuthTokensState>();

	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db : databases) {
		auto &catalog = db->GetCatalog();
		if (catalog.GetCatalogType() != "vgi") {
			continue;
		}
		auto &vgi_catalog = catalog.Cast<VgiCatalog>();
		const auto &params = vgi_catalog.attach_parameters();
		if (!params) {
			continue;
		}

		OAuthTokensState::TokenRow row;
		row.catalog_name = catalog.GetName();
		row.origin = vgi::ExtractOrigin(params->worker_path());

		auto &auth = params->auth();
		if (!auth) {
			row.status = "none";
			row.has_expires = false;
			row.expires_in_seconds = 0;
			row.has_refresh_token = false;
		} else {
			auto bearer = auth->GetToken();
			if (!bearer.empty()) {
				row.status = "active";
			} else {
				row.status = "expired";
			}

			vgi::CatalogAuth::TokenInfo token_info;
			if (auth->GetTokenInfo(token_info)) {
				row.has_refresh_token = token_info.has_refresh_token;
				row.has_expires = token_info.has_expires;
				row.expires_in_seconds = token_info.expires_in_seconds;
			} else {
				row.has_refresh_token = false;
				row.has_expires = false;
				row.expires_in_seconds = 0;
			}
		}

		state->rows.push_back(std::move(row));
	}

	return std::move(state);
}

static void OAuthTokensFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<OAuthTokensState>();
	idx_t count = 0;
	while (state.offset < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = state.rows[state.offset];
		output.SetValue(0, count, Value(row.catalog_name));
		output.SetValue(1, count, Value(row.origin));
		output.SetValue(2, count, Value(row.status));
		if (row.status == "active" && row.has_expires) {
			output.SetValue(3, count, Value::INTERVAL(Interval::FromMicro(row.expires_in_seconds * Interval::MICROS_PER_SEC)));
		} else {
			output.SetValue(3, count, Value());
		}
		output.SetValue(4, count, Value::BOOLEAN(row.has_refresh_token));
		state.offset++;
		count++;
	}
	output.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// vgi_catalog_identity() table function
//===--------------------------------------------------------------------===//
//
// Emits one row per attached VGI catalog showing the OIDC identity parsed
// from the OAuth id_token (if any). Catalogs without OAuth state appear
// with authenticated=false and NULL identity fields.

struct CatalogIdentityState : public GlobalTableFunctionState {
	struct IdentityRow {
		std::string catalog_name;
		std::string origin;
		bool authenticated = false;
		bool has_identity = false;
		std::string sub;
		std::string email;
		std::string name;
		std::string issuer;
		std::string claims_json;  // empty when no id_token was parsed
	};
	std::vector<IdentityRow> rows;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> CatalogIdentityBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("catalog_name");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("origin");
	return_types.push_back(LogicalType::BOOLEAN);
	names.push_back("authenticated");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("sub");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("email");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("name");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("issuer");
	// Raw decoded JWT payload — provider-specific claims (Entra preferred_username/oid/tid,
	// group/role arrays, custom attributes) reachable via JSON path expressions.
	return_types.push_back(LogicalType::JSON());
	names.push_back("claims");
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> CatalogIdentityInit(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<CatalogIdentityState>();

	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db : databases) {
		auto &catalog = db->GetCatalog();
		if (catalog.GetCatalogType() != "vgi") {
			continue;
		}
		auto &vgi_catalog = catalog.Cast<VgiCatalog>();
		const auto &params = vgi_catalog.attach_parameters();
		if (!params) {
			continue;
		}

		CatalogIdentityState::IdentityRow row;
		row.catalog_name = catalog.GetName();
		row.origin = vgi::ExtractOrigin(params->worker_path());

		// Get identity from per-catalog auth state
		auto &auth = params->auth();
		if (auth) {
			vgi::OAuthTokenSet token_copy;
			if (auth->GetTokenSetCopy(token_copy)) {
				row.authenticated = token_copy.IsValid();
				if (token_copy.identity.present) {
					row.has_identity = true;
					row.sub = token_copy.identity.sub;
					row.email = token_copy.identity.email;
					row.name = token_copy.identity.name;
					row.issuer = token_copy.identity.issuer;
					row.claims_json = token_copy.identity.claims_json;
				}
			}
		}

		state->rows.push_back(std::move(row));
	}

	return std::move(state);
}

static void CatalogIdentityFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<CatalogIdentityState>();
	idx_t count = 0;
	while (state.offset < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = state.rows[state.offset];
		output.SetValue(0, count, Value(row.catalog_name));
		output.SetValue(1, count, Value(row.origin));
		output.SetValue(2, count, Value::BOOLEAN(row.authenticated));
		output.SetValue(3, count, row.has_identity && !row.sub.empty() ? Value(row.sub) : Value());
		output.SetValue(4, count, row.has_identity && !row.email.empty() ? Value(row.email) : Value());
		output.SetValue(5, count, row.has_identity && !row.name.empty() ? Value(row.name) : Value());
		output.SetValue(6, count, row.has_identity && !row.issuer.empty() ? Value(row.issuer) : Value());
		// The claims column is typed JSON — DuckDB stores JSON as VARCHAR with an alias,
		// so a plain Value(string) lands correctly and returns as JSON on read.
		output.SetValue(7, count,
		                row.has_identity && !row.claims_json.empty() ? Value(row.claims_json) : Value());
		state.offset++;
		count++;
	}
	output.SetCardinality(count);
}

static void LoadInternal(ExtensionLoader &loader) {
#if defined(__EMSCRIPTEN__) && VGI_ASYNC_INIT_ENABLED
	// Pre-spawn a bounded pool of background workers at extension load. Required
	// under MAIN_MODULE=1 + pthreads: pthread_create after side-modules are
	// dlopen'd is unreliable (emsdk #19425/#19199/#13303), so we spawn once at
	// load time and keep the workers parked in cv_.wait() until tasks arrive.
	// Pool size must fit within PTHREAD_POOL_SIZE alongside DuckDB's worker
	// threads — see duckdb-wasm/lib/CMakeLists.txt.
	//
	// Only spawned when VGI_ASYNC_INIT_ENABLED == 1 (off by default on WASM).
	// With async init disabled, the pool would sit idle forever — skip it.
	duckdb::vgi::VgiWasmAsyncPool::Instance().EnsureStarted(3);
#endif
#if VGI_POSIX_TRANSPORT
	// Ignore SIGPIPE - we handle broken pipes via EPIPE error from write()
	// This prevents the process from being killed when a worker dies unexpectedly.
	// Windows has no SIGPIPE; the (future) Win32 subprocess backend maps
	// ERROR_BROKEN_PIPE to the same EPIPE-equivalent status instead.
	struct sigaction sa;
	sa.sa_handler = SIG_IGN;
	sigemptyset(&sa.sa_mask);
	sa.sa_flags = 0;
	if (sigaction(SIGPIPE, &sa, nullptr) == -1) {
		VGI_STDERR_DEBUG("[VGI] extension.load sigpipe_ignore_failed errno=%d\n", errno);
	}
#endif // VGI_POSIX_TRANSPORT

	// Register profiling atexit handler if VGI_PROFILE is enabled
	vgi::VgiProfileRegisterAtExit();

	// Register VGI log type
	auto &log_manager = loader.GetDatabaseInstance().GetLogManager();
	log_manager.RegisterLogType(make_uniq<VgiLogType>());

	QueryFarmSendTelemetry(loader, "vgi", VGI_EXTENSION_VERSION);

	// Register VGI storage extension
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	StorageExtension::Register(config, "vgi", make_shared_ptr<VgiStorageExtension>(loader.GetDatabaseInstance()));

	// -------------------------------------------------------------------------
	// Register native GeoArrow Arrow extension types
	// -------------------------------------------------------------------------
	// Enable round-tripping GEOMETRY through Arrow IPC as native struct/list
	// layouts instead of opaque WKB blobs.  Each entry maps a GeoArrow
	// extension name to a DuckDB struct type and a GeometryStorageType used
	// by the spatial extension's Geometry::ToVectorizedFormat /
	// FromVectorizedFormat for conversion.
	//
	// ArrowToDuck (worker → client): struct vector → GEOMETRY via cast
	// DuckToArrow (client → worker): GEOMETRY → struct vector via ToVectorizedFormat
	{
		auto xy = LogicalType::STRUCT({{"x", LogicalType::DOUBLE}, {"y", LogicalType::DOUBLE}});

		struct NativeGeoArrow {
			static void ArrowToDuck(ClientContext &context, Vector &source, Vector &result, idx_t count) {
				auto &cast_set = CastFunctionSet::Get(context);
				GetCastFunctionInput cast_input(context);
				auto info = cast_set.GetCastFunction(source.GetType(), result.GetType(), cast_input);
				CastParameters params(info.cast_data.get(), false, nullptr, nullptr);
				info.function(source, result, count, params);
			}
		};

		// DuckToArrow closures per geometry storage type
		// Each lambda captures the storage type for ToVectorizedFormat
#define VGI_GEOARROW_DUCK_TO_ARROW(STORAGE_TYPE)                                                                       \
	[](ClientContext &, Vector &source, Vector &result, idx_t count) {                                                 \
		Geometry::ToVectorizedFormat(source, result, count, STORAGE_TYPE);                                              \
	}

		struct GeoArrowEntry {
			const char *name;
			LogicalType arrow_type;
			cast_duck_arrow_t duck_to_arrow;
		};

		GeoArrowEntry entries[] = {
		    {"geoarrow.point", xy,
		     VGI_GEOARROW_DUCK_TO_ARROW(GeometryStorageType::POINT_XY)},
		    {"geoarrow.linestring", LogicalType::LIST(xy),
		     VGI_GEOARROW_DUCK_TO_ARROW(GeometryStorageType::LINESTRING_XY)},
		    {"geoarrow.polygon", LogicalType::LIST(LogicalType::LIST(xy)),
		     VGI_GEOARROW_DUCK_TO_ARROW(GeometryStorageType::POLYGON_XY)},
		    {"geoarrow.multipoint", LogicalType::LIST(xy),
		     VGI_GEOARROW_DUCK_TO_ARROW(GeometryStorageType::MULTIPOINT_XY)},
		    {"geoarrow.multilinestring", LogicalType::LIST(LogicalType::LIST(xy)),
		     VGI_GEOARROW_DUCK_TO_ARROW(GeometryStorageType::MULTILINESTRING_XY)},
		    {"geoarrow.multipolygon", LogicalType::LIST(LogicalType::LIST(LogicalType::LIST(xy))),
		     VGI_GEOARROW_DUCK_TO_ARROW(GeometryStorageType::MULTIPOLYGON_XY)},
		};

#undef VGI_GEOARROW_DUCK_TO_ARROW

		for (auto &entry : entries) {
			try {
				config.RegisterArrowExtension(
				    {entry.name, nullptr, nullptr,
				     make_shared_ptr<ArrowTypeExtensionData>(LogicalType::GEOMETRY(), entry.arrow_type,
				                                             NativeGeoArrow::ArrowToDuck, entry.duck_to_arrow)});
			} catch (...) {
				// Extension may already be registered (e.g., by spatial extension in a future version)
			}
		}
	}

	// Register catalog timeout setting (used for subprocess transport)
	config.AddExtensionOption("vgi_catalog_timeout_seconds",
	                          "Timeout in seconds for VGI subprocess catalog operations (list schemas, functions, etc.)",
	                          LogicalType::BIGINT, Value::BIGINT(vgi::CATALOG_OPERATION_TIMEOUT_SECONDS));

	// Register HTTP timeout setting
	config.AddExtensionOption("vgi_http_timeout_seconds",
	                          "Timeout in seconds for VGI HTTP requests (catalog, init, and exchange operations)",
	                          LogicalType::BIGINT, Value::BIGINT(300));

	// Register OAuth settings
	config.AddExtensionOption("vgi_oauth_timeout_seconds",
	                          "Timeout in seconds for OAuth browser authentication flow",
	                          LogicalType::BIGINT, Value::BIGINT(120));
	config.AddExtensionOption("vgi_oauth_enabled",
	                          "Enable OAuth PKCE authentication on HTTP 401 (set false to fail fast)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));
	config.AddExtensionOption("vgi_oauth_flow",
	                          "OAuth flow type: auto (default), device_code, or pkce",
	                          LogicalType::VARCHAR, Value("auto"));
	config.AddExtensionOption("vgi_oauth_prompt",
	                          "OAuth prompt behavior: none (default), login, select_account, or consent",
	                          LogicalType::VARCHAR, Value("none"));

	// Register async prefetch setting. Default is off: async prefetch returns
	// SourceResultType::BLOCKED from table-scan sources, which DuckDB's
	// PositionalTableScanner operator does not handle (it throws
	// NotImplementedException). Other join forms (CROSS / NATURAL / ASOF /
	// LATERAL / INNER / HASH) route through PipelineExecutor and suspend
	// correctly on BLOCKED, so users running queries without POSITIONAL JOIN
	// can opt in via ``SET vgi_async_prefetch=true;`` to reclaim the
	// subprocess-RPC latency hiding.
	config.AddExtensionOption("vgi_async_prefetch",
	                          "Enable async I/O prefetch for VGI table function scans (default off: "
	                          "DuckDB's POSITIONAL JOIN operator does not handle BLOCKED sources)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));
	config.AddExtensionOption("vgi_cancel_enabled",
	                          "Notify VGI workers (on both subprocess and HTTP transports) when a stream is torn "
	                          "down early so their on_cancel hook can release resources. Set to false to disable: "
	                          "destructors skip the cancel dispatch entirely, on_cancel is never invoked, and "
	                          "workers learn the stream is gone only via normal stream-close / HTTP TTL.",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));

	// Register join key pushdown settings + optimizer
	config.AddExtensionOption("vgi_join_keys_limit",
	                          "Max distinct join key values to push to VGI workers (0 = disabled)",
	                          LogicalType::UBIGINT, Value::UBIGINT(100000));
	config.AddExtensionOption("vgi_join_keys_max_bytes",
	                          "Max estimated byte size for join keys batch (skip pushdown if exceeded)",
	                          LogicalType::UBIGINT, Value::UBIGINT(67108864)); // 64MB

	// =========================================================================
	// Multi-scan rewriter MUST register BEFORE VgiJoinOptimizer. Both are
	// pre_optimize_function; they fire in registration order. The rewriter
	// replaces the marker LogicalGet with a LogicalSetOperation(UNION_ALL,
	// [LogicalGet(vgi_fn), ...]) — only after that swap do the inner
	// LogicalGets carry `function == VgiTableFunctionScan`, which is what
	// VgiJoinOptimizer's IsVgiScan() detects to raise the InFilter
	// threshold. If JoinOptimizer ran first, it would walk the unrewritten
	// plan, see only markers (function == MultiBranchMarkerExecute), and
	// miss the join+VGI heuristic.
	//
	// Phase-split rationale (PRE-pushdown rewrite vs. POST-pushdown rewrite
	// for buffered_table) is documented at the buffered_table registration
	// site below. The multi-scan rewriter is intentionally pre-pushdown so
	// that DuckDB's standard filter-pushdown distributes parent filters
	// into each arm after we've produced LogicalSetOperation. =========================================
	config.AddExtensionOption(
	    "vgi_multi_branch_scans",
	    "Rewrite VGI multi-branch table scans into LogicalSetOperation(UNION_ALL, ...) "
	    "via the optimizer extension. Set to false to disable the rewrite — multi-branch "
	    "table scans will then throw at execution time (the marker placeholder's loud-fail). "
	    "Emergency-rollback knob; not generally useful.",
	    LogicalType::BOOLEAN, Value::BOOLEAN(true));
	vgi::RegisterVgiMultiScanRewriter(config);

	OptimizerExtension::Register(config, VgiJoinOptimizer());

	// Streaming-window optimizer rule: rewrite eligible LogicalWindow ->
	// LogicalVgiStreamingWindow. Gate on a session setting so benchmarks /
	// fallback paths can disable it without recompiling.
	config.AddExtensionOption("vgi_streaming_window",
	                          "Route eligible OVER (...) queries against VGI aggregates with "
	                          "streaming_partitioned=true through the custom streaming operator. "
	                          "Set to false to fall back to PhysicalWindow / WindowCustomAggregator.",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));
	OptimizerExtension::Register(config, VgiStreamingWindowOptimizer());

	// Table sink+source function rewriter — replace LogicalGet of any
	// TableBufferingFunction subclass with our Sink+Source physical op.
	// Gated on a session setting so a regression has an emergency rollback
	// path. When disabled, the loud-failure asserts in VgiTableInOutFunction /
	// VgiTableInOutFinalize will surface a clear error rather than the
	// pre-fix UNION-ALL corruption.
	config.AddExtensionOption("vgi_table_buffering",
	                          "Rewrite calls to TableBufferingFunction subclasses through "
	                          "the Sink+Source PhysicalVgiTableBufferingFunction operator. "
	                          "Set to false to disable the rewrite — table_buffering queries "
	                          "will then throw a clear InternalException instead of running.",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));
	OptimizerExtension::Register(config, VgiTableBufferingRewriter());

	// VgiMultiScanRewriter is registered BEFORE VgiJoinOptimizer above —
	// see the comment block at that site for ordering rationale. It's
	// pre_optimize_function (rewrites into standard DuckDB operators that
	// benefit from filter pushdown). VgiStreamingWindowOptimizer and
	// VgiTableBufferingRewriter above are optimize_function (post-pushdown)
	// because their LogicalExtensionOperator outputs are opaque to pushdown.

	// Register worker pool settings
	config.AddExtensionOption("vgi_worker_pool_idle_limit_seconds",
	                          "Maximum idle time in seconds before pooled workers are removed", LogicalType::BIGINT,
	                          Value::BIGINT(5));
	config.AddExtensionOption("vgi_worker_pool_max", "Default per-path pool limit for VGI workers (0 = disabled)",
	                          LogicalType::BIGINT, Value::BIGINT(256));

	// Eager-load thresholds. Per-kind: a schema's estimated_object_count[kind]
	// is compared against the corresponding value here; below or equal triggers
	// a single bulk LoadEntries() instead of per-name single-entry RPCs. Read
	// once at ATTACH time and snapshotted onto the VgiCatalog — mid-session SET
	// changes do NOT affect already-attached catalogs.
	{
		vector<Value> threshold_keys;
		vector<Value> threshold_values;
		for (const char *kind : {"table", "view", "index", "scalar_function", "aggregate_function",
		                         "table_function", "macro"}) {
			threshold_keys.emplace_back(kind);
			threshold_values.emplace_back(Value::BIGINT(1000));
		}
		auto default_threshold = Value::MAP(LogicalType::VARCHAR, LogicalType::BIGINT,
		                                    std::move(threshold_keys), std::move(threshold_values));
		config.AddExtensionOption(
		    "vgi_eager_load_threshold",
		    "Per-object-kind threshold (keyed by VgiCatalogSet::CacheKindName: table, view, index, "
		    "scalar_function, aggregate_function, table_function, macro). When a schema's "
		    "estimated_object_count[kind] is <= the threshold, the first GetEntry() triggers a single "
		    "bulk LoadEntries() instead of N per-name RPCs. Read at ATTACH; mid-session SET requires "
		    "re-ATTACH to take effect.",
		    LogicalType::MAP(LogicalType::VARCHAR, LogicalType::BIGINT), std::move(default_threshold));
	}

	// When a worker reports estimated_object_count[kind] == 0, the client
	// treats it as a hard guarantee and skips both the bulk
	// catalog_schema_contents_* RPC and the per-name single-entry RPCs for
	// that kind (table/view/index). Set this to false to disable the bypass —
	// every elided RPC will fire instead. Used by support engineers to rule
	// the bypass in/out without restarting the worker; read per-call so it
	// takes effect immediately (no re-attach needed).
	config.AddExtensionOption(
	    "vgi_trust_empty_kinds",
	    "Trust worker assertions that estimated_object_count[kind] == 0 means the kind is empty "
	    "(skip catalog_schema_contents_* RPC). Set to false to force every RPC to fire even when "
	    "the worker reports zero — debug escape hatch for diagnosing worker bugs.",
	    LogicalType::BOOLEAN, Value::BOOLEAN(true));

	// Set default pool settings for paths without explicit per-path config
	// (e.g., direct vgi_table_function() calls that don't go through ATTACH)
	vgi::VgiWorkerPool::Instance().SetDefaultSettings({256, 5});

	// Register VGI table functions
	RegisterVgiCatalogsFunction(loader);
	RegisterVgiTableFunction(loader);

	// Register worker pool diagnostic functions
	vgi::RegisterVgiWorkerPoolFunction(loader);
	vgi::RegisterVgiWorkerPoolStatsFunction(loader);
	vgi::RegisterVgiWorkerPoolFlushFunction(loader);

	// Register table statistics diagnostic function
	vgi::RegisterVgiTableStatisticsFunction(loader);

	// Register cache management function
	vgi::RegisterVgiClearCacheFunction(loader);

	// Register multi-branch diagnostic function — one row per branch per
	// VGI table, across every attached VGI catalog. See vgi_table_branches_function.cpp.
	vgi::RegisterVgiTableBranchesFunction(loader);

	// Register OAuth diagnostic/management functions
	{
		// vgi_oauth_logout([origin])
		TableFunction logout_func("vgi_oauth_logout", {}, OAuthLogoutFunction, OAuthLogoutBind, OAuthLogoutInit);
		logout_func.varargs = LogicalType::VARCHAR;
		loader.RegisterFunction(logout_func);

		// vgi_oauth_tokens()
		TableFunction tokens_func("vgi_oauth_tokens", {}, OAuthTokensFunction, OAuthTokensBind, OAuthTokensInit);
		loader.RegisterFunction(tokens_func);

		// vgi_catalog_identity() — surfaces OIDC identity claims per attached VGI catalog
		TableFunction identity_func("vgi_catalog_identity", {}, CatalogIdentityFunction, CatalogIdentityBind,
		                            CatalogIdentityInit);
		loader.RegisterFunction(identity_func);
	}
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

namespace vgi {
VgiCancelDispatcher *FindVgiCancelDispatcher(DatabaseInstance &db) {
	return ::duckdb::VgiStorageExtension::FindCancelDispatcher(db);
}
} // namespace vgi

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(vgi, loader) {
	duckdb::LoadInternal(loader);
}
}
