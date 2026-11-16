// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_settings_defaults.hpp"
#include "vgi_table_function_impl.hpp"
#include "vgi_table_in_out_impl.hpp"
#include "duckdb/main/profiler/profiling_node.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/function/scalar/struct_utils.hpp"
#include "vgi_platform.hpp" // VGI_ASYNC_INIT_ENABLED
#ifdef __EMSCRIPTEN__
#include <pthread.h>
#include <unistd.h>
#include "vgi_wasm_async_pool.hpp"
#include "vgi_batch_validation.hpp"
#endif

#include "storage/vgi_table_entry.hpp"
#include "storage/vgi_catalog.hpp"
#include "vgi_bind_protocol.hpp"
#include "vgi_cached_replay_connection.hpp"
#include "vgi_arrow_ipc.hpp" // SerializeRecordBatch (capture)
#include "vgi_cancel_dispatcher.hpp"
#include "vgi_exchange_cache_key.hpp" // SerializeSettingsForKey / SerializeProjectionForKey (shared)
#include "vgi_sha256.hpp"             // VgiSha256Hex (per-partition discriminator)
#include "vgi_catalog_rpc.hpp"
#include "vgi_constraint_contract.hpp"
#include "vgi_exception.hpp"
#include "vgi_extension.hpp"
#include "vgi_logging.hpp"
#include "vgi_oauth.hpp" // ComputeCatalogIdentityFingerprint (cache identity scoping)
#include "vgi_protocol.hpp"
#include "vgi_query_function.hpp"
#include "vgi_worker_pool.hpp"


#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/planner/table_filter.hpp"

#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include "yyjson.hpp"

#include <algorithm>
#include <cstring>
#include <functional>
#include <limits>

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {
namespace vgi {

namespace {

bool ReadCancelEnabledSetting(ClientContext &ctx) {
	Value v;
	if (ctx.TryGetCurrentSetting("vgi_cancel_enabled", v)) {
		return v.GetValue<bool>();
	}
	return true;
}

} // namespace

VgiTableFunctionLocalState::VgiTableFunctionLocalState(
    unique_ptr<ArrowArrayWrapper> current_chunk, ClientContext &ctx,
    std::shared_ptr<VgiAttachParameters> attach_params)
    : ArrowScanLocalState(std::move(current_chunk), ctx),
      cancel_enabled(ReadCancelEnabledSetting(ctx)),
      prefetch_slot_(std::make_shared<VgiPrefetchSlot>()),
      db_(*ctx.db),
      context_(ctx),
      attach_params_(std::move(attach_params)) {
}

VgiTableFunctionLocalState::~VgiTableFunctionLocalState() noexcept {
	// Racy by design: a task already mid-RPC finishes, kept alive by the slot.
	prefetch_slot_->cancelled.store(true, std::memory_order_release);

	// A prefetch task still holds the slot: touching the connection would
	// corrupt its stream, so skip both pool release and cancel.
	if (prefetch_slot_.use_count() != 1) {
		return;
	}

	auto &connection = prefetch_slot_->connection;
	if (!connection) {
		return;
	}

	// Early exit: hand the connection to the cancel dispatcher, never the pool.
	if (!done && cancel_enabled) {
		if (auto *dispatcher = FindVgiCancelDispatcher(db_)) {
			auto token = connection->GetLastStateToken();
			CancelRequest req;
			req.connection = std::move(connection);
			req.state_token = !token.empty() ? std::move(token) : prefetch_slot_->last_state_token;
			// Accepted or not, the connection is gone; if refused it is dropped
			// without a cancel and the server's stream expires by TTL.
			(void)dispatcher->Enqueue(std::move(req));
			return;
		}
	}

	// Natural end or cancel disabled: return to the pool.
	if (attach_params_ && attach_params_->use_pool() && connection) {
		auto worker_pid = connection->GetSubprocessPid().value_or(-1);
		auto conn_id = connection->GetConnIdHex();
		if (auto pooled = connection->ReleaseForPooling()) {
			try {
				auto rr = VgiWorkerPool::Instance().Release(std::move(pooled));
				PoolReleaseLogFields lf;
				lf.conn_id = conn_id;
				lf.worker_path = attach_params_->worker_path();
				lf.worker_pid = worker_pid;
				LogWorkerPoolRelease(context_, lf, rr.pooled, rr.skip_reason, rr.pool_size, rr.total_pool_size);
			} catch (...) {
				// noexcept destructor; swallow.
			}
		}
	}
}

// Apply a BindResult to bind_data and derive return_types / names. Shared by
// the RPC path and the inline-bind short-circuit (which passes conn_id ""
// and worker_pid -1; they are only logged).
static void ApplyBindResultToBindData(ClientContext &context, VgiTableFunctionBindData &bind_data,
                                       BindResult bind_result, const std::string &conn_id,
                                       int worker_pid, vector<LogicalType> &return_types,
                                       vector<Identifier> &names) {
	bind_data.max_processes = 1;
	bind_data.cardinality_estimate = -1;
	bind_data.secret_dependent = bind_data.secret_dependent || bind_result.secret_dependent;
	bind_data.bind_result = std::move(bind_result);

	try {
		ArrowSchemaToDuckDBTypes(context, bind_data.bind_result.output_schema, bind_data.c_schema,
		                         bind_data.arrow_table, return_types, names);
	} catch (const std::exception &e) {
		throw IOException(ErrorInfo()
		                      .Function(bind_data.function_name, "TABLE")
		                      .Worker(bind_data.worker_path(), worker_pid)
		                      .Merge(ExtraInfoOf(e)),
		                  "Failed to convert output schema for function '%s': %s", bind_data.function_name,
		                  RawMessageOf(e));
	}

	if (bind_data.rowid_worker_col_index >= 0) {
		auto idx = static_cast<size_t>(bind_data.rowid_worker_col_index);
		if (idx < return_types.size()) {
			// Capture the name BEFORE erasing (see rowid_column_name).
			bind_data.rowid_column_name = names[idx].GetIdentifierName();
			return_types.erase(return_types.begin() + idx);
			names.erase(names.begin() + idx);
		}
	}

	bind_data.all_column_names = IdentifiersToStrings(names);
	bind_data.all_column_types = return_types;

	{
		vector<pair<string, string>> fields;
		fields.emplace_back("conn", conn_id);
		fields.emplace_back("worker_path", bind_data.worker_path());
		if (worker_pid > 0) {
			fields.emplace_back("worker_pid", std::to_string(worker_pid));
		}
		fields.emplace_back("function_name", bind_data.function_name);
		fields.emplace_back("num_columns", std::to_string(bind_data.bind_result.output_schema->num_fields()));
		VGI_LOG(context, "table_function.bind_result", fields);
	}
}

bool VgiTableFunctionBindData::SupportStatementCache() const {
	// The table binder copies this flag before optimizer callbacks run. An
	// offered scan must rebind even if its particular proposal later declines.
	// A query-scoped snapshot also cannot survive into a prepared execution.
	return !explicit_query && bind_result.repeatability == BindRepeatability::UNKNOWN &&
	       projection_response_bytes.empty() &&
	       std::find(bind_result.projection_expression_profiles.begin(),
	                 bind_result.projection_expression_profiles.end(),
	                 VGI_CONSTRAINT_SCALAR_READ) == bind_result.projection_expression_profiles.end();
}

unique_ptr<FunctionData> VgiTableFunctionBindData::Copy() const {
	auto result = make_uniq<VgiTableFunctionBindData>();
	result->explicit_query = explicit_query;
	result->projection_request_bytes = projection_request_bytes;
	result->projection_response_bytes = projection_response_bytes;
	result->query_preparation = query_preparation;

	result->attach_params = attach_params;
	result->attach_opaque_data = attach_opaque_data;
	result->transaction_opaque_data = transaction_opaque_data;
	result->function_name = function_name;
	result->schema_name = schema_name;
	result->arguments = arguments;
	result->settings = settings;
	result->required_secrets = required_secrets;
	result->secret_dependent = secret_dependent;

	// arrow_table's shared_ptr<ArrowType> entries are immutable, so sharing is
	// safe; the clone gets its own C-ABI schema export.
	result->arrow_table = arrow_table;
	if (bind_result.output_schema) {
		ExportSchema(bind_result.output_schema, result->c_schema);
	}

	result->max_processes = max_processes;
	result->fixed_order = fixed_order;
	result->supports_batch_index = supports_batch_index;
	result->supports_splits = supports_splits;
	result->split_token_ttl_seconds = split_token_ttl_seconds;
	result->partition_kind = partition_kind;
	result->partition_column_indices = partition_column_indices;
	result->cardinality_estimate = cardinality_estimate;
	result->cardinality_max = cardinality_max;
	result->cardinality_fetched = cardinality_fetched;
	result->projection_pushdown = projection_pushdown;
	result->filter_semantic_profiles = filter_semantic_profiles;
	result->additional_filter_functions = additional_filter_functions;

	result->all_column_names = all_column_names;
	result->all_column_types = all_column_types;
	result->table_entry = table_entry;
	result->rowid_worker_col_index = rowid_worker_col_index;
	result->rowid_type = rowid_type;
	result->rowid_column_name = rowid_column_name;
	result->bind_result = bind_result;
	// Empty on the VGI bind path (column ids live on the local state); copied
	// so a future writer can't lose it on clone.
	result->column_ids = column_ids;

	// The statistics cache starts fresh. order_by_hint / table_sample_hint are
	// NOT copied: they belong to the original plan node, and a stale ORDER BY +
	// LIMIT hint would wrongly cap the clone's output.

	result->at_unit = at_unit;
	result->at_value = at_value;
	result->copy_from = copy_from;

	return std::move(result);
}

void PerformVgiTableFunctionBind(ClientContext &context, VgiTableFunctionBindData &bind_data,
                                 vector<LogicalType> &return_types, vector<Identifier> &names) {
	VGI_LOG(context, "table_function.bind",
	        {{"worker_path", bind_data.worker_path()},
	         {"function_name", bind_data.function_name},
	         {"num_args", bind_data.arguments ? std::to_string(bind_data.arguments.length()) : "0"}});

	D_ASSERT(!bind_data.arguments || (bind_data.arguments.batch->num_columns() == 1 &&
	                                  std::strcmp(bind_data.arguments.batch->schema()->field(0).format, "+s") == 0));

	// Inline-bind short-circuit: a catalog table whose TableInfo carries a
	// pre-bound `bind_result` builds bind_request_bytes locally and skips the
	// bind RPC. On any error, fall through to the RPC.
	if (bind_data.table_entry) {
		auto &tinfo = bind_data.table_entry->Cast<VgiTableEntry>().GetTableInfo();
		if (tinfo.bind_result.has_value()) {
			try {
				auto resolved_secrets = vgi::ExtractVgiSecrets(context, bind_data.required_secrets);
				auto bind_request_bytes = vgi::BuildBindRequestBytes(
				    context, bind_data.function_name, "TABLE",
				    bind_data.arguments.batch, /*input_schema=*/nullptr,
				    bind_data.attach_opaque_data, bind_data.transaction_opaque_data,
				    bind_data.settings, resolved_secrets,
				    /*resolved_secrets_provided=*/false,
				    bind_data.worker_path(),
				    bind_data.at_unit, bind_data.at_value,
				    /*copy_from=*/nullptr, /*copy_to=*/nullptr, bind_data.schema_name);
				auto bind_result =
				    vgi::BuildBindResultFromInlinedBytes(std::move(bind_request_bytes), *tinfo.bind_result,
				                                         bind_data.worker_path(), !bind_data.required_secrets.empty(),
				                                         resolved_secrets);

				// Catalog metadata can outlive the query that produced it. A
				// query-scoped snapshot must come from a fresh bind below.
				if (bind_result.repeatability == BindRepeatability::UNKNOWN) {
					VGI_LOG(context, "table_function.inline_bind_used",
					        {{"worker_path", bind_data.worker_path()}, {"function_name", bind_data.function_name}});
					ApplyBindResultToBindData(context, bind_data, std::move(bind_result),
					                          /*conn_id=*/"", /*worker_pid=*/-1, return_types, names);
					return;
				}
				VGI_LOG(context, "table_function.inline_bind_query_snapshot_bypassed",
				        {{"function_name", bind_data.function_name}});
			} catch (const std::exception &e) {
				VGI_LOG(context, "table_function.inline_bind_error",
				        {{"worker_path", bind_data.worker_path()},
				         {"function_name", bind_data.function_name},
				         {"error", e.what()}});
				// fall through to AcquireAndBindConnection
			}
		}
	}

	// AcquireAndBindConnection handles pool acquire and stale-connection retry.
	FunctionConnectionParams params;
	params.attach_params = bind_data.attach_params;
	params.attach_opaque_data = bind_data.attach_opaque_data;
	params.function_name = bind_data.function_name;
	params.schema_name = bind_data.schema_name;
	params.arguments = bind_data.arguments;
	params.transaction_opaque_data = bind_data.transaction_opaque_data;
	params.settings = bind_data.settings;
	params.required_secrets = bind_data.required_secrets;
	params.phase = "bind";
	params.function_type = "TABLE";
	params.at_unit = bind_data.at_unit;
	params.at_value = bind_data.at_value;
	params.copy_from = bind_data.copy_from;

	auto result = AcquireAndBindConnection(context, params);
	auto bind_worker_pid = result.connection ? result.connection->GetSubprocessPid().value_or(-1) : -1;
	auto bind_conn_id = result.connection ? result.connection->GetConnIdHex() : "";

	// Bind is a unary RPC, so the worker can serve other planner RPCs and the
	// eventual init. Init carries bind_request_bytes, so any pooled worker can
	// serve it.
	if (bind_data.use_pool() && result.connection) {
		if (auto pooled = result.connection->ReleaseForPooling()) {
			auto rr = VgiWorkerPool::Instance().Release(std::move(pooled));
			vector<pair<string, string>> fields;
			fields.emplace_back("conn", bind_conn_id);
			fields.emplace_back("worker_path", bind_data.worker_path());
			if (bind_worker_pid > 0) {
				fields.emplace_back("worker_pid", std::to_string(bind_worker_pid));
			}
			fields.emplace_back("phase", "bind");
			fields.emplace_back("pooled", rr.pooled ? "true" : "false");
			if (!rr.skip_reason.empty()) {
				fields.emplace_back("skip_reason", rr.skip_reason);
			}
			fields.emplace_back("pool_size", std::to_string(rr.pool_size));
			fields.emplace_back("total", std::to_string(rr.total_pool_size));
			VGI_LOG(context, "worker_pool.release", fields);
		}
		result.connection.reset();
	}

	ApplyBindResultToBindData(context, bind_data, std::move(result.bind_result),
	                          bind_conn_id, bind_worker_pid, return_types, names);
}

// --- Expression filter pushdown ---------------------------------------------

namespace {

bool HasFilterFunctionCapability(const std::vector<VgiFilterFunctionCapability> &capabilities,
                                 const string &namespace_name, const string &name, uint64_t version) {
	for (const auto &capability : capabilities) {
		if (capability.namespace_name == namespace_name && capability.name == name && capability.version == version) {
			return true;
		}
	}
	return false;
}

bool IsIntersectsExtentFunction(const BoundFunctionExpression &function,
                                const std::vector<VgiFilterFunctionCapability> &additional_functions) {
	auto name = StringUtil::Lower(function.Function().GetName().GetIdentifierName());
	return (name == "&&" || name == "st_intersects_extent") && function.GetChildren().size() == 2 &&
	       function.GetReturnType().id() == LogicalTypeId::BOOLEAN &&
	       function.GetChildren()[0]->GetReturnType().id() == LogicalTypeId::GEOMETRY &&
	       function.GetChildren()[1]->GetReturnType().id() == LogicalTypeId::GEOMETRY &&
	       HasFilterFunctionCapability(additional_functions, "duckdb.spatial", "intersects_extent", 1);
}

const char *StandardFilterFunctionName(const BoundFunctionExpression &function) {
	if (function.GetChildren().size() != 2 || function.GetReturnType().id() != LogicalTypeId::BOOLEAN ||
	    function.Function().GetCatalogName() != "system" || function.Function().GetSchemaName() != "main") {
		return nullptr;
	}
	auto name = StringUtil::Lower(function.Function().GetName().GetIdentifierName());
	auto &first_type = function.GetChildren()[0]->GetReturnType();
	auto &second_type = function.GetChildren()[1]->GetReturnType();
	auto has_collation = [](const LogicalType &type) {
		return TypeVisitor::Contains(type, [](const LogicalType &nested_type) {
			return nested_type.id() == LogicalTypeId::VARCHAR && !StringType::GetCollation(nested_type).empty();
		});
	};
	if ((name == "starts_with" || name == "prefix") && first_type.id() == LogicalTypeId::VARCHAR &&
	    second_type.id() == LogicalTypeId::VARCHAR && !has_collation(first_type) && !has_collation(second_type)) {
		return "starts_with";
	}
	if ((name == "ends_with" || name == "suffix") && first_type.id() == LogicalTypeId::VARCHAR &&
	    second_type.id() == LogicalTypeId::VARCHAR && !has_collation(first_type) && !has_collation(second_type)) {
		return "ends_with";
	}
	if (name == "contains" && first_type.id() == LogicalTypeId::VARCHAR && second_type.id() == LogicalTypeId::VARCHAR &&
	    !has_collation(first_type) && !has_collation(second_type)) {
		return "contains";
	}
	if ((name == "list_contains" || name == "array_contains" || name == "contains") &&
	    first_type.id() == LogicalTypeId::LIST && ListType::GetChildType(first_type) == second_type &&
	    !has_collation(first_type)) {
		return "list_contains";
	}
	return nullptr;
}

//! Pure eligibility check shared by the pushdown callback and v2 serializer.
//! DuckDB calls this before replacing the sole BoundColumnRef with a
//! BoundReferenceExpression, so both forms are accepted as the same root.
bool ExpressionTreeIsSupported(const Expression &expr,
                               const std::vector<VgiFilterFunctionCapability> &additional_functions) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_REF:
		return expr.Cast<BoundReferenceExpression>().Index() == 0;
	case ExpressionClass::BOUND_COLUMN_REF:
	case ExpressionClass::BOUND_CONSTANT:
		return true;
	case ExpressionClass::BOUND_FUNCTION: {
		auto &function = expr.Cast<BoundFunctionExpression>();
		idx_t struct_index;
		if (TryGetStructExtractChildIndex(function, struct_index) && !function.GetChildren().empty()) {
			return ExpressionTreeIsSupported(*function.GetChildren()[0], additional_functions);
		}
		if (BoundComparisonExpression::IsComparison(function)) {
			switch (function.GetExpressionType()) {
			case ExpressionType::COMPARE_EQUAL:
			case ExpressionType::COMPARE_NOTEQUAL:
			case ExpressionType::COMPARE_GREATERTHAN:
			case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			case ExpressionType::COMPARE_LESSTHAN:
			case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			case ExpressionType::COMPARE_DISTINCT_FROM:
			case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
				return ExpressionTreeIsSupported(BoundComparisonExpression::Left(function), additional_functions) &&
				       ExpressionTreeIsSupported(BoundComparisonExpression::Right(function), additional_functions);
			default:
				return false;
			}
		}
		if (BoundCastExpression::IsCast(function)) {
			// vgi.none.v1 supports only context-independent numeric coercions.
			return BoundCastExpression::HasValidBindData(function) && !BoundCastExpression::IsTryCast(function) &&
			       BoundCastExpression::SourceType(function).IsNumeric() && function.GetReturnType().IsNumeric() &&
			       ExpressionTreeIsSupported(BoundCastExpression::Child(function), additional_functions);
		}
		auto standard_name = StandardFilterFunctionName(function);
		if (!standard_name && !IsIntersectsExtentFunction(function, additional_functions)) {
			return false;
		}
		for (const auto &child : function.GetChildren()) {
			if (!ExpressionTreeIsSupported(*child, additional_functions)) {
				return false;
			}
		}
		return true;
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conj_expr = expr.Cast<BoundConjunctionExpression>();
		if ((conj_expr.GetExpressionType() != ExpressionType::CONJUNCTION_AND &&
		     conj_expr.GetExpressionType() != ExpressionType::CONJUNCTION_OR) ||
		    conj_expr.GetChildren().size() < 2) {
			return false;
		}
		for (auto &child : conj_expr.GetChildren()) {
			if (!ExpressionTreeIsSupported(*child, additional_functions)) {
				return false;
			}
		}
		return true;
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op = expr.Cast<BoundOperatorExpression>();
		switch (op.GetExpressionType()) {
		case ExpressionType::OPERATOR_IS_NULL:
		case ExpressionType::OPERATOR_IS_NOT_NULL:
		case ExpressionType::OPERATOR_NOT:
			return op.GetChildren().size() == 1 && ExpressionTreeIsSupported(*op.GetChildren()[0], additional_functions);
		case ExpressionType::COMPARE_IN:
		case ExpressionType::COMPARE_NOT_IN:
			if (op.GetChildren().empty() || !ExpressionTreeIsSupported(*op.GetChildren()[0], additional_functions)) {
				return false;
			}
			for (idx_t i = 1; i < op.GetChildren().size(); i++) {
				if (op.GetChildren()[i]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
					return false;
				}
			}
			return true;
		default:
			return false;
		}
	}
	default:
		return false;
	}
}

bool PushdownExpression(ClientContext &context, Expression &expr, const string &function_name,
                        const std::vector<std::string> &semantic_profiles,
                        const std::vector<VgiFilterFunctionCapability> &additional_functions) {
	if (std::find(semantic_profiles.begin(), semantic_profiles.end(), "vgi.duckdb.standard.v1") ==
	    semantic_profiles.end()) {
		return false;
	}
	bool supported = ExpressionTreeIsSupported(expr, additional_functions);
	if (supported) {
		VGI_LOG(context, "table_function.expression_filter_accepted",
		        {{"function_name", function_name}, {"expression", expr.ToString()}});
	} else {
		VGI_LOG(context, "table_function.expression_filter_rejected",
		        {{"function_name", function_name},
		         {"expression", expr.ToString()},
		         {"reason", "expression tree contains unsupported function or node type"}});
	}
	return supported;
}

} // anonymous namespace

bool VgiPushdownExpression(ClientContext &context, const LogicalGet &get, Expression &expr) {
	if (!get.bind_data) {
		return false;
	}
	auto &bind_data = get.bind_data->Cast<VgiTableFunctionBindData>();
	return PushdownExpression(context, expr, bind_data.function_name, bind_data.filter_semantic_profiles,
	                          bind_data.additional_filter_functions);
}

bool VgiTableInOutPushdownExpression(ClientContext &context, const LogicalGet &get, Expression &expr) {
	if (!get.bind_data) {
		return false;
	}
	auto &bind_data = get.bind_data->Cast<VgiTableInOutBindData>();
	return PushdownExpression(context, expr, bind_data.function_name, bind_data.filter_semantic_profiles,
	                          bind_data.additional_filter_functions);
}

// --- FilterSerializer: builds the JSON filter document and collects values --

namespace {

const char *ExpressionTypeToOp(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		return "eq";
	case ExpressionType::COMPARE_NOTEQUAL:
		return "ne";
	case ExpressionType::COMPARE_GREATERTHAN:
		return "gt";
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return "ge";
	case ExpressionType::COMPARE_LESSTHAN:
		return "lt";
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return "le";
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return "distinct_from";
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return "not_distinct_from";
	default:
		return "unknown";
	}
}

//! Info about a join key column to be serialized as a flat Arrow batch.
//! Owns constant values until their external-set batch has been serialized.
struct JoinKeysInfo {
	string column_name;
	idx_t column_index;
	LogicalType type;
	vector<Value> values; // owned while the external-set batch is built
};

} // namespace (anonymous) — closed so VgiContainsDynamicFilter has external
  // linkage matching its header declaration.

//! Runtime-valued internal filter functions (Top-N dynamic filter, bloom
//! filter, prefix range). Static serialization must skip a whole
//! OptionalFilter subtree containing one: e.g. OR(IsNull, DynamicFilter) with
//! the dynamic child elided becomes OR(IsNull), a *stricter* filter that drops
//! rows. Top-N bounds are pushed at tick time instead.
static bool ContainsVgiDynamicExpression(const Expression &expr) {
	return ExpressionFilter::ContainsInternalFunction(expr, DynamicFilterScalarFun::NAME) ||
	       ExpressionFilter::ContainsInternalFunction(expr, BloomFilterScalarFun::NAME) ||
	       ExpressionFilter::ContainsInternalFunction(expr, PrefixRangeScalarFun::NAME);
}

bool VgiContainsDynamicFilter(const TableFilter &filter) {
	const auto &expression = ExpressionFilter::GetExpressionFilter(filter, "VGI dynamic filter detection");
	return ContainsVgiDynamicExpression(*expression.expr);
}

static bool VgiAdvisoryFilterCanThrow(const Expression &expr) {
	if (auto child = ExpressionFilter::GetOptionalFilterChild(expr)) {
		return VgiAdvisoryFilterCanThrow(*child);
	}
	return expr.CanThrow();
}

namespace {

//! Walks the filter tree, builds JSON, and collects the literal values.
class FilterSerializer {
public:
	FilterSerializer(const string &worker_path, idx_t join_keys_max_bytes, bool allow_external_sets = true,
	                 std::vector<VgiFilterFunctionCapability> additional_functions = {})
	    : doc_(yyjson_mut_doc_new(nullptr)), worker_path_(worker_path), join_keys_max_bytes_(join_keys_max_bytes),
	      allow_external_sets_(allow_external_sets), additional_functions_(std::move(additional_functions)) {
	}

	~FilterSerializer() {
		if (doc_) {
			yyjson_mut_doc_free(doc_);
		}
	}

	yyjson_mut_val *SerializeFilter(const TableFilter &filter, idx_t column_index, const string &column_name,
	                                bool &advisory) {
		auto input = ColumnRef(column_index, column_name);
		return SerializeFilterExpression(filter, input, column_index, column_name, advisory);
	}

	const vector<Value> &GetValues() const {
		return values_;
	}

	const vector<LogicalType> &GetValueTypes() const {
		return value_types_;
	}
	const vector<string> &GetValueNames() const {
		return value_names_;
	}

	char *WriteJson(yyjson_mut_val *root) {
		return yyjson_mut_val_write(root, 0, nullptr);
	}

	yyjson_mut_doc *GetDoc() {
		return doc_;
	}

	bool HasJoinKeys() const {
		return !join_key_columns_.empty();
	}

	const vector<JoinKeysInfo> &GetJoinKeyColumns() const {
		return join_key_columns_;
	}

private:
	yyjson_mut_val *ColumnRef(idx_t column_index, const string &column_name) {
		auto obj = yyjson_mut_obj(doc_);
		yyjson_mut_obj_add_str(doc_, obj, "node", "column_ref");
		yyjson_mut_obj_add_uint(doc_, obj, "column_index", column_index);
		yyjson_mut_obj_add_strcpy(doc_, obj, "column_name", column_name.c_str());
		return obj;
	}

	yyjson_mut_val *Literal(const Value &value) {
		auto obj = yyjson_mut_obj(doc_);
		yyjson_mut_obj_add_str(doc_, obj, "node", "literal");
		yyjson_mut_obj_add_uint(doc_, obj, "value_ref", AddValue(value));
		return obj;
	}

	yyjson_mut_val *SerializeFilterExpression(const TableFilter &filter, yyjson_mut_val *, idx_t column_index,
	                                          const string &column_name, bool &advisory) {
		const auto &expression = ExpressionFilter::GetExpressionFilter(filter, "VGI filter serialization");
		const Expression *expr = expression.expr.get();
		if (ExpressionFilter::IsOptionalExpression(*expr)) {
			advisory = true;
			if (ContainsVgiDynamicExpression(*expr)) {
				return nullptr;
			}
		}
		while (auto child = ExpressionFilter::GetOptionalFilterChild(*expr)) {
			expr = child.get();
		}
		if (!ExpressionTreeIsSupported(*expr, additional_functions_)) {
			throw InvalidInputException("VGI required expression filter is not eligible for v2 serialization");
		}
		return SerializeExpression(*expr, column_index, column_name);
	}

	idx_t AddValue(const Value &value) {
		auto ref = value_ref_count_++;
		values_.push_back(value);
		value_types_.push_back(value.type());
		value_names_.push_back("value_" + std::to_string(ref));
		return ref;
	}

	idx_t AddType(const LogicalType &type) {
		auto ref = type_ref_count_++;
		values_.push_back(Value(type));
		value_types_.push_back(type);
		value_names_.push_back("type_" + std::to_string(ref));
		return ref;
	}

	yyjson_mut_val *SerializeExpression(const Expression &expr, idx_t column_index, const string &column_name) {
		auto obj = yyjson_mut_obj(doc_);
		switch (expr.GetExpressionClass()) {
		case ExpressionClass::BOUND_REF: {
			auto &ref = expr.Cast<BoundReferenceExpression>();
			if (ref.Index() != 0) {
				throw InvalidInputException("VGI expression filter references more than one column");
			}
			return ColumnRef(column_index, column_name);
		}
		case ExpressionClass::BOUND_CONSTANT:
			return Literal(expr.Cast<BoundConstantExpression>().GetValue());
		case ExpressionClass::BOUND_FUNCTION: {
			auto &function = expr.Cast<BoundFunctionExpression>();
			idx_t struct_index;
			if (TryGetStructExtractChildIndex(function, struct_index) && !function.GetChildren().empty()) {
				const auto &source = function.GetChildren()[0]->GetReturnType();
				if (source.id() != LogicalTypeId::STRUCT || struct_index >= StructType::GetChildCount(source)) {
					throw InvalidInputException("Invalid VGI struct field reference");
				}
				yyjson_mut_obj_add_str(doc_, obj, "node", "field_ref");
				yyjson_mut_obj_add_val(doc_, obj, "expression",
				    SerializeExpression(*function.GetChildren()[0], column_index, column_name));
				yyjson_mut_obj_add_uint(doc_, obj, "field_index", struct_index);
				yyjson_mut_obj_add_strcpy(doc_, obj, "field_name", StructType::GetChildName(source, struct_index).c_str());
				return obj;
			}
			if (BoundComparisonExpression::IsComparison(function)) {
				auto op = ExpressionTypeToOp(function.GetExpressionType());
				if (string(op) == "unknown") {
					throw InvalidInputException("Unsupported VGI comparison operator");
				}
				yyjson_mut_obj_add_str(doc_, obj, "node", "comparison");
				yyjson_mut_obj_add_str(doc_, obj, "op", op);
				yyjson_mut_obj_add_val(doc_, obj, "left",
				    SerializeExpression(BoundComparisonExpression::Left(function), column_index, column_name));
				yyjson_mut_obj_add_val(doc_, obj, "right",
				    SerializeExpression(BoundComparisonExpression::Right(function), column_index, column_name));
				return obj;
			}
			if (BoundCastExpression::IsCast(function)) {
				if (!BoundCastExpression::HasValidBindData(function) || BoundCastExpression::IsTryCast(function) ||
				    !BoundCastExpression::SourceType(function).IsNumeric() || !function.GetReturnType().IsNumeric()) {
					break;
				}
				yyjson_mut_obj_add_str(doc_, obj, "node", "cast");
				yyjson_mut_obj_add_val(doc_, obj, "expression",
				    SerializeExpression(BoundCastExpression::Child(function), column_index, column_name));
				yyjson_mut_obj_add_uint(doc_, obj, "type_ref", AddType(function.GetReturnType()));
				return obj;
			}
			yyjson_mut_obj_add_str(doc_, obj, "node", "call");
			if (auto standard_name = StandardFilterFunctionName(function)) {
				yyjson_mut_obj_add_str(doc_, obj, "function", standard_name);
			} else if (IsIntersectsExtentFunction(function, additional_functions_)) {
				auto identity = yyjson_mut_obj(doc_);
				yyjson_mut_obj_add_str(doc_, identity, "namespace", "duckdb.spatial");
				yyjson_mut_obj_add_str(doc_, identity, "name", "intersects_extent");
				yyjson_mut_obj_add_uint(doc_, identity, "version", 1);
				yyjson_mut_obj_add_val(doc_, obj, "function", identity);
			} else {
				break;
			}
			auto arguments = yyjson_mut_arr(doc_);
			for (const auto &child : function.GetChildren()) {
				yyjson_mut_arr_append(arguments, SerializeExpression(*child, column_index, column_name));
			}
			yyjson_mut_obj_add_val(doc_, obj, "arguments", arguments);
			return obj;
		}
		case ExpressionClass::BOUND_CONJUNCTION: {
			auto &conjunction = expr.Cast<BoundConjunctionExpression>();
			if (conjunction.GetChildren().size() < 2) {
				throw InvalidInputException("VGI boolean conjunction requires at least two children");
			}
			yyjson_mut_obj_add_str(doc_, obj, "node",
			                       conjunction.GetExpressionType() == ExpressionType::CONJUNCTION_AND ? "and" : "or");
			auto children = yyjson_mut_arr(doc_);
			for (auto &child : conjunction.GetChildren()) {
				yyjson_mut_arr_append(children, SerializeExpression(*child, column_index, column_name));
			}
			yyjson_mut_obj_add_val(doc_, obj, "children", children);
			return obj;
		}
		case ExpressionClass::BOUND_OPERATOR: {
			auto &op = expr.Cast<BoundOperatorExpression>();
			if ((op.GetExpressionType() == ExpressionType::OPERATOR_IS_NULL ||
			     op.GetExpressionType() == ExpressionType::OPERATOR_IS_NOT_NULL) &&
			    op.GetChildren().size() == 1) {
				yyjson_mut_obj_add_str(doc_, obj, "node", "is_null");
				yyjson_mut_obj_add_val(doc_, obj, "expression",
				                       SerializeExpression(*op.GetChildren()[0], column_index, column_name));
				yyjson_mut_obj_add_bool(doc_, obj, "negated",
				                        op.GetExpressionType() == ExpressionType::OPERATOR_IS_NOT_NULL);
				return obj;
			}
			if (op.GetExpressionType() == ExpressionType::OPERATOR_NOT && op.GetChildren().size() == 1) {
				yyjson_mut_obj_add_str(doc_, obj, "node", "not");
				yyjson_mut_obj_add_val(doc_, obj, "expression",
				                       SerializeExpression(*op.GetChildren()[0], column_index, column_name));
				return obj;
			}
			if ((op.GetExpressionType() == ExpressionType::COMPARE_IN ||
			     op.GetExpressionType() == ExpressionType::COMPARE_NOT_IN) && op.GetChildren().size() == 1) {
				return Literal(Value::BOOLEAN(op.GetExpressionType() == ExpressionType::COMPARE_NOT_IN));
			}
			if ((op.GetExpressionType() == ExpressionType::COMPARE_IN ||
			     op.GetExpressionType() == ExpressionType::COMPARE_NOT_IN) &&
			    op.GetChildren().size() > 1) {
				vector<Value> values;
				for (idx_t i = 1; i < op.GetChildren().size(); i++) {
					if (op.GetChildren()[i]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
						throw InvalidInputException("VGI IN expression candidates must be constants");
					}
					values.push_back(op.GetChildren()[i]->Cast<BoundConstantExpression>().GetValue());
				}
				yyjson_mut_obj_add_str(doc_, obj, "node", "in");
				yyjson_mut_obj_add_val(doc_, obj, "expression",
				                       SerializeExpression(*op.GetChildren()[0], column_index, column_name));
				auto set = yyjson_mut_obj(doc_);
				auto value_type = values[0].type();
				auto estimated_bytes = EstimateJoinKeyBytes(values);
				if (join_keys_max_bytes_ > 0 && estimated_bytes > join_keys_max_bytes_) {
					throw InvalidInputException("VGI IN filter exceeds vgi_join_keys_max_bytes");
				}
				if (!allow_external_sets_ || estimated_bytes <= INLINE_SET_MAX_BYTES) {
					yyjson_mut_obj_add_str(doc_, set, "kind", "literal");
					yyjson_mut_obj_add_uint(doc_, set, "value_ref", AddValue(Value::LIST(value_type, std::move(values))));
				} else {
					yyjson_mut_obj_add_str(doc_, set, "kind", "external");
					yyjson_mut_obj_add_uint(doc_, set, "batch_index", join_key_columns_.size());
					yyjson_mut_obj_add_uint(doc_, set, "column_index", 0);
					yyjson_mut_obj_add_strcpy(doc_, set, "column_name", column_name.c_str());
					join_key_columns_.push_back({column_name, column_index, value_type, std::move(values)});
				}
				yyjson_mut_obj_add_val(doc_, obj, "set", set);
				yyjson_mut_obj_add_bool(doc_, obj, "negated", op.GetExpressionType() == ExpressionType::COMPARE_NOT_IN);
				return obj;
			}
			break;
		}
		default:
			break;
		}
		throw InvalidInputException(
		    "VGI expression filter serialization failed for worker '%s': unsupported expression class %d", worker_path_,
		    static_cast<int>(expr.GetExpressionClass()));
	}

	//! Estimate the serialized byte size of join key values without overflow.
	static idx_t EstimateJoinKeyBytes(const vector<Value> &values) {
		if (values.empty()) {
			return 0;
		}
		auto internal_type = values[0].type().InternalType();
		if (internal_type == PhysicalType::VARCHAR) {
			idx_t total_bytes = 0;
			for (auto &value : values) {
				auto value_bytes = value.IsNull() ? 0 : StringValue::Get(value).size();
				if (total_bytes > std::numeric_limits<idx_t>::max() - 4 ||
				    value_bytes > std::numeric_limits<idx_t>::max() - total_bytes - 4) {
					return std::numeric_limits<idx_t>::max();
				}
				total_bytes += value_bytes + 4;
			}
			return total_bytes;
		}
		auto type_size = GetTypeIdSize(internal_type);
		if (type_size > 0 && values.size() > std::numeric_limits<idx_t>::max() / type_size) {
			return std::numeric_limits<idx_t>::max();
		}
		return values.size() * type_size;
	}

	yyjson_mut_doc *doc_;
	string worker_path_;
	idx_t join_keys_max_bytes_;
	bool allow_external_sets_;
	std::vector<VgiFilterFunctionCapability> additional_functions_;
	vector<Value> values_;
	vector<LogicalType> value_types_;
	vector<string> value_names_;
	vector<JoinKeysInfo> join_key_columns_;
	idx_t value_ref_count_ = 0;
	idx_t type_ref_count_ = 0;
	static constexpr idx_t INLINE_SET_MAX_BYTES = 4096;
};

} // anonymous namespace

// --- Filter serialization to Arrow IPC --------------------------------------

//! DuckDB's Arrow export of `chunk` (all rows) as a batch with fields `names`.
static VgiBatchPtr AppendChunkToBatch(ClientContext &context, DataChunk &chunk, const vector<LogicalType> &types,
                                      const vector<string> &names, ClientProperties &client_props) {
	const idx_t count = chunk.size();
	ArrowAppender appender(types, count, client_props, ArrowTypeExtensionData::GetExtensionTypes(context, types));
	appender.Append(chunk, 0, count, count);
	ArrowArray arr = appender.Finalize();
	VgiSchemaPtr schema;
	try {
		ArrowSchema c_schema;
		ArrowConverter::ToArrowSchema(&c_schema, types, names, client_props);
		schema = VgiSchema::Adopt(&c_schema);
	} catch (...) {
		if (arr.release) {
			arr.release(&arr);
		}
		throw;
	}
	return VgiBatch::Adopt(std::move(schema), &arr);
}

//! A one-row filter batch: `chunk` (filter_spec VARCHAR + value columns) exported by
//! DuckDB, with column 0 as the required `filter_spec: utf8 not null` and the V2
//! schema-level metadata. Import failures throw IOException("<import_error>: ...").
static VgiBatchPtr MakeFilterSpecBatch(ClientContext &context, DataChunk &chunk, const vector<LogicalType> &types,
                                       const vector<string> &names, ClientProperties &client_props,
                                       const char *import_error) {
	VgiBatchPtr batch;
	try {
		batch = AppendChunkToBatch(context, chunk, types, names, client_props);
	} catch (const std::exception &e) {
		throw IOException("%s: %s", import_error, ErrorData(e).RawMessage());
	}
	// V2 metadata is schema-level. The filter_spec cell itself is required.
	static const VgiMetadata kFilterMetadata {{"vgi_filter_encoding", "vgi.filters.v2"},
	                                          {"vgi_filter_version", "2"},
	                                          {"vgi_evaluation_context", "vgi.none.v1"}};
	ArrowSchema spec_field;
	BuildSchema(type::Utf8().Named("filter_spec", /*is_nullable=*/false), &spec_field);
	VgiSchemaPtr target;
	try {
		target = batch->schema()->SetField(0, spec_field)->WithMetadata(kFilterMetadata);
	} catch (...) {
		spec_field.release(&spec_field);
		throw;
	}
	spec_field.release(&spec_field);
	// Zero-copy relabel when DuckDB exported VARCHAR as utf8 (the default); converts a
	// large_utf8 export (arrow_large_buffer_size) instead of mislabeling its buffers.
	return ReconcileBatchToSchema(batch, target);
}

//! `batch` as a complete IPC stream (schema, the batch, EOS), uncompressed.
static VgiBufferPtr SerializeIpcStream(const VgiBatch &batch, const char *open_error, const char *write_error,
                                       const char *close_error) {
	auto sink = std::make_shared<VgiBufferOutputStream>();
	std::unique_ptr<VgiIpcWriter> writer;
	try {
		writer = VgiIpcWriter::Open(sink, batch.schema());
	} catch (const std::exception &e) {
		throw IOException("%s: %s", open_error, ErrorData(e).RawMessage());
	}
	try {
		writer->Write(batch);
	} catch (const std::exception &e) {
		throw IOException("%s: %s", write_error, ErrorData(e).RawMessage());
	}
	try {
		writer->Close();
	} catch (const std::exception &e) {
		throw IOException("%s: %s", close_error, ErrorData(e).RawMessage());
	}
	return sink->Finish();
}

SerializedFilters VgiSerializeFilters(ClientContext &context, const vector<column_t> &column_ids,
                                      optional_ptr<TableFilterSet> filters, const vector<string> &column_names,
                                      const string &worker_path, const string &rowid_column_name,
                                      int64_t rowid_worker_col_index, const std::set<idx_t> *exclude_filter_keys,
                                      VgiFilterColumnIndexDomain index_domain,
                                      const std::vector<VgiFilterFunctionCapability> &additional_functions) {
	if (!filters || !filters->HasFilters()) {
		return {nullptr, {}};
	}

	if (filters->HasMultiColumnFilters()) {
		throw InvalidInputException("VGI multi-column filter serialization is not supported");
	}

	idx_t join_keys_max_bytes = defaults::JOIN_KEYS_MAX_BYTES;
	Value max_bytes_val;
	if (context.TryGetCurrentSetting("vgi_join_keys_max_bytes", max_bytes_val) && !max_bytes_val.IsNull()) {
		join_keys_max_bytes = max_bytes_val.GetValue<idx_t>();
	}

	// Build a v2 snapshot. This adapter accepts single-column filters, and
	// every reference still names the unprojected bind-schema field.
	FilterSerializer serializer(worker_path, join_keys_max_bytes, true, additional_functions);
	auto document = yyjson_mut_obj(serializer.GetDoc());
	yyjson_mut_obj_add_str(serializer.GetDoc(), document, "encoding", "vgi.filters.v2");
	yyjson_mut_obj_add_str(serializer.GetDoc(), document, "semantics", "vgi.duckdb.standard.v1");
	yyjson_mut_obj_add_str(serializer.GetDoc(), document, "kind", "snapshot");
	auto predicates = yyjson_mut_arr(serializer.GetDoc());
	idx_t predicate_index = 0;

	for (auto &entry : *filters) {
		idx_t col_idx = entry.GetIndex().GetIndex();
		auto &filter = entry.Filter();

		// Residual serialization (per-partition cache) skips partition-column keys.
		if (exclude_filter_keys && exclude_filter_keys->count(col_idx)) {
			continue;
		}

		// DuckDB's TableFilterSet uses indices into the projected column list.
		// VGI v2 column_ref instead identifies the unprojected bind schema.
		idx_t original_col_idx;
		if (index_domain == VgiFilterColumnIndexDomain::BIND_SCHEMA) {
			original_col_idx = col_idx;
		} else {
			if (col_idx >= column_ids.size()) {
				throw InvalidInputException(
				    "VGI filter column %llu has no projected-to-bind-schema mapping for worker '%s'",
				    static_cast<unsigned long long>(col_idx), worker_path);
			}
			original_col_idx = column_ids[col_idx];
		}
		// A rowid filter carries the COLUMN_IDENTIFIER_ROW_ID sentinel; the worker
		// resolves columns by name, so use the rowid field name (passed explicitly:
		// column_names has the rowid erased). Ordinary columns at or after the
		// erased rowid shift up by one in the worker schema.
		string col_name;
		idx_t worker_col_idx;
		if (original_col_idx == COLUMN_IDENTIFIER_ROW_ID && !rowid_column_name.empty()) {
			col_name = rowid_column_name;
			if (rowid_worker_col_index < 0) {
				throw InvalidInputException("VGI rowid filter has no worker-schema index for worker '%s'", worker_path);
			}
			worker_col_idx = NumericCast<idx_t>(rowid_worker_col_index);
		} else {
			if (original_col_idx == COLUMN_IDENTIFIER_ROW_ID || original_col_idx >= column_names.size()) {
				throw InvalidInputException("VGI filter bind-schema column %llu is invalid for worker '%s'",
				                            static_cast<unsigned long long>(original_col_idx), worker_path);
			}
			col_name = column_names[original_col_idx];
			worker_col_idx = original_col_idx;
			if (rowid_worker_col_index >= 0 && worker_col_idx >= NumericCast<idx_t>(rowid_worker_col_index)) {
				worker_col_idx++;
			}
		}

		std::function<void(const TableFilter &, bool)> append_filter;
		append_filter = [&](const TableFilter &candidate, bool inherited_advisory) {
			bool advisory = inherited_advisory || ExpressionFilter::IsOptionalFilter(candidate);
			const auto &candidate_expression = ExpressionFilter::GetExpressionFilter(candidate, "VGI advisory conjuncts");
			// Runtime join bounds may share an advisory AND with an unsupported
			// Bloom/prefix filter. Each conjunct is independently safe to omit;
			// do not discard serializable min/max bounds with its sibling.
			const Expression *expr = candidate_expression.expr.get();
			while (auto child = ExpressionFilter::GetOptionalFilterChild(*expr)) {
				expr = child.get();
			}
			if (advisory && expr->GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
				for (const auto &child : expr->Cast<BoundConjunctionExpression>().GetChildren()) {
					ExpressionFilter conjunct(child->Copy());
					append_filter(conjunct, true);
				}
				return;
			}
			try {
				auto expression = serializer.SerializeFilter(candidate, worker_col_idx, col_name, advisory);
				if (!expression) {
					if (advisory) {
						return;
					}
					throw InvalidInputException(
					    "VGI required filter for worker '%s' could not be serialized atomically", worker_path);
				}
				auto predicate = yyjson_mut_obj(serializer.GetDoc());
				auto id = string(advisory ? "advisory:" : "query:") + std::to_string(predicate_index++);
				yyjson_mut_obj_add_strcpy(serializer.GetDoc(), predicate, "id", id.c_str());
				yyjson_mut_obj_add_uint(serializer.GetDoc(), predicate, "revision", 0);
				yyjson_mut_obj_add_str(serializer.GetDoc(), predicate, "mode", advisory ? "advisory" : "required");
				yyjson_mut_obj_add_str(serializer.GetDoc(), predicate, "source", advisory ? "other" : "query");
				yyjson_mut_obj_add_val(serializer.GetDoc(), predicate, "expression", expression);
				yyjson_mut_arr_append(predicates, predicate);
			} catch (const InvalidInputException &) {
				if (!advisory) {
					throw;
				}
			}
		};

		const auto &expression = ExpressionFilter::GetExpressionFilter(filter, "VGI conjunct serialization");
		if (expression.expr->GetExpressionType() != ExpressionType::CONJUNCTION_AND) {
			append_filter(filter, false);
			continue;
		}
		// PushFilter builds nested binary ANDs. Separate advisory leaves at
		// every AND level, keeping required leaves together in evaluation order.
		vector<unique_ptr<Expression>> required;
		std::function<void(const Expression &)> collect_conjuncts;
		collect_conjuncts = [&](const Expression &child) {
			if (child.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
				for (const auto &nested : child.Cast<BoundConjunctionExpression>().GetChildren()) {
					collect_conjuncts(*nested);
				}
			} else if (ExpressionFilter::IsOptionalExpression(child)) {
				if (!VgiAdvisoryFilterCanThrow(child)) {
					ExpressionFilter advisory(child.Copy());
					append_filter(advisory, true);
				}
			} else {
				required.push_back(child.Copy());
			}
		};
		collect_conjuncts(*expression.expr);
		if (!required.empty()) {
			ExpressionFilter mandatory(BoundConjunctionExpression::Create(ExpressionType::CONJUNCTION_AND, std::move(required)));
			append_filter(mandatory, false);
		}
	}
	// An empty snapshot still establishes the evaluation context for later
	// Top-N deltas. Keep that envelope even when every static predicate was optional.
	yyjson_mut_obj_add_val(serializer.GetDoc(), document, "predicates", predicates);

	char *json_str = serializer.WriteJson(document);
	if (!json_str) {
		throw IOException("Failed to serialize filters to JSON");
	}
	string filter_spec(json_str);
	free(json_str);

	// One-row batch: filter_spec + the literal value columns.
	auto &values = serializer.GetValues();
	auto &value_types = serializer.GetValueTypes();
	auto &value_names = serializer.GetValueNames();

	vector<LogicalType> types;
	vector<string> names;
	types.push_back(LogicalType::VARCHAR);
	names.push_back("filter_spec");
	for (idx_t i = 0; i < value_types.size(); i++) {
		types.push_back(value_types[i]);
		names.push_back(value_names[i]);
	}

	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), types);
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value(filter_spec));
	for (idx_t i = 0; i < values.size(); i++) {
		chunk.SetValue(i + 1, 0, values[i]);
	}

	ClientProperties client_props = VgiWireClientProperties(context);
	auto record_batch = MakeFilterSpecBatch(context, chunk, types, names, client_props, "Failed to import filter RecordBatch");

	auto filter_bytes = SerializeIpcStream(*record_batch, "Failed to create IPC stream writer",
	                                       "Failed to write filter RecordBatch to IPC stream",
	                                       "Failed to close IPC stream writer");

	// One single-column IPC batch per external IN set (cardinalities may differ).
	std::vector<VgiBufferPtr> join_keys_buffers;
	if (serializer.HasJoinKeys()) {
		auto &key_columns = serializer.GetJoinKeyColumns();

		for (auto &kc : key_columns) {
			idx_t count = kc.values.size();

			vector<LogicalType> types = {kc.type};
			vector<string> names = {kc.column_name};
			DataChunk chunk;
			chunk.Initialize(Allocator::DefaultAllocator(), types, count);
			chunk.SetCardinality(count);
			for (idx_t row = 0; row < count; row++) {
				chunk.SetValue(0, row, kc.values[row]);
			}

			VgiBatchPtr key_batch;
			try {
				key_batch = AppendChunkToBatch(context, chunk, types, names, client_props);
			} catch (const std::exception &e) {
				throw IOException("Failed to import join keys RecordBatch for '%s': %s", kc.column_name,
				                  ErrorData(e).RawMessage());
			}

			key_batch = key_batch->WithSchema(key_batch->schema()->WithMetadata(VgiMetadata {{"vgi_join_keys_version", "2"}}));

			join_keys_buffers.push_back(SerializeIpcStream(*key_batch, "Failed to create IPC writer for join keys",
			                                               "Failed to write join keys RecordBatch",
			                                               "Failed to close join keys IPC writer"));
		}
	}

	return {std::move(filter_bytes), std::move(join_keys_buffers)};
}

void VgiTableFunctionGlobalState::EnsureInitApplied() const {
	if (init_applied.load(std::memory_order_acquire)) {
		return;
	}
	std::lock_guard<std::mutex> lk(init_apply_mutex);
	if (init_applied.load(std::memory_order_relaxed)) {
		return;
	}
	if (pending_init.valid()) {
		auto applied = pending_init.get();  // blocks until RPC completes
		global_execution_id = std::move(applied.first.execution_id);
		max_processes = static_cast<idx_t>(applied.first.max_workers);
		init_opaque_data = std::move(applied.first.opaque_data);
		// The async init returns the connection alongside the InitResult.
		std::lock_guard<std::mutex> conn_lk(connection_mutex);
		primary_connection = std::move(applied.second);
	}
	init_applied.store(true, std::memory_order_release);
}

// --- Result cache: key computation, eligibility, capture commit -------------
// SerializeSettingsForKey / SerializeProjectionForKey live in
// vgi_exchange_cache_key.cpp so both caches compute keys byte-identically.
namespace {

struct CacheEligibility {
	bool eligible = false;                  //! query is a cache candidate (opt-in gates pass)
	const char *ineligible_reason = nullptr; //! set when !eligible (for logging)
	VgiResultCacheKey key;                  //! populated when eligible (catalog-scoped: txn_id empty)
	std::string catalog_name;
	bool catalog_version_frozen = false;
	//! Current transaction id. The KEY stays catalog-scoped (scope is advertised
	//! only on the first batch); a `scope=transaction` result folds this in at
	//! commit, and lookups probe both the catalog key and key + this txn_id.
	std::string transaction_id;

	// --- Per-partition cache classification (SINGLE_VALUE_PARTITIONS) ---------
	//! Splittable: the captured result may be split into per-partition entries.
	//! False when not partition-scope-eligible, the setting is off, or a partition
	//! column has a non-`=`/`IN` predicate; the whole-scan cache is unaffected.
	bool partition_splittable = false;
	//! Enumerable: every partition column is `=`/`IN`-constrained, so the requested
	//! tuples are known (`partition_enumerated_tuples`). Only an enumerable scan
	//! can be SERVED from per-partition entries; otherwise it only POPULATES them.
	bool partition_enumerable = false;
	std::vector<std::vector<Value>> partition_enumerated_tuples; // parallel to partition_types
	std::string partition_residual_filter_bytes;                 // filter_bytes minus the partition predicate
	std::vector<idx_t> partition_column_indices;                 // output-schema indices, declared order
	std::vector<LogicalType> partition_types;                    // matching declared partition types
	std::vector<std::string> partition_names;                    // matching declared partition column names
	uint64_t partition_max = defaults::RESULT_CACHE_PARTITION_MAX_ENUMERATED;                               // cap on distinct/enumerated partitions
};

//! Resolve partition column names from the bind output schema (the index space
//! of `partition_column_indices`, which INCLUDES the rowid) and their types BY
//! NAME from all_column_names/types (rowid erased). False if any is unresolved.
static bool ResolvePartitionColumns(const VgiTableFunctionBindData &bind_data,
                                    std::vector<std::string> &names, std::vector<LogicalType> &types) {
	const auto &out_schema = bind_data.bind_result.output_schema;
	if (!out_schema) {
		return false;
	}
	for (idx_t pi : bind_data.partition_column_indices) {
		if (static_cast<int>(pi) >= out_schema->num_fields()) {
			return false;
		}
		const std::string nm = out_schema->field_name(static_cast<int>(pi));
		bool found = false;
		for (idx_t c = 0; c < bind_data.all_column_names.size(); c++) {
			if (bind_data.all_column_names[c] == nm) {
				names.push_back(nm);
				types.push_back(bind_data.all_column_types[c]);
				found = true;
				break;
			}
		}
		if (!found) {
			return false;
		}
	}
	return true;
}

//! True iff the (single-column) filter is an `=`, an `IN`, or an `OR` of those,
//! appending its constants to `out`; false for range / NULL / AND / other shapes.
static bool ExtractPartitionEqualityExpression(const Expression &expr, std::vector<Value> &out) {
	if (auto child = ExpressionFilter::GetOptionalFilterChild(expr)) {
		return ExtractPartitionEqualityExpression(*child, out);
	}
	if (BoundComparisonExpression::IsComparison(expr)) {
		auto comparison = expr.GetExpressionType();
		auto constant = ExpressionFilter::TryGetColumnConstantComparison(expr.Cast<BoundFunctionExpression>(), comparison);
		if (constant && comparison == ExpressionType::COMPARE_EQUAL) {
			out.push_back(constant->GetValue());
			return true;
		}
		return false;
	}
	if (expr.GetExpressionType() == ExpressionType::COMPARE_IN && expr.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR) {
		const auto &children = expr.Cast<BoundOperatorExpression>().GetChildren();
		if (children.size() < 2 || !ExpressionFilter::IsSimpleFilterColumnRef(*children[0])) return false;
		for (idx_t i = 1; i < children.size(); i++) {
			if (children[i]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) return false;
			out.push_back(children[i]->Cast<BoundConstantExpression>().GetValue());
		}
		return true;
	}
	if (expr.GetExpressionType() == ExpressionType::CONJUNCTION_OR) {
		const auto &children = expr.Cast<BoundConjunctionExpression>().GetChildren();
		if (children.empty()) return false;
		for (const auto &child : children) {
			if (!ExtractPartitionEqualityExpression(*child, out)) return false;
		}
		return true;
	}
	return false;
}

static bool ExtractPartitionEqualityValues(const TableFilter &filter, std::vector<Value> &out) {
	return ExtractPartitionEqualityExpression(*ExpressionFilter::GetExpressionFilter(filter, "VGI partition cache").expr, out);
}

//! Fill the partition_* fields of `e` for per-partition caching. Called only once
//! the base scan is cache-eligible (dynamic filters already rejected).
static void ClassifyPartitionFilters(ClientContext &context, const VgiTableFunctionBindData &bind_data,
                                     TableFunctionInitInput &input, CacheEligibility &e) {
	if (bind_data.partition_kind != VgiPartitionKind::SingleValuePartitions) {
		return;
	}
	Value pv;
	if (context.TryGetCurrentSetting("vgi_result_cache_partition_scope", pv) && !pv.IsNull() &&
	    !pv.GetValue<bool>()) {
		return;
	}
	// Concatenated per-partition entries can't honor a global ORDER BY, and
	// sampling semantics differ per partition: leave these to the whole-scan cache.
	if (bind_data.order_by_hint || bind_data.table_sample_hint) {
		return;
	}
	Value maxv;
	if (context.TryGetCurrentSetting("vgi_result_cache_partition_max_enumerated", maxv) && !maxv.IsNull()) {
		e.partition_max = maxv.GetValue<uint64_t>();
	}
	if (!ResolvePartitionColumns(bind_data, e.partition_names, e.partition_types)) {
		return;
	}
	e.partition_column_indices = bind_data.partition_column_indices;

	std::map<std::string, idx_t> name_to_pos;
	for (idx_t i = 0; i < e.partition_names.size(); i++) {
		name_to_pos[e.partition_names[i]] = i;
	}
	std::vector<std::vector<Value>> per_col(e.partition_names.size());
	std::vector<bool> col_constrained(e.partition_names.size(), false);
	std::set<idx_t> exclude_keys; // projected filter-map keys that are partition predicates
	bool splittable = true;
	bool any_partition_pred = false;

	if (input.filters) {
		for (auto &entry : *input.filters) {
			const idx_t col_idx = entry.GetIndex().GetIndex();
			auto &filter = entry.Filter();
			const idx_t original_col_idx =
			    col_idx < input.column_ids.size() ? input.column_ids[col_idx] : col_idx;
			std::string col_name;
			if (original_col_idx == COLUMN_IDENTIFIER_ROW_ID && !bind_data.rowid_column_name.empty()) {
				col_name = bind_data.rowid_column_name;
			} else {
				col_name = original_col_idx < bind_data.all_column_names.size()
				               ? bind_data.all_column_names[original_col_idx]
				               : std::to_string(original_col_idx);
			}
			auto it = name_to_pos.find(col_name);
			if (it == name_to_pos.end()) {
				continue; // non-partition predicate → stays in the residual
			}
			any_partition_pred = true;
			exclude_keys.insert(col_idx);
			const idx_t pos = it->second;
			std::vector<Value> vals;
			if (!ExtractPartitionEqualityValues(filter, vals)) {
				splittable = false; // range/other on a partition column → ineligible
				break;
			}
			for (auto &v : vals) {
				per_col[pos].push_back(v);
			}
			col_constrained[pos] = true;
		}
	}
	if (!splittable) {
		return; // whole-scan cache still operates; per-partition layer disabled
	}
	e.partition_splittable = true;

	// Residual = pushed filters MINUS partition predicates. CRITICAL: if every
	// filter is a partition predicate the residual must be "" (as for an unfiltered
	// scan) so `WHERE part=X` reuses entries a full scan stored; serializing the
	// all-excluded set would yield a non-empty IPC batch that never matches.
	bool has_residual_filter = false;
	if (input.filters) {
		for (auto &entry : *input.filters) {
			if (!exclude_keys.count(entry.GetIndex().GetIndex())) {
				has_residual_filter = true;
				break;
			}
		}
	}
	if (has_residual_filter) {
		try {
			auto sf = VgiSerializeFilters(context, input.column_ids, input.filters,
			                              bind_data.all_column_names, bind_data.worker_path(),
			                              bind_data.rowid_column_name, bind_data.rowid_worker_col_index,
			                              &exclude_keys, VgiFilterColumnIndexDomain::PROJECTED,
			                              bind_data.additional_filter_functions);
			if (sf.filter_bytes) {
				e.partition_residual_filter_bytes.append(reinterpret_cast<const char *>(sf.filter_bytes->data()),
				                                         static_cast<size_t>(sf.filter_bytes->size()));
			}
			for (auto &jk : sf.join_keys_buffers) {
				e.partition_residual_filter_bytes.push_back('|');
				e.partition_residual_filter_bytes.append(reinterpret_cast<const char *>(jk->data()),
				                                         static_cast<size_t>(jk->size()));
			}
		} catch (const std::exception &) {
			e.partition_splittable = false;
			return;
		}
	}

	// Enumerable requires a partition predicate on EVERY partition column.
	if (!any_partition_pred) {
		return; // full/partition-free scan: populate-only (can't enumerate the universe)
	}
	for (bool c : col_constrained) {
		if (!c) {
			return; // some partition column free → populate-only
		}
	}
	// Cross product of the per-column value sets → the requested tuple set.
	std::vector<std::vector<Value>> tuples;
	tuples.emplace_back();
	for (idx_t i = 0; i < per_col.size(); i++) {
		std::vector<std::vector<Value>> next;
		for (auto &prefix : tuples) {
			for (auto &v : per_col[i]) {
				auto t = prefix;
				t.push_back(v);
				next.push_back(std::move(t));
				if (next.size() > e.partition_max) {
					return; // too many → populate-only (no serve)
				}
			}
		}
		tuples = std::move(next);
	}
	e.partition_enumerated_tuples = std::move(tuples);
	e.partition_enumerable = true;
}

//! "p:"-prefixed input_hash discriminator for a partition tuple. Serve (filter
//! constants) and capture (decoded partition values) MUST both use this with the
//! same `partition_types` so identical tuples match.
static std::string PartitionDiscriminator(ClientContext &context, const std::vector<Value> &tuple,
                                          const std::vector<LogicalType> &partition_types) {
	return "p:" + VgiSha256Hex(vgi::CanonicalPartitionTupleKey(context, tuple, partition_types));
}

//! Per-partition key from the whole-scan key: residual filter, "p:" discriminator,
//! and no order/sample/transaction dimensions (catalog-scoped by construction).
static VgiResultCacheKey BuildPartitionKey(const VgiResultCacheKey &base, const std::string &residual,
                                           const std::string &disc) {
	VgiResultCacheKey k = base;
	k.filter_bytes = residual;
	k.order_by_hint.clear();
	k.sample_hint.clear();
	k.transaction_id.clear();
	k.input_hash = disc;
	return k;
}

//! "name=value, name2=value2" label of a partition tuple (diagnostics).
static std::string PartitionLabel(const std::vector<std::string> &names, const std::vector<Value> &tuple) {
	std::string out;
	for (idx_t i = 0; i < names.size() && i < tuple.size(); i++) {
		if (i) {
			out += ", ";
		}
		out += names[i];
		out += "=";
		out += tuple[i].IsNull() ? "NULL" : tuple[i].ToString();
	}
	return out;
}

//! Evaluate result-cache eligibility for a scan and build its key.
CacheEligibility EvaluateCacheEligibility(ClientContext &context,
                                          const VgiTableFunctionBindData &bind_data,
                                          TableFunctionInitInput &input,
                                          const std::vector<int32_t> &projection_ids) {
	CacheEligibility e;
	// Cache keys do not include the provider snapshot in opaque_data. A
	// subsequent bind may observe different rows even at the same catalog version.
	if (bind_data.bind_result.repeatability == BindRepeatability::STABLE_WITHIN_QUERY) {
		e.ineligible_reason = "query_scoped_repeatability";
		return e;
	}
	if (!bind_data.projection_response_bytes.empty()) {
		e.ineligible_reason = "expression_projection";
		return e;
	}
	Value master;
	if (context.TryGetCurrentSetting("vgi_result_cache", master) && !master.GetValue<bool>()) {
		e.ineligible_reason = "disabled_global";
		return e;
	}
	if (!bind_data.attach_params->cache()) {
		e.ineligible_reason = "disabled_attach";
		return e;
	}
	// A secret-dependent result is keyed on a fingerprint of the secrets its bind
	// sent (never the values), so a rotated or dropped secret misses. No
	// fingerprint: fail closed.
	const std::string &secret_scope = bind_data.bind_result.secret_scope;
	if (bind_data.secret_dependent && secret_scope.empty()) {
		e.ineligible_reason = "secret_fingerprint_missing";
		return e;
	}
	// Identity + version dimensions:
	//   - catalog table (table_entry set): identity = catalog name + principal,
	//     bounded by catalog_version (0 = unknown → never cache: a version bump is
	//     how catalog data invalidates).
	//   - table-function call (no table_entry): identity = worker_path +
	//     principal, no version dimension, TTL-only freshness.
	std::string identity_scope;
	int64_t version = 0;
	std::string catalog_label;
	if (bind_data.table_entry) {
		// const_cast: GetCatalogType()/Cast() are non-const.
		auto &catalog = const_cast<Catalog &>(bind_data.table_entry->ParentCatalog());
		if (catalog.GetCatalogType() != "vgi") {
			e.ineligible_reason = "not_vgi";
			return e;
		}
		auto &vcat = catalog.Cast<VgiCatalog>();
		version = vcat.GetKnownCatalogVersion();
		if (version == 0) {
			e.ineligible_reason = "unknown_version";
			return e;
		}
		// SECURITY-CRITICAL: fold the caller's auth principal into the identity
		// scope so two attaches with the same alias+worker+args but different
		// bearer/OAuth identities NEVER share a cache entry. "" = configured but
		// unresolvable → fail closed (never cache under an ambiguous principal).
		catalog_label = bind_data.attach_params->catalog_name();
		identity_scope =
		    vgi::BuildCatalogIdentityScope(catalog_label, bind_data.attach_params->auth());
		if (identity_scope.empty()) {
			e.ineligible_reason = "identity_unresolved";
			return e;
		}
	} else {
		// version stays 0 here: "no version dimension", NOT "unknown version".
		// The auth principal is still folded in; "" fails closed.
		catalog_label = bind_data.worker_path();
		std::string principal =
		    vgi::ComputeCatalogIdentityFingerprint(bind_data.attach_params->auth());
		if (principal.empty()) {
			e.ineligible_reason = "identity_unresolved";
			return e;
		}
		identity_scope = "worker:" + bind_data.worker_path() + "\x1f" + principal;
	}

	// Static pushdowns (filter / order / sample) are KEY COMPONENTS. Two kinds
	// DISABLE caching: runtime-valued filters (no stable key at init) and
	// unseeded TABLESAMPLE (non-deterministic).
	std::string filter_key;
	if (input.filters) {
		for (auto &kv : *input.filters) {
			if (VgiContainsDynamicFilter(kv.Filter())) {
				e.ineligible_reason = "dynamic_filter";
				return e;
			}
		}
		if (input.filters->HasFilters()) {
			// Unserializable filter: fail closed.
			try {
				auto sf = VgiSerializeFilters(context, input.column_ids, input.filters,
				                              bind_data.all_column_names, bind_data.worker_path(),
				                              bind_data.rowid_column_name, bind_data.rowid_worker_col_index, nullptr,
				                              VgiFilterColumnIndexDomain::PROJECTED,
				                              bind_data.additional_filter_functions);
				if (sf.filter_bytes) {
					filter_key.append(reinterpret_cast<const char *>(sf.filter_bytes->data()),
					                  static_cast<size_t>(sf.filter_bytes->size()));
				}
				for (auto &jk : sf.join_keys_buffers) {
					filter_key.push_back('|');
					filter_key.append(reinterpret_cast<const char *>(jk->data()),
					                  static_cast<size_t>(jk->size()));
				}
			} catch (const std::exception &) {
				e.ineligible_reason = "unserializable_filter";
				return e;
			}
		}
	}
	std::string order_key;
	if (bind_data.order_by_hint) {
		const auto &o = *bind_data.order_by_hint;
		order_key = o.column_name + "/" + o.direction + "/" + o.null_order + "/" +
		            std::to_string(o.row_limit);
	}
	std::string sample_key;
	if (bind_data.table_sample_hint) {
		const auto &s = *bind_data.table_sample_hint;
		if (s.seed < 0) {
			e.ineligible_reason = "unseeded_sample";
			return e;
		}
		sample_key = std::to_string(s.sample_percentage) + "/" + std::to_string(s.seed);
	}

	e.key.identity_scope = identity_scope;
	e.key.worker_path = bind_data.worker_path();
	e.key.function_name = bind_data.function_name;
	e.key.schema_name = bind_data.schema_name;
	e.key.canonical_arguments = SerializeArgumentsForKey(bind_data.arguments.batch);
	e.key.canonical_settings = SerializeSettingsForKey(bind_data.settings);
	e.key.attach_options = bind_data.attach_params->attach_options_canonical();
	e.key.projection = SerializeProjectionForKey(projection_ids);
	e.key.attached_data_version = bind_data.attach_params->data_version_spec();
	e.key.implementation_version = bind_data.attach_params->implementation_version();
	e.key.catalog_version = version;
	e.key.at_unit = bind_data.at_unit;
	e.key.at_value = bind_data.at_value;
	e.key.secret_scope = bind_data.secret_dependent ? secret_scope : "";
	e.key.filter_bytes = std::move(filter_key);
	e.key.order_by_hint = std::move(order_key);
	e.key.sample_hint = std::move(sample_key);
	e.transaction_id = std::to_string(static_cast<uint64_t>(context.ActiveTransaction().global_transaction_id));
	e.catalog_name = catalog_label;
	ClassifyPartitionFilters(context, bind_data, input, e);
	e.eligible = true;
	return e;
}

void LogResultCache(ClientContext &context, const string &event,
                    const vector<pair<string, string>> &fields) {
	VGI_LOG(context, event, fields);
}

} // namespace

// Drain a RAM substream into the spill writer, freeing it. False if the writer
// refused (per-entry disk budget exceeded).
static bool DrainSubstreamToWriter(CachedStream &s, VgiCaptureDiskWriter &w) {
	bool ok = true;
	for (auto &cb : s.batches) {
		if (ok && cb.ipc) {
			// Pre-threshold batches are uncompressed bytes: transcode to the
			// writer's codec; the logical length stays the uncompressed size.
			auto payload = TranscodeIpcWithCodec(cb.ipc, w.CodecName(), w.Level());
			ok = w.AppendBatch(cb.has_batch_index, cb.batch_index, cb.rows, cb.partition_values_bytes,
			                   payload->data(), static_cast<int64_t>(payload->size()),
			                   static_cast<int64_t>(cb.ipc->size()));
		}
	}
	s.batches.clear(); // free the RAM regardless (streaming from here on)
	s.bytes = 0;
	s.rows = 0;
	return ok;
}

static void SplitAndStorePartitionEntries(ClientContext *ctx, VgiResultCaptureCtx &cap,
                                          const VgiResultCacheEntry &meta, bool allow_disk);

static void InstallProtocolSplitPartitionData(ClientContext &context,
                                              const VgiTableFunctionBindData &bind_data,
                                              const VgiScanPlan &plan,
                                              VgiTableFunctionGlobalState &global_state);

// Commits only a fully drained capture the worker advertised as cacheable.
VgiTableFunctionGlobalState::~VgiTableFunctionGlobalState() {
	if (!capture) {
		return;
	}
	auto &cap = *capture;
	// Release the in-flight budget first so no early return leaks it.
	int64_t reserved = cap.reserved_inflight_bytes.exchange(0, std::memory_order_relaxed);
	if (reserved > 0) {
		(*cap.cache).ReleaseInflightCapture(reserved);
	}
	// Every non-commit path must discard a streamed temp blob.
	auto abort_stream = [&]() {
		if (cap.disk_writer) {
			(*cap.cache).AbortStreamingCapture(*cap.disk_writer);
		}
	};
	// Best-effort diagnostics; the ClientContext is alive during query teardown.
	auto log_store = [&](const string &event, const vector<pair<string, string>> &fields) {
		if (client_context_for_explain) {
			try {
				VGI_LOG(*client_context_for_explain, event, fields);
			} catch (...) {
			}
		}
	};
	auto skip = [&](const char *reason) {
		log_store("result_cache.store_skipped",
		          {{"function", cap.key.function_name}, {"key_hash", cap.key.HexDigest()}, {"reason", reason}});
	};

	// The split path also needs every split drained (see
	// VgiResultCaptureCtx::split_path).
	const bool splits_all_drained =
	    !cap.split_path || (cap.total_splits > 0 && cap.completed_splits.load() == cap.total_splits);
	const bool complete = cap.cc_seen && cap.cc.Cacheable() && !cap.aborted.load() &&
	                      cap.launched.load() > 0 && cap.eos.load() == cap.launched.load() &&
	                      splits_all_drained;
	if (!complete) {
		abort_stream();
		return;
	}
	// scope=transaction: fold the txn id into the key so other transactions miss.
	const bool txn_scoped = cap.cc.TransactionScoped();
	if (txn_scoped && cap.transaction_id.empty()) {
		skip("no_transaction_id");
		abort_stream();
		return;
	}
	auto entry = std::make_shared<VgiResultCacheEntry>();
	entry->key = cap.key;
	if (txn_scoped) {
		entry->key.transaction_id = cap.transaction_id;
	}
	entry->catalog_name = cap.catalog_name;
	entry->scope = cap.cc.scope;
	entry->etag = cap.cc.etag;
	entry->last_modified = cap.cc.last_modified;
	entry->revalidatable = cap.cc.revalidatable;
	entry->stored_at = std::chrono::steady_clock::now();
	// ttl wins over expires; frozen/at-pinned may never expire. Clamp the
	// settings default too so `stored_at + seconds(ttl)` cannot overflow.
	int64_t ttl = cap.cc.ttl_seconds.value_or(static_cast<int64_t>(cap.default_ttl_seconds));
	if (ttl > VGI_CACHE_MAX_TTL_SECONDS) {
		ttl = VGI_CACHE_MAX_TTL_SECONDS;
	}
	if (ttl <= 0 && cap.cc.expires_rfc3339.empty()) {
		if (cap.catalog_version_frozen || !cap.key.at_value.empty()) {
			entry->never_expires = true;
		} else if (cap.cc.revalidatable && !cap.cc.etag.empty()) {
			// HTTP "no-cache": store immediately stale (expires_at == stored_at)
			// so every read revalidates with a conditional request.
		} else {
			skip("no_freshness");
			abort_stream();
			return; // no freshness basis (Cacheable() should have refused)
		}
	}
	entry->expires_at = entry->stored_at + std::chrono::seconds(ttl > 0 ? ttl : 0);

	if (cap.streaming()) {
		// A spilled entry is disk-only, but LookupForRevalidation probes only
		// memory and the disk loaders reject expired entries: an immediately-stale
		// result would be an unloadable orphan blob. Refuse.
		if (!entry->never_expires && entry->expires_at <= entry->stored_at) {
			skip("immediately_stale");
			abort_stream();
			return;
		}
		// Transaction-scoped entries are memory-only (process-local txn id), and a
		// spilled one has no RAM copy.
		if (txn_scoped) {
			skip("transaction_scoped_spill");
			abort_stream();
			return;
		}
		// Drain substreams that finished BEFORE the spill (all producers are done),
		// then finalize. A failed drain/commit caches nothing.
		for (auto &sp : cap.streams) {
			if (!sp->batches.empty() && !DrainSubstreamToWriter(*sp, *cap.disk_writer)) {
				skip("drain_failed");
				(*cap.cache).AbortStreamingCapture(*cap.disk_writer);
				return;
			}
		}
		if ((*cap.cache).CommitStreamingCapture(*cap.disk_writer, *entry)) {
			log_store("result_cache.store", {{"function", cap.key.function_name},
			                                 {"key_hash", entry->key.HexDigest()},
			                                 {"tier", "disk"},
			                                 {"rows", std::to_string(cap.disk_writer->Rows())},
			                                 {"bytes", std::to_string(cap.disk_writer->Bytes())}});
		}
		// No RAM batches remain to split by partition.
		if (cap.partition_split_requested && cap.cc.partition_scope) {
			skip("partition_spilled");
		}
		return;
	}
	// Per-partition split, BEFORE the whole-scan move consumes the substreams.
	// Skipped for transaction-scoped results (partition entries are
	// catalog-scoped). MEMORY-ONLY: the combined serve probes only memory
	// (LookupBatch), and a persisted input_hash-keyed entry would be routed into
	// the packed exchange store.
	if (cap.partition_split_requested && cap.cc.partition_scope && !txn_scoped) {
		SplitAndStorePartitionEntries(client_context_for_explain, cap, *entry, /*allow_disk=*/false);
	}
	// Not spilled: move the substreams into the entry.
	int64_t rows = 0;
	int64_t bytes = 0;
	for (auto &sp : cap.streams) {
		rows += sp->rows;
		bytes += sp->bytes;
		entry->streams.push_back(std::move(*sp));
	}
	entry->rows = rows;
	entry->total_bytes = bytes;
	const std::string stored_key_hash = entry->key.HexDigest();
	if ((*cap.cache).Insert(std::move(entry), /*allow_disk=*/!txn_scoped)) {
		log_store("result_cache.store", {{"function", cap.key.function_name},
		                                 {"key_hash", stored_key_hash},
		                                 // Insert writes through to disk when allowed.
		                                 {"tier", txn_scoped ? "memory" : "memory_or_disk"},
		                                 {"rows", std::to_string(rows)},
		                                 {"bytes", std::to_string(bytes)}});
	} else {
		skip("too_large_for_memory");
	}
}

// Catalog LogicalGet IDs include virtual generated fields. Worker schemas contain
// only physical fields; normalize once before any init projection/cache processing.
// The existing table_entry survives bind-data copies and plan deserialization.
static bool HasGeneratedCatalogColumns(const VgiTableFunctionBindData &bind_data) {
	return bind_data.table_entry && bind_data.table_entry->GetColumns().PhysicalColumnCount() !=
	                                    bind_data.table_entry->GetColumns().LogicalColumnCount();
}

static TableFunctionInitInput PhysicalCatalogScanInput(const TableFunctionInitInput &logical_input) {
	auto input = logical_input;
	auto &bind_data = input.bind_data->Cast<VgiTableFunctionBindData>();
	if (!HasGeneratedCatalogColumns(bind_data)) {
		return input;
	}
	auto &columns = bind_data.table_entry->GetColumns();
	for (idx_t i = 0; i < input.column_ids.size(); ++i) {
		auto &column_id = input.column_ids[i];
		if (column_id == COLUMN_IDENTIFIER_ROW_ID) {
			continue;
		}
		if (column_id == COLUMN_IDENTIFIER_EMPTY) {
			// Row-count scans still read one physical field; VGI cannot request
			// a zero-column worker projection yet. Its type matches this virtual.
			column_id = 0;
			input.column_indexes[i] = ColumnIndex(0);
			continue;
		}
		if (column_id >= columns.LogicalColumnCount() || columns.GetColumn(LogicalIndex(column_id)).Generated()) {
			throw InvalidInputException("VGI generated column must be expanded before scanning physical fields");
		}
		column_id = columns.GetColumn(LogicalIndex(column_id)).Physical().index;
		input.column_indexes[i] = ColumnIndex(column_id, input.column_indexes[i].GetChildIndexes());
	}
	return input;
}

unique_ptr<GlobalTableFunctionState> VgiTableFunctionInitGlobal(ClientContext &context,
                                                                TableFunctionInitInput &logical_input) {
	auto input = PhysicalCatalogScanInput(logical_input);
	auto &bind_data = input.bind_data->Cast<VgiTableFunctionBindData>();

	// TABLESAMPLE pushdown (SYSTEM_SAMPLE percentage only).
	if (input.sample_options) {
		double pct = input.sample_options->sample_size.GetValue<double>();
		int64_t seed = input.sample_options->seed.IsValid()
		    ? static_cast<int64_t>(input.sample_options->seed.GetIndex()) : -1;
		bind_data.table_sample_hint = TableSampleHint{pct, seed};
	}

	// Worker projection ids, only when the function supports projection
	// pushdown. BuildInitRequest encodes an empty vector as NULL ("no projection
	// pushdown": send every column), not an empty list. Known limitation: a
	// genuine zero-column projection (`[]`, which vgi-python accepts) cannot be
	// expressed without making this chain carry std::optional.
	std::vector<int32_t> projection_ids;
	if (bind_data.projection_pushdown) {
		projection_ids.reserve(input.column_ids.size());
		for (auto col_id : input.column_ids) {
			if (col_id == COLUMN_IDENTIFIER_ROW_ID && bind_data.rowid_worker_col_index >= 0) {
				projection_ids.push_back(static_cast<int32_t>(bind_data.rowid_worker_col_index));
			} else if (bind_data.rowid_worker_col_index >= 0 &&
			           col_id != COLUMN_IDENTIFIER_ROW_ID &&
			           col_id >= static_cast<idx_t>(bind_data.rowid_worker_col_index)) {
				// Shift past the rowid column erased from DuckDB's column list.
				projection_ids.push_back(static_cast<int32_t>(col_id + 1));
			} else {
				projection_ids.push_back(static_cast<int32_t>(col_id));
			}
		}
	}

	// --- Result cache: eligibility + serve (cache hit short-circuits worker) ---
	// Sync the process-global cache config from this query's settings (shared
	// with the exchange path; a no-op when unchanged).
	SyncResultCacheSettings(context);
	auto cache_eval = EvaluateCacheEligibility(context, bind_data, input, projection_ids);
	// A stale-but-revalidatable entry, set below; arms revalidation mode when the
	// gstate is built on the miss path.
	std::shared_ptr<const VgiResultCacheEntry> revalidation_entry;
	// INVARIANT: never revalidate on the split path. ``revalidating`` clamps
	// MaxThreads() to 1, so one local state drains EVERY split; a not_modified on
	// the first split would swap in a replay of the WHOLE entry and splits 1..N
	// would duplicate it. The probe itself is skipped because
	// LookupForRevalidation() arms revalidation mode as a side effect.
	//
	// An ORDINARY hit IS allowed on the split path: it returns before the plan RPC,
	// so the scan never becomes a split scan. Do not gate the whole block on
	// !split_path, or split-capable functions never serve from cache.
	const bool split_path = bind_data.supports_splits;
	if (cache_eval.eligible) {
		auto now_tp = std::chrono::steady_clock::now();
		// Probe for a stale revalidatable entry FIRST: Lookup() drops stale
		// entries. Never on the split path (see above).
		auto reval = split_path ? nullptr
		                        : GetResultCache(context).LookupForRevalidation(cache_eval.key, now_tp);
		if (reval) {
			// Only worth a conditional round-trip for a large enough payload.
			Value rmv;
			uint64_t reval_min_bytes =
			    context.TryGetCurrentSetting("vgi_result_cache_revalidate_min_bytes", rmv) && !rmv.IsNull()
			        ? rmv.GetValue<uint64_t>()
			        : defaults::RESULT_CACHE_REVALIDATE_MIN_BYTES;
			if (static_cast<uint64_t>(reval->total_bytes) >= reval_min_bytes) {
				revalidation_entry = std::move(reval);
				LogResultCache(context, "result_cache.revalidate",
				               {{"catalog", cache_eval.catalog_name},
				                {"function", bind_data.function_name},
				                {"key_hash", cache_eval.key.HexDigest()},
				                {"outcome", "conditional_request"}});
			}
			// else: too small — fall through to a plain refetch (capture below).
		}
		if (!revalidation_entry) {
			const char *hit_tier = "memory";
			auto entry = GetResultCache(context).Lookup(cache_eval.key, now_tp, &hit_tier);
			if (!entry && !cache_eval.transaction_id.empty()) {
				// A scope=transaction result from THIS transaction lives under
				// key + txn_id.
				VgiResultCacheKey txn_key = cache_eval.key;
				txn_key.transaction_id = cache_eval.transaction_id;
				entry = GetResultCache(context).Lookup(txn_key, now_tp, &hit_tier);
			}
			if (entry) {
				// HIT — serve from cache; no worker acquire / init RPC / pool touch.
				auto gstate = make_uniq<VgiTableFunctionGlobalState>();
				gstate->serving_from_cache = true;
				gstate->serving_entry = std::move(entry);
				gstate->max_processes = 1;
				gstate->init_applied.store(true, std::memory_order_release);
				gstate->client_context_for_explain = &context;
				LogResultCache(context, "result_cache.hit",
				               {{"catalog", cache_eval.catalog_name},
				                {"function", bind_data.function_name},
				                {"key_hash", cache_eval.key.HexDigest()},
				                {"tier", hit_tier}});
				return unique_ptr<GlobalTableFunctionState>(std::move(gstate));
			}
			LogResultCache(context, "result_cache.miss",
			               {{"catalog", cache_eval.catalog_name},
			                {"function", bind_data.function_name},
			                {"key_hash", cache_eval.key.HexDigest()}});

			// Per-partition serve: if the scan enumerates its partition set and every
			// per-partition entry is an in-memory hit, serve the union. Row-exact:
			// every entry keys on the same residual filter, the enumerated set equals
			// the requested set, and SINGLE_VALUE puts each row in exactly one entry.
			if (cache_eval.partition_enumerable && !cache_eval.partition_enumerated_tuples.empty()) {
				std::vector<VgiResultCacheKey> pkeys;
				pkeys.reserve(cache_eval.partition_enumerated_tuples.size());
				for (auto &tuple : cache_eval.partition_enumerated_tuples) {
					auto disc = PartitionDiscriminator(context, tuple, cache_eval.partition_types);
					pkeys.push_back(
					    BuildPartitionKey(cache_eval.key, cache_eval.partition_residual_filter_bytes, disc));
				}
				auto hits = GetResultCache(context).LookupBatch(pkeys, now_tp);
				bool all_hit = !pkeys.empty();
				int64_t missing = 0;
				for (auto &h : hits) {
					// A disk-backed entry can't join the combined replay: a miss.
					if (!h || h->disk_backed) {
						all_hit = false;
						++missing;
					}
				}
				if (all_hit) {
					// One partition-contiguous stream with fresh sequential batch_index,
					// so CachedReplayConnection's sort keeps each partition together
					// (PhysicalPartitionedAggregate requires it).
					auto combined = std::make_shared<VgiResultCacheEntry>();
					combined->key = cache_eval.key;
					combined->catalog_name = cache_eval.catalog_name;
					combined->never_expires = true; // transient serve entry (not inserted)
					CachedStream merged;
					uint64_t seq = 0;
					int64_t total_rows = 0;
					int64_t total_bytes = 0;
					for (auto &h : hits) {
						for (auto &s : h->streams) {
							for (auto &b : s.batches) {
								VgiCachedBatch nb = b; // shared_ptr ipc copy
								nb.has_batch_index = true;
								nb.batch_index = seq++;
								total_rows += b.rows;
								merged.batches.push_back(std::move(nb));
							}
						}
						total_bytes += h->total_bytes;
					}
					merged.rows = total_rows;
					merged.bytes = total_bytes;
					combined->streams.push_back(std::move(merged));
					combined->rows = total_rows;
					combined->total_bytes = total_bytes;

					auto gstate = make_uniq<VgiTableFunctionGlobalState>();
					gstate->serving_from_cache = true;
					gstate->serving_entry = std::move(combined);
					gstate->max_processes = 1;
					gstate->init_applied.store(true, std::memory_order_release);
					gstate->client_context_for_explain = &context;
					GetResultCache(context).RecordPartitionHit();
					LogResultCache(context, "result_cache.partition_hit",
					               {{"catalog", cache_eval.catalog_name},
					                {"function", bind_data.function_name},
					                {"key_hash", cache_eval.key.HexDigest()},
					                {"partitions", std::to_string(pkeys.size())}});
					return unique_ptr<GlobalTableFunctionState>(std::move(gstate));
				}
				GetResultCache(context).RecordPartitionMiss();
				LogResultCache(context, "result_cache.partition_miss",
				               {{"catalog", cache_eval.catalog_name},
				                {"function", bind_data.function_name},
				                {"key_hash", cache_eval.key.HexDigest()},
				                {"requested", std::to_string(pkeys.size())},
				                {"missing", std::to_string(missing)}});
				// fall through to the worker scan (which repopulates per-partition entries).
			}
		}
	} else if (cache_eval.ineligible_reason) {
		LogResultCache(context, "result_cache.ineligible",
		               {{"function", bind_data.function_name},
		                {"reason", cache_eval.ineligible_reason}});
	}

	// No second bind: PerformInit carries the cached BindResult inline.
	FunctionConnectionParams acquire_params;
	acquire_params.attach_params = bind_data.attach_params;
	acquire_params.attach_opaque_data = bind_data.attach_opaque_data;
	acquire_params.function_name = bind_data.function_name;
	acquire_params.schema_name = bind_data.schema_name;
	acquire_params.arguments = bind_data.arguments;
	acquire_params.transaction_opaque_data = bind_data.transaction_opaque_data;
	acquire_params.settings = bind_data.settings;
	acquire_params.required_secrets = bind_data.required_secrets;
	acquire_params.phase = "init_global";
	acquire_params.function_type = "TABLE";
	auto acquired = AcquireConnectionForInit(context, acquire_params);
	auto connection = std::move(acquired.connection);
	const auto &bind_result = bind_data.bind_result;

	// DuckDB has delegated required filters to the scan, so a serialization
	// failure must fail closed rather than drop them.
	auto serialized_filters =
	    VgiSerializeFilters(context, input.column_ids, input.filters, bind_data.all_column_names,
	                        bind_data.worker_path(), bind_data.rowid_column_name, bind_data.rowid_worker_col_index,
	                        nullptr, VgiFilterColumnIndexDomain::PROJECTED,
	                        bind_data.additional_filter_functions);
	if (serialized_filters.filter_bytes) {
		VGI_LOG(context, "table_function.filters_serialized",
		        {{"function_name", bind_data.function_name},
		         {"filter_bytes_size", std::to_string(serialized_filters.filter_bytes->size())}});
	}
	if (!serialized_filters.join_keys_buffers.empty()) {
		idx_t total_size = 0;
		for (auto &buf : serialized_filters.join_keys_buffers) {
			total_size += buf->size();
		}
		VGI_LOG(context, "table_function.join_keys_serialized",
		        {{"function_name", bind_data.function_name},
		         {"join_keys_count", std::to_string(serialized_filters.join_keys_buffers.size())},
		         {"join_keys_total_bytes", std::to_string(total_size)}});
	}
	auto &filter_bytes = serialized_filters.filter_bytes;
	auto &join_keys_buffers = serialized_filters.join_keys_buffers;

	// Capture Top-N DynamicFilters for tick-based pushdown: optional dynamic
	// comparisons and optional OR(IS NULL, dynamic), also inside a conjunction.
	// Other shapes cannot be reduced to a threshold.
	vector<VgiDynamicFilterInfo> dynamic_filters;
	if (input.filters) {
		auto capture = [&](idx_t column_index, const string &column_name, const Expression &root) {
			std::function<void(const Expression &, bool, bool)> visit;
			visit = [&](const Expression &expr, bool optional, bool nulls_first) {
				if (auto child = ExpressionFilter::GetOptionalFilterChild(expr)) {
					visit(*child, true, nulls_first);
					return;
				}
				if (optional && expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
					const auto &function = expr.Cast<BoundFunctionExpression>();
					if (function.Function().GetName() == DynamicFilterScalarFun::NAME && function.BindInfo() &&
					    function.GetChildren().size() == 1 && ExpressionFilter::IsSimpleFilterColumnRef(*function.GetChildren()[0])) {
						auto data = function.BindInfo()->Cast<DynamicFilterFunctionData>().filter_data;
						if (data) {
							VgiDynamicFilterInfo info;
							info.filter_data = std::move(data);
							info.column_index = column_index;
							info.column_name = column_name;
							info.comparison_type = info.filter_data->comparison_type;
							info.nulls_first = nulls_first;
							dynamic_filters.push_back(std::move(info));
						}
						return;
					}
				}
				if (expr.GetExpressionType() == ExpressionType::CONJUNCTION_OR) {
					const auto &children = expr.Cast<BoundConjunctionExpression>().GetChildren();
					// Only OR(IS NULL, dynamic) is a representable Top-N threshold.
					if (!optional || children.size() != 2) {
						return;
					}
					for (idx_t i = 0; i < children.size(); i++) {
						if (children[i]->GetExpressionType() != ExpressionType::OPERATOR_IS_NULL) {
							continue;
						}
						const auto &null_children = children[i]->Cast<BoundOperatorExpression>().GetChildren();
						const auto &threshold = *children[1 - i];
						if (null_children.size() == 1 &&
						    ExpressionFilter::IsSimpleFilterColumnRef(*null_children[0]) &&
						    threshold.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
						    threshold.Cast<BoundFunctionExpression>().Function().GetName() == DynamicFilterScalarFun::NAME) {
							visit(threshold, true, true);
						}
					}
					return;
				}
				if (expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
					for (const auto &child : expr.Cast<BoundConjunctionExpression>().GetChildren()) {
						visit(*child, optional, nulls_first);
					}
				}
			};
			visit(root, false, false);
		};

		for (auto &entry : *input.filters) {
			idx_t col_idx = entry.GetIndex().GetIndex();
			auto &filter = entry.Filter();

			idx_t original_col_idx = col_idx < input.column_ids.size() ? input.column_ids[col_idx] : col_idx;
			// Same rowid / column-shift remap as VgiSerializeFilters.
			string col_name;
			idx_t worker_col_idx;
			if (original_col_idx == COLUMN_IDENTIFIER_ROW_ID && !bind_data.rowid_column_name.empty()) {
				col_name = bind_data.rowid_column_name;
				if (bind_data.rowid_worker_col_index < 0) {
					continue;
				}
				worker_col_idx = NumericCast<idx_t>(bind_data.rowid_worker_col_index);
			} else {
				if (original_col_idx == COLUMN_IDENTIFIER_ROW_ID ||
				    original_col_idx >= bind_data.all_column_names.size()) {
					continue;
				}
				col_name = bind_data.all_column_names[original_col_idx];
				worker_col_idx = original_col_idx;
				if (bind_data.rowid_worker_col_index >= 0 &&
				    worker_col_idx >= NumericCast<idx_t>(bind_data.rowid_worker_col_index)) {
					worker_col_idx++;
				}
			}

			capture(worker_col_idx, col_name, *ExpressionFilter::GetExpressionFilter(filter, "VGI dynamic capture").expr);
		}
	}

	if (!dynamic_filters.empty()) {
		for (idx_t i = 0; i < dynamic_filters.size(); i++) {
			dynamic_filters[i].predicate_id = "top_n:" + std::to_string(i);
		}
		VGI_LOG(context, "table_function.dynamic_filters_captured",
		        {{"function_name", bind_data.function_name},
		         {"count", std::to_string(dynamic_filters.size())}});
	}

	// Updated from DynamicFilterData by the scan, read by the connection for ticks.
	shared_ptr<TickFilterState> tick_filter_state;
	if (!dynamic_filters.empty()) {
		tick_filter_state = make_shared_ptr<TickFilterState>();
		connection->SetTickFilterState(tick_filter_state);
	}

	// Build everything that doesn't depend on the init RPC now; PerformInit runs
	// on a background thread and is awaited by EnsureInitApplied.
	auto global_state = make_uniq<VgiTableFunctionGlobalState>();
	global_state->client_context_for_explain = &context;
	global_state->supports_splits = bind_data.supports_splits;
	// Provisional; EnsureInitApplied or the split plan below sets the real
	// value. Must be assigned BEFORE the plan, or it would overwrite the plan's.
	global_state->max_processes = 1;

	// Split path: plan the scan into splits BEFORE any reader starts. The plan uses
	// STATIC filters only; dynamic values still reach the worker per tick.
	if (bind_data.supports_splits) {
		// Refuse, BEFORE planning, a token lifetime shorter than this client needs:
		// nothing re-plans when a split token expires, so the query would fail
		// partway. An absent TTL means UNBOUNDED and always passes.
		if (bind_data.split_token_ttl_seconds.has_value()) {
			Value min_ttl_value;
			int64_t min_ttl = defaults::SPLIT_TOKEN_MIN_TTL_SECONDS;
			if (context.TryGetCurrentSetting("vgi_split_token_min_ttl_seconds", min_ttl_value) &&
			    !min_ttl_value.IsNull()) {
				min_ttl = static_cast<int64_t>(min_ttl_value.GetValue<uint64_t>());
			}
			const auto declared = *bind_data.split_token_ttl_seconds;
			if (min_ttl > 0 && declared < min_ttl) {
				throw InvalidInputException(
				    "VGI function '%s' declares a split-token lifetime of %lld second(s), but this client "
				    "needs at least %lld between planning a scan and reading it. Nothing re-plans when a "
				    "split token expires, so the scan would fail partway rather than degrade. Raise "
				    "vgi_split_token_min_ttl_seconds if that horizon is wrong for this deployment, or fix "
				    "the worker to declare a longer TTL.",
				    bind_data.function_name, static_cast<long long>(declared),
				    static_cast<long long>(min_ttl));
			}
		}

		std::vector<uint8_t> filter_bytes_vec;
		if (filter_bytes) {
			filter_bytes_vec.assign(filter_bytes->data(), filter_bytes->data() + filter_bytes->size());
		}
		// min_splits = thread count (the one sizing fact DuckDB knows);
		// target_split_bytes only when the operator set it.
		const int64_t min_splits = static_cast<int64_t>(TaskScheduler::GetScheduler(context).NumberOfThreads());
		int64_t target_split_bytes = 0;
		Value tsb;
		if (context.TryGetCurrentSetting("vgi_target_split_bytes", tsb) && !tsb.IsNull()) {
			target_split_bytes = static_cast<int64_t>(tsb.GetValue<uint64_t>());
		}

		CatalogRpcContext rpc_ctx{bind_data.attach_params, bind_data.attach_opaque_data,
		                          bind_data.transaction_opaque_data};
		auto plan = InvokeTableFunctionPlan(rpc_ctx, bind_data.bind_result.bind_request_bytes,
		                                    bind_data.bind_result.opaque_data, projection_ids, filter_bytes_vec,
		                                    min_splits, target_split_bytes, bind_data.function_name, context);

		InstallProtocolSplitPartitionData(context, bind_data, plan, *global_state);
		global_state->splits = std::move(plan.splits);
		if (plan.max_workers > 0) {
			global_state->max_processes = static_cast<idx_t>(plan.max_workers);
		}
		if (!plan.init_opaque_data.empty()) {
			global_state->init_opaque_data = plan.init_opaque_data;
		}
		// No primary init on this path: readers init per split.
		global_state->init_applied = true;

		VGI_LOG(context, "scan.plan",
		        {{"function_name", bind_data.function_name},
		         {"splits", std::to_string(global_state->splits.size())},
		         {"max_workers", std::to_string(plan.max_workers)},
		         {"pages", std::to_string(plan.pages)}});
	}
	global_state->cache_eligible = cache_eval.eligible; // an eligible non-hit is a genuine miss (EXPLAIN)
	global_state->dynamic_filters = std::move(dynamic_filters);
	global_state->static_filter_bytes = filter_bytes;
	global_state->tick_filter_state = tick_filter_state;
	global_state->projection_ids = projection_ids;
	global_state->join_keys_buffers = join_keys_buffers;
	global_state->order_by_hint = bind_data.order_by_hint;
	global_state->table_sample_hint = bind_data.table_sample_hint;
	global_state->fixed_order = bind_data.fixed_order;

	// Eligible MISS: set up capture (committed by the gstate destructor).
	if (cache_eval.eligible) {
		auto cap = std::make_shared<VgiResultCaptureCtx>();
		cap->cache = &GetResultCache(context);
		cap->key = cache_eval.key;
		cap->catalog_name = cache_eval.catalog_name;
		cap->catalog_version_frozen = cache_eval.catalog_version_frozen;
		cap->transaction_id = cache_eval.transaction_id;
		// Required for the split arm of the commit gate.
		cap->split_path = bind_data.supports_splits;
		cap->total_splits = global_state->splits.size();
		Value sv;
		cap->max_entry_bytes = context.TryGetCurrentSetting("vgi_result_cache_max_entry_bytes", sv)
		                           ? static_cast<int64_t>(sv.GetValue<uint64_t>())
		                           : defaults::RESULT_CACHE_MAX_ENTRY_BYTES;
		cap->max_bytes = context.TryGetCurrentSetting("vgi_result_cache_max_bytes", sv)
		                     ? static_cast<int64_t>(sv.GetValue<uint64_t>())
		                     : defaults::RESULT_CACHE_MAX_BYTES;
		cap->default_ttl_seconds =
		    context.TryGetCurrentSetting("vgi_result_cache_default_ttl_seconds", sv)
		        ? sv.GetValue<uint64_t>()
		        : 0;
		// Disk config for the threshold spill (see VgiResultCaptureCtx).
#ifndef __EMSCRIPTEN__
		// WASM is memory-only (an oversized capture aborts to uncached): the spill
		// writer's MbedTlsWrapper::SHA256State is not resolvable from the side
		// module. VgiResultCache::Configure gates the disk tier likewise.
		if (context.TryGetCurrentSetting("vgi_result_cache_dir", sv) && !sv.IsNull()) {
			cap->disk_dir = sv.GetValue<std::string>();
		}
#endif
		if (context.TryGetCurrentSetting("vgi_result_cache_disk_max_bytes", sv) && !sv.IsNull()) {
			cap->disk_max = sv.GetValue<uint64_t>();
		}
		if (context.TryGetCurrentSetting("vgi_result_cache_disk_compression", sv) && !sv.IsNull()) {
			cap->disk_compression = sv.GetValue<std::string>();
		}
		if (context.TryGetCurrentSetting("vgi_result_cache_disk_compression_level", sv) && !sv.IsNull()) {
			cap->disk_compression_level = sv.GetValue<uint64_t>();
		}
		// Arm the per-partition split (also gated on cc.partition_scope at commit).
		if (cache_eval.partition_splittable) {
			cap->partition_split_requested = true;
			cap->partition_column_indices = cache_eval.partition_column_indices;
			cap->partition_types = cache_eval.partition_types;
			cap->partition_names = cache_eval.partition_names;
			cap->residual_filter_bytes = cache_eval.partition_residual_filter_bytes;
			cap->partition_max = cache_eval.partition_max;
		}
		global_state->capture = std::move(cap);
	}

	// Arm revalidation: init carries the validators, GetNextBatch honors a
	// not_modified reply, and the capture above records fresh data otherwise.
	if (revalidation_entry) {
		global_state->revalidating = true;
		global_state->revalidate_if_none_match = revalidation_entry->etag;
		global_state->revalidate_if_modified_since = revalidation_entry->last_modified;
		global_state->revalidation_entry = std::move(revalidation_entry);
	}

	// Split path: readers init per split. A primary init here would carry NO
	// split token (a split-only worker rejects it, others stream a whole unsplit
	// scan nobody reads), and with init_applied set its error is never observed.
	if (bind_data.supports_splits) {
		return std::move(global_state);
	}

	auto perform_init_with_retry =
	    [acquire_params, &context_ref = context,
	     bind_result, projection_ids, filter_bytes, join_keys_buffers,
	     order_by_hint = bind_data.order_by_hint,
	     table_sample_hint = bind_data.table_sample_hint,
	     worker_path = bind_data.worker_path(),
	     function_name = bind_data.function_name,
	     reval_inm = global_state->revalidate_if_none_match,
	     reval_ims = global_state->revalidate_if_modified_since](
	        std::unique_ptr<IFunctionConnection> conn,
	        bool from_pool) mutable
	    -> std::pair<InitResult, std::unique_ptr<IFunctionConnection>> {
		auto run = [&](IFunctionConnection &c) {
			// Conditional-revalidation validators, when armed.
			if (!reval_inm.empty() || !reval_ims.empty()) {
				c.SetConditionalRequest(reval_inm, reval_ims);
			}
			return c.PerformInit(bind_result, projection_ids, filter_bytes, join_keys_buffers,
			                     "", order_by_hint, table_sample_hint);
		};
		try {
			auto r = run(*conn);
			return std::make_pair(std::move(r), std::move(conn));
		} catch (const IOException &e) {
			if (!from_pool) {
				throw;
			}
			VGI_LOG(context_ref, "worker_pool.stale",
			        {{"worker_path", worker_path},
			         {"function_name", function_name},
			         {"phase", "init_global"},
			         {"error", e.what()}});
			auto fresh = AcquireConnectionForInit(context_ref, acquire_params, /*force_fresh=*/true);
			auto fresh_conn = std::move(fresh.connection);
			auto r = run(*fresh_conn);
			return std::make_pair(std::move(r), std::move(fresh_conn));
		}
	};

	// PerformInit on a background thread; the lambda owns the connection until
	// EnsureInitApplied folds it back into the gstate.
#if VGI_ASYNC_INIT_ENABLED
#  ifdef __EMSCRIPTEN__
	// The pre-spawned VgiWasmAsyncPool: std::async would pthread_create after
	// side-module dlopen, which is unreliable in MAIN_MODULE=1 builds.
	global_state->pending_init = duckdb::vgi::VgiWasmAsyncPool::Instance().Submit(
	    [run = std::move(perform_init_with_retry),
	     conn = std::move(connection),
	     from_pool = acquired.from_pool]() mutable {
		    return run(std::move(conn), from_pool);
	    });
#  else
	global_state->pending_init = std::async(std::launch::async,
	                                          [run = std::move(perform_init_with_retry),
	                                           conn = std::move(connection),
	                                           from_pool = acquired.from_pool]() mutable {
		                                          return run(std::move(conn), from_pool);
	                                          });
#  endif
#else
	// Synchronous init (WASM default; see vgi_platform.hpp): fold the result in
	// as EnsureInitApplied() would. pending_init stays invalid, so
	// EnsureInitApplied() is a no-op.
	auto applied = perform_init_with_retry(std::move(connection), acquired.from_pool);
	global_state->global_execution_id = std::move(applied.first.execution_id);
	global_state->max_processes = static_cast<idx_t>(applied.first.max_workers);
	global_state->init_opaque_data = std::move(applied.first.opaque_data);
	{
		std::lock_guard<std::mutex> conn_lk(global_state->connection_mutex);
		global_state->primary_connection = std::move(applied.second);
	}
	global_state->init_applied.store(true, std::memory_order_release);
#endif

	{
		vector<pair<string, string>> fields;
		fields.emplace_back("worker_path", bind_data.worker_path());
		fields.emplace_back("function_name", bind_data.function_name);
		fields.emplace_back("max_processes", std::to_string(global_state->max_processes));
		fields.emplace_back("num_projection_columns", std::to_string(projection_ids.size()));
		VGI_LOG(context, "table_function.init_global", fields);
	}

	if (bind_data.order_by_hint) {
		VGI_LOG(context, "table_function.order_pushdown",
		        {{"function_name", bind_data.function_name},
		         {"order_column", bind_data.order_by_hint->column_name},
		         {"direction", bind_data.order_by_hint->direction},
		         {"null_order", bind_data.order_by_hint->null_order},
		         {"row_limit", std::to_string(bind_data.order_by_hint->row_limit)}});
	}

	if (bind_data.table_sample_hint) {
		VGI_LOG(context, "table_function.sample_pushdown",
		        {{"function_name", bind_data.function_name},
		         {"percentage", std::to_string(bind_data.table_sample_hint->sample_percentage)},
		         {"seed", std::to_string(bind_data.table_sample_hint->seed)}});
	}

	return global_state;
}

unique_ptr<LocalTableFunctionState> VgiTableFunctionInitLocal(ExecutionContext &context,
                                                              TableFunctionInitInput &logical_input,
                                                              GlobalTableFunctionState *global_state_p) {
	auto input = PhysicalCatalogScanInput(logical_input);
	auto &bind_data = input.bind_data->Cast<VgiTableFunctionBindData>();
	auto &global_state = global_state_p->Cast<VgiTableFunctionGlobalState>();

	global_state.EnsureInitApplied();

	auto current_chunk = make_uniq<ArrowArrayWrapper>();
	auto local_state = make_uniq<VgiTableFunctionLocalState>(std::move(current_chunk), context.client,
	                                                         bind_data.attach_params);
	if (bind_data.explicit_query) {
		local_state->cancel_enabled = true;
	}

	// Generated catalog scans always project locally, even when the worker sends
	// its complete physical schema. Worker projection remains an independent flag.
	if (bind_data.projection_pushdown || HasGeneratedCatalogColumns(bind_data)) {
		local_state->column_ids = input.column_ids;
		// Remap to worker-schema indices (rowid → its worker index, later columns
		// +1): ArrowToDuckDB's type info was built from the full worker schema.
		if (bind_data.rowid_worker_col_index >= 0) {
			for (auto &col_id : local_state->column_ids) {
				if (col_id == COLUMN_IDENTIFIER_ROW_ID) {
					col_id = static_cast<idx_t>(bind_data.rowid_worker_col_index);
				} else if (col_id >= static_cast<idx_t>(bind_data.rowid_worker_col_index)) {
					col_id += 1;
				}
			}
		}
	}

	// Cache serve: only the FIRST local state replays the entry; any extra one
	// (not expected with MaxThreads()==1) gets an empty replay.
	if (global_state.serving_from_cache) {
		const bool first = !global_state.serve_claimed.exchange(true);
		std::shared_ptr<const VgiResultCacheEntry> entry =
		    first ? global_state.serving_entry : std::make_shared<const VgiResultCacheEntry>();
		local_state->prefetch_slot_->connection = make_uniq<CachedReplayConnection>(std::move(entry));
		VGI_LOG(context.client, "table_function.init_local",
		        {{"conn", "cachehit"},
		         {"worker_path", bind_data.worker_path()},
		         {"function_name", bind_data.function_name},
		         {"worker_type", first ? "cache_serve" : "cache_serve_empty"}});
		return unique_ptr<LocalTableFunctionState>(std::move(local_state));
	}

	std::unique_ptr<IFunctionConnection> primary_connection;
	{
		std::lock_guard<std::mutex> lock(global_state.connection_mutex);
		if (global_state.primary_connection) {
			primary_connection = std::move(global_state.primary_connection);
		}
	}

	if (primary_connection) {
		{
			auto &conn = *primary_connection;
			vector<pair<string, string>> fields;
			fields.emplace_back("conn", conn.GetConnIdHex());
			fields.emplace_back("worker_path", bind_data.worker_path());
			AppendSubprocessPidField(fields, conn);
			fields.emplace_back("function_name", bind_data.function_name);
			fields.emplace_back("global_execution_id", BytesToHex(global_state.global_execution_id));
			fields.emplace_back("worker_type", "primary");
			VGI_LOG(context.client, "table_function.init_local", fields);
		}

		local_state->prefetch_slot_->connection = std::move(primary_connection);
	} else {
		if (bind_data.explicit_query) {
			throw IOException("vgi_query cannot initialize a secondary reader");
		}
		// Secondary worker: no bind RPC (PerformInit carries the cached
		// BindResult and global_execution_id). Stale-pool retry is on the init.
		FunctionConnectionParams params;
		params.attach_params = bind_data.attach_params;
		params.attach_opaque_data = bind_data.attach_opaque_data;
		params.function_name = bind_data.function_name;
		params.schema_name = bind_data.schema_name;
		params.arguments = bind_data.arguments;
		params.transaction_opaque_data = bind_data.transaction_opaque_data;
		params.global_execution_id = global_state.global_execution_id;
		params.settings = bind_data.settings;
		params.required_secrets = bind_data.required_secrets;
		params.phase = "init_local_secondary";
		params.function_type = "TABLE";

		auto acquired = AcquireConnectionForInit(context.client, params);
		local_state->prefetch_slot_->connection = std::move(acquired.connection);
		const auto &secondary_bind_result = bind_data.bind_result;

		// Secondaries get the primary's pushdowns and init_opaque_data; see the
		// VgiTableFunctionGlobalState fields for why.
		auto secondary_init = [&]() {
			// Every connection needs the tick filter state, projection or not.
			local_state->connection()->SetTickFilterState(global_state.tick_filter_state);

			// Split path: acquire only; each reader inits per split in
			// AdvanceToNextSplit. The stale-pool retry below wraps an init this
			// path skips, so hand the reader a reacquire for its first split.
			if (bind_data.supports_splits) {
				if (acquired.from_pool) {
					local_state->reacquire_fresh_connection =
					    [params](ClientContext &ctx) mutable -> std::unique_ptr<IFunctionConnection> {
						auto fresh = AcquireConnectionForInit(ctx, params, /*force_fresh=*/true);
						return std::unique_ptr<IFunctionConnection>(fresh.connection.release());
					};
				}
				return;
			}

			if (bind_data.projection_pushdown) {
				local_state->connection()->PerformInit(
				    secondary_bind_result,
				    global_state.projection_ids, global_state.static_filter_bytes,
				    global_state.join_keys_buffers, "", global_state.order_by_hint,
				    global_state.table_sample_hint, global_state.init_opaque_data);
			} else {
				local_state->connection()->PerformInit(
				    secondary_bind_result, {}, global_state.static_filter_bytes,
				    global_state.join_keys_buffers, "",
				    std::nullopt, std::nullopt, global_state.init_opaque_data);
			}
		};
		try {
			secondary_init();
		} catch (const IOException &e) {
			if (!acquired.from_pool) {
				throw;
			}
			VGI_LOG(context.client, "worker_pool.stale",
			        {{"worker_path", bind_data.worker_path()},
			         {"function_name", bind_data.function_name},
			         {"phase", "init_local_secondary"},
			         {"error", e.what()}});
			acquired = AcquireConnectionForInit(context.client, params, /*force_fresh=*/true);
			local_state->prefetch_slot_->connection = std::move(acquired.connection);
			secondary_init();
		}

		{
			auto &conn = *local_state->connection();
			vector<pair<string, string>> fields;
			fields.emplace_back("conn", conn.GetConnIdHex());
			fields.emplace_back("worker_path", bind_data.worker_path());
			AppendSubprocessPidField(fields, conn);
			fields.emplace_back("function_name", bind_data.function_name);
			fields.emplace_back("global_execution_id", BytesToHex(global_state.global_execution_id));
			fields.emplace_back("worker_type", "secondary");
			VGI_LOG(context.client, "table_function.init_local", fields);
		}
	}

	// This local state's capture substream (increments `launched`).
	if (global_state.capture) {
		local_state->capture_stream = global_state.capture->NewStream();
	}

	return local_state;
}

VgiBufferPtr VgiSerializeDynamicFilterDelta(ClientContext &context, const string &worker_path,
                                                              const vector<VgiDynamicFilterDeltaUpdate> &updates) {
	FilterSerializer serializer(worker_path, 0, /*allow_external_sets=*/false);
	auto document = yyjson_mut_obj(serializer.GetDoc());
	yyjson_mut_obj_add_str(serializer.GetDoc(), document, "encoding", "vgi.filters.v2");
	yyjson_mut_obj_add_str(serializer.GetDoc(), document, "semantics", "vgi.duckdb.standard.v1");
	yyjson_mut_obj_add_str(serializer.GetDoc(), document, "kind", "delta");
	auto json_updates = yyjson_mut_arr(serializer.GetDoc());
	for (auto &update : updates) {
		auto obj = yyjson_mut_obj(serializer.GetDoc());
		bool remove = update.filter == nullptr;
		yyjson_mut_obj_add_str(serializer.GetDoc(), obj, "operation", remove ? "remove" : "upsert");
		yyjson_mut_obj_add_strcpy(serializer.GetDoc(), obj, "id", update.predicate_id.c_str());
		yyjson_mut_obj_add_uint(serializer.GetDoc(), obj, "revision", update.revision);
		if (!remove) {
			bool advisory = true;
			auto expression =
			    serializer.SerializeFilter(*update.filter, update.column_index, update.column_name, advisory);
			if (!expression) {
				throw InvalidInputException("VGI dynamic filter could not be serialized atomically");
			}
			yyjson_mut_obj_add_str(serializer.GetDoc(), obj, "mode", "advisory");
			yyjson_mut_obj_add_str(serializer.GetDoc(), obj, "source", "top_n");
			yyjson_mut_obj_add_val(serializer.GetDoc(), obj, "expression", expression);
		}
		yyjson_mut_arr_append(json_updates, obj);
	}
	yyjson_mut_obj_add_val(serializer.GetDoc(), document, "updates", json_updates);

	auto json = serializer.WriteJson(document);
	if (!json) {
		throw IOException("Failed to serialize dynamic filter delta to JSON");
	}
	string filter_spec(json);
	free(json);

	auto &values = serializer.GetValues();
	auto &value_types = serializer.GetValueTypes();
	auto &value_names = serializer.GetValueNames();
	vector<LogicalType> types {LogicalType::VARCHAR};
	vector<string> names {"filter_spec"};
	for (idx_t i = 0; i < value_types.size(); i++) {
		types.push_back(value_types[i]);
		names.push_back(value_names[i]);
	}
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), types);
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value(filter_spec));
	for (idx_t i = 0; i < values.size(); i++) {
		chunk.SetValue(i + 1, 0, values[i]);
	}

	auto client_props = VgiWireClientProperties(context);
	auto batch = MakeFilterSpecBatch(context, chunk, types, names, client_props, "Failed to import dynamic filter delta");
	return SerializeIpcStream(*batch, "Failed to create dynamic filter delta writer",
	                          "Failed to write dynamic filter delta", "Failed to close dynamic filter delta");
}

//! Refresh the TickFilterState from current DynamicFilter values before each
//! ReadDataBatch. A new delta is produced ONLY when some filter changed: a
//! revision identifies a value, and HTTP workers replay applied deltas every
//! turn, so re-sending unchanged values costs quadratic validation. Each delta
//! describes EVERY emitted filter, so a stream that skipped deltas converges
//! from the latest; per-connection progress is tracked by `generation`.
static void UpdateDynamicFilterState(VgiTableFunctionGlobalState &global_state, ClientContext &context,
                                     const VgiTableFunctionBindData &bind_data) {
	if (global_state.dynamic_filters.empty() || !global_state.tick_filter_state) {
		return;
	}

	// Revisions and active/tombstone transitions are protected with the same
	// lock the connection uses to read encoded tick metadata.
	auto &tick_state = *global_state.tick_filter_state;
	lock_guard<mutex> state_lock(tick_state.lock);
	bool changed = false;
	vector<unique_ptr<TableFilter>> update_filters;
	vector<VgiDynamicFilterDeltaUpdate> updates;
	for (auto &df : global_state.dynamic_filters) {
		if (!df.filter_data->initialized.load()) {
			if (df.active) {
				df.revision++;
				df.active = false;
				changed = true;
			}
			// A filter that was ever emitted keeps riding every later delta as a
			// tombstone at its current revision: a stream that already removed it
			// sees a stale no-op, and one that skipped the removal still gets it.
			if (df.revision > 0) {
				updates.push_back({df.predicate_id, df.revision, df.column_index, df.column_name, nullptr});
			}
			continue;
		}

		// Read the current value under lock; bump the revision only if it moved.
		ExpressionType comparison;
		Value constant;
		{
			annotated_shared_lock<annotated_shared_mutex> l(df.filter_data->lock);
			comparison = df.filter_data->comparison_type;
			constant = df.filter_data->constant;
		}
		if (!df.active || comparison != df.emitted_comparison ||
		    !Value::NotDistinctFrom(constant, df.emitted_constant)) {
			df.revision++;
			df.active = true;
			df.emitted_comparison = comparison;
			df.emitted_constant = std::move(constant);
			changed = true;
		}

		auto filter_copy = ExpressionFilter::CreateComparisonFilter(df.emitted_comparison, df.emitted_constant);
		unique_ptr<TableFilter> pushed;
		if (df.nulls_first) {
			vector<unique_ptr<Expression>> children;
			children.push_back(ExpressionFilter::CreateNullCheckExpression(
			    make_uniq<BoundReferenceExpression>(df.emitted_constant.type(), 0), ExpressionType::OPERATOR_IS_NULL));
			children.push_back(std::move(filter_copy->expr));
			pushed = make_uniq<ExpressionFilter>(BoundConjunctionExpression::Create(ExpressionType::CONJUNCTION_OR, std::move(children)));
		} else {
			pushed = std::move(filter_copy);
		}
		auto filter_ptr = pushed.get();
		update_filters.push_back(std::move(pushed));
		updates.push_back({df.predicate_id, df.revision, df.column_index, df.column_name, filter_ptr});
	}

	if (!changed) {
		// The latest delta (or the absence of one) still describes every filter.
		return;
	}
	tick_state.generation++;

	// Runtime metadata carries a delta, never a replacement snapshot. Every
	// upsert is advisory; a reset emits a revisioned remove tombstone.
	try {
		auto bytes = VgiSerializeDynamicFilterDelta(context, bind_data.worker_path(), updates);
		tick_state.encoded_filters =
		    Blob::ToBase64(string_t(reinterpret_cast<const char *>(bytes->data()), static_cast<idx_t>(bytes->size())));
		tick_state.has_filters = true;
	} catch (...) {
		// Dynamic filters are advisory. A failure omits this complete delta.
		tick_state.encoded_filters.clear();
		tick_state.has_filters = false;
	}
}

//! ``PipelineBuildState::BATCH_INCREMENT``: an out-of-range source batch index
//! throws an opaque ``InternalException`` in
//! ``PipelineExecutor::ToPipelinePartitionData``, so InstallBatch rejects it
//! first with an error naming the worker contract.
static constexpr idx_t VGI_BATCH_INDEX_CAP = 10000000000000ULL;  // 10^13
//! That function maps source index i to base + i + 1 and throws when the result
//! is >= base + CAP - 1, so the largest index it accepts is CAP - 3.
static constexpr idx_t VGI_MAX_BATCH_INDEX = VGI_BATCH_INDEX_CAP - 3;

//! Type equality with Arrow C++ ``DataType::Equals`` semantics: a node's own
//! nullability belongs to its field, not its type, so only nested children compare it.
static bool SameDataType(const ArrowSchema &a, const ArrowSchema &b) {
	if (std::strcmp(a.format, b.format) != 0 || a.n_children != b.n_children) {
		return false;
	}
	const int64_t type_flags = ARROW_FLAG_DICTIONARY_ORDERED | ARROW_FLAG_MAP_KEYS_SORTED;
	if ((a.flags & type_flags) != (b.flags & type_flags)) {
		return false;
	}
	for (int64_t c = 0; c < a.n_children; c++) {
		if (!TypesEqual(*a.children[c], *b.children[c])) {
			return false;
		}
	}
	if ((a.dictionary == nullptr) != (b.dictionary == nullptr)) {
		return false;
	}
	return !a.dictionary || SameDataType(*a.dictionary, *b.dictionary);
}

//! Convert a 1-row exact-value or 2-row ``(min, max)`` batch per partition
//! column into ``ColumnPartitionData`` through the same Arrow conversion as data
//! batches, for lossless handling of every type. The caller has validated the
//! shape against the bind schema's partition fields; conversion errors throw.
static duckdb::vector<ColumnPartitionData> ConvertPartitionValuesBatch(
    ClientContext &context,
    const VgiBatchPtr &pv_batch) {
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
	vector<LogicalType> types;
	vector<string> names;
	ArrowSchemaToDuckDBTypes(context, pv_batch->schema(), c_schema, arrow_table, types, names);
	const idx_t row_count = static_cast<idx_t>(pv_batch->num_rows());
	D_ASSERT(row_count == 1 || row_count == 2);

	auto chunk_wrapper = make_uniq<ArrowArrayWrapper>();
	ExportRecordBatch(pv_batch, *chunk_wrapper);

	ArrowScanLocalState local_scan_state(std::move(chunk_wrapper), context);
	local_scan_state.chunk_offset = 0;

	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), types, row_count);
	chunk.SetCardinality(row_count);
	ArrowTableFunction::ArrowToDuckDB(local_scan_state, arrow_table.GetColumns(), chunk,
	                                  /*arrow_scan_is_projected=*/false,
	                                  COLUMN_IDENTIFIER_ROW_ID);

	// Row 0 = min, last row = max (a one-row exact key gives min == max).
	duckdb::vector<ColumnPartitionData> out;
	out.reserve(chunk.ColumnCount());
	for (idx_t col = 0; col < chunk.ColumnCount(); ++col) {
		Value min_val = chunk.GetValue(col, 0);
		Value max_val = chunk.GetValue(col, row_count - 1);
		out.emplace_back(std::move(min_val));
		out.back().max_val = std::move(max_val);
	}
	return out;
}

//! Bridge protocol 3.1's exact split keys into DuckDB 2.0's bind-time
//! SINGLE_VALUE_PARTITIONS interface. The optimizer declaration itself still
//! comes only from Meta.partition_kind + annotated fields: PlanResponse arrives
//! after physical planning and cannot safely introduce that declaration.
static void InstallProtocolSplitPartitionData(ClientContext &context,
                                              const VgiTableFunctionBindData &bind_data,
                                              const VgiScanPlan &plan,
                                              VgiTableFunctionGlobalState &global_state) {
	if (bind_data.partition_kind != VgiPartitionKind::SingleValuePartitions ||
	    !plan.identity_key_columns.has_value()) {
		return;
	}

	const auto &declared_indices = bind_data.partition_column_indices;
	const auto &bind_schema = bind_data.bind_result.output_schema;
	const auto &grouping_columns = *plan.identity_key_columns;
	if (grouping_columns.size() != declared_indices.size()) {
		return;
	}
	for (idx_t i = 0; i < declared_indices.size(); ++i) {
		if (grouping_columns[i] != bind_schema->field_name(static_cast<int>(declared_indices[i]))) {
			return;
		}
	}

	auto violation = [&]() {
		return ErrorInfo(error_subtype::kProtocolViolation)
		    .Rpc("table_function_plan")
		    .Function(bind_data.function_name, "TABLE")
		    .Worker(bind_data.worker_path());
	};

	std::vector<duckdb::vector<ColumnPartitionData>> values;
	values.reserve(plan.splits.size());
	for (idx_t split_index = 0; split_index < plan.splits.size(); ++split_index) {
		const auto &split = plan.splits[split_index];
		if (!split.key_value.has_value()) {
			throw IOException(violation().Set(error_key::kField, "splits.key_value")
			                      .Set(error_key::kColumnIndex, static_cast<int64_t>(split_index)),
			                  "VGI function '%s' declared an exact key grouping but split %llu has no key_value",
			                  bind_data.function_name, static_cast<unsigned long long>(split_index));
		}

		auto key_batch = DeserializeFromIpcBytes(*split.key_value, GetWorkerBatchValidation(&context));
		if (!key_batch || key_batch->num_rows() != 1) {
			const auto actual = key_batch ? key_batch->num_rows() : 0;
			throw IOException(violation().Set(error_key::kField, "splits.key_value")
			                      .Set(error_key::kColumnIndex, static_cast<int64_t>(split_index))
			                      .ExpectedActual("1", std::to_string(actual)),
			                  "VGI function '%s' split %llu key_value must contain exactly one row (got %lld)",
			                  bind_data.function_name, static_cast<unsigned long long>(split_index),
			                  static_cast<long long>(actual));
		}
		if (static_cast<idx_t>(key_batch->num_columns()) != declared_indices.size()) {
			throw IOException(violation().Set(error_key::kField, "splits.key_value")
			                      .Set(error_key::kColumnIndex, static_cast<int64_t>(split_index))
			                      .ExpectedActual(std::to_string(declared_indices.size()),
			                                      std::to_string(key_batch->num_columns())),
			                  "VGI function '%s' split %llu key_value has %d columns; expected %llu",
			                  bind_data.function_name, static_cast<unsigned long long>(split_index),
			                  key_batch->num_columns(), static_cast<unsigned long long>(declared_indices.size()));
		}
		for (idx_t i = 0; i < declared_indices.size(); ++i) {
			const int declared_idx = static_cast<int>(declared_indices[i]);
			const auto &declared = bind_schema->field(declared_idx);
			const auto &actual = key_batch->schema()->field(static_cast<int>(i));
			const std::string declared_name = bind_schema->field_name(declared_idx);
			if (!SameDataType(actual, declared)) {
				const std::string declared_type = TypeToString(declared);
				const std::string actual_type = TypeToString(actual);
				throw IOException(violation().Set(error_key::kField, "splits.key_value")
				                      .Set(error_key::kColumnIndex, static_cast<int64_t>(i))
				                      .Set(error_key::kColumnName, declared_name)
				                      .ExpectedActual(declared_type, actual_type),
				                  "VGI function '%s' split %llu key_value column '%s' has type %s; expected %s",
				                  bind_data.function_name, static_cast<unsigned long long>(split_index),
				                  declared_name, actual_type, declared_type);
			}
		}
		values.push_back(ConvertPartitionValuesBatch(context, key_batch));
	}

	global_state.split_partition_data = std::move(values);
	global_state.split_partition_data_enabled = true;
}

//! Decode a captured batch's vgi_partition_values bytes to the per-column MIN values
//! (row 0; == max for SINGLE_VALUE). Returns false on any decode/shape problem (the
//! caller then aborts the whole split — a batch we can't classify must not be dropped).
static bool DecodePartitionMinTuple(ClientContext &context, const std::string &pv_bytes,
                                    std::vector<Value> &out) {
	if (pv_bytes.empty()) {
		return false;
	}
	// Borrowed: pv_bytes outlives the decode, and the batch never escapes this call.
	VgiBatchPtr pv_batch;
	try {
		auto reader = VgiIpcReader::Open(std::make_shared<VgiBufferReader>(
		    VgiBuffer::Wrap(pv_bytes.data(), static_cast<int64_t>(pv_bytes.size()))));
		pv_batch = reader->ReadNext();
	} catch (const std::exception &) {
		return false;
	}
	if (!pv_batch) {
		return false;
	}
	if (pv_batch->num_rows() != 2) {
		return false;
	}
	try {
		ValidateWorkerBatch(&context, pv_batch.get(), "partition values");
	} catch (const std::exception &) {
		return false;
	}
	auto cpd = ConvertPartitionValuesBatch(context, pv_batch);
	out.clear();
	out.reserve(cpd.size());
	for (auto &c : cpd) {
		out.push_back(c.min_val);
	}
	return true;
}

// Bucket the captured batches by partition tuple and insert one entry per partition
// (see BuildPartitionKey). Best-effort: a decode failure or too many partitions
// skips the split; the whole-scan entry stands.
static void SplitAndStorePartitionEntries(ClientContext *ctx, VgiResultCaptureCtx &cap,
                                          const VgiResultCacheEntry &meta, bool allow_disk) {
	if (!ctx || cap.partition_types.empty()) {
		return;
	}
	struct Bucket {
		std::vector<VgiCachedBatch> batches;
		int64_t rows = 0;
		int64_t bytes = 0;
		std::string label;
	};
	std::map<std::string, Bucket> buckets; // "p:"-disc -> bucket (map = deterministic order)
	for (auto &sp : cap.streams) {
		for (auto &b : sp->batches) {
			std::vector<Value> tuple;
			if (!DecodePartitionMinTuple(*ctx, b.partition_values_bytes, tuple) ||
			    tuple.size() != cap.partition_types.size()) {
				LogResultCache(*ctx, "result_cache.store_skipped",
				               {{"function", cap.key.function_name},
				                {"key_hash", cap.key.HexDigest()},
				                {"reason", "partition_decode_failed"}});
				return; // can't classify a batch → abandon the split entirely
			}
			auto disc = PartitionDiscriminator(*ctx, tuple, cap.partition_types);
			auto &bk = buckets[disc];
			if (bk.batches.empty()) {
				bk.label = PartitionLabel(cap.partition_names, tuple);
			}
			bk.rows += b.rows;
			bk.bytes += static_cast<int64_t>(b.ipc ? b.ipc->size() : 0);
			bk.batches.push_back(b); // shared_ptr ipc copy
		}
	}
	if (buckets.empty()) {
		return;
	}
	if (buckets.size() > cap.partition_max) {
		LogResultCache(*ctx, "result_cache.ineligible",
		               {{"function", cap.key.function_name}, {"reason", "partition_too_many"}});
		return;
	}
	int64_t stored = 0;
	for (auto &kv : buckets) {
		auto pe = std::make_shared<VgiResultCacheEntry>();
		pe->key = BuildPartitionKey(cap.key, cap.residual_filter_bytes, kv.first);
		pe->catalog_name = meta.catalog_name;
		pe->scope = meta.scope;
		// Non-revalidatable (the combined serve never sends conditional requests);
		// freshness mirrors the whole-scan entry.
		pe->revalidatable = false;
		pe->stored_at = meta.stored_at;
		pe->expires_at = meta.expires_at;
		pe->never_expires = meta.never_expires;
		pe->partition_label = kv.second.label;
		CachedStream s;
		s.batches = std::move(kv.second.batches);
		s.rows = kv.second.rows;
		s.bytes = kv.second.bytes;
		pe->rows = kv.second.rows;
		pe->total_bytes = kv.second.bytes;
		pe->streams.push_back(std::move(s));
		if ((*cap.cache).Insert(std::move(pe), allow_disk)) {
			(*cap.cache).RecordPartitionStore();
			++stored;
		}
	}
	if (stored > 0) {
		LogResultCache(*ctx, "result_cache.partition_store",
		               {{"function", cap.key.function_name},
		                {"key_hash", cap.key.HexDigest()},
		                {"partitions", std::to_string(stored)}});
	}
}

//! Install arrow_batch (from ReadDataBatch or the prefetch slot) as the active
//! chunk. Returns false on EOS (null batch). Runs on the pipeline thread.
static bool InstallBatch(ClientContext &context, const VgiTableFunctionBindData &bind_data,
                         VgiTableFunctionGlobalState &global_state,
                         VgiTableFunctionLocalState &local_state,
                         VgiBatchPtr arrow_batch) {
	if (!arrow_batch) {
		local_state.done = true;
		// Capture: this local state reached clean EOS.
		if (global_state.capture && local_state.capture_stream) {
			global_state.capture->eos.fetch_add(1, std::memory_order_relaxed);
		}
		if (VgiInfoLogActive(context)) {
			auto &conn = *local_state.connection();
			vector<pair<string, string>> fields;
			fields.emplace_back("conn", conn.GetConnIdHex());
			fields.emplace_back("worker_path", bind_data.worker_path());
			AppendSubprocessPidField(fields, conn);
			fields.emplace_back("function_name", bind_data.function_name);
			VGI_LOG(context, "table_function.scan_complete", fields);
		}
		return false;
	}

	// batch_index contract (present, under the cap, monotone), applied here on
	// the pipeline thread; see current_batch_index for why not in ReadDataBatch.
	const idx_t parsed_index = local_state.connection()->GetLastBatchIndex();
	// extra_info for a worker that broke a wire-metadata contract.
	auto metadata_violation = [&](const char *metadata_key) {
		return ErrorInfo(error_subtype::kProtocolViolation)
		    .Function(bind_data.function_name, "TABLE")
		    .Worker(bind_data.worker_path(), -1, local_state.connection()->GetExecutionIdHex())
		    .Set(error_key::kMetadataKey, metadata_key);
	};
	if (bind_data.supports_batch_index) {
		if (parsed_index == DConstants::INVALID_INDEX) {
			throw IOException(metadata_violation("vgi_batch_index"),
			                  "VGI function '%s' with supports_batch_index=true emitted a data "
			                  "batch without vgi_batch_index metadata",
			                  bind_data.function_name);
		}
		if (parsed_index > VGI_MAX_BATCH_INDEX) {
			throw IOException(metadata_violation("vgi_batch_index")
			                      .Set(error_key::kLimit, static_cast<int64_t>(VGI_MAX_BATCH_INDEX)),
			                  "VGI function '%s' emitted vgi_batch_index %llu that exceeds DuckDB's "
			                  "per-pipeline cap (maximum %llu); choose a smaller index space",
			                  bind_data.function_name, static_cast<unsigned long long>(parsed_index),
			                  static_cast<unsigned long long>(VGI_MAX_BATCH_INDEX));
		}
		if (local_state.current_batch_index != DConstants::INVALID_INDEX &&
		    parsed_index < local_state.current_batch_index) {
			throw IOException(metadata_violation("vgi_batch_index"),
			                  "VGI function '%s' emitted vgi_batch_index %llu after %llu on the "
			                  "same stream — batch_index must be monotone non-decreasing per "
			                  "worker / per stream",
			                  bind_data.function_name, static_cast<unsigned long long>(parsed_index),
			                  static_cast<unsigned long long>(local_state.current_batch_index));
		}
		local_state.current_batch_index = parsed_index;
	}
	// A vgi_batch_index from a function that did not opt in is ignored.

	// PartitionColumns: decode + validate the 2-row partition-values batch here
	// on the pipeline thread for uniform error reporting across transports.
	if (bind_data.partition_kind != VgiPartitionKind::NotPartitioned &&
	    !local_state.partition_data_from_split) {
		const std::string &pv_bytes = local_state.connection()->GetLastPartitionValuesBytes();
		if (pv_bytes.empty()) {
			throw IOException(metadata_violation("vgi_partition_values#b64"),
			                  "VGI function '%s' with partition columns declared emitted a "
			                  "non-empty data batch without vgi_partition_values metadata",
			                  bind_data.function_name);
		}
		// Borrowed: pv_bytes (the connection's last partition values) outlives this
		// decode, and pv_batch is converted to Values before InstallBatch returns.
		std::unique_ptr<VgiIpcReader> reader;
		try {
			reader = VgiIpcReader::Open(std::make_shared<VgiBufferReader>(
			    VgiBuffer::Wrap(pv_bytes.data(), static_cast<int64_t>(pv_bytes.size()))));
		} catch (const std::exception &e) {
			throw IOException(metadata_violation("vgi_partition_values#b64"),
			                  "VGI function '%s' emitted invalid Arrow IPC payload in "
			                  "vgi_partition_values#b64: %s",
			                  bind_data.function_name, ErrorData(e).RawMessage());
		}
		VgiBatchPtr pv_batch;
		try {
			pv_batch = reader->ReadNext();
		} catch (const std::exception &) {
			pv_batch = nullptr;
		}
		if (!pv_batch) {
			throw IOException(metadata_violation("vgi_partition_values#b64"),
			                  "VGI function '%s' emitted empty IPC stream in "
			                  "vgi_partition_values#b64",
			                  bind_data.function_name);
		}
		try {
			ValidateWorkerBatch(&context, pv_batch.get(),
			                    StringUtil::Format("function '%s' vgi_partition_values", bind_data.function_name));
		} catch (const IOException &e) {
			// The label names the payload in the message (unchanged); report the
			// real worker as worker_path and the payload as the metadata key.
			ErrorInfo info = ExtraInfoOf(e);
			info.erase(error_key::kWorkerPath);
			info.erase(error_key::kTransport);
			throw IOException(metadata_violation("vgi_partition_values#b64").Merge(info), RawMessageOf(e));
		}
		if (pv_batch->num_rows() != 2) {
			throw IOException(metadata_violation("vgi_partition_values#b64")
			                      .ExpectedActual("2", std::to_string(pv_batch->num_rows())),
			                  "VGI function '%s' emitted vgi_partition_values with %lld rows "
			                  "(expected exactly 2: row 0 = min, row 1 = max)",
			                  bind_data.function_name,
			                  static_cast<long long>(pv_batch->num_rows()));
		}
		const auto &declared_indices = bind_data.partition_column_indices;
		if (static_cast<idx_t>(pv_batch->num_columns()) != declared_indices.size()) {
			throw IOException(metadata_violation("vgi_partition_values#b64")
			                      .ExpectedActual(std::to_string(declared_indices.size()),
			                                      std::to_string(pv_batch->num_columns())),
			                  "VGI function '%s' emitted vgi_partition_values with %d columns "
			                  "(expected %llu — one per partition-annotated bind-schema field)",
			                  bind_data.function_name, pv_batch->num_columns(),
			                  static_cast<unsigned long long>(declared_indices.size()));
		}
		// Name + type check against the bind schema BEFORE conversion, so the
		// error names the column.
		const auto &bind_schema = bind_data.bind_result.output_schema;
		for (idx_t i = 0; i < declared_indices.size(); ++i) {
			const int declared_idx = static_cast<int>(declared_indices[i]);
			const auto &declared_field = bind_schema->field(declared_idx);
			const auto &pv_field = pv_batch->schema()->field(static_cast<int>(i));
			const std::string declared_name = bind_schema->field_name(declared_idx);
			const std::string pv_name = pv_batch->schema()->field_name(static_cast<int>(i));
			if (pv_name != declared_name) {
				throw IOException(metadata_violation("vgi_partition_values#b64")
				                      .Set(error_key::kColumnIndex, static_cast<int64_t>(i))
				                      .ExpectedActual(declared_name, pv_name),
				                  "VGI function '%s' vgi_partition_values column %llu name "
				                  "mismatch: declared '%s', got '%s'",
				                  bind_data.function_name, static_cast<unsigned long long>(i),
				                  declared_name, pv_name);
			}
			if (!SameDataType(pv_field, declared_field)) {
				const std::string declared_type = TypeToString(declared_field);
				const std::string pv_type = TypeToString(pv_field);
				throw IOException(metadata_violation("vgi_partition_values#b64")
				                      .Set(error_key::kColumnIndex, static_cast<int64_t>(i))
				                      .Set(error_key::kColumnName, declared_name)
				                      .ExpectedActual(declared_type, pv_type),
				                  "VGI function '%s' vgi_partition_values column '%s' type "
				                  "mismatch: declared %s, got %s",
				                  bind_data.function_name, declared_name, declared_type, pv_type);
			}
		}

		local_state.current_partition_data =
		    ConvertPartitionValuesBatch(context, pv_batch);

		// Defense in depth for SINGLE_VALUE_PARTITIONS: DuckDB's own
		// assertion in ``PhysicalPartitionedAggregate::Sink`` is
		// ``D_ASSERT`` (debug only); we re-check in release.
		if (bind_data.partition_kind == VgiPartitionKind::SingleValuePartitions) {
			for (idx_t i = 0; i < local_state.current_partition_data.size(); ++i) {
				const auto &entry = local_state.current_partition_data[i];
				if (!Value::NotDistinctFrom(entry.min_val, entry.max_val)) {
					throw IOException(metadata_violation("vgi_partition_values#b64")
					                      .Set(error_key::kColumnName,
					                           bind_schema->field_name(static_cast<int>(declared_indices[i])))
					                      .ExpectedActual(entry.min_val.ToString(), entry.max_val.ToString()),
					                  "VGI function '%s' SINGLE_VALUE_PARTITIONS contract "
					                  "violated on column '%s': min (%s) != max (%s)",
					                  bind_data.function_name,
					                  bind_schema->field_name(static_cast<int>(declared_indices[i])),
					                  entry.min_val.ToString(), entry.max_val.ToString());
				}
			}
		}
		// Synthetic per-batch index so NextBatch refreshes the sink's
		// partition_data; it MUST be global (see synthetic_batch_index).
		if (!bind_data.supports_batch_index) {
			local_state.current_batch_index =
			    global_state.synthetic_batch_index.fetch_add(1, std::memory_order_relaxed);
		}
	}

	// Type-confusion guard: the wire batch's column types must match what the
	// worker declared at bind. ArrowToDuckDB reads each buffer by the BIND-TIME
	// type, so a mismatch is a misread, not a cast. Skipped on a cached-replay
	// connection: it replays our own captured bytes (validated at capture), and
	// the result cache may serve a projection subset from a wider entry, so the
	// replay batch need not match this query's projection_ids.
	if (!local_state.connection()->IsCachedReplay()) {
		ValidateProjectedWireBatch(&context, *arrow_batch, bind_data.bind_result.output_schema,
		                           global_state.projection_ids, bind_data.worker_path(), bind_data.function_name);
	}

	auto chunk = make_uniq<ArrowArrayWrapper>();
	ExportRecordBatch(arrow_batch, *chunk);

	local_state.chunk = shared_ptr<ArrowArrayWrapper>(chunk.release());
	local_state.chunk_offset = 0;
	// REQUIRED: Reset() clears array_states' owned_data so ArrowToDuckDB repoints
	// it at the new chunk instead of the released previous one.
	local_state.Reset();

	// Batch-shape accounting for EXPLAIN ANALYZE; TotalBufferSize is O(columns).
	const auto batch_rows = static_cast<idx_t>(arrow_batch->num_rows());
	const auto batch_bytes = static_cast<idx_t>(arrow_batch->TotalBufferSize());
	global_state.RecordBatchStats(batch_rows, batch_bytes);

	if (VgiInfoLogActive(context)) {
		auto &conn = *local_state.connection();
		vector<pair<string, string>> fields;
		fields.emplace_back("conn", conn.GetConnIdHex());
		fields.emplace_back("worker_path", bind_data.worker_path());
		AppendSubprocessPidField(fields, conn);
		fields.emplace_back("function_name", bind_data.function_name);
		fields.emplace_back("batch_rows", std::to_string(batch_rows));
		fields.emplace_back("batch_bytes", std::to_string(batch_bytes));
		VGI_LOG(context, "table_function.batch_received", fields);
	}

	if (global_state.capture && local_state.capture_stream) {
		auto &cap = *global_state.capture;
		if (!cap.aborted.load(std::memory_order_relaxed)) {
			auto *conn = local_state.connection();
			// A worker advertises cache control on its stream's first batch.
			if (!local_state.capture_checked_first_batch) {
				local_state.capture_checked_first_batch = true;
				VgiCacheControl cc = conn->GetLastCacheControl();
				if (cc.present) {
					std::lock_guard<std::mutex> lk(cap.mu);
					if (!cap.cc_seen) {
						cap.cc = cc;
						cap.cc_seen = true;
					}
				}
			}
			// A non-cacheable advertisement aborts the capture.
			bool not_cacheable = false;
			{
				std::lock_guard<std::mutex> lk(cap.mu);
				not_cacheable = cap.cc_seen && !cap.cc.Cacheable();
			}
			auto spill_append = [&](const VgiBufferPtr &ipc, int64_t logical_len) -> bool {
				idx_t bi = conn->GetLastBatchIndex();
				bool has_bi = bi != DConstants::INVALID_INDEX;
				return cap.disk_writer->AppendBatch(
				    has_bi, has_bi ? static_cast<uint64_t>(bi) : 0,
				    static_cast<int64_t>(arrow_batch->num_rows()), conn->GetLastPartitionValuesBytes(),
				    ipc->data(), static_cast<int64_t>(ipc->size()), logical_len);
			};
			auto log_disk_too_large = [&]() {
				cap.aborted.store(true, std::memory_order_relaxed);
				GetResultCache(context).NoteCaptureAbort();
				LogResultCache(context, "result_cache.abort",
				               {{"key_hash", cap.key.HexDigest()},
				                {"reason", "disk_entry_too_large"},
				                {"bytes_seen", std::to_string(cap.disk_writer->Bytes())}});
			};
			if (not_cacheable) {
				// Only the first thread to flip `aborted` logs.
				if (!cap.aborted.exchange(true, std::memory_order_relaxed)) {
					LogResultCache(context, "result_cache.store_skipped",
					               {{"function", cap.key.function_name},
					                {"key_hash", cap.key.HexDigest()},
					                {"reason", "not_cacheable"}});
				}
			} else if (cap.streaming()) {
				// SPILLED: drain this thread's RAM substream once, then append to disk.
				if (!local_state.capture_spilled) {
					if (!DrainSubstreamToWriter(*local_state.capture_stream, *cap.disk_writer)) {
						log_disk_too_large();
					}
					local_state.capture_spilled = true;
				}
				if (!cap.aborted.load(std::memory_order_relaxed)) {
					// Compress at source (one serialize pass). logical_len is the batch's
					// buffer bytes, not the uncompressed IPC size the memory path counts,
					// so the advisory logical bytes= may differ by IPC framing; the disk
					// CAP uses the compressed on-disk size either way.
					const int64_t logical_len = static_cast<int64_t>(batch_bytes);
					auto ipc = SerializeRecordBatch(arrow_batch, nullptr, cap.disk_writer->CodecName(),
					                                cap.disk_writer->Level());
					if (!spill_append(ipc, logical_len)) {
						log_disk_too_large();
					}
				}
			} else {
				auto ipc = SerializeRecordBatch(arrow_batch);
				const int64_t sz = static_cast<int64_t>(ipc->size());
				const int64_t running = cap.total_bytes.fetch_add(sz, std::memory_order_relaxed) + sz;
				if (running > cap.max_entry_bytes) {
					// Over the per-entry memory cap: SPILL to disk if the tier is on.
					bool spilled = false;
					if (!cap.disk_dir.empty() && cap.disk_max > 0) {
						std::lock_guard<std::mutex> lk(cap.mu);
						if (!cap.disk_writer && !cap.aborted.load(std::memory_order_relaxed)) {
							cap.disk_writer = GetResultCache(context).BeginStreamingCapture(
							    cap.key, cap.disk_dir, cap.disk_max, cap.disk_compression,
							    cap.disk_compression_level);
							if (cap.disk_writer) {
								cap.spilling.store(true, std::memory_order_release);
							}
						}
						spilled = cap.disk_writer != nullptr;
					}
					if (spilled) {
						// Append THIS batch, then drain the whole substream in order.
						VgiCachedBatch cb;
						cb.ipc = std::move(ipc);
						cb.rows = arrow_batch->num_rows();
						idx_t bi = conn->GetLastBatchIndex();
						if (bi != DConstants::INVALID_INDEX) {
							cb.batch_index = static_cast<uint64_t>(bi);
							cb.has_batch_index = true;
						}
						cb.partition_values_bytes = conn->GetLastPartitionValuesBytes();
						local_state.capture_stream->batches.push_back(std::move(cb));
						if (!DrainSubstreamToWriter(*local_state.capture_stream, *cap.disk_writer)) {
							log_disk_too_large();
						}
						local_state.capture_spilled = true;
					} else {
						// No disk tier: abort the capture; the scan continues.
						cap.aborted.store(true, std::memory_order_relaxed);
						GetResultCache(context).NoteCaptureAbort();
						LogResultCache(context, "result_cache.abort",
						               {{"key_hash", cap.key.HexDigest()},
						                {"reason", "entry_too_large"},
						                {"bytes_seen", std::to_string(running)}});
					}
				} else if (!GetResultCache(context).TryReserveInflightCapture(sz)) {
					// Database-wide in-flight capture budget exhausted: abort this
					// capture; the scan continues.
					cap.aborted.store(true, std::memory_order_relaxed);
					GetResultCache(context).NoteCaptureAbort();
					LogResultCache(context, "result_cache.abort",
					               {{"key_hash", cap.key.HexDigest()},
					                {"reason", "inflight_budget"},
					                {"bytes_seen", std::to_string(running)}});
				} else {
					cap.reserved_inflight_bytes.fetch_add(sz, std::memory_order_relaxed);
					VgiCachedBatch cb;
					cb.ipc = std::move(ipc);
					cb.rows = arrow_batch->num_rows();
					idx_t bi = conn->GetLastBatchIndex();
					if (bi != DConstants::INVALID_INDEX) {
						cb.batch_index = static_cast<uint64_t>(bi);
						cb.has_batch_index = true;
					}
					cb.partition_values_bytes = conn->GetLastPartitionValuesBytes();
					local_state.capture_stream->bytes += sz;
					local_state.capture_stream->rows += cb.rows;
					local_state.capture_stream->batches.push_back(std::move(cb));
				}
			}
		}
	}

	return true;
}

// On not_modified, re-insert the stored entry with a slid TTL (and any refreshed
// validators). Best-effort: a failed insert means the next scan revalidates.
static void MaybeSlideRevalidatedEntry(ClientContext &context, const VgiTableFunctionBindData &bind_data,
                                       VgiTableFunctionGlobalState &global_state,
                                       const VgiCacheControl &cc) {
	const auto &orig = *global_state.revalidation_entry;
	auto fresh = std::make_shared<VgiResultCacheEntry>(orig); // shallow copy (shared Buffers)
	fresh->stored_at = std::chrono::steady_clock::now();
	// A ttl on the not_modified batch wins; else reuse the prior lifetime.
	int64_t ttl = cc.ttl_seconds.value_or(0);
	if (ttl <= 0 && cc.expires_rfc3339.empty() && !orig.never_expires) {
		auto prior = std::chrono::duration_cast<std::chrono::seconds>(orig.expires_at - orig.stored_at).count();
		ttl = prior > 0 ? prior : 0;
	}
	if (!orig.never_expires) {
		fresh->expires_at = fresh->stored_at + std::chrono::seconds(ttl > 0 ? ttl : 0);
	}
	// A worker may refresh validators alongside the 304.
	if (!cc.etag.empty()) {
		fresh->etag = cc.etag;
	}
	if (!cc.last_modified.empty()) {
		fresh->last_modified = cc.last_modified;
	}
	GetResultCache(context).Insert(fresh);
	LogResultCache(context, "result_cache.revalidate",
	               {{"catalog", global_state.revalidation_entry->catalog_name},
	                {"function", bind_data.function_name},
	                {"key_hash", global_state.revalidation_entry->key.HexDigest()},
	                {"outcome", "not_modified"}});
}

//! Claim (``next_split``) and initialize the next split for this reader. True
//! when a split was claimed and the connection re-initialized; false when out of
//! work. One split per claim: the client cannot see per-split cost
//! (`estimated_bytes` is an optional, often uncorrelated hint), so it does no
//! grouping.
static bool AdvanceToNextSplit(ClientContext &context, const VgiTableFunctionBindData &bind_data,
                               VgiTableFunctionGlobalState &global_state,
                               VgiTableFunctionLocalState &local_state) {
	if (!bind_data.supports_splits) {
		return false;
	}

	// The split we were reading (if any) is fully drained (see completed_splits).
	const bool had_split = local_state.has_split;
	if (had_split) {
		global_state.completed_splits.fetch_add(1, std::memory_order_relaxed);
		// The capture's counter is the one the commit gate reads; bump both.
		if (global_state.capture) {
			global_state.capture->completed_splits.fetch_add(1, std::memory_order_relaxed);
		}
		local_state.has_split = false;
	}

	const idx_t idx = global_state.next_split.fetch_add(1, std::memory_order_relaxed);
	if (idx >= global_state.splits.size()) {
		return false; // out of work — the caller ends this reader
	}

	// Ascending-claim contract; batch_index monotonicity depends on it.
	D_ASSERT(!local_state.has_split || idx > local_state.last_split_index);
	local_state.last_split_index = idx;
	if (global_state.split_partition_data_enabled) {
		local_state.current_partition_data = global_state.split_partition_data[idx];
		local_state.partition_data_from_split = true;
		if (!bind_data.supports_batch_index) {
			local_state.current_batch_index =
			    global_state.synthetic_batch_index.fetch_add(1, std::memory_order_relaxed);
		}
	} else {
		local_state.current_partition_data.clear();
		local_state.partition_data_from_split = false;
	}

	// A pooled worker that died while idle surfaces at the first split's init;
	// retry once, on the first claim only (see reacquire_fresh_connection).
	auto retry_stale_pooled = [&](const IOException &e) {
		if (had_split || !local_state.reacquire_fresh_connection) {
			return false;
		}
		VGI_LOG(context, "worker_pool.stale",
		        {{"worker_path", bind_data.worker_path()},
		         {"function_name", bind_data.function_name},
		         {"phase", "split_init"},
		         {"error", e.what()}});
		local_state.prefetch_slot_->connection = local_state.reacquire_fresh_connection(context);
		local_state.reacquire_fresh_connection = nullptr;
		local_state.connection()->SetTickFilterState(global_state.tick_filter_state);
		return true;
	};

	// Re-reads the connection each call: the retry swaps it.
	auto do_split_init = [&]() {
		auto &conn = *local_state.connection();
		try {
			// The first claim has no prior stream (InitLocal only acquired).
			if (had_split) {
				conn.ResetForNextSplit();
			}
			// A fresh init: every input is re-supplied. The tick filter state is a
			// connection member the init never touches, so it carries over.
			conn.PerformInit(bind_data.bind_result,       // bind
			                 global_state.projection_ids, // projection
			                 global_state.static_filter_bytes,
			                 global_state.join_keys_buffers,
			                 "",             // phase
			                 std::nullopt,   // order_by
			                 std::nullopt,   // table_sample
			                 global_state.init_opaque_data,
			                 std::nullopt,                // finalize_state_id
			                 {global_state.splits[idx].token}); // one split per claim
		} catch (...) {
			// Poison on EVERY failed split init, or the connection could be pooled
			// with an unanswered init on the wire.
			conn.MarkSplitInitFailed();
			throw;
		}
	};

	try {
		do_split_init();
	} catch (const IOException &e) {
		if (!retry_stale_pooled(e)) {
			throw;
		}
		do_split_init();
	}
	local_state.reacquire_fresh_connection = nullptr;
	local_state.has_split = true;
	return true;
}

static bool GetNextBatch(ClientContext &context, const VgiTableFunctionBindData &bind_data,
                         VgiTableFunctionGlobalState &global_state,
                         VgiTableFunctionLocalState &local_state) {
	if (local_state.done) {
		return false;
	}
	// Split path: claim the FIRST split before reading (InitLocal only acquired).
	// NOT on a cache serve: the hit planned no splits, so claiming would end the
	// reader and turn the hit into an EMPTY result.
	if (bind_data.supports_splits && !global_state.serving_from_cache && !local_state.has_split) {
		if (!AdvanceToNextSplit(context, bind_data, global_state, local_state)) {
			// Zero splits (fully pruned) is legal: an empty result.
			return InstallBatch(context, bind_data, global_state, local_state, nullptr);
		}
	}
	// Skip empty batches (log batches are already consumed in ReadDataBatch).
	while (true) {
		// Empty batches and split claims loop here without returning to DuckDB,
		// whose own interrupt check runs only between chunks.
		if (context.IsInterrupted()) {
			throw InterruptException();
		}
		UpdateDynamicFilterState(global_state, context, bind_data);
		auto arrow_batch = local_state.connection()->ReadDataBatch();
		if (bind_data.explicit_query) {
			ValidateVgiQueryResultBatch(arrow_batch, bind_data.bind_result.output_schema);
		}
		if (!arrow_batch) {
			// End of THIS stream; on the split path, claim the next split. Only this
			// synchronous path claims, so the split path disables async prefetch
			// (see VgiTableFunctionScan). An empty split loops on instead of
			// returning a 0-row chunk, which DuckDB treats as FINISHED.
			if (AdvanceToNextSplit(context, bind_data, global_state, local_state)) {
				continue;
			}
			return InstallBatch(context, bind_data, global_state, local_state, nullptr);
		}
		if (arrow_batch->num_rows() == 0) {
			// A 0-row vgi.cache.not_modified batch: slide the stored entry's TTL and
			// replay it (race-free: MaxThreads()==1 while revalidating).
			if (global_state.revalidating && global_state.revalidation_entry &&
			    !global_state.revalidation_served.exchange(true)) {
				auto cc = local_state.connection()->GetLastCacheControl();
				if (cc.not_modified) {
					MaybeSlideRevalidatedEntry(context, bind_data, global_state, cc);
					// Abandon the live connection and the capture.
					local_state.prefetch_slot_->connection =
					    make_uniq<CachedReplayConnection>(global_state.revalidation_entry);
					if (global_state.capture) {
						global_state.capture->aborted.store(true);
					}
					continue; // next ReadDataBatch pulls from the replay connection
				}
			}
			if (VgiInfoLogActive(context)) {
				auto &conn = *local_state.connection();
				vector<pair<string, string>> fields;
				fields.emplace_back("conn", conn.GetConnIdHex());
				fields.emplace_back("worker_path", bind_data.worker_path());
				AppendSubprocessPidField(fields, conn);
				fields.emplace_back("function_name", bind_data.function_name);
				VGI_LOG(context, "table_function.batch_empty_skipped", fields);
			}
			continue;
		}
		return InstallBatch(context, bind_data, global_state, local_state, std::move(arrow_batch));
	}
}

void VgiPrefetchTask::Execute() {
	// Local state torn down (cancellation): skip the RPC and leave the state
	// IN_FLIGHT; nothing will read it.
	if (slot_->cancelled.load(std::memory_order_acquire)) {
		return;
	}
	try {
		// Skip 0-row batches, as the sync path does.
		VgiBatchPtr batch;
		while (true) {
			// Return now rather than after the next RPC: DuckDB waits for every
			// executor task before an interrupted query ends.
			if (context_.IsInterrupted()) {
				throw InterruptException();
			}
			batch = slot_->connection->ReadDataBatch();
			if (!batch || batch->num_rows() > 0) {
				break;
			}
		}
		slot_->batch = std::move(batch);
		slot_->state.store(PrefetchState::READY);
	} catch (...) {
		slot_->exception = std::current_exception();
		slot_->state.store(PrefetchState::ERROR);
	}
}

static void LaunchPrefetch(ClientContext &context, TableFunctionInput &input,
                           VgiTableFunctionLocalState &local_state) {
	local_state.prefetch_slot_->state.store(PrefetchState::IN_FLIGHT);
	vector<unique_ptr<AsyncTask>> tasks;
	tasks.push_back(make_uniq<VgiPrefetchTask>(context, local_state.prefetch_slot_));
	input.async_result = AsyncResult(std::move(tasks));
}

static void ConsumePrefetchedBatch(ClientContext &context, const VgiTableFunctionBindData &bind_data,
                                   VgiTableFunctionGlobalState &global_state,
                                   VgiTableFunctionLocalState &local_state) {
	auto arrow_batch = std::move(local_state.prefetch_slot_->batch);
	local_state.prefetch_slot_->batch.reset();
	local_state.prefetch_slot_->state.store(PrefetchState::IDLE);
	InstallBatch(context, bind_data, global_state, local_state, std::move(arrow_batch));
}

static void ConvertCurrentBatch(const VgiTableFunctionBindData &bind_data,
                                VgiTableFunctionGlobalState &global_state,
                                VgiTableFunctionLocalState &local_state, DataChunk &output) {
	idx_t output_size =
	    MinValue<idx_t>(STANDARD_VECTOR_SIZE, local_state.chunk->arrow_array.length - local_state.chunk_offset);

	output.SetCardinality(output_size);

	if (output_size > 0) {
		idx_t rowid_col = (!bind_data.projection_pushdown && !HasGeneratedCatalogColumns(bind_data) &&
		                    bind_data.rowid_worker_col_index >= 0)
		                      ? static_cast<idx_t>(bind_data.rowid_worker_col_index)
		                      : COLUMN_IDENTIFIER_ROW_ID;
		// ArrowToDuckDB indexes arrow_array.children with this same projection /
		// rowid mapping and would read past the end (SIGSEGV) for a batch with
		// fewer columns than declared, so bounds-check first.
		const int64_t n_children = local_state.chunk->arrow_array.n_children;
		for (idx_t idx = 0; idx < output.ColumnCount(); idx++) {
			idx_t col_idx = local_state.column_ids.empty() ? idx : local_state.column_ids[idx];
			idx_t arrow_array_idx = bind_data.projection_pushdown ? idx : col_idx;
			if (rowid_col != COLUMN_IDENTIFIER_ROW_ID) {
				if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
					arrow_array_idx = rowid_col;
				} else if (col_idx >= rowid_col) {
					arrow_array_idx += 1;
				}
			} else if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
				continue; // core skips the rowid column when no rowid index is defined
			}
			if (static_cast<int64_t>(arrow_array_idx) >= n_children) {
				throw IOException(
				    ErrorInfo(error_subtype::kSchemaMismatch)
				        .Function(bind_data.function_name, "TABLE")
				        .Worker(bind_data.worker_path())
				        .Set(error_key::kColumnIndex, static_cast<int64_t>(arrow_array_idx))
				        .ExpectedActual(std::to_string(arrow_array_idx + 1) + "+ columns",
				                        std::to_string(n_children) + " columns"),
				    "VGI function '%s' emitted a batch with %lld column(s) but the scan needs column "
				    "index %llu; the worker's scan-time output is inconsistent with its declared bind "
				    "output schema",
				    bind_data.function_name, static_cast<long long>(n_children),
				    static_cast<unsigned long long>(arrow_array_idx));
			}
		}
		ArrowTableFunction::ArrowToDuckDB(local_state, bind_data.arrow_table.GetColumns(), output,
		                                  bind_data.projection_pushdown, rowid_col);
	}

	local_state.chunk_offset += output.size();
	global_state.rows_read.fetch_add(output.size(), std::memory_order_relaxed);
	output.Verify();
}

static bool BatchExhausted(const VgiTableFunctionLocalState &local_state) {
	return !local_state.chunk || !local_state.chunk->arrow_array.release ||
	       local_state.chunk_offset >= static_cast<idx_t>(local_state.chunk->arrow_array.length);
}

void VgiTableFunctionScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<VgiTableFunctionBindData>();
	auto &global_state = input.global_state->Cast<VgiTableFunctionGlobalState>();
	auto &local_state = input.local_state->Cast<VgiTableFunctionLocalState>();

	bool is_async = (input.results_execution_mode == AsyncResultsExecutionMode::TASK_EXECUTOR);
	if (is_async) {
		Value async_val;
		if (context.TryGetCurrentSetting("vgi_async_prefetch", async_val)) {
			is_async = async_val.GetValue<bool>();
		}
	}
	// Never on the split path: VgiPrefetchTask reads the connection directly and
	// never claims the next split, so splits 1..N would be silently dropped (and
	// the clean-looking result even cached).
	is_async = is_async && !bind_data.supports_splits;
	// Explicit SQL: one ordered reader; every batch goes through GetNextBatch's
	// validation and cancellation checks.
	is_async = is_async && !bind_data.explicit_query;

	if (!BatchExhausted(local_state)) {
		ConvertCurrentBatch(bind_data, global_state, local_state, output);
		return;
	}

	if (local_state.done) {
		output.SetCardinality(0);
		return;
	}

	// Synchronous: GetNextBatch returns a non-empty batch or EOS.
	if (!is_async) {
		if (!GetNextBatch(context, bind_data, global_state, local_state)) {
			output.SetCardinality(0);
			return;
		}
		ConvertCurrentBatch(bind_data, global_state, local_state, output);
		return;
	}

	auto state = local_state.prefetch_slot_->state.load();

	if (state == PrefetchState::ERROR) {
		local_state.prefetch_slot_->state.store(PrefetchState::IDLE);
		std::rethrow_exception(local_state.prefetch_slot_->exception);
	}

	if (state == PrefetchState::READY) {
		ConsumePrefetchedBatch(context, bind_data, global_state, local_state);
		if (local_state.done) {
			output.SetCardinality(0);
			return;
		}
		ConvertCurrentBatch(bind_data, global_state, local_state, output);
		// Guard: if DuckDB-side filtering empties the chunk, IMPLICIT + empty = FINISHED.
		// Signal HAVE_MORE_OUTPUT to prevent premature termination.
		if (output.size() == 0) {
			input.async_result = AsyncResultType::HAVE_MORE_OUTPUT;
		}
		return;
	}

	if (state == PrefetchState::IDLE) {
		if (local_state.first_scan_call_) {
			// First batch synchronously, avoiding an extra BLOCKED round-trip.
			local_state.first_scan_call_ = false;
			if (!GetNextBatch(context, bind_data, global_state, local_state)) {
				output.SetCardinality(0);
				return;
			}
			ConvertCurrentBatch(bind_data, global_state, local_state, output);
			if (output.size() == 0) {
				input.async_result = AsyncResultType::HAVE_MORE_OUTPUT;
			}
			return;
		}
		UpdateDynamicFilterState(global_state, context, bind_data);
		// Later batches: launch a prefetch and return BLOCKED.
		LaunchPrefetch(context, input, local_state);
		output.SetCardinality(0);
		return;
	}

	// DuckDB does not call scan while the task runs.
	D_ASSERT(state != PrefetchState::IN_FLIGHT);
	throw InternalException("VgiTableFunctionScan: unexpected prefetch state IN_FLIGHT");
}

unique_ptr<NodeStatistics> VgiTableFunctionCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<VgiTableFunctionBindData>();

	// One lazy table_function_cardinality RPC.
	if (!bind_data.cardinality_fetched && !bind_data.bind_result.bind_request_bytes.empty()) {
		bind_data.cardinality_fetched = true;
		try {
			auto rpc_params = bind_data.attach_params ? bind_data.attach_params
			    : std::make_shared<VgiAttachParameters>(bind_data.worker_path(), "", bind_data.worker_debug(), bind_data.use_pool());
			CatalogRpcContext rpc_ctx{rpc_params, bind_data.attach_opaque_data, bind_data.transaction_opaque_data};
			auto result = InvokeTableFunctionCardinality(rpc_ctx, bind_data.bind_result.bind_request_bytes,
			                                             bind_data.bind_result.opaque_data, context);
			bind_data.cardinality_estimate = result.estimate;
			bind_data.cardinality_max = result.max;
		} catch (const std::exception &e) {
			// Not critical: cardinality stays unknown.
			VGI_LOG(context, "table_function.cardinality_error",
			        {{"worker_path", bind_data.worker_path()},
			         {"function_name", bind_data.function_name},
			         {"error", e.what()}});
		}
	}

	const bool have_est = bind_data.cardinality_estimate >= 0;
	const bool have_max = bind_data.cardinality_max >= 0;
	VGI_LOG(context, "table_function.cardinality",
	        {{"worker_path", bind_data.worker_path()},
	         {"function_name", bind_data.function_name},
	         {"cardinality_estimate", have_est ? std::to_string(bind_data.cardinality_estimate) : "unknown"},
	         {"cardinality_max", have_max ? std::to_string(bind_data.cardinality_max) : "unknown"}});

	if (have_est && have_max) {
		return make_uniq<NodeStatistics>(static_cast<idx_t>(bind_data.cardinality_estimate),
		                                  static_cast<idx_t>(bind_data.cardinality_max));
	}
	if (have_est) {
		return make_uniq<NodeStatistics>(static_cast<idx_t>(bind_data.cardinality_estimate));
	}
	if (have_max) {
		// max-only: still give the optimizer the upper bound.
		auto stats = make_uniq<NodeStatistics>();
		stats->has_max_cardinality = true;
		stats->max_cardinality = static_cast<idx_t>(bind_data.cardinality_max);
		return stats;
	}
	return make_uniq<NodeStatistics>();
}

double VgiTableFunctionProgress(ClientContext &context, const FunctionData *bind_data_p,
                                const GlobalTableFunctionState *global_state_p) {
	auto &bind_data = bind_data_p->Cast<VgiTableFunctionBindData>();
	auto &global_state = global_state_p->Cast<VgiTableFunctionGlobalState>();

	if (bind_data.cardinality_estimate > 0) {
		idx_t rows_read = global_state.rows_read.load();
		double progress =
		    (static_cast<double>(rows_read) / static_cast<double>(bind_data.cardinality_estimate)) * 100.0;
		progress = MinValue(progress, 100.0);
		return progress;
	}

	return -1.0;
}

InsertionOrderPreservingMap<string> VgiTableFunctionToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind_data = input.bind_data->Cast<VgiTableFunctionBindData>();
	result["Worker"] = bind_data.worker_path();
	result["Function"] = bind_data.function_name;
	if (bind_data.order_by_hint) {
		result["Order Hint"] = bind_data.order_by_hint->column_name + " " +
		                       bind_data.order_by_hint->direction;
		if (bind_data.order_by_hint->row_limit >= 0) {
			result["Row Limit Hint"] = std::to_string(bind_data.order_by_hint->row_limit);
		}
	}
	if (bind_data.table_sample_hint) {
		result["Sample Hint"] = std::to_string(bind_data.table_sample_hint->sample_percentage) + "%";
		if (bind_data.table_sample_hint->seed >= 0) {
			result["Sample Seed"] = std::to_string(bind_data.table_sample_hint->seed);
		}
	}
	return result;
}

// --- EXPLAIN ANALYZE metrics (get_metrics) ----------------------------------
// Called once per scan thread from OperatorProfiler::FinishSource; per-thread
// maps merge last-write-wins, so every call reports the same gstate-wide
// values. The user-diagnostics RPC goes through the worker pool (like
// cardinality/statistics), not the per-stream connection.

static InsertionOrderPreservingMap<string> CollectVgiTableFunctionMetrics(TableFunctionGetMetricsInput &input) {
	InsertionOrderPreservingMap<string> result;
	if (!input.bind_data || !input.global_state) {
		return result;
	}
	auto &bind_data = input.bind_data->Cast<VgiTableFunctionBindData>();
	auto &global_state = input.global_state->Cast<VgiTableFunctionGlobalState>();

	// Intrinsic keys. No row count (DuckDB prints it) or thread count
	// (max_workers is only an upper bound); workers may add their own.
	result["Worker"] = bind_data.worker_path();
	result["Function"] = bind_data.function_name;

	// Before the early return: a hit has no global_execution_id.
	if (global_state.serving_from_cache) {
		result["Cache"] = (global_state.serving_entry && global_state.serving_entry->disk_backed)
		                      ? "hit (disk_streaming)"
		                      : "hit (memory)";
	} else if (global_state.cache_eligible) {
		result["Cache"] = "miss";
	}

	// Worker batch shape (a cache hit reports its stored shape).
	const auto batches = global_state.batches_received.load(std::memory_order_relaxed);
	if (batches > 0) {
		const auto rows = global_state.batch_rows_total.load(std::memory_order_relaxed);
		const auto rows_min = global_state.batch_rows_min.load(std::memory_order_relaxed);
		const auto rows_max = global_state.batch_rows_max.load(std::memory_order_relaxed);
		const auto bytes = global_state.batch_bytes_total.load(std::memory_order_relaxed);
		result["Batches"] = StringUtil::Format("%llu (rows: min %llu, avg %llu, max %llu)",
		                                       static_cast<unsigned long long>(batches),
		                                       static_cast<unsigned long long>(rows_min),
		                                       static_cast<unsigned long long>(rows / batches),
		                                       static_cast<unsigned long long>(rows_max));
		result["Batch Bytes"] = StringUtil::BytesToHumanReadableString(bytes);
	}

	// User-defined diagnostics via worker RPC, when there is enough context
	// (bind request, execution id, and the context captured at InitGlobal).
	if (bind_data.bind_result.bind_request_bytes.empty() || global_state.global_execution_id.empty() ||
	    global_state.client_context_for_explain == nullptr) {
		return result;
	}

	try {
		auto rpc_params = bind_data.attach_params
		    ? bind_data.attach_params
		    : std::make_shared<VgiAttachParameters>(bind_data.worker_path(), "",
		                                            bind_data.worker_debug(), bind_data.use_pool());
		CatalogRpcContext rpc_ctx{rpc_params, bind_data.attach_opaque_data, bind_data.transaction_opaque_data};
		auto user_map = InvokeTableFunctionDynamicToString(
		    rpc_ctx, bind_data.bind_result.bind_request_bytes, bind_data.bind_result.opaque_data,
		    global_state.global_execution_id, *global_state.client_context_for_explain);
		// User keys may override intrinsic ones.
		for (const auto &it : user_map) {
			result[it.first] = it.second;
		}
	} catch (const std::exception &e) {
		VGI_LOG(*global_state.client_context_for_explain, "table_function.dynamic_to_string_error",
		        {{"worker_path", bind_data.worker_path()},
		         {"function_name", bind_data.function_name},
		         {"error", e.what()}});
	}
	return result;
}

void VgiTableFunctionGetMetrics(TableFunctionGetMetricsInput &input) {
	for (const auto &entry : CollectVgiTableFunctionMetrics(input)) {
		input.operator_metrics.AddExtraInfo(entry.first, entry.second);
	}
}

// --- Bind info / virtual (rowid) column callbacks ---------------------------

BindInfo VgiTableScanGetBindInfo(TableFunctionGetBindInfoInput &input) {
	auto &bind_data = input.bind_data->Cast<VgiTableFunctionBindData>();
	return BindInfo(bind_data.table_entry ? ScanType::TABLE : ScanType::EXTERNAL);
}

optional_ptr<TableCatalogEntry> VgiTableScanGetTableEntry(optional_ptr<const FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<VgiTableFunctionBindData>();
	return bind_data.table_entry;
}

virtual_column_map_t VgiTableScanGetVirtualColumns(ClientContext &context, optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<VgiTableFunctionBindData>();
	virtual_column_map_t result;
	if (HasGeneratedCatalogColumns(bind_data)) {
		result.insert({COLUMN_IDENTIFIER_EMPTY,
		               TableColumn("", bind_data.table_entry->GetColumns().GetColumn(PhysicalIndex(0)).Type())});
	}
	if (bind_data.rowid_worker_col_index >= 0) {
		result.insert({COLUMN_IDENTIFIER_ROW_ID, TableColumn("rowid", bind_data.rowid_type)});
	}
	return result;
}

vector<column_t> VgiTableScanGetRowIdColumns(ClientContext &context, optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<VgiTableFunctionBindData>();
	if (bind_data.rowid_worker_col_index < 0) {
		return {};
	}
	return {COLUMN_IDENTIFIER_ROW_ID};
}

void VgiSetScanOrder(unique_ptr<RowGroupOrderOptions> order_options, optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<VgiTableFunctionBindData>();

	auto col_idx = order_options->column_idx.GetPrimaryIndex();
	if (col_idx >= bind_data.all_column_names.size()) {
		return;
	}

	std::string direction;
	switch (order_options->order_type) {
	case OrderType::ASCENDING:
		direction = "ASC";
		break;
	case OrderType::DESCENDING:
		direction = "DESC";
		break;
	default:
		// ORDER_DEFAULT / INVALID: the planner should have resolved it.
		return;
	}

	std::string null_order;
	switch (order_options->null_order) {
	case OrderByNullType::NULLS_FIRST:
		null_order = "NULLS_FIRST";
		break;
	case OrderByNullType::NULLS_LAST:
		null_order = "NULLS_LAST";
		break;
	case OrderByNullType::ORDER_DEFAULT:
		// DuckDB's default is NULLS_LAST.
		null_order = "NULLS_LAST";
		break;
	default:
		return;
	}

	// Combined limit+offset, -1 when absent.
	int64_t row_limit = -1;
	if (order_options->row_limit.IsValid()) {
		row_limit = static_cast<int64_t>(order_options->row_limit.GetIndex());
	}

	bind_data.order_by_hint = OrderByHint {
	    bind_data.all_column_names[col_idx],
	    std::move(direction),
	    std::move(null_order),
	    row_limit
	};
}

//! Map a DuckDB BASE (table) column index to a WORKER-schema index: the worker
//! schema (and ``partition_column_indices``) INCLUDES the hidden rowid column
//! that DuckDB's column list omits. Same shift as ``VgiSerializeFilters``;
//! without it a rowid before a partition column silently disables the
//! partitioned aggregate.
static idx_t WorkerIndexFromBaseColumn(const VgiTableFunctionBindData &bind_data, column_t base_col) {
	if (base_col == COLUMN_IDENTIFIER_ROW_ID) {
		return bind_data.rowid_worker_col_index >= 0
		           ? NumericCast<idx_t>(bind_data.rowid_worker_col_index)
		           : base_col;
	}
	idx_t worker_col = base_col;
	if (bind_data.rowid_worker_col_index >= 0 &&
	    worker_col >= NumericCast<idx_t>(bind_data.rowid_worker_col_index)) {
		worker_col++;
	}
	return worker_col;
}

// get_partition_data: see the header. Called by
// ``PhysicalTableScan::GetPartitionData`` per source chunk; the values were
// set by ``InstallBatch`` (see ``current_batch_index`` for thread safety and
// why INVALID is unreachable for batch_index functions).
//
// IMPORTANT: ``PhysicalPartitionedAggregate::Sink`` reads ``partition_data[i]``
// by ITS partition-column position, so ``partition_data`` must be re-ordered
// to match ``input.partition_info.partition_columns``.
OperatorPartitionData VgiGetPartitionData(ClientContext &, TableFunctionGetPartitionInput &input) {
	auto &local_state = input.local_state->Cast<VgiTableFunctionLocalState>();
	auto &bind_data = input.bind_data->Cast<VgiTableFunctionBindData>();
	OperatorPartitionData result(local_state.current_batch_index);

	// Re-order from declared order to the sink's requested order, emitting only
	// the requested columns.
	if (!local_state.current_partition_data.empty() &&
	    !input.partition_info.partition_columns.empty()) {
		// The sink's indices are SCAN-LOCAL (positions in this scan's
		// column_ids): DuckDB maps them to base columns only while planning
		// (PhysicalPlanGenerator::HasSingleValuePartitions) and keeps the
		// unmapped vector on the aggregate. Map to worker-schema indices first,
		// or a projected scan hits the FATAL InternalException below. The
		// gstate's projection_ids is that mapping (rowid shift included); it is
		// empty without projection pushdown, where only the rowid shift applies.
		optional_ptr<VgiTableFunctionGlobalState> gstate;
		if (input.global_state) {
			gstate = &input.global_state->Cast<VgiTableFunctionGlobalState>();
		}
		auto to_worker_index = [&](column_t requested_col) -> idx_t {
			if (requested_col == COLUMN_IDENTIFIER_ROW_ID) {
				return bind_data.rowid_worker_col_index >= 0
				           ? NumericCast<idx_t>(bind_data.rowid_worker_col_index)
				           : requested_col;
			}
			if (gstate && !gstate->projection_ids.empty()) {
				if (requested_col < gstate->projection_ids.size()) {
					return NumericCast<idx_t>(gstate->projection_ids[requested_col]);
				}
				return requested_col;
			}
			// No projection pushdown: the sink's index IS the base-column index.
			return WorkerIndexFromBaseColumn(bind_data, requested_col);
		};

		result.partition_data.reserve(input.partition_info.partition_columns.size());
		for (column_t requested_col : input.partition_info.partition_columns) {
			const idx_t worker_col = to_worker_index(requested_col);
			idx_t declared_pos = DConstants::INVALID_INDEX;
			for (idx_t i = 0; i < bind_data.partition_column_indices.size(); ++i) {
				if (bind_data.partition_column_indices[i] == worker_col) {
					declared_pos = i;
					break;
				}
			}
			if (declared_pos == DConstants::INVALID_INDEX) {
				// Unexpected: VgiGetPartitionInfo answers NOT_PARTITIONED for
				// undeclared columns, so the planner should not get here.
				throw InternalException(
				    "VGI function '%s': sink requested partition column %llu "
				    "(worker-schema %llu) that is not in the declared partition set",
				    bind_data.function_name,
				    static_cast<unsigned long long>(requested_col),
				    static_cast<unsigned long long>(worker_col));
			}
			result.partition_data.push_back(local_state.current_partition_data[declared_pos]);
		}
	}
	return result;
}

// get_partition_info: see the header; called during GROUP BY planning.
TablePartitionInfo VgiGetPartitionInfo(ClientContext &, TableFunctionPartitionInput &input) {
	auto &bind_data = input.bind_data->Cast<VgiTableFunctionBindData>();
	if (bind_data.partition_kind == VgiPartitionKind::NotPartitioned) {
		return TablePartitionInfo::NOT_PARTITIONED;
	}
	// Every requested column must be declared. ``partition_ids`` are BASE
	// column indices; the declared set is worker-schema (see
	// WorkerIndexFromBaseColumn).
	for (auto col_id : input.partition_ids) {
		const idx_t worker_col = WorkerIndexFromBaseColumn(bind_data, col_id);
		bool found = false;
		for (idx_t declared : bind_data.partition_column_indices) {
			if (declared == worker_col) {
				found = true;
				break;
			}
		}
		if (!found) {
			return TablePartitionInfo::NOT_PARTITIONED;
		}
	}
	switch (bind_data.partition_kind) {
	case VgiPartitionKind::SingleValuePartitions:
		return TablePartitionInfo::SINGLE_VALUE_PARTITIONS;
	case VgiPartitionKind::OverlappingPartitions:
		return TablePartitionInfo::OVERLAPPING_PARTITIONS;
	case VgiPartitionKind::DisjointPartitions:
		return TablePartitionInfo::DISJOINT_PARTITIONS;
	case VgiPartitionKind::NotPartitioned:
	default:
		return TablePartitionInfo::NOT_PARTITIONED;
	}
}

// --- Statistics callback -----------------------------------------------------
// Catalog tables try VgiTableEntry::GetStatistics first (cached on the entry);
// unless TableInfo inlined column_statistics, a null falls back to the
// function's table_function_statistics RPC (cached on the bind data), so a
// catalog table backed by a function need not repeat the function's stats.

static unique_ptr<BaseStatistics> FetchFunctionStatistics(ClientContext &context,
                                                          const VgiTableFunctionBindData &bind_data,
                                                          const std::string &col_name) {
	if (bind_data.bind_result.bind_request_bytes.empty()) {
		return nullptr;
	}

	std::lock_guard<std::mutex> lock(bind_data.statistics_mutex);
	if (!bind_data.statistics_fetched) {
		bind_data.statistics_fetched = true;
		try {
			auto rpc_params = bind_data.attach_params
			    ? bind_data.attach_params
			    : std::make_shared<VgiAttachParameters>(bind_data.worker_path(), "",
			                                             bind_data.worker_debug(), bind_data.use_pool());
			CatalogRpcContext rpc_ctx{rpc_params, bind_data.attach_opaque_data, bind_data.transaction_opaque_data};
			bind_data.statistics_cache = InvokeTableFunctionStatistics(
			    rpc_ctx, bind_data.bind_result.bind_request_bytes, bind_data.bind_result.opaque_data,
			    bind_data.all_column_types, bind_data.all_column_names,
			    bind_data.function_name, context);
		} catch (const std::exception &e) {
			VGI_LOG(context, "table_function.statistics_error",
			        {{"worker_path", bind_data.worker_path()},
			         {"function_name", bind_data.function_name},
			         {"error", e.what()}});
		}
	}

	auto it = bind_data.statistics_cache.find(col_name);
	if (it != bind_data.statistics_cache.end() && it->second) {
		return it->second->ToUnique();
	}
	return nullptr;
}

unique_ptr<BaseStatistics> VgiTableFunctionStatistics(ClientContext &context, const FunctionData *bind_data_p,
                                                       column_t column_index) {
	auto &bind_data = bind_data_p->Cast<VgiTableFunctionBindData>();

	if (bind_data.table_entry) {
		auto &entry = const_cast<TableCatalogEntry &>(*bind_data.table_entry);
		auto entry_stats = entry.GetStatistics(context, column_index);
		if (entry_stats) {
			return entry_stats;
		}
		// Inlined column_statistics are authoritative: a null means "no stats",
		// not "ask the function".
		auto *vgi_entry = dynamic_cast<VgiTableEntry *>(&entry);
		if (vgi_entry && vgi_entry->GetTableInfo().column_statistics.has_value()) {
			return nullptr;
		}
	}

	if (column_index >= bind_data.all_column_names.size()) {
		return nullptr;
	}
	return FetchFunctionStatistics(context, bind_data, bind_data.all_column_names[column_index]);
}

} // namespace vgi
} // namespace duckdb
