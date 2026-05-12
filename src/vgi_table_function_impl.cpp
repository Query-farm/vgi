#include "vgi_table_function_impl.hpp"
#ifdef __EMSCRIPTEN__
#include <pthread.h>
#include <unistd.h>
#include "vgi_wasm_async_pool.hpp"
#endif

#include "storage/vgi_table_entry.hpp"
#include "vgi_bind_protocol.hpp"
#include "vgi_cancel_dispatcher.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_exception.hpp"
#include "vgi_extension.hpp"
#include "vgi_logging.hpp"
#include "vgi_protocol.hpp"
#include "vgi_worker_pool.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/planner/table_filter.hpp"

#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include "yyjson.hpp"

#include <arrow/c/bridge.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {
namespace vgi {

// ============================================================================
// VgiTableFunctionLocalState out-of-line definitions
// ============================================================================

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
	// Signal any in-flight prefetch task to discard its result once it
	// wakes up. Racy by design — a task already mid-RPC will finish;
	// the slot's shared_ptr keeps the connection alive for it.
	prefetch_slot_->cancelled.store(true, std::memory_order_release);

	// If a prefetch task still holds a reference, we can't touch the
	// connection without corrupting the subprocess stream. Defer all
	// further work — pool release and cancel alike.
	if (prefetch_slot_.use_count() != 1) {
		return;
	}

	auto &connection = prefetch_slot_->connection;
	if (!connection) {
		return;
	}

	// Early-exit path: stream is not done and cancel is enabled —
	// hand the connection to the dispatcher instead of returning it
	// to the pool. A cancelled connection's state is unsafe to reuse.
	if (!done && cancel_enabled) {
		if (auto *dispatcher = FindVgiCancelDispatcher(db_)) {
			auto token = connection->GetLastStateToken();
			CancelRequest req;
			req.connection = std::move(connection);
			req.state_token = !token.empty() ? std::move(token) : prefetch_slot_->last_state_token;
			if (dispatcher->Enqueue(std::move(req))) {
				return;
			}
			// Saturation: fall back to normal pool-release below so the
			// connection is not leaked. req.connection holds the unique_ptr
			// only if Enqueue returned false — move it back.
			connection = std::move(req.connection);
		}
	}

	// Natural-end or cancel-disabled: return to pool as before.
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

// ============================================================================
// Perform Bind - Common bind logic for VGI table functions
// ============================================================================

// Apply a freshly-built BindResult to bind_data and derive return_types /
// names. Shared by the on-demand RPC path and the inline-bind short-circuit
// so both populate bind_data identically. `conn_id` and `worker_pid` are
// included on the table_function.bind_result log for parity with downstream
// trace tooling; the inline path passes empty/-1.
static void ApplyBindResultToBindData(ClientContext &context, VgiTableFunctionBindData &bind_data,
                                       BindResult bind_result, const std::string &conn_id,
                                       int worker_pid, vector<LogicalType> &return_types,
                                       vector<string> &names) {
	bind_data.max_processes = 1;
	bind_data.cardinality_estimate = -1;
	bind_data.bind_result = std::move(bind_result);

	try {
		ArrowSchemaToDuckDBTypes(context, bind_data.bind_result.output_schema, bind_data.c_schema,
		                         bind_data.arrow_table, return_types, names);
	} catch (const std::exception &e) {
		throw IOException("Failed to convert output schema for function '%s': %s",
		                  bind_data.function_name, e.what());
	}

	if (bind_data.rowid_worker_col_index >= 0) {
		auto idx = static_cast<size_t>(bind_data.rowid_worker_col_index);
		if (idx < return_types.size()) {
			return_types.erase(return_types.begin() + idx);
			names.erase(names.begin() + idx);
		}
	}

	bind_data.all_column_names = names;
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

void PerformVgiTableFunctionBind(ClientContext &context, VgiTableFunctionBindData &bind_data,
                                 vector<LogicalType> &return_types, vector<string> &names) {
	// Log the invocation
	VGI_LOG(context, "table_function.bind",
	        {{"worker_path", bind_data.worker_path()},
	         {"function_name", bind_data.function_name},
	         {"num_args", bind_data.arguments.array ? std::to_string(bind_data.arguments.array->length()) : "0"}});

	// Validate that arguments type is a struct (defensive check)
	D_ASSERT(!bind_data.arguments.type || bind_data.arguments.type->id() == arrow::Type::STRUCT);

	// Inline-bind short-circuit: when the catalog-routed scan has a
	// `bind_result` carried on TableInfo (worker pre-bound at
	// schema_contents time), build equivalent bind_request_bytes locally
	// and skip the bind RPC entirely. Cardinality / statistics / init all
	// read through bind_data.bind_result.* — same shape as the RPC path.
	// On any deserialize/parse error, fall through to the on-demand RPC.
	if (bind_data.table_entry) {
		auto &tinfo = bind_data.table_entry->Cast<VgiTableEntry>().GetTableInfo();
		if (tinfo.bind_result.has_value()) {
			try {
				auto resolved_secrets = vgi::ExtractVgiSecrets(context, bind_data.required_secrets);
				auto bind_request_bytes = vgi::BuildBindRequestBytes(
				    context, bind_data.function_name, "TABLE",
				    bind_data.arguments.array, /*input_schema=*/nullptr,
				    bind_data.attach_id, bind_data.transaction_id,
				    bind_data.settings, resolved_secrets,
				    /*resolved_secrets_provided=*/false,
				    bind_data.worker_path());
				auto bind_result = vgi::BuildBindResultFromInlinedBytes(
				    std::move(bind_request_bytes), *tinfo.bind_result,
				    bind_data.worker_path());

				VGI_LOG(context, "table_function.inline_bind_used",
				        {{"worker_path", bind_data.worker_path()},
				         {"function_name", bind_data.function_name}});

				ApplyBindResultToBindData(context, bind_data, std::move(bind_result),
				                          /*conn_id=*/"", /*worker_pid=*/-1,
				                          return_types, names);
				return;
			} catch (const std::exception &e) {
				VGI_LOG(context, "table_function.inline_bind_error",
				        {{"worker_path", bind_data.worker_path()},
				         {"function_name", bind_data.function_name},
				         {"error", e.what()}});
				// fall through to AcquireAndBindConnection
			}
		}
	}

	// Create connection to worker and perform bind handshake.
	// Uses helper that handles pool acquire and stale connection retry.
	FunctionConnectionParams params;
	params.attach_params = bind_data.attach_params;
	params.attach_id = bind_data.attach_id;
	params.function_name = bind_data.function_name;
	params.arguments = bind_data.arguments;
	params.transaction_id = bind_data.transaction_id;
	params.settings = bind_data.settings;
	params.required_secrets = bind_data.required_secrets;
	params.phase = "bind";
	params.function_type = "TABLE";

	auto result = AcquireAndBindConnection(context, params);
	auto bind_worker_pid = result.connection ? result.connection->GetSubprocessPid().value_or(-1) : -1;
	auto bind_conn_id = result.connection ? result.connection->GetConnIdHex() : "";

	// Release the bind worker to the pool. Bind is a unary RPC; the worker
	// has looped back to its accept loop and is available to serve other
	// planner RPCs (e.g. table_function_cardinality) and the eventual init
	// in InitGlobal. Init re-binds on whichever pooled worker it acquires —
	// the bind RPC is cheap relative to a fresh spawn.
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

// ============================================================================
// Expression Filter Pushdown Support
// ============================================================================

namespace {

//! Recursively check whether an expression tree only contains functions
//! that the worker has declared support for.
//! Returns false if any unsupported function or unsupported node type is found.
bool ExpressionTreeIsSupported(const Expression &expr, const std::vector<std::string> &supported_functions) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_REF:
	case ExpressionClass::BOUND_COLUMN_REF:
		return true;
	case ExpressionClass::BOUND_CONSTANT:
		return true;
	case ExpressionClass::BOUND_FUNCTION: {
		auto &func_expr = expr.Cast<BoundFunctionExpression>();
		bool found = false;
		for (auto &s : supported_functions) {
			if (s == func_expr.function.name) {
				found = true;
				break;
			}
		}
		if (!found) {
			return false;
		}
		for (auto &child : func_expr.children) {
			if (!ExpressionTreeIsSupported(*child, supported_functions)) {
				return false;
			}
		}
		return true;
	}
	case ExpressionClass::BOUND_COMPARISON: {
		auto &comp_expr = expr.Cast<BoundComparisonExpression>();
		return ExpressionTreeIsSupported(*comp_expr.left, supported_functions) &&
		       ExpressionTreeIsSupported(*comp_expr.right, supported_functions);
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conj_expr = expr.Cast<BoundConjunctionExpression>();
		for (auto &child : conj_expr.children) {
			if (!ExpressionTreeIsSupported(*child, supported_functions)) {
				return false;
			}
		}
		return true;
	}
	case ExpressionClass::BOUND_CAST:
		// v1: reject casts rather than serializing them
		return false;
	default:
		return false;
	}
}

} // anonymous namespace

bool VgiPushdownExpression(ClientContext &context, const LogicalGet &get, Expression &expr) {
	if (!get.bind_data) {
		return false;
	}
	auto &bind_data = get.bind_data->Cast<VgiTableFunctionBindData>();
	if (bind_data.supported_expression_filters.empty()) {
		return false;
	}
	bool supported = ExpressionTreeIsSupported(expr, bind_data.supported_expression_filters);
	if (supported) {
		VGI_LOG(context, "table_function.expression_filter_accepted",
		        {{"function_name", bind_data.function_name}, {"expression", expr.ToString()}});
	} else {
		VGI_LOG(context, "table_function.expression_filter_rejected",
		        {{"function_name", bind_data.function_name},
		         {"expression", expr.ToString()},
		         {"reason", "expression tree contains unsupported function or node type"}});
	}
	return supported;
}

// ============================================================================
// FilterSerializer - Builds JSON filter structure and collects values
// ============================================================================

namespace {

//! Convert ExpressionType to operator string for JSON
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
	default:
		return "unknown";
	}
}

//! Info about a join key column to be serialized as a flat Arrow batch.
//! Holds a non-owning pointer to the InFilter::values vector.
//! Lifetime contract: the TableFilterSet passed to VgiSerializeFilters must outlive the FilterSerializer.
struct JoinKeysInfo {
	string column_name;
	idx_t column_index;
	LogicalType type;
	const vector<Value> *values; // borrowed from InFilter::values
};

//! Returns true if any descendant of ``filter`` is a DynamicFilter (Top-N
//! tick-time bound). The static filter serializer must skip an entire
//! OptionalFilter subtree when this is true: the DynamicFilter has no value
//! at init time, so any partial serialization (e.g. OR(IsNull, DynamicFilter)
//! collapsing to a one-child OR(IsNull) when DynamicFilter is elided) yields
//! a *stricter* filter than the OptionalFilter promised, and consumers can
//! drop rows that were supposed to flow. The dynamic-filter mechanism in
//! TickFilterPushdown re-pushes the filter at tick time once Top-N has
//! established its threshold.
static bool ContainsDynamicFilter(const TableFilter &filter) {
	switch (filter.filter_type) {
	case TableFilterType::DYNAMIC_FILTER:
		return true;
	case TableFilterType::CONJUNCTION_AND: {
		auto &conj = filter.Cast<ConjunctionAndFilter>();
		for (auto &child : conj.child_filters) {
			if (ContainsDynamicFilter(*child)) {
				return true;
			}
		}
		return false;
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &conj = filter.Cast<ConjunctionOrFilter>();
		for (auto &child : conj.child_filters) {
			if (ContainsDynamicFilter(*child)) {
				return true;
			}
		}
		return false;
	}
	case TableFilterType::OPTIONAL_FILTER: {
		auto &opt = filter.Cast<OptionalFilter>();
		return opt.child_filter && ContainsDynamicFilter(*opt.child_filter);
	}
	case TableFilterType::STRUCT_EXTRACT: {
		auto &sf = filter.Cast<StructFilter>();
		return sf.child_filter && ContainsDynamicFilter(*sf.child_filter);
	}
	default:
		return false;
	}
}

//! FilterSerializer walks the filter tree, builds JSON, and collects values
class FilterSerializer {
public:
	FilterSerializer(const string &worker_path, idx_t join_keys_max_bytes = 0)
	    : doc_(yyjson_mut_doc_new(nullptr)), worker_path_(worker_path),
	      join_keys_max_bytes_(join_keys_max_bytes) {
	}

	~FilterSerializer() {
		if (doc_) {
			yyjson_mut_doc_free(doc_);
		}
	}

	//! Serialize a single filter for a column. Returns nullptr if the filter was skipped
	//! (e.g., join keys exceeded byte-size limit).
	yyjson_mut_val *SerializeColumnFilter(idx_t column_index, const string &column_name, const TableFilter &filter) {
		auto obj = yyjson_mut_obj(doc_);
		yyjson_mut_obj_add_strcpy(doc_, obj, "column_name", column_name.c_str());
		yyjson_mut_obj_add_uint(doc_, obj, "column_index", column_index);

		if (!SerializeFilterInto(obj, filter, column_index, column_name)) {
			return nullptr; // filter was skipped
		}
		return obj;
	}

	//! Get the collected values
	const vector<Value> &GetValues() const {
		return values_;
	}

	//! Get the collected value types
	const vector<LogicalType> &GetValueTypes() const {
		return value_types_;
	}

	//! Write the JSON to a string (caller must free with free())
	char *WriteJson(yyjson_mut_val *root) {
		return yyjson_mut_val_write(root, 0, nullptr);
	}

	//! Get the document for creating arrays
	yyjson_mut_doc *GetDoc() {
		return doc_;
	}

	//! Whether any join key columns were accumulated
	bool HasJoinKeys() const {
		return !join_key_columns_.empty();
	}

	//! Get the accumulated join key columns
	const vector<JoinKeysInfo> &GetJoinKeyColumns() const {
		return join_key_columns_;
	}

private:
	//! Serialize filter fields into an existing object. Returns false if the filter was
	//! skipped (e.g., join keys exceeded byte-size limit), in which case the obj should be discarded.
	//! column_index and column_name are passed through for child filters in conjunctions
	bool SerializeFilterInto(yyjson_mut_val *obj, const TableFilter &filter, idx_t column_index,
	                         const string &column_name) {
		switch (filter.filter_type) {
		case TableFilterType::CONSTANT_COMPARISON: {
			auto &const_filter = filter.Cast<ConstantFilter>();
			yyjson_mut_obj_add_str(doc_, obj, "type", "constant");
			yyjson_mut_obj_add_str(doc_, obj, "op", ExpressionTypeToOp(const_filter.comparison_type));
			yyjson_mut_obj_add_uint(doc_, obj, "value_ref", AddValue(const_filter.constant));
			break;
		}
		case TableFilterType::IS_NULL: {
			yyjson_mut_obj_add_str(doc_, obj, "type", "is_null");
			break;
		}
		case TableFilterType::IS_NOT_NULL: {
			yyjson_mut_obj_add_str(doc_, obj, "type", "is_not_null");
			break;
		}
		case TableFilterType::IN_FILTER: {
			auto &in_filter = filter.Cast<InFilter>();
			if (in_filter.values.empty()) {
				return false; // empty IN filter — skip
			}
			// Estimate serialized byte size to decide whether to push join keys
			auto estimated_bytes = EstimateJoinKeyBytes(in_filter.values);
			if (join_keys_max_bytes_ > 0 && estimated_bytes > join_keys_max_bytes_) {
				// Too large — skip this filter. DuckDB still applies it client-side.
				return false;
			}
			// Each IN filter gets its own single-column batch in the join_keys array.
			yyjson_mut_obj_add_str(doc_, obj, "type", "join_keys");
			yyjson_mut_obj_add_strcpy(doc_, obj, "keys_column", column_name.c_str());
			join_key_columns_.push_back({column_name, column_index, in_filter.values[0].type(), &in_filter.values});
			break;
		}
		case TableFilterType::CONJUNCTION_AND: {
			auto &conj_filter = filter.Cast<ConjunctionAndFilter>();
			yyjson_mut_obj_add_str(doc_, obj, "type", "and");
			auto children = yyjson_mut_arr(doc_);
			for (auto &child : conj_filter.child_filters) {
				auto child_obj = yyjson_mut_obj(doc_);
				yyjson_mut_obj_add_strcpy(doc_, child_obj, "column_name", column_name.c_str());
				yyjson_mut_obj_add_uint(doc_, child_obj, "column_index", column_index);
				try {
					if (!SerializeFilterInto(child_obj, *child, column_index, column_name)) {
						continue; // child skipped (e.g., DynamicFilter not yet initialized)
					}
				} catch (const InvalidInputException &) {
					continue; // skip unserializable children (e.g., BloomFilter)
				}
				yyjson_mut_arr_append(children, child_obj);
			}
			if (yyjson_mut_arr_size(children) == 0) {
				return false; // all children skipped
			}
			yyjson_mut_obj_add_val(doc_, obj, "children", children);
			break;
		}
		case TableFilterType::CONJUNCTION_OR: {
			auto &conj_filter = filter.Cast<ConjunctionOrFilter>();
			yyjson_mut_obj_add_str(doc_, obj, "type", "or");
			auto children = yyjson_mut_arr(doc_);
			for (auto &child : conj_filter.child_filters) {
				auto child_obj = yyjson_mut_obj(doc_);
				yyjson_mut_obj_add_strcpy(doc_, child_obj, "column_name", column_name.c_str());
				yyjson_mut_obj_add_uint(doc_, child_obj, "column_index", column_index);
				try {
					if (!SerializeFilterInto(child_obj, *child, column_index, column_name)) {
						continue;
					}
				} catch (const InvalidInputException &) {
					continue;
				}
				yyjson_mut_arr_append(children, child_obj);
			}
			if (yyjson_mut_arr_size(children) == 0) {
				return false;
			}
			yyjson_mut_obj_add_val(doc_, obj, "children", children);
			break;
		}
		case TableFilterType::STRUCT_EXTRACT: {
			auto &struct_filter = filter.Cast<StructFilter>();
			yyjson_mut_obj_add_str(doc_, obj, "type", "struct");
			yyjson_mut_obj_add_uint(doc_, obj, "child_index", struct_filter.child_idx);
			yyjson_mut_obj_add_strcpy(doc_, obj, "child_name", struct_filter.child_name.c_str());
			auto child_filter_obj = yyjson_mut_obj(doc_);
			// Struct child filter inherits column info from parent struct column
			yyjson_mut_obj_add_strcpy(doc_, child_filter_obj, "column_name", column_name.c_str());
			yyjson_mut_obj_add_uint(doc_, child_filter_obj, "column_index", column_index);
			SerializeFilterInto(child_filter_obj, *struct_filter.child_filter, column_index, column_name);
			yyjson_mut_obj_add_val(doc_, obj, "child_filter", child_filter_obj);
			break;
		}
		case TableFilterType::DYNAMIC_FILTER: {
			// DynamicFilter values are not available at init time (Top-N hasn't
			// processed any rows yet). They are sent per-tick via custom metadata
			// once the Top-N heap establishes a boundary. Skip without throwing
			// so sibling filters in a ConjunctionAnd are still serialized.
			return false;
		}
		case TableFilterType::EXPRESSION_FILTER: {
			auto &expr_filter = filter.Cast<ExpressionFilter>();
			yyjson_mut_obj_add_str(doc_, obj, "type", "expression");
			auto expr_json = SerializeExpression(*expr_filter.expr);
			yyjson_mut_obj_add_val(doc_, obj, "expr", expr_json);
			break;
		}
		case TableFilterType::BLOOM_FILTER: {
			throw InvalidInputException(
			    "VGI filter pushdown failed for worker '%s': BloomFilter cannot be serialized because it contains a "
			    "large binary buffer from join optimization",
			    worker_path_);
		}
		case TableFilterType::OPTIONAL_FILTER: {
			auto &optional_filter = filter.Cast<OptionalFilter>();
			if (!optional_filter.child_filter) {
				return false;
			}
			// Skip the entire OptionalFilter when its subtree contains a
			// DynamicFilter — see the comment on ContainsDynamicFilter for why.
			// The dynamic-filter mechanism captures DynamicFilters via
			// try_capture_from_optional and pushes them per-tick once Top-N
			// has established a threshold; serializing the static portion
			// here would produce a stricter filter than the OptionalFilter
			// promised and silently drop correct rows. (Repro:
			// SELECT n FROM filter_echo(10) ORDER BY n NULLS FIRST LIMIT 3
			// returned 0 rows because OR(IsNull, DynamicFilter) collapsed
			// to a one-child OR(IsNull) once the DynamicFilter was elided.)
			if (ContainsDynamicFilter(*optional_filter.child_filter)) {
				return false;
			}
			return SerializeFilterInto(obj, *optional_filter.child_filter, column_index, column_name);
		}
		default: {
			throw InvalidInputException(
			    "VGI filter pushdown failed for worker '%s': unknown filter type %d cannot be serialized",
			    worker_path_, static_cast<int>(filter.filter_type));
		}
		}
		return true;
	}

	//! Add a value and return its reference index
	idx_t AddValue(const Value &value) {
		idx_t ref = values_.size();
		values_.push_back(value);
		value_types_.push_back(value.type());
		return ref;
	}

	//! Recursively serialize a bound expression tree to JSON
	yyjson_mut_val *SerializeExpression(const Expression &expr) {
		auto obj = yyjson_mut_obj(doc_);

		switch (expr.GetExpressionClass()) {
		case ExpressionClass::BOUND_REF: {
			auto &ref_expr = expr.Cast<BoundReferenceExpression>();
			yyjson_mut_obj_add_str(doc_, obj, "expr_type", "column_ref");
			yyjson_mut_obj_add_uint(doc_, obj, "index", ref_expr.index);
			break;
		}
		case ExpressionClass::BOUND_COLUMN_REF: {
			// BOUND_COLUMN_REF is replaced with BOUND_REF by ReplaceWithBoundReference
			// before ExpressionFilter is created, so this case should not be reached.
			// Handle it defensively by serializing as column_ref with index 0.
			yyjson_mut_obj_add_str(doc_, obj, "expr_type", "column_ref");
			yyjson_mut_obj_add_uint(doc_, obj, "index", 0);
			break;
		}
		case ExpressionClass::BOUND_CONSTANT: {
			auto &const_expr = expr.Cast<BoundConstantExpression>();
			yyjson_mut_obj_add_str(doc_, obj, "expr_type", "constant");
			yyjson_mut_obj_add_uint(doc_, obj, "value_ref", AddValue(const_expr.value));
			break;
		}
		case ExpressionClass::BOUND_FUNCTION: {
			auto &func_expr = expr.Cast<BoundFunctionExpression>();
			yyjson_mut_obj_add_str(doc_, obj, "expr_type", "function");
			yyjson_mut_obj_add_strcpy(doc_, obj, "function_name", func_expr.function.name.c_str());
			auto children = yyjson_mut_arr(doc_);
			for (auto &child : func_expr.children) {
				yyjson_mut_arr_append(children, SerializeExpression(*child));
			}
			yyjson_mut_obj_add_val(doc_, obj, "children", children);
			break;
		}
		case ExpressionClass::BOUND_COMPARISON: {
			auto &comp_expr = expr.Cast<BoundComparisonExpression>();
			yyjson_mut_obj_add_str(doc_, obj, "expr_type", "comparison");
			yyjson_mut_obj_add_str(doc_, obj, "op", ExpressionTypeToOp(comp_expr.GetExpressionType()));
			yyjson_mut_obj_add_val(doc_, obj, "left", SerializeExpression(*comp_expr.left));
			yyjson_mut_obj_add_val(doc_, obj, "right", SerializeExpression(*comp_expr.right));
			break;
		}
		case ExpressionClass::BOUND_CONJUNCTION: {
			auto &conj_expr = expr.Cast<BoundConjunctionExpression>();
			yyjson_mut_obj_add_str(doc_, obj, "expr_type", "conjunction");
			yyjson_mut_obj_add_str(
			    doc_, obj, "conjunction_type",
			    conj_expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND ? "and" : "or");
			auto children = yyjson_mut_arr(doc_);
			for (auto &child : conj_expr.children) {
				yyjson_mut_arr_append(children, SerializeExpression(*child));
			}
			yyjson_mut_obj_add_val(doc_, obj, "children", children);
			break;
		}
		default: {
			throw InvalidInputException(
			    "VGI expression filter serialization failed for worker '%s': unsupported expression class %d",
			    worker_path_, static_cast<int>(expr.GetExpressionClass()));
		}
		}

		return obj;
	}

	//! Estimate the serialized byte size of join key values.
	//! For fixed-width types this is exact; for strings, samples the first 64 values.
	static idx_t EstimateJoinKeyBytes(const vector<Value> &values) {
		if (values.empty()) {
			return 0;
		}
		auto internal_type = values[0].type().InternalType();
		if (internal_type == PhysicalType::VARCHAR) {
			// Sample first N values to estimate average string length
			constexpr idx_t SAMPLE_SIZE = 64;
			idx_t sample_count = MinValue<idx_t>(SAMPLE_SIZE, values.size());
			idx_t total_sample_bytes = 0;
			idx_t non_null_count = 0;
			for (idx_t i = 0; i < sample_count; i++) {
				if (!values[i].IsNull()) {
					total_sample_bytes += StringValue::Get(values[i]).size();
					non_null_count++;
				}
			}
			idx_t avg_len = non_null_count > 0 ? total_sample_bytes / non_null_count : 0;
			return values.size() * (avg_len + 4); // +4 for Arrow string offsets
		}
		// Fixed-width: exact calculation
		return values.size() * GetTypeIdSize(internal_type);
	}

	yyjson_mut_doc *doc_;
	string worker_path_;
	idx_t join_keys_max_bytes_;
	vector<Value> values_;
	vector<LogicalType> value_types_;
	vector<JoinKeysInfo> join_key_columns_;
};

} // anonymous namespace

// ============================================================================
// Serialize Filters - Convert TableFilterSet to Arrow IPC bytes for worker
// ============================================================================

SerializedFilters VgiSerializeFilters(ClientContext &context, const vector<column_t> &column_ids,
                                      optional_ptr<TableFilterSet> filters,
                                      const vector<string> &column_names, const string &worker_path) {
	// Return empty if no filters
	if (!filters || filters->filters.empty()) {
		return {nullptr, {}};
	}

	// Read byte size limit for join keys from settings
	idx_t join_keys_max_bytes = 0;
	Value max_bytes_val;
	if (context.TryGetCurrentSetting("vgi_join_keys_max_bytes", max_bytes_val) && !max_bytes_val.IsNull()) {
		join_keys_max_bytes = max_bytes_val.GetValue<idx_t>();
	}

	// Build JSON filter structure and collect values
	FilterSerializer serializer(worker_path, join_keys_max_bytes);
	auto filter_array = yyjson_mut_arr(serializer.GetDoc());

	for (auto &entry : filters->filters) {
		idx_t col_idx = entry.first;
		auto &filter = *entry.second;

		// DuckDB's TableFilterSet uses indices into the projected column list (column_ids).
		// Map through column_ids to get the original schema column name, but keep the
		// projected index as column_index since the worker output follows projection order.
		idx_t original_col_idx = col_idx < column_ids.size() ? column_ids[col_idx] : col_idx;
		string col_name =
		    original_col_idx < column_names.size() ? column_names[original_col_idx] : std::to_string(original_col_idx);

		if (filter.filter_type == TableFilterType::OPTIONAL_FILTER) {
			// Optional filters (e.g., DynamicFilter from TOP-N) may contain unserializable
			// children. Skip them rather than failing the entire filter set.
			try {
				auto filter_obj = serializer.SerializeColumnFilter(col_idx, col_name, filter);
				if (filter_obj) {
					yyjson_mut_arr_append(filter_array, filter_obj);
				}
			} catch (const InvalidInputException &) {
				continue;
			}
		} else {
			auto filter_obj = serializer.SerializeColumnFilter(col_idx, col_name, filter);
			if (filter_obj) {
				yyjson_mut_arr_append(filter_array, filter_obj);
			}
		}
	}

	// Write JSON string
	char *json_str = serializer.WriteJson(filter_array);
	if (!json_str) {
		throw IOException("Failed to serialize filters to JSON");
	}
	string filter_spec(json_str);
	free(json_str);

	// Build Arrow RecordBatch with filter_spec + value columns
	auto &values = serializer.GetValues();
	auto &value_types = serializer.GetValueTypes();

	// Build types and names: filter_spec (VARCHAR) + value columns
	vector<LogicalType> types;
	vector<string> names;
	types.push_back(LogicalType::VARCHAR);
	names.push_back("filter_spec");
	for (idx_t i = 0; i < value_types.size(); i++) {
		types.push_back(value_types[i]);
		names.push_back("_val_" + std::to_string(i));
	}

	// Create single-row DataChunk and populate
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), types);
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value(filter_spec));
	for (idx_t i = 0; i < values.size(); i++) {
		chunk.SetValue(i + 1, 0, values[i]);
	}

	// Convert to Arrow via ArrowAppender
	ClientProperties client_props = context.GetClientProperties();
	ArrowAppender appender(types, 1, client_props, ArrowTypeExtensionData::GetExtensionTypes(context, types));
	appender.Append(chunk, 0, 1, 1);
	ArrowArray arr = appender.Finalize();

	// Build Arrow schema via C API
	ArrowSchema c_schema;
	ArrowConverter::ToArrowSchema(&c_schema, types, names, client_props);

	// Import to Arrow C++ RecordBatch
	auto import_result = arrow::ImportRecordBatch(&arr, &c_schema);
	if (!import_result.ok()) {
		throw IOException("Failed to import filter RecordBatch: %s", import_result.status().ToString());
	}
	auto record_batch = import_result.ValueUnsafe();

	// Add version metadata to filter_spec field (field 0)
	// We need to rebuild the schema with the metadata
	auto filter_spec_field = record_batch->schema()->field(0);
	auto metadata = arrow::KeyValueMetadata::Make({"vgi_filter_version"}, {"1"});
	auto new_field = filter_spec_field->WithMetadata(metadata);

	// Build new schema with the updated field
	std::vector<std::shared_ptr<arrow::Field>> new_fields;
	new_fields.push_back(new_field);
	for (int i = 1; i < record_batch->schema()->num_fields(); i++) {
		new_fields.push_back(record_batch->schema()->field(i));
	}
	auto new_schema = arrow::schema(new_fields);

	// Create new RecordBatch with updated schema
	record_batch = arrow::RecordBatch::Make(new_schema, record_batch->num_rows(), record_batch->columns());

	// Serialize to Arrow IPC stream format (schema + record batch)
	auto buffer_result = arrow::io::BufferOutputStream::Create();
	if (!buffer_result.ok()) {
		throw IOException("Failed to create buffer for filter IPC: %s", buffer_result.status().ToString());
	}
	auto buffer_stream = buffer_result.ValueUnsafe();

	auto writer_result = arrow::ipc::MakeStreamWriter(buffer_stream, record_batch->schema());
	if (!writer_result.ok()) {
		throw IOException("Failed to create IPC stream writer: %s", writer_result.status().ToString());
	}
	auto writer = writer_result.ValueUnsafe();

	auto write_status = writer->WriteRecordBatch(*record_batch);
	if (!write_status.ok()) {
		throw IOException("Failed to write filter RecordBatch to IPC stream: %s", write_status.ToString());
	}

	auto close_status = writer->Close();
	if (!close_status.ok()) {
		throw IOException("Failed to close IPC stream writer: %s", close_status.ToString());
	}

	auto finish_result = buffer_stream->Finish();
	if (!finish_result.ok()) {
		throw IOException("Failed to finish filter IPC buffer: %s", finish_result.status().ToString());
	}

	auto filter_bytes = finish_result.ValueUnsafe();

	// Build one IPC buffer per join key column (each is a single-column batch).
	// Each IN filter gets its own batch, so different cardinalities are supported.
	std::vector<std::shared_ptr<arrow::Buffer>> join_keys_buffers;
	if (serializer.HasJoinKeys()) {
		auto &key_columns = serializer.GetJoinKeyColumns();

		for (auto &kc : key_columns) {
			idx_t count = kc.values->size();

			// Build single-column DataChunk
			vector<LogicalType> types = {kc.type};
			vector<string> names = {kc.column_name};
			DataChunk chunk;
			chunk.Initialize(Allocator::DefaultAllocator(), types, count);
			chunk.SetCardinality(count);
			for (idx_t row = 0; row < count; row++) {
				chunk.SetValue(0, row, (*kc.values)[row]);
			}

			// Convert to Arrow via ArrowAppender
			ArrowAppender appender(types, count, client_props,
			                       ArrowTypeExtensionData::GetExtensionTypes(context, types));
			appender.Append(chunk, 0, count, count);
			ArrowArray arr = appender.Finalize();

			ArrowSchema c_schema;
			ArrowConverter::ToArrowSchema(&c_schema, types, names, client_props);

			auto import_res = arrow::ImportRecordBatch(&arr, &c_schema);
			if (!import_res.ok()) {
				throw IOException("Failed to import join keys RecordBatch for '%s': %s",
				                  kc.column_name, import_res.status().ToString());
			}
			auto key_batch = import_res.ValueUnsafe();

			// Add version metadata
			auto metadata = arrow::KeyValueMetadata::Make({"vgi_join_keys_version"}, {"2"});
			key_batch = key_batch->ReplaceSchemaMetadata(metadata);

			// Serialize to Arrow IPC
			auto buf_result = arrow::io::BufferOutputStream::Create();
			if (!buf_result.ok()) {
				throw IOException("Failed to create buffer for join keys IPC: %s", buf_result.status().ToString());
			}
			auto buf_stream = buf_result.ValueUnsafe();

			auto writer_res = arrow::ipc::MakeStreamWriter(buf_stream, key_batch->schema());
			if (!writer_res.ok()) {
				throw IOException("Failed to create IPC writer for join keys: %s", writer_res.status().ToString());
			}
			auto writer = writer_res.ValueUnsafe();

			auto write_status = writer->WriteRecordBatch(*key_batch);
			if (!write_status.ok()) {
				throw IOException("Failed to write join keys RecordBatch: %s", write_status.ToString());
			}
			auto close_status = writer->Close();
			if (!close_status.ok()) {
				throw IOException("Failed to close join keys IPC writer: %s", close_status.ToString());
			}
			auto finish_res = buf_stream->Finish();
			if (!finish_res.ok()) {
				throw IOException("Failed to finish join keys IPC buffer: %s", finish_res.status().ToString());
			}
			join_keys_buffers.push_back(finish_res.ValueUnsafe());
		}
	}

	return {std::move(filter_bytes), std::move(join_keys_buffers)};
}

// ============================================================================
// VgiTableFunctionGlobalState::EnsureInitApplied
// ============================================================================
// Folds the deferred init RPC's result back into the gstate. Called from
// init_local (and any other code that needs `global_execution_id` /
// `primary_connection` populated). First caller pays the wait; the rest
// see `init_applied=true` and return immediately.
void VgiTableFunctionGlobalState::EnsureInitApplied() {
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
		// primary_connection is uninitialized when async init kicks off —
		// the lambda returns the connection alongside the InitResult.
		std::lock_guard<std::mutex> conn_lk(connection_mutex);
		primary_connection = std::move(applied.second);
	}
	init_applied.store(true, std::memory_order_release);
}

// ============================================================================
// Init Global Function - Performs init handshake with existing connection
// ============================================================================

unique_ptr<GlobalTableFunctionState> VgiTableFunctionInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<VgiTableFunctionBindData>();

	// Read TABLESAMPLE pushdown from DuckDB optimizer (SYSTEM_SAMPLE percentage only)
	if (input.sample_options) {
		double pct = input.sample_options->sample_size.GetValue<double>();
		int64_t seed = input.sample_options->seed.IsValid()
		    ? static_cast<int64_t>(input.sample_options->seed.GetIndex()) : -1;
		bind_data.table_sample_hint = TableSampleHint{pct, seed};
	}

	// Extract projection IDs from input.column_ids for the worker.
	// Only send projection IDs when the function supports projection pushdown.
	// When unsupported, send empty list (meaning "all columns" in the protocol).
	std::vector<int32_t> projection_ids;
	if (bind_data.projection_pushdown) {
		projection_ids.reserve(input.column_ids.size());
		for (auto col_id : input.column_ids) {
			if (col_id == COLUMN_IDENTIFIER_ROW_ID && bind_data.rowid_worker_col_index >= 0) {
				// Map rowid request to the actual worker column index
				projection_ids.push_back(static_cast<int32_t>(bind_data.rowid_worker_col_index));
			} else if (bind_data.rowid_worker_col_index >= 0 &&
			           col_id != COLUMN_IDENTIFIER_ROW_ID &&
			           col_id >= static_cast<idx_t>(bind_data.rowid_worker_col_index)) {
				// Shift to account for the excluded row_id column in worker's schema
				projection_ids.push_back(static_cast<int32_t>(col_id + 1));
			} else {
				projection_ids.push_back(static_cast<int32_t>(col_id));
			}
		}
	}

	// Acquire a connection for the init/scan phase. The planner-phase BindResult
	// is already cached on bind_data; PerformInit's payload carries it inline,
	// so no second on-wire bind is needed. AcquireConnectionForInit pool-hits
	// when possible (subprocess) or constructs a fresh HTTP connection.
	FunctionConnectionParams acquire_params;
	acquire_params.attach_params = bind_data.attach_params;
	acquire_params.attach_id = bind_data.attach_id;
	acquire_params.function_name = bind_data.function_name;
	acquire_params.arguments = bind_data.arguments;
	acquire_params.transaction_id = bind_data.transaction_id;
	acquire_params.settings = bind_data.settings;
	acquire_params.required_secrets = bind_data.required_secrets;
	acquire_params.phase = "init_global";
	acquire_params.function_type = "TABLE";
	auto acquired = AcquireConnectionForInit(context, acquire_params);
	auto connection = std::move(acquired.connection);
	const auto &bind_result = bind_data.bind_result;

	// Serialize the filters (returns empty if no filters or if serialization fails)
	SerializedFilters serialized_filters;
	try {
		serialized_filters =
		    VgiSerializeFilters(context, input.column_ids, input.filters, bind_data.all_column_names, bind_data.worker_path());
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
	} catch (const InvalidInputException &e) {
		// Filter contains unsupported types - skip pushdown, let DuckDB filter locally
		VGI_LOG(context, "table_function.filter_pushdown_skipped",
		        {{"function_name", bind_data.function_name},
		         {"reason", e.what()}});
	}
	auto &filter_bytes = serialized_filters.filter_bytes;
	auto &join_keys_buffers = serialized_filters.join_keys_buffers;

	// Capture DynamicFilter references for tick-based pushdown.
	// Walk the filter set looking for OptionalFilter → DynamicFilter (from Top-N)
	// or OptionalFilter → ConjunctionOrFilter → DynamicFilter (nulls_first case).
	// Also traverse into ConjunctionAndFilter children, since DuckDB may combine
	// a regular filter with an OptionalFilter(DynamicFilter) on the same column.
	vector<VgiDynamicFilterInfo> dynamic_filters;
	if (input.filters) {
		// Helper: try to capture a DynamicFilter from an OptionalFilter entry
		auto try_capture_from_optional = [&](idx_t original_col_idx, const string &col_name,
		                                     const OptionalFilter &opt) {
			if (!opt.child_filter) {
				return;
			}
			if (opt.child_filter->filter_type == TableFilterType::DYNAMIC_FILTER) {
				auto &df = opt.child_filter->Cast<DynamicFilter>();
				if (df.filter_data && df.filter_data->filter) {
					VgiDynamicFilterInfo info;
					info.filter_data = df.filter_data;
					info.column_index = original_col_idx;
					info.column_name = col_name;
					info.comparison_type = df.filter_data->filter->comparison_type;
					info.nulls_first = false;
					dynamic_filters.push_back(std::move(info));
				}
			} else if (opt.child_filter->filter_type == TableFilterType::CONJUNCTION_OR) {
				// NULLS_FIRST case: OptionalFilter(ConjunctionOr(IsNull, DynamicFilter))
				auto &conj = opt.child_filter->Cast<ConjunctionOrFilter>();
				for (auto &child : conj.child_filters) {
					if (child->filter_type == TableFilterType::DYNAMIC_FILTER) {
						auto &df = child->Cast<DynamicFilter>();
						if (df.filter_data && df.filter_data->filter) {
							VgiDynamicFilterInfo info;
							info.filter_data = df.filter_data;
							info.column_index = original_col_idx;
							info.column_name = col_name;
							info.comparison_type = df.filter_data->filter->comparison_type;
							info.nulls_first = true;
							dynamic_filters.push_back(std::move(info));
						}
					}
				}
			}
		};

		for (auto &entry : input.filters->filters) {
			auto col_idx = entry.first;
			auto &filter = *entry.second;

			idx_t original_col_idx = col_idx < input.column_ids.size() ? input.column_ids[col_idx] : col_idx;
			string col_name = original_col_idx < bind_data.all_column_names.size()
			                      ? bind_data.all_column_names[original_col_idx]
			                      : std::to_string(original_col_idx);

			if (filter.filter_type == TableFilterType::OPTIONAL_FILTER) {
				try_capture_from_optional(original_col_idx, col_name, filter.Cast<OptionalFilter>());
			} else if (filter.filter_type == TableFilterType::CONJUNCTION_AND) {
				// DuckDB may combine ConstantFilter + OptionalFilter(DynamicFilter)
				// on the same column into a ConjunctionAndFilter
				auto &conj = filter.Cast<ConjunctionAndFilter>();
				for (auto &child : conj.child_filters) {
					if (child->filter_type == TableFilterType::OPTIONAL_FILTER) {
						try_capture_from_optional(original_col_idx, col_name, child->Cast<OptionalFilter>());
					}
				}
			}
		}
	}

	if (!dynamic_filters.empty()) {
		VGI_LOG(context, "table_function.dynamic_filters_captured",
		        {{"function_name", bind_data.function_name},
		         {"count", std::to_string(dynamic_filters.size())}});
	}

	// Create shared tick filter state if we have dynamic filters.
	// This is shared between the global state (which updates it from DynamicFilterData)
	// and the connection (which reads it when building tick batches).
	shared_ptr<TickFilterState> tick_filter_state;
	if (!dynamic_filters.empty()) {
		tick_filter_state = make_shared_ptr<TickFilterState>();
		// If we have static filters, pre-populate with those (they'll be merged with dynamic on each update)
		if (filter_bytes) {
			// Base64-encode the static filter bytes for the initial tick state
			auto encoded = Blob::ToBase64(string_t(reinterpret_cast<const char *>(filter_bytes->data()),
			                                       static_cast<idx_t>(filter_bytes->size())));
			tick_filter_state->encoded_filters = encoded;
			tick_filter_state->has_filters = true;
		}
		connection->SetTickFilterState(tick_filter_state);
	}

	// Build the gstate up front (everything that doesn't depend on the init
	// RPC result) and defer PerformInit to a background thread. The
	// VGI_SYNC_INIT_GLOBAL env var force-disables this fast-path for
	// regression bisecting.
	auto global_state = make_uniq<VgiTableFunctionGlobalState>();
	global_state->client_context_for_explain = &context;
	global_state->dynamic_filters = std::move(dynamic_filters);
	global_state->static_filter_bytes = filter_bytes;
	global_state->tick_filter_state = tick_filter_state;
	global_state->projection_ids = projection_ids;
	global_state->join_keys_buffers = join_keys_buffers;
	global_state->order_by_hint = bind_data.order_by_hint;
	global_state->table_sample_hint = bind_data.table_sample_hint;
	// Provisional max_processes — actual value is folded in by
	// EnsureInitApplied once the init future resolves. Setting 1 here
	// matches the conservative default; for fan-outs of small metadata
	// reads (Ducklake bind-time scan plan) it has no observable effect.
	global_state->max_processes = 1;

	const char *sync_env = std::getenv("VGI_SYNC_INIT_GLOBAL");
	const bool force_sync = sync_env && *sync_env && std::string(sync_env) != "0";

	auto perform_init_with_retry =
	    [acquire_params, &context_ref = context,
	     bind_result, projection_ids, filter_bytes, join_keys_buffers,
	     order_by_hint = bind_data.order_by_hint,
	     table_sample_hint = bind_data.table_sample_hint,
	     worker_path = bind_data.worker_path(),
	     function_name = bind_data.function_name](
	        std::unique_ptr<IFunctionConnection> conn,
	        bool from_pool) mutable
	    -> std::pair<InitResult, std::unique_ptr<IFunctionConnection>> {
#ifdef __EMSCRIPTEN__
#endif
		auto run = [&](IFunctionConnection &c) {
#ifdef __EMSCRIPTEN__
#endif
			auto rr = c.PerformInit(bind_result, projection_ids, filter_bytes, join_keys_buffers,
			                        "", order_by_hint, table_sample_hint);
#ifdef __EMSCRIPTEN__
#endif
			return rr;
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

	if (force_sync) {
		auto applied = perform_init_with_retry(std::move(connection), acquired.from_pool);
		global_state->global_execution_id = std::move(applied.first.execution_id);
		global_state->max_processes = static_cast<idx_t>(applied.first.max_workers);
		global_state->primary_connection = std::move(applied.second);
		global_state->init_applied.store(true, std::memory_order_release);
	} else {
		// Hot path: kick off PerformInit on a background thread. The lambda
		// owns the connection until init returns (then EnsureInitApplied
		// folds the result + connection back into gstate). Multiple
		// pipelines hit this path back-to-back during scheduling and run
		// their RPCs concurrently instead of serializing on the main
		// thread.
#ifdef __EMSCRIPTEN__
		// Dispatch onto the pre-spawned VgiWasmAsyncPool. pthread_create
		// after side modules are dlopen'd is unreliable in MAIN_MODULE=1
		// builds, so std::async(launch::async) would crash; the pool's
		// workers were spawned once at extension load.
		global_state->pending_init = duckdb::vgi::VgiWasmAsyncPool::Instance().Submit(
		    [run = std::move(perform_init_with_retry),
		     conn = std::move(connection),
		     from_pool = acquired.from_pool]() mutable {
			    return run(std::move(conn), from_pool);
		    });
#else
		global_state->pending_init = std::async(std::launch::async,
		                                          [run = std::move(perform_init_with_retry),
		                                           conn = std::move(connection),
		                                           from_pool = acquired.from_pool]() mutable {
			                                          return run(std::move(conn), from_pool);
		                                          });
#endif
	}

	{
		vector<pair<string, string>> fields;
		fields.emplace_back("worker_path", bind_data.worker_path());
		fields.emplace_back("function_name", bind_data.function_name);
		fields.emplace_back("max_processes", std::to_string(global_state->max_processes));
		fields.emplace_back("num_projection_columns", std::to_string(projection_ids.size()));
		fields.emplace_back("init_mode", force_sync ? "sync" : "async");
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

// ============================================================================
// Init Local Function - Create local state for scanning
// ============================================================================

unique_ptr<LocalTableFunctionState> VgiTableFunctionInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                              GlobalTableFunctionState *global_state_p) {
	auto &bind_data = input.bind_data->Cast<VgiTableFunctionBindData>();
	auto &global_state = global_state_p->Cast<VgiTableFunctionGlobalState>();

	// Block on the deferred init RPC if init_global hand-off was async.
	// This is the latest point we can wait — running on a task-scheduler
	// worker thread, not the main thread that schedules events. Multiple
	// pipelines reach here concurrently so their RPCs effectively ran in
	// parallel.
	global_state.EnsureInitApplied();

	auto current_chunk = make_uniq<ArrowArrayWrapper>();
	auto local_state = make_uniq<VgiTableFunctionLocalState>(std::move(current_chunk), context.client,
	                                                         bind_data.attach_params);

	// Set column_ids for ArrowToDuckDB projection support if function supports it
	// This tells ArrowToDuckDB which columns to extract from the Arrow arrays
	if (bind_data.projection_pushdown) {
		local_state->column_ids = input.column_ids;
		// When a rowid column exists, remap column_ids to match the worker's full schema indices.
		// COLUMN_IDENTIFIER_ROW_ID maps to the actual worker column index.
		// Physical columns at or after the rowid position shift by +1.
		// This allows ArrowToDuckDB to look up the correct type info in arrow_convert_data
		// (which was built from the full worker schema including the rowid column).
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

	// Try to claim the primary connection from global_state (thread-safe check-and-move)
	std::unique_ptr<IFunctionConnection> primary_connection;
	{
		std::lock_guard<std::mutex> lock(global_state.connection_mutex);
		if (global_state.primary_connection) {
			primary_connection = std::move(global_state.primary_connection);
		}
	}

	if (primary_connection) {
		// Primary worker: use connection from bind phase
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
		// Secondary worker: acquire a connection without firing a redundant
		// bind RPC. The cached planner-phase BindResult on bind_data is what
		// PerformInit needs; the global_execution_id is threaded via
		// FunctionConnectionParams and PerformInit places it in the
		// InitRequest. Stale-pool detection lives on the init RPC.
		FunctionConnectionParams params;
		params.attach_params = bind_data.attach_params;
		params.attach_id = bind_data.attach_id;
		params.function_name = bind_data.function_name;
		params.arguments = bind_data.arguments;
		params.transaction_id = bind_data.transaction_id;
		params.global_execution_id = global_state.global_execution_id;
		params.settings = bind_data.settings;
		params.required_secrets = bind_data.required_secrets;
		params.phase = "init_local_secondary";
		params.function_type = "TABLE";

		auto acquired = AcquireConnectionForInit(context.client, params);
		local_state->prefetch_slot_->connection = std::move(acquired.connection);
		const auto &secondary_bind_result = bind_data.bind_result;

		// Secondary workers must receive the same projection / filter / hint
		// pushdown info as the primary so they emit batches with a matching
		// shape. Without this, secondary workers emit the full unprojected
		// schema while the primary emits projected batches, and ArrowToDuckDB
		// (which assumes projected layout when projection_pushdown=true)
		// reads the wrong column positions from the unprojected secondary
		// batches.
		auto secondary_init = [&]() {
			if (bind_data.projection_pushdown) {
				local_state->connection()->SetTickFilterState(global_state.tick_filter_state);
				local_state->connection()->PerformInit(
				    secondary_bind_result,
				    global_state.projection_ids, global_state.static_filter_bytes,
				    global_state.join_keys_buffers, "", global_state.order_by_hint,
				    global_state.table_sample_hint);
			} else {
				local_state->connection()->PerformInit(secondary_bind_result);
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

	return local_state;
}

// ============================================================================
// Helper: Get next batch from worker and convert to Arrow C ABI
// ============================================================================

//! Update the TickFilterState with current DynamicFilter values.
//! Called before each ReadDataBatch to ensure the tick carries the latest filter.
static void UpdateDynamicFilterState(VgiTableFunctionGlobalState &global_state, ClientContext &context,
                                     const VgiTableFunctionBindData &bind_data) {
	if (global_state.dynamic_filters.empty() || !global_state.tick_filter_state) {
		return;
	}

	// Build a merged TableFilterSet with static + current dynamic filters
	TableFilterSet merged;

	// Add all static filters (from init-time serialization)
	// We need to re-create them from the original input.filters that were serialized.
	// For now, we serialize just the dynamic filters as ConstantFilters.
	// The static filters were already sent at init time — the worker has them.

	bool any_initialized = false;
	for (auto &df : global_state.dynamic_filters) {
		if (!df.filter_data->initialized.load()) {
			continue;
		}
		any_initialized = true;

		// Read the current value under lock
		lock_guard<mutex> l(df.filter_data->lock);
		auto &const_filter = *df.filter_data->filter;
		auto filter_copy = make_uniq<ConstantFilter>(const_filter.comparison_type, const_filter.constant);

		unique_ptr<TableFilter> pushed;
		if (df.nulls_first) {
			auto or_filter = make_uniq<ConjunctionOrFilter>();
			or_filter->child_filters.push_back(make_uniq<IsNullFilter>());
			or_filter->child_filters.push_back(std::move(filter_copy));
			pushed = std::move(or_filter);
		} else {
			pushed = std::move(filter_copy);
		}

		merged.filters[df.column_index] = std::move(pushed);
	}

	if (!any_initialized) {
		// No dynamic filters initialized yet (or Reset was called for recursive CTEs).
		// Clear any stale filter state from a previous iteration.
		lock_guard<mutex> l(global_state.tick_filter_state->lock);
		if (global_state.tick_filter_state->has_filters) {
			global_state.tick_filter_state->encoded_filters.clear();
			global_state.tick_filter_state->has_filters = false;
		}
		return;
	}

	// Serialize the merged filters
	try {
		auto serialized = VgiSerializeFilters(context, {}, &merged, bind_data.all_column_names, bind_data.worker_path());
		if (serialized.filter_bytes) {
			auto &fb = serialized.filter_bytes;
			auto encoded = Blob::ToBase64(string_t(reinterpret_cast<const char *>(fb->data()),
			                                       static_cast<idx_t>(fb->size())));
			lock_guard<mutex> l(global_state.tick_filter_state->lock);
			global_state.tick_filter_state->encoded_filters = encoded;
			global_state.tick_filter_state->has_filters = true;
		}
	} catch (...) {
		// Serialization failure is not fatal — the filter is optional
	}
}

//! Install arrow_batch as the active chunk. Returns false on EOS (null batch).
//! Caller is responsible for having already obtained arrow_batch either
//! synchronously (via ReadDataBatch) or from the prefetch slot.
static bool InstallBatch(ClientContext &context, const VgiTableFunctionBindData &bind_data,
                         VgiTableFunctionLocalState &local_state,
                         std::shared_ptr<arrow::RecordBatch> arrow_batch) {
	if (!arrow_batch) {
		local_state.done = true;
		{
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

	auto chunk = make_uniq<ArrowArrayWrapper>();
	ExportRecordBatch(arrow_batch, *chunk);

	local_state.chunk = shared_ptr<ArrowArrayWrapper>(chunk.release());
	local_state.chunk_offset = 0;
	// Reset() clears owned_data in array_states, which is REQUIRED so that ArrowToDuckDB
	// will update owned_data to point to the new chunk. Without this, owned_data still
	// points to the previous chunk, and when that chunk is released, the data becomes invalid.
	local_state.Reset();

	{
		auto &conn = *local_state.connection();
		vector<pair<string, string>> fields;
		fields.emplace_back("conn", conn.GetConnIdHex());
		fields.emplace_back("worker_path", bind_data.worker_path());
		AppendSubprocessPidField(fields, conn);
		fields.emplace_back("function_name", bind_data.function_name);
		fields.emplace_back("batch_rows", std::to_string(arrow_batch->num_rows()));
		VGI_LOG(context, "table_function.batch_received", fields);
	}

	return true;
}

static bool GetNextBatch(ClientContext &context, const VgiTableFunctionBindData &bind_data,
                         VgiTableFunctionGlobalState &global_state,
                         VgiTableFunctionLocalState &local_state) {
	if (local_state.done) {
		return false;
	}
	// Read from the worker, skipping empty non-log batches. Log batches
	// (vgi_rpc.log_* metadata) are already consumed inside ReadDataBatch via
	// HandleBatchLogMessage, so any 0-row batch surfaced here is a real empty
	// response from the worker — pointless for producer-mode table functions.
	while (true) {
		// Update dynamic filter state before each tick is sent
		UpdateDynamicFilterState(global_state, context, bind_data);
		auto arrow_batch = local_state.connection()->ReadDataBatch();
		if (!arrow_batch) {
			return InstallBatch(context, bind_data, local_state, nullptr);
		}
		if (arrow_batch->num_rows() == 0) {
			auto &conn = *local_state.connection();
			vector<pair<string, string>> fields;
			fields.emplace_back("conn", conn.GetConnIdHex());
			fields.emplace_back("worker_path", bind_data.worker_path());
			AppendSubprocessPidField(fields, conn);
			fields.emplace_back("function_name", bind_data.function_name);
			VGI_LOG(context, "table_function.batch_empty_skipped", fields);
			continue;
		}
		return InstallBatch(context, bind_data, local_state, std::move(arrow_batch));
	}
}

// ============================================================================
// Async Prefetch Helpers
// ============================================================================

void VgiPrefetchTask::Execute() {
	// If the local_state was torn down (query cancellation) before we ran,
	// the cancelled flag is set. We skip the RPC and let the slot — which we
	// still hold via shared_ptr — go out of scope normally, releasing the
	// connection at the end. We leave the state as IN_FLIGHT since nothing
	// is going to read it.
	if (slot_->cancelled.load(std::memory_order_acquire)) {
		return;
	}
	try {
		// Skip 0-row batches here so the consumer always sees either EOS or a
		// non-empty batch, matching the sync path's invariant.
		std::shared_ptr<arrow::RecordBatch> batch;
		while (true) {
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

static void LaunchPrefetch(TableFunctionInput &input, VgiTableFunctionLocalState &local_state) {
	local_state.prefetch_slot_->state.store(PrefetchState::IN_FLIGHT);
	vector<unique_ptr<AsyncTask>> tasks;
	tasks.push_back(make_uniq<VgiPrefetchTask>(local_state.prefetch_slot_));
	input.async_result = AsyncResult(std::move(tasks));
}

static void ConsumePrefetchedBatch(ClientContext &context, const VgiTableFunctionBindData &bind_data,
                                   VgiTableFunctionLocalState &local_state) {
	auto arrow_batch = std::move(local_state.prefetch_slot_->batch);
	local_state.prefetch_slot_->batch.reset();
	local_state.prefetch_slot_->state.store(PrefetchState::IDLE);
	InstallBatch(context, bind_data, local_state, std::move(arrow_batch));
}

// ============================================================================
// Helper: Convert current batch slice to DuckDB output
// ============================================================================

static void ConvertCurrentBatch(const VgiTableFunctionBindData &bind_data,
                                VgiTableFunctionGlobalState &global_state,
                                VgiTableFunctionLocalState &local_state, DataChunk &output) {
	idx_t output_size =
	    MinValue<idx_t>(STANDARD_VECTOR_SIZE, local_state.chunk->arrow_array.length - local_state.chunk_offset);

	output.SetCardinality(output_size);

	if (output_size > 0) {
		idx_t rowid_col = (!bind_data.projection_pushdown && bind_data.rowid_worker_col_index >= 0)
		                      ? static_cast<idx_t>(bind_data.rowid_worker_col_index)
		                      : COLUMN_IDENTIFIER_ROW_ID;
		ArrowTableFunction::ArrowToDuckDB(local_state, bind_data.arrow_table.GetColumns(), output,
		                                  bind_data.projection_pushdown, rowid_col);
	}

	local_state.chunk_offset += output.size();
	global_state.rows_read.fetch_add(output.size(), std::memory_order_relaxed);
	output.Verify();
}

//! Check whether the current batch has been fully consumed
static bool BatchExhausted(const VgiTableFunctionLocalState &local_state) {
	return !local_state.chunk || !local_state.chunk->arrow_array.release ||
	       local_state.chunk_offset >= static_cast<idx_t>(local_state.chunk->arrow_array.length);
}

// ============================================================================
// Scan Function
// ============================================================================

void VgiTableFunctionScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<VgiTableFunctionBindData>();
	auto &global_state = input.global_state->Cast<VgiTableFunctionGlobalState>();
	auto &local_state = input.local_state->Cast<VgiTableFunctionLocalState>();

	// Determine whether async prefetch is enabled for this scan
	bool is_async = (input.results_execution_mode == AsyncResultsExecutionMode::TASK_EXECUTOR);
	if (is_async) {
		Value async_val;
		if (context.TryGetCurrentSetting("vgi_async_prefetch", async_val)) {
			is_async = async_val.GetValue<bool>();
		}
	}

	// Fast path: current batch still has rows to consume
	if (!BatchExhausted(local_state)) {
		ConvertCurrentBatch(bind_data, global_state, local_state, output);
		return;
	}

	// Need a new batch
	if (local_state.done) {
		output.SetCardinality(0);
		return;
	}

	// Synchronous fallback: inline blocking I/O (original behavior).
	// GetNextBatch now guarantees a non-empty batch on success, so a single
	// call is sufficient — it internally loops past any empty worker batches.
	if (!is_async) {
		if (!GetNextBatch(context, bind_data, global_state, local_state)) {
			output.SetCardinality(0);
			return;
		}
		ConvertCurrentBatch(bind_data, global_state, local_state, output);
		return;
	}

	// --- Async path ---
	auto state = local_state.prefetch_slot_->state.load();

	if (state == PrefetchState::ERROR) {
		local_state.prefetch_slot_->state.store(PrefetchState::IDLE);
		std::rethrow_exception(local_state.prefetch_slot_->exception);
	}

	if (state == PrefetchState::READY) {
		ConsumePrefetchedBatch(context, bind_data, local_state);
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
			// First batch: fetch synchronously to avoid an extra BLOCKED round-trip.
			// GetNextBatch loops past empty batches internally.
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
		// Update dynamic filter state before the prefetch task sends a tick
		UpdateDynamicFilterState(global_state, context, bind_data);
		// Subsequent batches: launch prefetch and return BLOCKED
		LaunchPrefetch(input, local_state);
		output.SetCardinality(0);
		return;
	}

	// IN_FLIGHT: should be impossible — DuckDB won't call scan while task is running
	D_ASSERT(state != PrefetchState::IN_FLIGHT);
	throw InternalException("VgiTableFunctionScan: unexpected prefetch state IN_FLIGHT");
}

// ============================================================================
// Cardinality Function - Returns row count estimate from bind
// ============================================================================

unique_ptr<NodeStatistics> VgiTableFunctionCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<VgiTableFunctionBindData>();

	// Lazy fetch: make a single table_function_cardinality RPC call on first invocation
	if (!bind_data.cardinality_fetched && !bind_data.bind_result.bind_request_bytes.empty()) {
		bind_data.cardinality_fetched = true;
		try {
			auto rpc_params = bind_data.attach_params ? bind_data.attach_params
			    : std::make_shared<VgiAttachParameters>(bind_data.worker_path(), "", bind_data.worker_debug(), bind_data.use_pool());
			CatalogRpcContext rpc_ctx{rpc_params, bind_data.attach_id, bind_data.transaction_id};
			auto result = InvokeTableFunctionCardinality(rpc_ctx, bind_data.bind_result.bind_request_bytes,
			                                             bind_data.bind_result.opaque_data, context);
			bind_data.cardinality_estimate = result.estimate;
			bind_data.cardinality_max = result.max;
		} catch (const std::exception &e) {
			// Not critical — continue with unknown cardinality
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
		// NodeStatistics(idx_t estimated, idx_t max) sets both flags.
		return make_uniq<NodeStatistics>(static_cast<idx_t>(bind_data.cardinality_estimate),
		                                  static_cast<idx_t>(bind_data.cardinality_max));
	}
	if (have_est) {
		return make_uniq<NodeStatistics>(static_cast<idx_t>(bind_data.cardinality_estimate));
	}
	if (have_max) {
		// max-only: build NodeStatistics manually so the optimizer can use the
		// upper bound even without a point estimate.
		auto stats = make_uniq<NodeStatistics>();
		stats->has_max_cardinality = true;
		stats->max_cardinality = static_cast<idx_t>(bind_data.cardinality_max);
		return stats;
	}
	return make_uniq<NodeStatistics>();
}

// ============================================================================
// Progress Function - Returns scan progress as percentage
// ============================================================================

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

	// No estimate available
	return -1.0;
}

// ============================================================================
// ToString Function - Returns info for EXPLAIN output
// ============================================================================

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

// ============================================================================
// DynamicToString Function - Returns post-execution diagnostics for EXPLAIN ANALYZE
// ============================================================================
//
// DuckDB calls this once per parallel scan thread inside
// `OperatorProfiler::FinishSource`. We always emit the intrinsic keys
// (`Worker`, `Function`, `Rows Read`, `Threads`) cheap from gstate, and
// additionally issue a unary RPC asking the worker to surface user-defined
// diagnostics under the global execution id. The RPC goes through the worker
// pool — same path `cardinality` and `statistics` use — so we don't have to
// multiplex on the per-stream `IFunctionConnection`.
//
// DuckDB merges per-thread maps with last-write-wins semantics, so doing this
// on every thread is fine: every call returns the same intrinsic values, and
// the user keys reflect whatever the user persisted up to that thread's
// FinishSource (the *last* finisher will have the most complete view).

InsertionOrderPreservingMap<string> VgiTableFunctionDynamicToString(TableFunctionDynamicToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	if (!input.bind_data || !input.global_state) {
		return result;
	}
	auto &bind_data = input.bind_data->Cast<VgiTableFunctionBindData>();
	auto &global_state = input.global_state->Cast<VgiTableFunctionGlobalState>();

	// Intrinsic keys, cheap and always available. ``Rows Read`` and
	// ``Threads`` are deliberately omitted: DuckDB already prints the
	// operator's row count in the profile box ("N rows"), and the
	// worker-advertised ``max_workers`` is an upper bound rather than the
	// actual concurrency DuckDB used — both add noise without insight.
	// Workers that want a specific row count or thread/worker view can
	// return one from their own ``dynamic_to_string``.
	result["Worker"] = bind_data.worker_path();
	result["Function"] = bind_data.function_name;

	// User-defined diagnostics via worker RPC. Skip if we don't have enough
	// context to make the call — happens for direct vgi_table_function() invocations
	// before bind has populated bind_request_bytes, or if InitGlobal wasn't
	// reached (cached ClientContext is null).
	if (bind_data.bind_result.bind_request_bytes.empty() || global_state.global_execution_id.empty() ||
	    global_state.client_context_for_explain == nullptr) {
		return result;
	}

	try {
		auto rpc_params = bind_data.attach_params
		    ? bind_data.attach_params
		    : std::make_shared<VgiAttachParameters>(bind_data.worker_path(), "",
		                                            bind_data.worker_debug(), bind_data.use_pool());
		CatalogRpcContext rpc_ctx{rpc_params, bind_data.attach_id, bind_data.transaction_id};
		auto user_map = InvokeTableFunctionDynamicToString(
		    rpc_ctx, bind_data.bind_result.bind_request_bytes, bind_data.bind_result.opaque_data,
		    global_state.global_execution_id, *global_state.client_context_for_explain);
		// Merge user keys after intrinsics — user can override an intrinsic by
		// returning the same key (DuckDB's map semantics for duplicate keys
		// is "replace existing", which is what we want here).
		for (const auto &it : user_map) {
			result[it.first] = it.second;
		}
	} catch (const std::exception &e) {
		// Best-effort: any failure surfaces only the intrinsic keys.
		VGI_LOG(*global_state.client_context_for_explain, "table_function.dynamic_to_string_error",
		        {{"worker_path", bind_data.worker_path()},
		         {"function_name", bind_data.function_name},
		         {"error", e.what()}});
	}
	return result;
}

// ============================================================================
// Virtual Column Callbacks for Row ID Support
// ============================================================================

BindInfo VgiTableScanGetBindInfo(const optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<VgiTableFunctionBindData>();
	if (bind_data.table_entry) {
		return BindInfo(const_cast<TableCatalogEntry &>(*bind_data.table_entry));
	}
	return BindInfo(ScanType::EXTERNAL);
}

virtual_column_map_t VgiTableScanGetVirtualColumns(ClientContext &context, optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<VgiTableFunctionBindData>();
	if (bind_data.rowid_worker_col_index < 0) {
		return {};
	}
	virtual_column_map_t result;
	result.insert({COLUMN_IDENTIFIER_ROW_ID, TableColumn("rowid", bind_data.rowid_type)});
	return result;
}

vector<column_t> VgiTableScanGetRowIdColumns(ClientContext &context, optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<VgiTableFunctionBindData>();
	if (bind_data.rowid_worker_col_index < 0) {
		return {};
	}
	return {COLUMN_IDENTIFIER_ROW_ID};
}

// ============================================================================
// set_scan_order callback — captures ORDER BY + LIMIT hint from optimizer
// ============================================================================

void VgiSetScanOrder(unique_ptr<RowGroupOrderOptions> order_options, optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<VgiTableFunctionBindData>();

	// Bounds-check column index against known column names
	auto col_idx = order_options->column_idx.GetPrimaryIndex();
	if (col_idx >= bind_data.all_column_names.size()) {
		return;
	}

	// Map OrderType to string
	std::string direction;
	switch (order_options->order_type) {
	case OrderType::ASCENDING:
		direction = "ASC";
		break;
	case OrderType::DESCENDING:
		direction = "DESC";
		break;
	default:
		// ORDER_DEFAULT or INVALID — skip (planner should resolve before optimizer)
		return;
	}

	// Map OrderByNullType to string
	std::string null_order;
	switch (order_options->null_order) {
	case OrderByNullType::NULLS_FIRST:
		null_order = "NULLS_FIRST";
		break;
	case OrderByNullType::NULLS_LAST:
		null_order = "NULLS_LAST";
		break;
	case OrderByNullType::ORDER_DEFAULT:
		// DuckDB default is NULLS_LAST unconditionally
		null_order = "NULLS_LAST";
		break;
	default:
		return;
	}

	// Extract row_limit (combined limit+offset, or -1 if invalid)
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

// ============================================================================
// Statistics Callback — column stats per column
// ============================================================================
//
// Two sources, in order of preference:
//   1. Catalog-declared stats via VgiTableEntry::GetStatistics (table_entry path).
//      These are cached on the table entry and shared across queries.
//   2. Function-level stats via table_function_statistics RPC (direct path). These
//      are cached on the bind data for the lifetime of the query.
//
// For catalog-backed scans, the entry path is tried first. If it returns null
// (e.g. `supports_column_statistics=False`, or the column just isn't declared
// in the catalog stats), we fall back to asking the scan function for its own
// stats. This lets a catalog-registered table declare "my rows come from
// sequence(123456)" without also having to repeat the sequence's stats — the
// function computes them from its bind arguments.

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
			CatalogRpcContext rpc_ctx{rpc_params, bind_data.attach_id, bind_data.transaction_id};
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
		// When the catalog inlined column_statistics on TableInfo, the entry
		// path is authoritative — a per-column null means "no stats for this
		// column", not "ask the function". Skip the function-level fallback
		// to avoid one table_function_statistics RPC per scan.
		auto *vgi_entry = dynamic_cast<VgiTableEntry *>(&entry);
		if (vgi_entry && vgi_entry->GetTableInfo().column_statistics.has_value()) {
			return nullptr;
		}
		// Fall through to function-level stats if the catalog declined to answer.
	}

	if (column_index >= bind_data.all_column_names.size()) {
		return nullptr;
	}
	return FetchFunctionStatistics(context, bind_data, bind_data.all_column_names[column_index]);
}

} // namespace vgi
} // namespace duckdb
