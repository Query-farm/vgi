// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_table_function_impl.hpp"
#include "vgi_platform.hpp" // VGI_ASYNC_INIT_ENABLED
#ifdef __EMSCRIPTEN__
#include <pthread.h>
#include <unistd.h>
#include "vgi_wasm_async_pool.hpp"
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
#include "vgi_exception.hpp"
#include "vgi_extension.hpp"
#include "vgi_logging.hpp"
#include "vgi_oauth.hpp" // ComputeCatalogIdentityFingerprint (cache identity scoping)
#include "vgi_protocol.hpp"
#include "vgi_worker_pool.hpp"

#include "arrow/util/byte_size.h" // TotalBufferSize (batch-shape stats)

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
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
			// Capture the rowid field name BEFORE erasing it — a pushed filter on
			// the rowid virtual column (COLUMN_IDENTIFIER_ROW_ID) must carry this
			// name so the worker can match/apply it. all_column_names below has
			// the rowid removed, so indexing it by rowid_worker_col_index would
			// resolve to the wrong (shifted) column.
			bind_data.rowid_column_name = names[idx];
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

unique_ptr<FunctionData> VgiTableFunctionBindData::Copy() const {
	auto result = make_uniq<VgiTableFunctionBindData>();

	// Worker identification + invocation inputs (shared_ptr / value copies).
	result->attach_params = attach_params;
	result->attach_opaque_data = attach_opaque_data;
	result->transaction_opaque_data = transaction_opaque_data;
	result->function_name = function_name;
	result->schema_name = schema_name;
	result->arguments = arguments;
	result->settings = settings;
	result->required_secrets = required_secrets;

	// Arrow conversion state. arrow_table holds shared_ptr<ArrowType> entries
	// that are immutable and self-contained, so copy-by-value is safe and needs
	// no ClientContext. The C-ABI export is rebuilt from the (copyable) arrow
	// schema so the clone owns its own ArrowSchema lifetime.
	result->arrow_table = arrow_table;
	if (bind_result.output_schema) {
		ExportSchema(bind_result.output_schema, result->c_schema);
	}

	// Execution hints + capability flags.
	result->max_processes = max_processes;
	result->fixed_order = fixed_order;
	result->supports_batch_index = supports_batch_index;
	result->partition_kind = partition_kind;
	result->partition_column_indices = partition_column_indices;
	result->cardinality_estimate = cardinality_estimate;
	result->cardinality_max = cardinality_max;
	result->cardinality_fetched = cardinality_fetched;
	result->projection_pushdown = projection_pushdown;
	result->supported_expression_filters = supported_expression_filters;

	result->all_column_names = all_column_names;
	result->all_column_types = all_column_types;
	result->table_entry = table_entry;
	result->rowid_worker_col_index = rowid_worker_col_index;
	result->rowid_type = rowid_type;
	result->rowid_column_name = rowid_column_name;
	result->bind_result = bind_result;
	// Base TableFunctionData::column_ids — the VGI bind path leaves this empty
	// (column ids live on the local state from input.column_ids, not here), but
	// copy it for robustness so a future writer can't silently lose it on clone.
	result->column_ids = column_ids;

	// statistics_mutex / statistics_cache / statistics_fetched are deliberately
	// left fresh — the clone re-fetches per-column stats lazily on demand.

	// order_by_hint / table_sample_hint are intentionally NOT copied. They are
	// per-plan-node optimizer hints (set by RowGroupPruner / SamplingPushdown
	// for the original scan position). The late-materialization clone is a
	// distinct plan node — propagating a stale ORDER BY + LIMIT hint onto it
	// would wrongly cap the cloned scan's row output.

	result->at_unit = at_unit;
	result->at_value = at_value;

	return std::move(result);
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
				    bind_data.attach_opaque_data, bind_data.transaction_opaque_data,
				    bind_data.settings, resolved_secrets,
				    /*resolved_secrets_provided=*/false,
				    bind_data.worker_path(),
				    bind_data.at_unit, bind_data.at_value,
				    /*copy_from=*/nullptr, /*copy_to=*/nullptr, bind_data.schema_name);
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

} // namespace (anonymous) — break out so VgiContainsDynamicFilter gets
  // external linkage matching its declaration in vgi_table_function_impl.hpp.
  // The VgiRequiredFiltersOptimizer in vgi_extension.cpp calls it across TU.

//! Returns true if any descendant of ``filter`` is a DynamicFilter (Top-N
//! tick-time bound). The static filter serializer must skip an entire
//! OptionalFilter subtree when this is true: the DynamicFilter has no value
//! at init time, so any partial serialization (e.g. OR(IsNull, DynamicFilter)
//! collapsing to a one-child OR(IsNull) when DynamicFilter is elided) yields
//! a *stricter* filter than the OptionalFilter promised, and consumers can
//! drop rows that were supposed to flow. The dynamic-filter mechanism in
//! TickFilterPushdown re-pushes the filter at tick time once Top-N has
//! established its threshold.
bool VgiContainsDynamicFilter(const TableFilter &filter) {
	switch (filter.filter_type) {
	case TableFilterType::DYNAMIC_FILTER:
		return true;
	case TableFilterType::CONJUNCTION_AND: {
		auto &conj = filter.Cast<ConjunctionAndFilter>();
		for (auto &child : conj.child_filters) {
			if (VgiContainsDynamicFilter(*child)) {
				return true;
			}
		}
		return false;
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &conj = filter.Cast<ConjunctionOrFilter>();
		for (auto &child : conj.child_filters) {
			if (VgiContainsDynamicFilter(*child)) {
				return true;
			}
		}
		return false;
	}
	case TableFilterType::OPTIONAL_FILTER: {
		auto &opt = filter.Cast<OptionalFilter>();
		return opt.child_filter && VgiContainsDynamicFilter(*opt.child_filter);
	}
	case TableFilterType::STRUCT_EXTRACT: {
		auto &sf = filter.Cast<StructFilter>();
		return sf.child_filter && VgiContainsDynamicFilter(*sf.child_filter);
	}
	default:
		return false;
	}
}

namespace { // reopen anonymous namespace for the file-local FilterSerializer.

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
			// Propagate a skipped child instead of emitting a half-built struct with a
			// "type"-less child_filter (which would make the worker fail to parse). This
			// matches the CONJUNCTION_AND/OR/OPTIONAL_FILTER handling. DuckDB only ever
			// places ConstantFilter/IsNull/IsNotNull under a struct_extract today (never
			// a DynamicFilter/BloomFilter/IN — those optimizers reject nested expressions),
			// so this is currently unreachable, but keeps the contract consistent.
			if (!SerializeFilterInto(child_filter_obj, *struct_filter.child_filter, column_index, column_name)) {
				return false;
			}
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
			// A BloomFilter is a probabilistic filter the optimizer pushes onto the
			// probe side of a (semi/hash) join; its large binary buffer can't be put
			// on the wire. Skip it instead of throwing: the join above the scan still
			// enforces exact membership, so dropping this redundant optimization never
			// changes results — it only means the worker isn't pre-filtered. This
			// mirrors DYNAMIC_FILTER above and the CONJUNCTION_AND/OR handling that
			// already catches and skips unserializable bloom-filter children.
			return false;
		}
		case TableFilterType::OPTIONAL_FILTER: {
			auto &optional_filter = filter.Cast<OptionalFilter>();
			if (!optional_filter.child_filter) {
				return false;
			}
			// Skip the entire OptionalFilter when its subtree contains a
			// DynamicFilter — see the comment on VgiContainsDynamicFilter for why.
			// The dynamic-filter mechanism captures DynamicFilters via
			// try_capture_from_optional and pushes them per-tick once Top-N
			// has established a threshold; serializing the static portion
			// here would produce a stricter filter than the OptionalFilter
			// promised and silently drop correct rows. (Repro:
			// SELECT n FROM filter_echo(10) ORDER BY n NULLS FIRST LIMIT 3
			// returned 0 rows because OR(IsNull, DynamicFilter) collapsed
			// to a one-child OR(IsNull) once the DynamicFilter was elided.)
			if (VgiContainsDynamicFilter(*optional_filter.child_filter)) {
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
                                      const vector<string> &column_names, const string &worker_path,
                                      const string &rowid_column_name,
                                      const std::set<idx_t> *exclude_filter_keys) {
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

		// Residual serialization (per-partition cache): skip the partition-column
		// predicate keys so the residual filter reflects only non-partition constraints.
		if (exclude_filter_keys && exclude_filter_keys->count(col_idx)) {
			continue;
		}

		// DuckDB's TableFilterSet uses indices into the projected column list (column_ids).
		// Map through column_ids to get the original schema column name, but keep the
		// projected index as column_index since the worker output follows projection order.
		idx_t original_col_idx = col_idx < column_ids.size() ? column_ids[col_idx] : col_idx;
		// A filter on the rowid virtual column carries the COLUMN_IDENTIFIER_ROW_ID
		// sentinel (UINT64_MAX). Without remapping, col_name would become the literal
		// "18446744073709551615" and the worker — which resolves pushed/join-key columns
		// by name — would silently drop it (the late-materialization semi-join still
		// guarantees correctness, but the rowid IN-list / min-max pushdown is lost).
		// Resolve to the worker's actual rowid field name (passed explicitly because
		// column_names has the rowid column erased — indexing it would mis-resolve).
		string col_name;
		if (original_col_idx == COLUMN_IDENTIFIER_ROW_ID && !rowid_column_name.empty()) {
			col_name = rowid_column_name;
		} else {
			col_name =
			    original_col_idx < column_names.size() ? column_names[original_col_idx] : std::to_string(original_col_idx);
		}

		// `col_idx` is the filter's position in the projected column_ids and is
		// sent as the wire `column_index`. The worker applies ConstantFilter /
		// InFilter by INDEX (`batch.column(column_index)`), so this only resolves
		// correctly because the worker emits its output batch in the same
		// projected order (`project_schema(projection_ids, ...)`): emitted-batch
		// position == projected column_ids position. A worker that reorders its
		// emitted columns relative to projection_ids would mis-apply pushed
		// filters. (Join-key IN filters are additionally matched by name.)
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
		// Captured for forwarding to secondary inits — see init_opaque_data
		// field comment for why this matters.
		init_opaque_data = std::move(applied.first.opaque_data);
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

// ============================================================================
// Result cache — key computation, eligibility, capture commit (milestone 1)
// ============================================================================
namespace {

// SerializeSettingsForKey / SerializeProjectionForKey moved to
// vgi_exchange_cache_key.{hpp,cpp} (shared with the exchange-mode cache key so
// both compute the key byte-identically). They are reached here via the header's
// duckdb::vgi declarations.

//! Result of a v1 cache-eligibility check for a scan.
struct CacheEligibility {
	bool eligible = false;                  //! query is a cache candidate (opt-in gates pass)
	const char *ineligible_reason = nullptr; //! set when !eligible (for logging)
	VgiResultCacheKey key;                  //! populated when eligible (catalog-scoped: txn_id empty)
	std::string catalog_name;
	bool catalog_version_frozen = false;
	//! Current transaction identifier. The KEY stays catalog-scoped (txn_id empty)
	//! because scope is worker-advertised on the first batch, not known here — but a
	//! `scope=transaction` result folds this into its key at commit and a lookup
	//! probes both the catalog key and (key + this txn_id).
	std::string transaction_id;

	// --- Per-partition cache classification (SINGLE_VALUE_PARTITIONS) ---------
	//! Splittable: eligible to split the captured result by partition value AND (if
	//! enumerable) serve a `=`/`IN` scan from per-partition entries. False when the
	//! function isn't partition-scope-eligible, the setting is off, or a partition
	//! column carries a non-`=`/`IN` (range/other) predicate. When false the whole
	//! per-partition layer is skipped (the whole-scan cache still operates normally).
	bool partition_splittable = false;
	//! Enumerable: every partition column is constrained by `=`/`IN`, so the requested
	//! partition set is finite and known here (in `partition_enumerated_tuples`). Only
	//! an enumerable scan can be SERVED from per-partition entries; a non-enumerable
	//! splittable scan only POPULATES them.
	bool partition_enumerable = false;
	std::vector<std::vector<Value>> partition_enumerated_tuples; // parallel to partition_types
	std::string partition_residual_filter_bytes;                 // filter_bytes minus the partition predicate
	std::vector<idx_t> partition_column_indices;                 // output-schema indices, declared order
	std::vector<LogicalType> partition_types;                    // matching declared partition types
	std::vector<std::string> partition_names;                    // matching declared partition column names
	uint64_t partition_max = 1024;                               // cap on distinct/enumerated partitions
};

//! Resolve partition column names (from the Arrow bind output schema — the index
//! space of `partition_column_indices`, which INCLUDES the rowid column) and their
//! DuckDB types (matched BY NAME into all_column_names/all_column_types, whose index
//! space has the rowid erased). Returns false if any partition column can't be
//! resolved. Keeps `names`/`types` in declared partition order.
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
		const std::string nm = out_schema->field(static_cast<int>(pi))->name();
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

//! Returns true iff `filter` constrains its (single) column to a FINITE enumerable set
//! of constants — an `=`, an `IN`, or an `OR` of those — appending the constants to
//! `out`. Returns false for any range / NULL / AND / other shape (not enumerable). A
//! TableFilterSet entry is per-column, so an OR here is `col=a OR col=b` on one column.
static bool ExtractPartitionEqualityValues(const TableFilter &filter, std::vector<Value> &out) {
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &cf = filter.Cast<ConstantFilter>();
		if (cf.comparison_type != ExpressionType::COMPARE_EQUAL) {
			return false;
		}
		out.push_back(cf.constant);
		return true;
	}
	case TableFilterType::IN_FILTER: {
		auto &inf = filter.Cast<InFilter>();
		if (inf.values.empty()) {
			return false;
		}
		for (auto &v : inf.values) {
			out.push_back(v);
		}
		return true;
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &orf = filter.Cast<ConjunctionOrFilter>();
		if (orf.child_filters.empty()) {
			return false;
		}
		for (auto &c : orf.child_filters) {
			if (!ExtractPartitionEqualityValues(*c, out)) {
				return false;
			}
		}
		return true;
	}
	case TableFilterType::OPTIONAL_FILTER: {
		// DuckDB wraps a pushed `col IN (...)` in an OptionalFilter ("not required for
		// query correctness" — enforced above the scan too). Its child is the real IN/=.
		// Any dynamic filter was already rejected upstream (VgiContainsDynamicFilter →
		// cache-ineligible), so an optional reaching here is static and safe to unwrap.
		auto &opt = filter.Cast<OptionalFilter>();
		return opt.child_filter && ExtractPartitionEqualityValues(*opt.child_filter, out);
	}
	default:
		return false; // range / IS NULL / AND → not enumerable
	}
}

//! Classify the pushed filter for per-partition caching, filling the partition_* fields
//! of `e`. Called only when the base scan is already cache-eligible (so dynamic filters
//! were rejected upstream). Sets `partition_splittable` (may populate per-partition
//! entries) and, when every partition column is `=`/`IN`-constrained, `partition_enumerable`
//! + `partition_enumerated_tuples` (may serve). A non-`=`/`IN` (range/other) predicate on
//! ANY partition column disables the whole per-partition layer (leaves it false).
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
	// Ordered/sampled scans can't use the combined per-partition serve (concatenation
	// can't honor a global ORDER BY; per-partition sampling semantics differ), so the
	// whole per-partition layer is skipped — the whole-scan cache handles them.
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
		for (auto &entry : input.filters->filters) {
			const idx_t col_idx = entry.first;
			auto &filter = *entry.second;
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

	// Residual = the pushed filters MINUS the partition predicates. CRITICAL: when EVERY
	// pushed filter is a partition predicate (all excluded), the residual must be the
	// empty string — byte-identical to a full/no-filter scan's residual — so a
	// `WHERE part=X` scan reuses the per-partition entry a full scan stored. Serializing
	// an all-excluded set would still emit a non-empty empty-filter IPC batch and never
	// match. So only serialize when a NON-excluded filter remains.
	bool has_residual_filter = false;
	if (input.filters) {
		for (auto &entry : input.filters->filters) {
			if (!exclude_keys.count(entry.first)) {
				has_residual_filter = true;
				break;
			}
		}
	}
	if (has_residual_filter) {
		try {
			auto sf = VgiSerializeFilters(context, input.column_ids, input.filters,
			                              bind_data.all_column_names, bind_data.worker_path(),
			                              bind_data.rowid_column_name, &exclude_keys);
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

//! The "p:"-prefixed input_hash discriminator for one partition-value tuple. Both the
//! serve side (filter constants) and the capture side (decoded partition min-values)
//! call this with the same `partition_types`, so identical tuples produce identical
//! discriminators (the per-partition-cache correctness linchpin).
static std::string PartitionDiscriminator(ClientContext &context, const std::vector<Value> &tuple,
                                          const std::vector<LogicalType> &partition_types) {
	return "p:" + VgiSha256Hex(vgi::CanonicalPartitionTupleKey(context, tuple, partition_types));
}

//! Build a per-partition cache key from the base (whole-scan) key: swap filter_bytes for
//! the residual (partition predicate stripped), set the "p:" input_hash discriminator,
//! and clear order/sample/transaction dimensions (partition entries are catalog-scoped,
//! order/sample-free by construction).
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

//! Human-readable "name=value, name2=value2" label of a partition tuple (diagnostics).
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

//! Evaluate v1 cache eligibility for a table scan and build the cache key.
//! v1 policy: catalog-attached path only, unfiltered full scans only (no
//! filter / order / sample / dynamic pushdown), known catalog version.
CacheEligibility EvaluateCacheEligibility(ClientContext &context,
                                          const VgiTableFunctionBindData &bind_data,
                                          TableFunctionInitInput &input,
                                          const std::vector<int32_t> &projection_ids) {
	CacheEligibility e;
	// Global master switch.
	Value master;
	if (context.TryGetCurrentSetting("vgi_result_cache", master) && !master.GetValue<bool>()) {
		e.ineligible_reason = "disabled_global";
		return e;
	}
	// Per-catalog opt-out.
	if (!bind_data.attach_params->cache()) {
		e.ineligible_reason = "disabled_attach";
		return e;
	}
	// Identity + version dimensions. Two paths (M5 adds the direct one):
	//   - catalog-attached (bind_data.table_entry set): identity = catalog name,
	//     freshness bounded by the runtime catalog_version (0 = unknown → never
	//     cache, since a version bump is how catalog data invalidates).
	//   - direct vgi_table_function() (no table_entry): no VgiCatalog and no
	//     version dimension; identity = worker_path, freshness is TTL-only.
	std::string identity_scope;
	int64_t version = 0;
	std::string catalog_label;
	if (bind_data.table_entry) {
		// ParentCatalog() comes through the const bind_data; the catalog is
		// logically mutable (a live attached catalog) — const_cast to call the
		// non-const GetCatalogType()/Cast().
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
		// Direct path: worker-path identity, TTL-only (version stays 0 here,
		// which means "no version dimension", NOT "unknown version"). No auth on
		// this path — anonymous scope.
		catalog_label = bind_data.worker_path();
		std::string principal =
		    vgi::ComputeCatalogIdentityFingerprint(bind_data.attach_params->auth());
		if (principal.empty()) {
			e.ineligible_reason = "identity_unresolved";
			return e;
		}
		identity_scope = "worker:" + bind_data.worker_path() + "\x1f" + principal;
	}

	// Static pushdowns are KEY COMPONENTS (M3): a filtered/ordered/sampled scan
	// caches under a key that includes the pushdown, so it can never be served
	// to a differently-shaped scan. Two classes still DISABLE caching:
	//   - dynamic filters (join-key IN pushdown / Top-N): their values arrive
	//     via ticks during the scan, so there's no stable key at decision time.
	//   - unseeded TABLESAMPLE: non-deterministic, must not be frozen.
	std::string filter_key;
	if (input.filters) {
		for (auto &kv : input.filters->filters) {
			if (VgiContainsDynamicFilter(*kv.second)) {
				e.ineligible_reason = "dynamic_filter";
				return e;
			}
		}
		if (!input.filters->filters.empty()) {
			// Serialize the STATIC filters deterministically for the key. If the
			// serializer can't represent a filter, fail closed (skip caching).
			try {
				auto sf = VgiSerializeFilters(context, input.column_ids, input.filters,
				                              bind_data.all_column_names, bind_data.worker_path(),
				                              bind_data.rowid_column_name);
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
			// Unseeded sample is non-deterministic — never cache.
			e.ineligible_reason = "unseeded_sample";
			return e;
		}
		sample_key = std::to_string(s.sample_percentage) + "/" + std::to_string(s.seed);
	}

	// Build the key (projection IN the key; transaction scope deferred).
	e.key.identity_scope = identity_scope;
	e.key.worker_path = bind_data.worker_path();
	e.key.function_name = bind_data.function_name;
	e.key.schema_name = bind_data.schema_name;
	e.key.canonical_arguments = bind_data.arguments.array ? bind_data.arguments.array->ToString() : "";
	e.key.canonical_settings = SerializeSettingsForKey(bind_data.settings);
	e.key.attach_options = bind_data.attach_params->attach_options_canonical();
	e.key.projection = SerializeProjectionForKey(projection_ids);
	e.key.attached_data_version = bind_data.attach_params->data_version_spec();
	e.key.implementation_version = bind_data.attach_params->implementation_version();
	e.key.catalog_version = version;
	e.key.at_unit = bind_data.at_unit;
	e.key.at_value = bind_data.at_value;
	// Static pushdown key components (M3).
	e.key.filter_bytes = std::move(filter_key);
	e.key.order_by_hint = std::move(order_key);
	e.key.sample_hint = std::move(sample_key);
	// The key stays catalog-scoped here (txn_id empty); the current transaction id
	// is carried separately for the scope=transaction commit + two-key lookup.
	e.transaction_id = std::to_string(static_cast<uint64_t>(context.ActiveTransaction().global_transaction_id));
	e.catalog_name = catalog_label;
	// Per-partition classification piggybacks on the base eligibility (same identity /
	// version / opt-out). Populates the partition_* fields; harmless when the function
	// isn't SINGLE_VALUE_PARTITIONS or the setting is off.
	ClassifyPartitionFilters(context, bind_data, input, e);
	e.eligible = true;
	return e;
}

//! Log a result_cache.* event.
void LogResultCache(ClientContext &context, const string &event,
                    const vector<pair<string, string>> &fields) {
	VGI_LOG(context, event, fields);
}

} // namespace

// Drain a RAM substream's buffered batches into the spill writer (freeing them as
// it goes). Returns false if the writer refused (per-entry disk budget exceeded).
static bool DrainSubstreamToWriter(CachedStream &s, VgiCaptureDiskWriter &w) {
	bool ok = true;
	for (auto &cb : s.batches) {
		if (ok && cb.ipc) {
			// These pre-threshold batches were serialized uncompressed (the live
			// RecordBatch is gone), so transcode to the writer's codec here. logical_len
			// is the uncompressed size (no-op transcode when codec=="none").
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

// Per-partition split-at-commit (SINGLE_VALUE_PARTITIONS). Defined after
// ConvertPartitionValuesBatch; forward-declared here so the commit dtor can call it.
static void SplitAndStorePartitionEntries(ClientContext *ctx, VgiResultCaptureCtx &cap,
                                          const VgiResultCacheEntry &meta, bool allow_disk);

// Commit a completed capture to the result cache. Called from the gstate
// destructor. Enforces the never-partial invariant: only commits when the full
// result was drained (every launched local state reached EOS) and the worker
// advertised a cacheable result.
VgiTableFunctionGlobalState::~VgiTableFunctionGlobalState() {
	if (!capture) {
		return;
	}
	auto &cap = *capture;
	// [S6] Release this capture's in-flight budget on EVERY teardown path (commit,
	// abort, incomplete) — the transient capture buffers are done being resident
	// (either committed into the LRU, whose bytes are accounted separately, or
	// freed). Do it first so no early-return leaks the reservation.
	int64_t reserved = cap.reserved_inflight_bytes.exchange(0, std::memory_order_relaxed);
	if (reserved > 0) {
		VgiResultCache::Instance().ReleaseInflightCapture(reserved);
	}
	// Any path that does NOT commit must discard a streamed temp blob (no ref is
	// written, so an un-aborted temp would linger until the reaper sweeps it).
	auto abort_stream = [&]() {
		if (cap.disk_writer) {
			VgiResultCache::Instance().AbortStreamingCapture(*cap.disk_writer);
		}
	};
	// Best-effort store diagnostics. The dtor runs during query teardown while the
	// ClientContext is still alive (same assumption as dynamic_to_string); guard + swallow.
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

	const bool complete = cap.cc_seen && cap.cc.Cacheable() && !cap.aborted.load() &&
	                      cap.launched.load() > 0 && cap.eos.load() == cap.launched.load();
	// v1: transaction-scoped results are not cached (transaction keying is a later
	// milestone). Refuse rather than mis-key.
	if (!complete) {
		abort_stream();
		return;
	}
	// scope=transaction: reusable only within THIS transaction. Fold the txn id into
	// the key so a different transaction (different id) misses it. Refuse if we have
	// no txn id to isolate by (shouldn't happen).
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
	// Freshness: ttl wins over expires; frozen/at-pinned may never expire. The
	// worker ttl was already clamped at parse; clamp the settings-derived default
	// too so `stored_at + seconds(ttl)` can never overflow the time_point.
	int64_t ttl = cap.cc.ttl_seconds.value_or(static_cast<int64_t>(cap.default_ttl_seconds));
	if (ttl > VGI_CACHE_MAX_TTL_SECONDS) {
		ttl = VGI_CACHE_MAX_TTL_SECONDS;
	}
	if (ttl <= 0 && cap.cc.expires_rfc3339.empty()) {
		if (cap.catalog_version_frozen || !cap.key.at_value.empty()) {
			entry->never_expires = true;
		} else if (cap.cc.revalidatable && !cap.cc.etag.empty()) {
			// "no-cache" semantic (HTTP): ttl=0 + a validator + revalidatable
			// stores the payload but marks it immediately stale, so every read
			// revalidates via a conditional request (a not_modified reply reuses
			// the stored bytes without re-streaming). expires_at == stored_at.
		} else {
			skip("no_freshness");
			abort_stream();
			return; // no freshness basis — refuse (shouldn't happen: Cacheable() gate)
		}
	}
	entry->expires_at = entry->stored_at + std::chrono::seconds(ttl > 0 ? ttl : 0);

	if (cap.streaming()) {
		// A spilled entry is disk-only, and conditional revalidation
		// (LookupForRevalidation) only probes the in-memory index — so a disk-only
		// entry can never be revalidated. An immediately-stale "no-cache" result
		// (ttl=0 + etag + revalidatable, expires_at == stored_at) would therefore be
		// written un-loadable (both disk loaders reject `now >= expires`) and orphan
		// one blob per scan. Refuse to spill it — recompute is the correct behavior.
		if (!entry->never_expires && entry->expires_at <= entry->stored_at) {
			skip("immediately_stale");
			abort_stream();
			return;
		}
		// Transaction-scoped entries are memory-only (the txn id is process-local and
		// ephemeral; a disk blob would orphan after the txn and be meaningless
		// cross-process). A spilled one has no RAM copy → refuse (recompute).
		if (txn_scoped) {
			skip("transaction_scoped_spill");
			abort_stream();
			return;
		}
		// Spilled path: most batches already streamed to the disk blob. Drain any
		// substreams whose producer finished BEFORE the spill (their RAM batches were
		// never streamed) — single-threaded here, all producers done. Then finalize
		// (hash → rename → ref); the entry is disk-only, discovered via its ref and
		// adopted into memory on a small serve. A failed drain/commit caches nothing.
		for (auto &sp : cap.streams) {
			if (!sp->batches.empty() && !DrainSubstreamToWriter(*sp, *cap.disk_writer)) {
				skip("drain_failed");
				VgiResultCache::Instance().AbortStreamingCapture(*cap.disk_writer);
				return;
			}
		}
		if (VgiResultCache::Instance().CommitStreamingCapture(*cap.disk_writer, *entry)) {
			log_store("result_cache.store", {{"function", cap.key.function_name},
			                                 {"key_hash", entry->key.HexDigest()},
			                                 {"tier", "disk"},
			                                 {"rows", std::to_string(cap.disk_writer->Rows())},
			                                 {"bytes", std::to_string(cap.disk_writer->Bytes())}});
		}
		// A spilled capture is disk-only and its RAM batches were drained — no per-batch
		// bytes remain to split by partition. Skip the per-partition layer (v1 boundary).
		if (cap.partition_split_requested && cap.cc.partition_scope) {
			skip("partition_spilled");
		}
		return;
	}
	// Per-partition split (additive): store one entry per distinct partition value BEFORE
	// the whole-scan move consumes the substreams. Gated on the worker's first-batch
	// opt-in (cc.partition_scope) and skipped for transaction-scoped results (their key
	// is process-local; partition entries are catalog-scoped). Copies batch shared_ptrs.
	// MEMORY-ONLY (allow_disk=false): the combined serve probes only the memory tier
	// (LookupBatch), so a disk copy would never be read back — and persisting an
	// input_hash-keyed entry would wrongly route it into the packed EXCHANGE store + its
	// ref-count cap. The whole-scan entry above still persists to disk for cross-process
	// warmth of full/identical-filter repeats.
	if (cap.partition_split_requested && cap.cc.partition_scope && !txn_scoped) {
		SplitAndStorePartitionEntries(client_context_for_explain, cap, *entry, /*allow_disk=*/false);
	}
	// RAM-buffered path (disk tier off): move the substreams in + insert into memory.
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
	// Transaction-scoped entries never persist to disk (ephemeral, process-local id).
	if (VgiResultCache::Instance().Insert(std::move(entry), /*allow_disk=*/!txn_scoped)) {
		log_store("result_cache.store", {{"function", cap.key.function_name},
		                                 {"key_hash", stored_key_hash},
		                                 {"tier", "memory"},
		                                 {"rows", std::to_string(rows)},
		                                 {"bytes", std::to_string(bytes)}});
	} else {
		skip("too_large_for_memory"); // > max_entry_bytes with disk off (or memory-only txn)
	}
}

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

	// --- Result cache: eligibility + serve (cache hit short-circuits worker) ---
	// Sync the process-global cache config from this query's settings so SET on any
	// cap / the disk dir / the packed backend takes effect. Shared with the exchange
	// path (single source of truth — no drift). [S1] ConfigureIfChanged skips the
	// global lock + evict entirely when the settings are unchanged (the steady state).
	SyncResultCacheSettings(context);
	auto cache_eval = EvaluateCacheEligibility(context, bind_data, input, projection_ids);
	// A stale-but-revalidatable entry the worker can cheaply confirm via a
	// conditional request. Set below the eligible block; consumed when the
	// gstate is built (miss path) to arm revalidation mode. Null otherwise.
	std::shared_ptr<const VgiResultCacheEntry> revalidation_entry;
	if (cache_eval.eligible) {
		auto now_tp = std::chrono::steady_clock::now();
		// Conditional revalidation takes precedence: probe for a stale
		// revalidatable entry FIRST, because Lookup() drops stale entries.
		auto reval = VgiResultCache::Instance().LookupForRevalidation(cache_eval.key, now_tp);
		if (reval) {
			// Only worth a conditional round-trip when re-streaming would cost
			// more than the check — gate on the payload size threshold.
			Value rmv;
			uint64_t reval_min_bytes =
			    context.TryGetCurrentSetting("vgi_result_cache_revalidate_min_bytes", rmv) && !rmv.IsNull()
			        ? rmv.GetValue<uint64_t>()
			        : 262144;
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
			auto entry = VgiResultCache::Instance().Lookup(cache_eval.key, now_tp);
			if (!entry && !cache_eval.transaction_id.empty()) {
				// The catalog key (txn_id empty) didn't match; a scope=transaction
				// result stored earlier in THIS transaction lives under (key + txn_id),
				// so probe that too. A different transaction has a different id → miss.
				VgiResultCacheKey txn_key = cache_eval.key;
				txn_key.transaction_id = cache_eval.transaction_id;
				entry = VgiResultCache::Instance().Lookup(txn_key, now_tp);
			}
			if (entry) {
				// HIT — serve from cache; no worker acquire / init RPC / pool touch.
				auto gstate = make_uniq<VgiTableFunctionGlobalState>();
				gstate->serving_from_cache = true;
				// [S8] A disk-backed entry streams off disk (never materialized); a
				// memory / adopted entry replays from RAM. Surface which so tests can
				// prove the flat-RAM streaming path was taken.
				const bool disk_streaming = entry->disk_backed;
				gstate->serving_entry = std::move(entry);
				gstate->max_processes = 1;
				gstate->init_applied.store(true, std::memory_order_release);
				gstate->client_context_for_explain = &context;
				LogResultCache(context, "result_cache.hit",
				               {{"catalog", cache_eval.catalog_name},
				                {"function", bind_data.function_name},
				                {"key_hash", cache_eval.key.HexDigest()},
				                {"tier", disk_streaming ? "disk_streaming" : "memory"}});
				return unique_ptr<GlobalTableFunctionState>(std::move(gstate));
			}
			LogResultCache(context, "result_cache.miss",
			               {{"catalog", cache_eval.catalog_name},
			                {"function", bind_data.function_name},
			                {"key_hash", cache_eval.key.HexDigest()}});

			// Per-partition serve (additive): the whole-scan key missed, but if this scan
			// enumerates its requested partition set (`=`/`IN` on every partition column)
			// AND every per-partition entry is an in-memory hit, serve the union without
			// the worker. Row-exact: each entry keys on the same residual filter, the
			// enumerated set equals the requested set, and SINGLE_VALUE guarantees each
			// cached row belongs to exactly its entry's partition (no super/subset).
			if (cache_eval.partition_enumerable && !cache_eval.partition_enumerated_tuples.empty()) {
				std::vector<VgiResultCacheKey> pkeys;
				pkeys.reserve(cache_eval.partition_enumerated_tuples.size());
				for (auto &tuple : cache_eval.partition_enumerated_tuples) {
					auto disc = PartitionDiscriminator(context, tuple, cache_eval.partition_types);
					pkeys.push_back(
					    BuildPartitionKey(cache_eval.key, cache_eval.partition_residual_filter_bytes, disc));
				}
				auto hits = VgiResultCache::Instance().LookupBatch(pkeys, now_tp);
				bool all_hit = !pkeys.empty();
				int64_t missing = 0;
				for (auto &h : hits) {
					// Combined serve is memory-only: a disk-backed (streaming) entry can't
					// be aggregated into one replay stream, so treat it as a miss.
					if (!h || h->disk_backed) {
						all_hit = false;
						++missing;
					}
				}
				if (all_hit) {
					// Aggregate every partition entry's batches into ONE partition-contiguous
					// replay stream with fresh sequential batch_index — so the sort in
					// CachedReplayConnection keeps each partition's batches together (required
					// by PhysicalPartitionedAggregate) regardless of the stored indices.
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
					VgiResultCache::Instance().RecordPartitionHit();
					LogResultCache(context, "result_cache.partition_hit",
					               {{"catalog", cache_eval.catalog_name},
					                {"function", bind_data.function_name},
					                {"key_hash", cache_eval.key.HexDigest()},
					                {"partitions", std::to_string(pkeys.size())}});
					return unique_ptr<GlobalTableFunctionState>(std::move(gstate));
				}
				VgiResultCache::Instance().RecordPartitionMiss();
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

	// Acquire a connection for the init/scan phase. The planner-phase BindResult
	// is already cached on bind_data; PerformInit's payload carries it inline,
	// so no second on-wire bind is needed. AcquireConnectionForInit pool-hits
	// when possible (subprocess) or constructs a fresh HTTP connection.
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

	// Serialize the filters (returns empty if no filters or if serialization fails)
	SerializedFilters serialized_filters;
	try {
		serialized_filters =
		    VgiSerializeFilters(context, input.column_ids, input.filters, bind_data.all_column_names,
		                        bind_data.worker_path(), bind_data.rowid_column_name);
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
			// Same rowid-sentinel remap as VgiSerializeFilters: name the rowid
			// column with the worker's actual field name. Defensive — the rowid
			// late-mat filter is a concrete CONJUNCTION_AND/InFilter, not a captured
			// DynamicFilter, so it normally doesn't reach this loop, but a future
			// rowid DynamicFilter would otherwise be misnamed.
			string col_name;
			if (original_col_idx == COLUMN_IDENTIFIER_ROW_ID && !bind_data.rowid_column_name.empty()) {
				col_name = bind_data.rowid_column_name;
			} else {
				col_name = original_col_idx < bind_data.all_column_names.size()
				               ? bind_data.all_column_names[original_col_idx]
				               : std::to_string(original_col_idx);
			}

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
	// RPC result) and defer PerformInit to a background thread. The future
	// is awaited lazily by EnsureInitApplied (driven by MaxThreads() and
	// init_local).
	auto global_state = make_uniq<VgiTableFunctionGlobalState>();
	global_state->client_context_for_explain = &context;
	global_state->cache_eligible = cache_eval.eligible; // an eligible non-hit is a genuine miss (EXPLAIN)
	global_state->dynamic_filters = std::move(dynamic_filters);
	global_state->static_filter_bytes = filter_bytes;
	global_state->tick_filter_state = tick_filter_state;
	global_state->projection_ids = projection_ids;
	global_state->join_keys_buffers = join_keys_buffers;
	global_state->order_by_hint = bind_data.order_by_hint;
	global_state->table_sample_hint = bind_data.table_sample_hint;
	// Propagated from bind so MaxThreads() can clamp the source to one
	// thread for FIXED_ORDER functions even after EnsureInitApplied folds
	// in the worker's max_workers.
	global_state->fixed_order = bind_data.fixed_order;
	// Provisional max_processes — actual value is folded in by
	// EnsureInitApplied once the init future resolves. Setting 1 here
	// matches the conservative default; for fan-outs of small metadata
	// reads (Ducklake bind-time scan plan) it has no observable effect.
	global_state->max_processes = 1;

	// Result cache: eligible MISS → set up capture. Each local state captures
	// its own substream; commit happens in the gstate destructor (never-partial).
	if (cache_eval.eligible) {
		auto cap = std::make_shared<VgiResultCaptureCtx>();
		cap->key = cache_eval.key;
		cap->catalog_name = cache_eval.catalog_name;
		cap->catalog_version_frozen = cache_eval.catalog_version_frozen;
		cap->transaction_id = cache_eval.transaction_id;
		Value sv;
		cap->max_entry_bytes = context.TryGetCurrentSetting("vgi_result_cache_max_entry_bytes", sv)
		                           ? static_cast<int64_t>(sv.GetValue<uint64_t>())
		                           : 67108864;
		cap->max_bytes = context.TryGetCurrentSetting("vgi_result_cache_max_bytes", sv)
		                     ? static_cast<int64_t>(sv.GetValue<uint64_t>())
		                     : 268435456;
		cap->default_ttl_seconds =
		    context.TryGetCurrentSetting("vgi_result_cache_default_ttl_seconds", sv)
		        ? sv.GetValue<uint64_t>()
		        : 0;
		// Disk config for the threshold spill: when a RAM capture would exceed
		// max_entry_bytes AND the disk tier is on, capture spills to a streaming disk
		// blob (RAM-flat) instead of aborting. Captured here; the writer is created
		// lazily in InstallBatch on the first threshold cross.
#ifndef __EMSCRIPTEN__
		// WASM is memory-only: leave cap->disk_dir empty so a >max_entry_bytes capture
		// aborts to uncached (streams to DuckDB) rather than spilling to a disk blob —
		// the spill writer's incremental hash (MbedTlsWrapper::SHA256State) is not
		// resolvable from the emscripten side-module. The singleton disk tier is gated
		// in parallel at VgiResultCache::Configure. Native is unchanged.
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
		// Arm the per-partition split (SINGLE_VALUE_PARTITIONS). The actual split at
		// commit is ADDITIONALLY gated on the worker's first-batch cc.partition_scope.
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

	// Conditional revalidation: arm the gstate so the init request carries the
	// stored validators and GetNextBatch can honor a not_modified reply. The
	// capture set up just above still records fresh data on the changed path.
	if (revalidation_entry) {
		global_state->revalidating = true;
		global_state->revalidate_if_none_match = revalidation_entry->etag;
		global_state->revalidate_if_modified_since = revalidation_entry->last_modified;
		global_state->revalidation_entry = std::move(revalidation_entry);
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
			// Arm conditional-revalidation validators (M6) before init; no-op
			// when both are empty (the common non-revalidation path).
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

	// Kick off PerformInit on a background thread. The lambda owns the
	// connection until init returns (then EnsureInitApplied folds the
	// result + connection back into gstate). Multiple pipelines hit this
	// path back-to-back during scheduling and run their RPCs concurrently
	// instead of serializing on the main thread.
#if VGI_ASYNC_INIT_ENABLED
#  ifdef __EMSCRIPTEN__
	// Dispatch onto the pre-spawned VgiWasmAsyncPool. pthread_create after
	// side modules are dlopen'd is unreliable in MAIN_MODULE=1 builds, so
	// std::async(launch::async) would crash; the pool's workers were
	// spawned once at extension load.
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
	// Synchronous init: run the RPC inline and fold the result straight into
	// the gstate, mirroring EnsureInitApplied()'s fold step. pending_init
	// stays default-constructed (invalid future), so subsequent
	// EnsureInitApplied() calls hit the `if (pending_init.valid())` branch
	// and no-op. Selected on WASM by default to dodge the emsdk
	// pthread-after-dlopen bug (#19425/#19199/#13303); flipping
	// -DVGI_ASYNC_INIT_ENABLED=1 restores the async path.
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

	// Result cache serve: replace the worker connection with a cached-replay
	// connection that returns the cached batches. Only the FIRST local state to
	// claim replays the full result; any extra local state (should not occur
	// with MaxThreads()==1, but guard anyway) gets an empty replay so rows are
	// never duplicated.
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

		// Secondary workers must receive the same projection / filter / hint
		// pushdown info as the primary so they emit batches with a matching
		// shape. Without this, secondary workers emit the full unprojected
		// schema while the primary emits projected batches, and ArrowToDuckDB
		// (which assumes projected layout when projection_pushdown=true)
		// reads the wrong column positions from the unprojected secondary
		// batches.
		// Forward the primary's init_response.opaque_data so the worker's
		// secondary-init branch (which echoes init_opaque_data straight into
		// the response and skips on_init) sees the same bytes the primary's
		// on_init produced — required by any function that round-trips state
		// through init opaque data (e.g. tx_cached_value).
		auto secondary_init = [&]() {
			if (bind_data.projection_pushdown) {
				local_state->connection()->SetTickFilterState(global_state.tick_filter_state);
				local_state->connection()->PerformInit(
				    secondary_bind_result,
				    global_state.projection_ids, global_state.static_filter_bytes,
				    global_state.join_keys_buffers, "", global_state.order_by_hint,
				    global_state.table_sample_hint, global_state.init_opaque_data);
			} else {
				local_state->connection()->PerformInit(
				    secondary_bind_result, {}, nullptr, {}, "",
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

	// Result cache capture: give this local state its own substream to append
	// to (increments launched). InstallBatch appends; the gstate destructor
	// commits when every launched local state reached EOS.
	if (global_state.capture) {
		local_state->capture_stream = global_state.capture->NewStream();
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

//! Per-DuckDB-pipeline cap on the batch_index space. Lives at
//! ``duckdb/src/include/duckdb/parallel/pipeline.hpp:51`` (``BATCH_INCREMENT``).
//! Returning a source-side index ``>= BATCH_INCREMENT - 1`` triggers an
//! ``InternalException`` from ``Pipeline::ScheduleEventsInternal``'s
//! ``NextBatch`` shift (see ``pipeline_executor.cpp:140-143``). We reject
//! before threading the value into ``GetPartitionData`` so the error
//! names the worker contract directly instead of surfacing as an opaque
//! DuckDB internal error.
static constexpr idx_t VGI_BATCH_INDEX_CAP = 10000000000000ULL;  // 10^13

//! Convert a 2-row ``arrow::RecordBatch`` carrying ``(min, max)`` per
//! partition column into a ``vector<ColumnPartitionData>`` via the
//! established VGI Arrow-conversion machinery (the same path
//! ``VgiTableFunctionScan`` uses for data batches —
//! ``ArrowSchemaToDuckDBTypes`` + ``ArrowTableFunction::ArrowToDuckDB``).
//! Reusing this code path is what guarantees lossless type handling
//! for timestamps-with-tz, decimals, dictionary-encoded strings,
//! extension types like ``geoarrow.wkb``, etc.
//!
//! Caller has already validated row-count, column-count, and column
//! name/type alignment against the bind schema's partition fields.
//! On failure (e.g. an Arrow type the conversion utility can't
//! handle) any exception bubbles up.
static duckdb::vector<ColumnPartitionData> ConvertPartitionValuesBatch(
    ClientContext &context,
    const std::shared_ptr<arrow::RecordBatch> &pv_batch) {
	// Build an ArrowTableSchema + DuckDB LogicalTypes for the
	// partition_values schema. ``names`` is filled too but unused.
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
	vector<LogicalType> types;
	vector<string> names;
	ArrowSchemaToDuckDBTypes(context, pv_batch->schema(), c_schema, arrow_table, types, names);

	// Export the 2-row record batch to the Arrow C ABI and stash it on
	// a local scan state — the same shape ``VgiTableFunctionScan`` uses
	// for the main data batch (vgi_table_function_impl.cpp:1757).
	auto chunk_wrapper = make_uniq<ArrowArrayWrapper>();
	ExportRecordBatch(pv_batch, *chunk_wrapper);

	ArrowScanLocalState local_scan_state(std::move(chunk_wrapper), context);
	local_scan_state.chunk_offset = 0;

	// Convert into a 2-row DataChunk via DuckDB's standard Arrow path.
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), types, 2);
	chunk.SetCardinality(2);
	ArrowTableFunction::ArrowToDuckDB(local_scan_state, arrow_table.GetColumns(), chunk,
	                                  /*arrow_scan_is_projected=*/false,
	                                  COLUMN_IDENTIFIER_ROW_ID);

	// Row 0 = min, row 1 = max per column. Pull as ``Value`` and
	// build the ``ColumnPartitionData`` vector.
	duckdb::vector<ColumnPartitionData> out;
	out.reserve(chunk.ColumnCount());
	for (idx_t col = 0; col < chunk.ColumnCount(); ++col) {
		Value min_val = chunk.GetValue(col, 0);
		Value max_val = chunk.GetValue(col, 1);
		out.emplace_back(std::move(min_val));
		out.back().max_val = std::move(max_val);
	}
	return out;
}

//! Decode a captured batch's vgi_partition_values bytes to the per-column MIN values
//! (row 0; == max for SINGLE_VALUE). Returns false on any decode/shape problem (the
//! caller then aborts the whole split — a batch we can't classify must not be dropped).
static bool DecodePartitionMinTuple(ClientContext &context, const std::string &pv_bytes,
                                    std::vector<Value> &out) {
	if (pv_bytes.empty()) {
		return false;
	}
	auto buf = arrow::Buffer::FromString(pv_bytes);
	auto input = std::make_shared<arrow::io::BufferReader>(buf);
	auto rr = arrow::ipc::RecordBatchStreamReader::Open(input);
	if (!rr.ok()) {
		return false;
	}
	auto reader = rr.ValueUnsafe();
	auto nr = reader->ReadNext();
	if (!nr.ok() || !nr.ValueUnsafe().batch) {
		return false;
	}
	auto pv_batch = nr.ValueUnsafe().batch;
	if (pv_batch->num_rows() != 2) {
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

// Per-partition split-at-commit: bucket the captured batches by partition-value tuple
// and Insert one entry per distinct partition, keyed by (static key + residual filter)
// with a "p:" discriminator. Additive to the whole-scan entry. Best-effort — any decode
// failure or an over-cap partition count skips the split (the whole-scan entry stands).
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
		// v1: per-partition entries are non-revalidatable (the combined serve never issues
		// conditional requests). Freshness mirrors the whole-scan entry.
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
		if (VgiResultCache::Instance().Insert(std::move(pe), allow_disk)) {
			VgiResultCache::Instance().RecordPartitionStore();
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

//! Install arrow_batch as the active chunk. Returns false on EOS (null batch).
//! Caller is responsible for having already obtained arrow_batch either
//! synchronously (via ReadDataBatch) or from the prefetch slot.
static bool InstallBatch(ClientContext &context, const VgiTableFunctionBindData &bind_data,
                         VgiTableFunctionGlobalState &global_state,
                         VgiTableFunctionLocalState &local_state,
                         std::shared_ptr<arrow::RecordBatch> arrow_batch) {
	if (!arrow_batch) {
		local_state.done = true;
		// Result-cache capture: this local state fully drained its stream. The
		// gstate destructor commits iff eos == launched (never-partial).
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

	// batch_index validation + monotonicity check. The raw uint64 was
	// parsed off the wire's custom_metadata inside ``ReadDataBatch`` and
	// stashed on the connection; here on the consumer thread we apply the
	// contract (missing-tag, cap, monotonicity) and only then assign to
	// local_state. Writing inside ReadDataBatch / VgiPrefetchTask::Execute
	// would race with VgiGetPartitionData on the pipeline-executor thread.
	const idx_t parsed_index = local_state.connection()->GetLastBatchIndex();
	if (bind_data.supports_batch_index) {
		if (parsed_index == DConstants::INVALID_INDEX) {
			throw IOException("VGI function '%s' with supports_batch_index=true emitted a data "
			                  "batch without vgi_batch_index metadata",
			                  bind_data.function_name);
		}
		if (parsed_index >= VGI_BATCH_INDEX_CAP - 1) {
			throw IOException("VGI function '%s' emitted vgi_batch_index %llu that exceeds DuckDB's "
			                  "per-pipeline cap (%llu); choose a smaller index space",
			                  bind_data.function_name, static_cast<unsigned long long>(parsed_index),
			                  static_cast<unsigned long long>(VGI_BATCH_INDEX_CAP));
		}
		if (local_state.current_batch_index != DConstants::INVALID_INDEX &&
		    parsed_index < local_state.current_batch_index) {
			throw IOException("VGI function '%s' emitted vgi_batch_index %llu after %llu on the "
			                  "same stream — batch_index must be monotone non-decreasing per "
			                  "worker / per stream",
			                  bind_data.function_name, static_cast<unsigned long long>(parsed_index),
			                  static_cast<unsigned long long>(local_state.current_batch_index));
		}
		local_state.current_batch_index = parsed_index;
	}
	// If the function did not opt in but the worker emitted vgi_batch_index
	// anyway, we silently ignore — non-opted-in functions shouldn't be
	// tagging, but the harmless-passthrough behaviour matches our other
	// metadata keys.

	// PartitionColumns: decode the 2-row IPC RecordBatch (raw bytes were
	// base64-decoded on the connection's reader thread; IPC decode +
	// validation happen here on the consumer thread for uniform error
	// reporting across transports).
	if (bind_data.partition_kind != VgiPartitionKind::NotPartitioned) {
		const std::string &pv_bytes = local_state.connection()->GetLastPartitionValuesBytes();
		if (pv_bytes.empty()) {
			throw IOException("VGI function '%s' with partition columns declared emitted a "
			                  "non-empty data batch without vgi_partition_values metadata",
			                  bind_data.function_name);
		}
		auto buf = arrow::Buffer::FromString(pv_bytes);
		auto input = std::make_shared<arrow::io::BufferReader>(buf);
		auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(input);
		if (!reader_result.ok()) {
			throw IOException("VGI function '%s' emitted invalid Arrow IPC payload in "
			                  "vgi_partition_values#b64: %s",
			                  bind_data.function_name,
			                  reader_result.status().ToString());
		}
		auto reader = reader_result.ValueUnsafe();
		auto next_result = reader->ReadNext();
		if (!next_result.ok() || !next_result.ValueUnsafe().batch) {
			throw IOException("VGI function '%s' emitted empty IPC stream in "
			                  "vgi_partition_values#b64",
			                  bind_data.function_name);
		}
		auto pv_batch = next_result.ValueUnsafe().batch;
		if (pv_batch->num_rows() != 2) {
			throw IOException("VGI function '%s' emitted vgi_partition_values with %lld rows "
			                  "(expected exactly 2: row 0 = min, row 1 = max)",
			                  bind_data.function_name,
			                  static_cast<long long>(pv_batch->num_rows()));
		}
		const auto &declared_indices = bind_data.partition_column_indices;
		if (static_cast<idx_t>(pv_batch->num_columns()) != declared_indices.size()) {
			throw IOException("VGI function '%s' emitted vgi_partition_values with %d columns "
			                  "(expected %llu — one per partition-annotated bind-schema field)",
			                  bind_data.function_name, pv_batch->num_columns(),
			                  static_cast<unsigned long long>(declared_indices.size()));
		}
		// Schema name + type cross-check against the bind output schema.
		// We do this BEFORE the conversion so a typed error names the
		// column rather than surfacing from deep inside ArrowToDuckDB.
		const auto &bind_schema = bind_data.bind_result.output_schema;
		for (idx_t i = 0; i < declared_indices.size(); ++i) {
			const auto &declared_field = bind_schema->field(declared_indices[i]);
			const auto &pv_field = pv_batch->schema()->field(static_cast<int>(i));
			if (pv_field->name() != declared_field->name()) {
				throw IOException("VGI function '%s' vgi_partition_values column %llu name "
				                  "mismatch: declared '%s', got '%s'",
				                  bind_data.function_name, static_cast<unsigned long long>(i),
				                  declared_field->name(), pv_field->name());
			}
			if (!pv_field->type()->Equals(*declared_field->type())) {
				throw IOException("VGI function '%s' vgi_partition_values column '%s' type "
				                  "mismatch: declared %s, got %s",
				                  bind_data.function_name, declared_field->name(),
				                  declared_field->type()->ToString(),
				                  pv_field->type()->ToString());
			}
		}

		// Convert the 2-row Arrow batch into ``vector<ColumnPartitionData>``
		// via the established ``ArrowSchemaToDuckDBTypes`` +
		// ``ArrowTableFunction::ArrowToDuckDB`` path — same machinery that
		// converts data batches in ``VgiTableFunctionScan``. Reusing this
		// is what gives lossless type handling for timestamps-with-tz,
		// decimals, dictionary-encoded strings, extension types, etc.
		local_state.current_partition_data =
		    ConvertPartitionValuesBatch(context, pv_batch);

		// Defense in depth for SINGLE_VALUE_PARTITIONS: DuckDB's own
		// assertion at physical_partitioned_aggregate.cpp:104 is
		// ``D_ASSERT`` (debug only); we re-check in release.
		if (bind_data.partition_kind == VgiPartitionKind::SingleValuePartitions) {
			for (idx_t i = 0; i < local_state.current_partition_data.size(); ++i) {
				const auto &entry = local_state.current_partition_data[i];
				if (!Value::NotDistinctFrom(entry.min_val, entry.max_val)) {
					throw IOException("VGI function '%s' SINGLE_VALUE_PARTITIONS contract "
					                  "violated on column '%s': min (%s) != max (%s)",
					                  bind_data.function_name,
					                  bind_schema->field(declared_indices[i])->name(),
					                  entry.min_val.ToString(), entry.max_val.ToString());
				}
			}
		}
		// Synthetic per-batch batch_index for PartitionColumns mode.
		// DuckDB's pipeline executor only fires the sink's ``NextBatch``
		// (which is what copies ``partition_data`` into the sink's local
		// state) when the source-returned ``batch_index`` *changes* from
		// the previous chunk (pipeline_executor.cpp:147-149). For
		// PartitionColumns-only functions the worker doesn't emit
		// ``vgi_batch_index``, so ``current_batch_index`` would stay
		// INVALID forever and the sink would never refresh its
		// ``partition_data``.
		//
		// The value MUST come from a GLOBAL monotonic counter, not a
		// per-local-state one that restarts at 0. DuckDB initializes each
		// scan thread's sink ``partition_info.batch_index`` from the global
		// batch-index pool (``Pipeline::RegisterNewBatchIndex`` returns the
		// current minimum), so a thread that registers after peers have
		// advanced inherits a value > 0. A per-thread 0 would map (via
		// ``base + value + 1`` in ``PipelineExecutor::NextBatch``) onto that
		// inherited minimum, ``NextBatch`` would see "no change", the
		// never-installed ``partition_data`` would be dereferenced empty in
		// ``PhysicalPartitionedAggregate::Sink`` -> "index 0 within vector of
		// size 0". A global counter guarantees every batch's index strictly
		// exceeds any thread's inherited minimum, so the first chunk always
		// refreshes. See ``synthetic_batch_index`` in the gstate header for
		// the full rationale.
		if (!bind_data.supports_batch_index) {
			local_state.current_batch_index =
			    global_state.synthetic_batch_index.fetch_add(1, std::memory_order_relaxed);
		}
	}

	auto chunk = make_uniq<ArrowArrayWrapper>();
	ExportRecordBatch(arrow_batch, *chunk);

	local_state.chunk = shared_ptr<ArrowArrayWrapper>(chunk.release());
	local_state.chunk_offset = 0;
	// Reset() clears owned_data in array_states, which is REQUIRED so that ArrowToDuckDB
	// will update owned_data to point to the new chunk. Without this, owned_data still
	// points to the previous chunk, and when that chunk is released, the data becomes invalid.
	local_state.Reset();

	// Batch-shape accounting for EXPLAIN ANALYZE. TotalBufferSize walks the
	// batch's buffer list (O(columns), not O(rows)), so this is cheap enough to
	// run unconditionally — unlike the log below, it has no sink to check.
	const auto batch_rows = static_cast<idx_t>(arrow_batch->num_rows());
	const auto batch_bytes = static_cast<idx_t>(arrow::util::TotalBufferSize(*arrow_batch));
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

	// Result-cache capture: append this batch to the local state's substream.
	if (global_state.capture && local_state.capture_stream) {
		auto &cap = *global_state.capture;
		if (!cap.aborted.load(std::memory_order_relaxed)) {
			auto *conn = local_state.connection();
			// First batch this thread sees: latch the cache-control advertisement
			// (a worker advertises on the first batch of its stream).
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
			// If a non-cacheable advertisement was latched (no_store / no
			// freshness key), abort — never commit. Buffered bytes are freed at
			// gstate teardown (bounded by max_entry_bytes).
			bool not_cacheable = false;
			{
				std::lock_guard<std::mutex> lk(cap.mu);
				not_cacheable = cap.cc_seen && !cap.cc.Cacheable();
			}
			auto spill_append = [&](const std::shared_ptr<arrow::Buffer> &ipc, int64_t logical_len) -> bool {
				idx_t bi = conn->GetLastBatchIndex();
				bool has_bi = bi != DConstants::INVALID_INDEX;
				return cap.disk_writer->AppendBatch(
				    has_bi, has_bi ? static_cast<uint64_t>(bi) : 0,
				    static_cast<int64_t>(arrow_batch->num_rows()), conn->GetLastPartitionValuesBytes(),
				    ipc->data(), static_cast<int64_t>(ipc->size()), logical_len);
			};
			auto log_disk_too_large = [&]() {
				cap.aborted.store(true, std::memory_order_relaxed);
				VgiResultCache::Instance().NoteCaptureAbort();
				LogResultCache(context, "result_cache.abort",
				               {{"key_hash", cap.key.HexDigest()},
				                {"reason", "disk_entry_too_large"},
				                {"bytes_seen", std::to_string(cap.disk_writer->Bytes())}});
			};
			if (not_cacheable) {
				// Worker advertised no_store / no freshness key. Log the skip once
				// (exchange → only the first thread to flip `aborted` logs).
				if (!cap.aborted.exchange(true, std::memory_order_relaxed)) {
					LogResultCache(context, "result_cache.store_skipped",
					               {{"function", cap.key.function_name},
					                {"key_hash", cap.key.HexDigest()},
					                {"reason", "not_cacheable"}});
				}
			} else if (cap.streaming()) {
				// Capture has SPILLED to disk. This thread first drains its own RAM
				// substream (once), then appends straight to disk — one batch resident
				// at a time, RAM flat at ~max_entry_bytes no matter the result size.
				if (!local_state.capture_spilled) {
					if (!DrainSubstreamToWriter(*local_state.capture_stream, *cap.disk_writer)) {
						log_disk_too_large();
					}
					local_state.capture_spilled = true;
				}
				if (!cap.aborted.load(std::memory_order_relaxed)) {
					// Compress AT SOURCE (the live batch is here) — one serialize pass, no
					// transcode round-trip on the multi-GB spill path. logical_len is the
					// uncompressed IPC size (cheap, no extra serialize).
					//
					// NOTE (bytes= unit): this uses GetRecordBatchSize (the RecordBatch IPC
					// message size) whereas the memory-capture path (and the pre-threshold
					// spill drain) uses SerializeRecordBatch(uncompressed)->size(), which also
					// counts any IPC stream framing. So the ref's logical bytes= can differ by
					// a few hundred bytes/batch of framing between a spilled-live entry and a
					// memory-persisted one. Deliberately NOT unified: matching it would add an
					// uncompressed serialize to this hot path, or shift reported total_bytes for
					// every memory entry — not worth it for a sub-batch-of-framing delta. The
					// disk byte CAP uses the on-disk COMPRESSED size (consistent), so only the
					// advisory logical bytes= / materialize-vs-stream threshold is affected.
					int64_t logical_len = 0;
					if (!arrow::ipc::GetRecordBatchSize(*arrow_batch, &logical_len).ok()) {
						logical_len = 0;
					}
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
					// Over the memory-tier per-entry cap. If the disk tier is on, SPILL
					// to a streaming disk blob instead of aborting — RAM stays flat from
					// here (a >max_entry_bytes result caches to disk, not uncached).
					bool spilled = false;
					if (!cap.disk_dir.empty() && cap.disk_max > 0) {
						std::lock_guard<std::mutex> lk(cap.mu);
						if (!cap.disk_writer && !cap.aborted.load(std::memory_order_relaxed)) {
							cap.disk_writer = VgiResultCache::Instance().BeginStreamingCapture(
							    cap.key, cap.disk_dir, cap.disk_max, cap.disk_compression,
							    cap.disk_compression_level);
							if (cap.disk_writer) {
								cap.spilling.store(true, std::memory_order_release);
							}
						}
						spilled = cap.disk_writer != nullptr;
					}
					if (spilled) {
						// Buffer THIS batch into my substream, then drain the whole
						// substream (my earlier RAM batches + this one) to disk, in order.
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
						// No disk tier → abort to uncached, keep streaming to DuckDB.
						cap.aborted.store(true, std::memory_order_relaxed);
						VgiResultCache::Instance().NoteCaptureAbort();
						LogResultCache(context, "result_cache.abort",
						               {{"key_hash", cap.key.HexDigest()},
						                {"reason", "entry_too_large"},
						                {"bytes_seen", std::to_string(running)}});
					}
				} else if (!VgiResultCache::Instance().TryReserveInflightCapture(sz)) {
					// [S6] Global in-flight capture budget exhausted (too many
					// concurrent captures) — abort THIS one to uncached, keep
					// streaming. Bounds total transient capture RAM under load.
					cap.aborted.store(true, std::memory_order_relaxed);
					VgiResultCache::Instance().NoteCaptureAbort();
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

// On a 304 not_modified reply, re-insert the stored entry with a slid TTL so
// future lookups hit fresh. The cached batches are unchanged; only freshness
// (and any refreshed validators) is updated. Best-effort — a failed re-insert
// just means the next scan revalidates again.
static void MaybeSlideRevalidatedEntry(ClientContext &context, const VgiTableFunctionBindData &bind_data,
                                       VgiTableFunctionGlobalState &global_state,
                                       const VgiCacheControl &cc) {
	const auto &orig = *global_state.revalidation_entry;
	auto fresh = std::make_shared<VgiResultCacheEntry>(orig); // shallow copy (shared Buffers)
	fresh->stored_at = std::chrono::steady_clock::now();
	// Fresh ttl/expires on the not_modified batch wins; else reuse the prior
	// lifetime duration so a validator-only 304 still slides forward.
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
	VgiResultCache::Instance().Insert(fresh);
	LogResultCache(context, "result_cache.revalidate",
	               {{"catalog", global_state.revalidation_entry->catalog_name},
	                {"function", bind_data.function_name},
	                {"key_hash", global_state.revalidation_entry->key.HexDigest()},
	                {"outcome", "not_modified"}});
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
			return InstallBatch(context, bind_data, global_state, local_state, nullptr);
		}
		if (arrow_batch->num_rows() == 0) {
			// Conditional revalidation (M6): a 0-row vgi.cache.not_modified batch
			// is a 304 — the stored payload is still fresh. Slide its TTL and swap
			// this (single) local state to replay the stored entry. Single-threaded
			// (MaxThreads()==1 while revalidating) makes the swap race-free.
			if (global_state.revalidating && global_state.revalidation_entry &&
			    !global_state.revalidation_served.exchange(true)) {
				auto cc = local_state.connection()->GetLastCacheControl();
				if (cc.not_modified) {
					MaybeSlideRevalidatedEntry(context, bind_data, global_state, cc);
					// Serve the stored entry from here on; abandon the live worker
					// connection and disable the (now-irrelevant) capture.
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
                                   VgiTableFunctionGlobalState &global_state,
                                   VgiTableFunctionLocalState &local_state) {
	auto arrow_batch = std::move(local_state.prefetch_slot_->batch);
	local_state.prefetch_slot_->batch.reset();
	local_state.prefetch_slot_->state.store(PrefetchState::IDLE);
	InstallBatch(context, bind_data, global_state, local_state, std::move(arrow_batch));
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
		// Defense in depth: ArrowTableFunction::ArrowToDuckDB dereferences
		// arrow_array.children[arrow_array_idx] per output column, using the same
		// projection / rowid-shift mapping replicated below. If the worker emitted
		// a batch with fewer children than that mapping reaches — a runtime
		// inconsistency the bind-time schema check in GetScanFunctionImpl didn't
		// catch (e.g. a worker whose scan-time output disagrees with its own
		// declared bind output_schema) — core walks off the end of the children
		// array and the client SIGSEGVs (arrow_conversion.cpp). Bounds-check here
		// so the failure surfaces as a clear IOException naming the function.
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
			CatalogRpcContext rpc_ctx{rpc_params, bind_data.attach_opaque_data, bind_data.transaction_opaque_data};
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

	// Result-cache serve indicator: a HIT skips the worker entirely, so EXPLAIN
	// ANALYZE should say so (and which tier). The worker RPC below is also skipped
	// on a hit (no global_execution_id), so surface this before the early return.
	if (global_state.serving_from_cache) {
		result["Cache"] = (global_state.serving_entry && global_state.serving_entry->disk_backed)
		                      ? "hit (disk_streaming)"
		                      : "hit (memory)";
	} else if (global_state.cache_eligible) {
		// Eligible but not served → a genuine miss (the worker ran; the result may
		// have been captured for next time). An ineligible scan gets no Cache line.
		result["Cache"] = "miss";
	}

	// Worker batch shape. Workers differ enormously in how they chunk a result —
	// one 2M-row batch versus two thousand 1k-row batches produce the same "N
	// rows" in the profile box but very different memory and latency profiles —
	// so report the count and the row distribution. Cache hits replay through
	// the same InstallBatch path, so a served entry reports its stored shape.
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
		CatalogRpcContext rpc_ctx{rpc_params, bind_data.attach_opaque_data, bind_data.transaction_opaque_data};
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
// get_partition_data callback — batch_index for ordered-sink reassembly
// ============================================================================
//
// Registered ONLY for table functions that opt in to
// ``Meta.supports_batch_index = True`` on the worker side (see
// ``vgi_table_function_set.cpp`` for the registration site). Called by
// ``PhysicalTableScan::GetPartitionData`` once per source chunk pulled by
// the pipeline executor; ordered sinks
// (BatchCollector / BatchInsert / BatchCopyToFile / Limit) use the
// returned batch_index to reassemble parallel output in worker-defined
// partition order.
//
// ``local_state.current_batch_index`` was set by ``InstallBatch`` on the
// consumer thread when the corresponding data batch arrived — see the
// ``current_batch_index`` field comment in vgi_table_function_impl.hpp
// for the thread-safety rationale.
//
// INVALID is unreachable here. DuckDB's
// ``pipeline_executor.cpp:130`` gates ``GetPartitionData`` on
// ``source_chunk.size() > 0``, which means ``InstallBatch`` already fired
// at least once with a tagged data batch.
//
// Both halves of ``OperatorPartitionData`` are populated when the
// function opts in to the respective mode; sinks pick what they need.
// For batch_index-only functions, ``current_partition_data`` is empty
// and ignored. For PartitionColumns-only functions,
// ``current_batch_index`` stays INVALID and is ignored (sinks that
// request ``PartitionColumns()`` don't read ``batch_index``).
//
// IMPORTANT: the sink (``PhysicalPartitionedAggregate::Sink``) reads
// ``partition_data[i]`` indexed by its OWN partition columns position
// (i.e. by GROUP BY column index 0..N-1), NOT by the source's
// declared partition column index. So we must return
// ``partition_data`` re-ordered to match
// ``input.partition_info.partition_columns`` — the column indices the
// sink is asking about.
OperatorPartitionData VgiGetPartitionData(ClientContext &, TableFunctionGetPartitionInput &input) {
	auto &local_state = input.local_state->Cast<VgiTableFunctionLocalState>();
	auto &bind_data = input.bind_data->Cast<VgiTableFunctionBindData>();
	OperatorPartitionData result(local_state.current_batch_index);

	// Re-order ``current_partition_data`` (indexed by declared order)
	// into ``result.partition_data`` (indexed by the sink's requested
	// column order). Only emit entries the sink asked for; ignore
	// declared partition columns the sink doesn't care about.
	if (!local_state.current_partition_data.empty() &&
	    !input.partition_info.partition_columns.empty()) {
		result.partition_data.reserve(input.partition_info.partition_columns.size());
		for (column_t requested_col : input.partition_info.partition_columns) {
			// Find this column in the declared partition indices.
			idx_t declared_pos = DConstants::INVALID_INDEX;
			for (idx_t i = 0; i < bind_data.partition_column_indices.size(); ++i) {
				if (bind_data.partition_column_indices[i] == requested_col) {
					declared_pos = i;
					break;
				}
			}
			if (declared_pos == DConstants::INVALID_INDEX) {
				// Should not happen: ``VgiGetPartitionInfo`` returns
				// NOT_PARTITIONED when the planner asks about a column
				// we didn't declare, so the planner shouldn't pick
				// PartitionedAggregate. Belt-and-suspenders.
				throw InternalException(
				    "VGI function '%s': sink requested partition column %llu that "
				    "is not in the declared partition set",
				    bind_data.function_name,
				    static_cast<unsigned long long>(requested_col));
			}
			result.partition_data.push_back(local_state.current_partition_data[declared_pos]);
		}
	}
	return result;
}

// get_partition_info: tells the planner whether the source guarantees a
// particular partition shape over a given set of columns. Called from
// ``CanUsePartitionedAggregate`` (plan_aggregate.cpp:108) during
// GROUP BY planning.
TablePartitionInfo VgiGetPartitionInfo(ClientContext &, TableFunctionPartitionInput &input) {
	auto &bind_data = input.bind_data->Cast<VgiTableFunctionBindData>();
	if (bind_data.partition_kind == VgiPartitionKind::NotPartitioned) {
		return TablePartitionInfo::NOT_PARTITIONED;
	}
	// Every column the planner asks about must be in our declared
	// partition set, otherwise we can't guarantee the partition shape.
	for (auto col_id : input.partition_ids) {
		bool found = false;
		for (idx_t declared : bind_data.partition_column_indices) {
			if (declared == col_id) {
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
