// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "storage/vgi_physical_write.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "storage/vgi_table_function_resolver.hpp"
#include "storage/vgi_transaction.hpp"
#include "vgi_catalog_rpc.hpp"
#include "vgi_column_expression.hpp"
#include "vgi_constraint_contract.hpp"
#include "vgi_exception.hpp"
#include "vgi_function_connection.hpp"
#include "vgi_logging.hpp"
#include "vgi_rpc_types.hpp"
#include "vgi_table_function_impl.hpp"
#include "vgi_transport.hpp"
#include "vgi_worker_pool.hpp"

#include "nanoarrow/nanoarrow.hpp"

#include <cstring>

namespace duckdb {

// Write-input schemas (the Arrow batches the extension streams to an insert,
// update or delete function) mark every field nullable, at every nesting
// level. DuckDB does not enforce a VGI table's NOT NULL before the write call:
// the worker does, per vgi.constraint.enforce.v1, and reports the violation
// itself. Keeping the table's non-nullable flags here would put an INSERT or
// UPDATE of NULL into a NOT NULL column on the wire as an invalid Arrow batch
// (a non-nullable field holding nulls) that a strict Arrow reader rejects
// before the worker can apply its constraint check.
// The one exception: a map's entries struct and its keys stay non-nullable, as
// the Arrow format requires (DuckDB map keys are never NULL); its values are
// marked like any other field.
static void MarkNullableRecursive(ArrowSchema &field) {
	field.flags |= ARROW_FLAG_NULLABLE;
	if (field.format && std::strcmp(field.format, "+m") == 0) {
		if (field.n_children == 1 && field.children[0]->n_children == 2) {
			MarkNullableRecursive(*field.children[0]->children[1]);
		}
		return;
	}
	for (int64_t i = 0; i < field.n_children; ++i) {
		MarkNullableRecursive(*field.children[i]);
	}
	if (field.dictionary) {
		MarkNullableRecursive(*field.dictionary);
	}
}

static vgi::VgiSchemaPtr NullableWriteInputSchema(const vgi::VgiSchemaPtr &schema) {
	nanoarrow::UniqueSchema output;
	schema->ExportTo(output.get());
	for (int64_t i = 0; i < output->n_children; ++i) {
		MarkNullableRecursive(*output->children[i]);
	}
	return vgi::VgiSchema::Adopt(output.get());
}

// Resolve a write-function discovery result, preferring the inlined value on
// VgiTableInfo when the worker populated it (skips the corresponding
// catalog_table_*_function_get RPC). The trampoline parameter dispatches to
// InvokeCatalogTableInsert/Update/DeleteFunctionGet on cache miss.
template <typename FetchFn>
static vgi::VgiScanFunctionResult
ResolveWriteFunction(ClientContext &context, VgiTableEntry &table, const char *operation,
                     const std::optional<vgi::VgiScanFunctionResult> &inlined, FetchFn &&fetch) {
	for (const auto &column : table.GetColumns().Logical()) {
		if (column.Generated()) {
			throw NotImplementedException("VGI native writes to generated-column tables are not supported");
		}
	}
	// Covers CTAS as well as INSERT/UPDATE/DELETE, before either inlined use or
	// fallback discovery. Reading trusted facts grants no write authority.
	vgi::RequireNativeConstraintWrite(*table.GetCatalog().Cast<VgiCatalog>().attach_result(), table.GetTableInfo());
	if (inlined.has_value()) {
		VGI_LOG(context, "vgi.write_function.inlined",
		        {{"schema", table.GetTableInfo().schema_name},
		         {"table", table.GetTableInfo().name},
		         {"operation", operation},
		         {"function", inlined->function_name}});
		return *inlined;
	}
	return fetch();
}

using namespace vgi;

// Resolve the write function's listing entry through the shared resolver
// (vgi_table_function_resolver.hpp), the one the scan paths use: visible
// entries in the worker-claimed schema, the table's schema and the catalog's
// default schema (a virtual ``topics.<topic>`` table may route INSERT to
// ``main.kafka_topic_insert``), then those schemas' hidden (protocol 3.3
// table_only) sets, where every SDK now lists write functions. Write functions
// are always worker RPCs, so the system catalog is never consulted, and an
// unlisted one is a BinderException: before 3.3 it silently lost has_finalize
// and the bind request named no schema.
struct ResolvedWriteFunction {
	//! The schema the function resolved in; the bind request names it, since a
	//! worker may register the same name in several schemas.
	string schema_name;
	//! The function declares a finalize stage (VgiTableFunctionInfo).
	bool has_finalize = false;
};

static ResolvedWriteFunction ResolveWriteFunctionEntry(ClientContext &context, VgiTableEntry &table,
                                                       const char *operation,
                                                       const VgiWriteFunctionResult &write_func) {
	auto &catalog = table.GetCatalog();
	vgi::VgiTableFunctionRequest request;
	request.catalog = &catalog;
	request.table_schema = table.GetTableInfo().schema_name;
	request.table_name = table.GetTableInfo().name;
	request.role = operation;
	request.function_name = write_func.function_name;
	request.worker_schema = write_func.schema_name;
	if (auto default_schema = catalog.GetDefaultSchema()) {
		request.default_schema = default_schema->GetIdentifierName();
	}
	request.allow_system_catalog = false;
	auto resolved = vgi::ResolveVgiTableFunction(context, request);
	ResolvedWriteFunction result;
	result.schema_name = resolved.schema_name;
	for (auto &overload : resolved.entry->functions.functions) {
		if (auto info = dynamic_cast<vgi::VgiTableFunctionInfo *>(overload->function_info.get())) {
			result.has_finalize = info->function_info().has_finalize;
			break;
		}
	}
	return result;
}

// ============================================================================
// Helper: Set up a write connection to a VGI worker function
// ============================================================================

// Build a one-row Arrow RecordBatch of write options to pass to the worker.
// The batch is self-describing and extensible — new options are just new fields.
//
// Protocol 3.3: the batch is exactly ``result_mode: utf8 not null``. The
// former ``on_conflict`` / ``on_conflict_columns`` fields are gone: DuckDB's
// binder rewrites every INSERT ... ON CONFLICT / INSERT OR REPLACE into a
// MERGE before PlanInsert (bind_insert.cpp GenerateMergeInto), so they could
// only ever say "throw" / empty. VGI emulates MERGE with ordinary write calls
// (VgiCatalog::PlanMergeInto).
static VgiBatchPtr BuildWriteOptions(bool return_chunk) {
	static const VgiSchemaPtr schema = MakeSchema({
	    type::Utf8().Named("result_mode", /*is_nullable=*/false),
	});
	VgiBatchBuilder builder(schema);
	append::String(builder.column(0), return_chunk ? "rows" : "count");
	builder.FinishRow();
	return builder.Finish();
}

struct SetupWriteResult {
	std::unique_ptr<IFunctionConnection> connection;
	BindResult bind_result;
};

static SetupWriteResult SetupWriteConnection(ClientContext &context, VgiTableEntry &table,
                                             const VgiWriteFunctionResult &write_func,
                                             const ResolvedWriteFunction &resolved, const VgiSchemaPtr &input_schema,
                                             const VgiBatchPtr &write_options,
                                             const std::vector<uint8_t> &transaction_opaque_data) {
	auto &catalog = table.GetCatalog().Cast<VgiCatalog>();
	auto &params = catalog.attach_parameters();
	auto &attach_result = catalog.attach_result();

	// Pass write_options as a named argument (serialized as binary)
	// Include positional arguments from the write function result (e.g., table_name for generic functions)
	vector<std::pair<string, Value>> named_args;
	if (write_options) {
		auto options_bytes = SerializeToIpcBytes(write_options);
		named_args.emplace_back("write_options", Value::BLOB(options_bytes.data(), options_bytes.size()));
	}
	for (auto &[k, v] : write_func.named_arguments) {
		named_args.emplace_back(k, v);
	}
	auto arguments = BuildArgumentsFromValues(context, write_func.positional_arguments, named_args);

	// Try to reuse a pooled worker connection (subprocess transport only).
	std::unique_ptr<IFunctionConnection> connection;
	if (!IsHttpTransport(params->worker_path())) {
		PoolKey pool_key {params->worker_path(), params->data_version_spec(), params->implementation_version()};
		auto pooled = VgiWorkerPool::Instance().TryAcquire(pool_key);
		if (pooled) {
			connection = CreateFunctionConnectionFromPool(std::move(pooled), write_func.function_name, arguments,
			                                              attach_result->attach_opaque_data, transaction_opaque_data,
			                                              context, "TABLE", {}, params->worker_debug());
		}
	}
	if (!connection) {
		connection = CreateFunctionConnection(params->worker_path(), write_func.function_name, arguments,
		                                      attach_result->attach_opaque_data, transaction_opaque_data, context,
		                                      "TABLE", {}, params->worker_debug(), {}, {}, params);
	}

	connection->SetInputSchema(input_schema);
	connection->SetSchemaName(resolved.schema_name);
	auto bind_result = connection->PerformBindRpc();
	connection->PerformInit(bind_result, {}, nullptr, {}, "INPUT");
	connection->OpenInputWriter();

	return SetupWriteResult {std::move(connection), std::move(bind_result)};
}

// Helper: Validate that a worker-emitted RETURNING batch matches the table's
// user-column schema. Without this, a worker that mistakenly emits a
// (count BIGINT) batch (or any wrong shape) for a RETURNING write triggers
// a SIGSEGV inside ArrowTableFunction::ArrowToDuckDB — that converter
// dereferences ArrowTableSchema metadata for each input column position and
// trusts the caller to have aligned the schema.
//
// We compare field count + per-field name + type. Nullability is intentionally
// ignored because DuckDB's Arrow exporter forces nullable=true on every field.
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

static void ValidateReturningSchema(const VgiSchemaPtr &expected, const VgiSchemaPtr &actual, const string &op_name,
                                    const string &table_name) {
	if (!expected) {
		throw InternalException("ValidateReturningSchema called without expected schema (op=%s table=%s)", op_name,
		                        table_name);
	}
	auto fail = [&](const string &reason) {
		throw IOException(ErrorInfo(error_subtype::kProtocolViolation)
		                      .Set(error_key::kTable, table_name)
		                      .Set(error_key::kOperation, op_name)
		                      .ExpectedActual(expected->ToString(), actual ? actual->ToString() : "<null>"),
		                  "VGI worker emitted an incompatible RETURNING batch for %s on table '%s': %s. "
		                  "Expected schema: %s. Actual schema: %s. "
		                  "The worker must advertise at least result mode 'rows' for this operation.",
		                  op_name, table_name, reason, expected->ToString(), actual ? actual->ToString() : "<null>");
	};
	if (!actual) {
		fail("response had no schema");
	}
	if (expected->num_fields() != actual->num_fields()) {
		fail(StringUtil::Format("expected %d field(s), got %d", expected->num_fields(), actual->num_fields()));
	}
	for (int i = 0; i < expected->num_fields(); i++) {
		const auto &ef = expected->field(i);
		const auto &af = actual->field(i);
		const string ef_name = expected->field_name(i);
		const string af_name = actual->field_name(i);
		if (ef_name != af_name) {
			fail(StringUtil::Format("field %d name mismatch (expected '%s', got '%s')", i, ef_name, af_name));
		}
		if (!SameDataType(ef, af)) {
			fail(StringUtil::Format("field '%s' type mismatch (expected %s, got %s)", ef_name, TypeToString(ef),
			                        TypeToString(af)));
		}
	}
}

// Helper: Read the count from a response batch (count column)
static idx_t ReadCountFromBatch(const VgiBatchPtr &batch) {
	if (!batch || batch->num_rows() == 0) {
		return 0;
	}
	const auto &schema = batch->schema();
	if (!schema || schema->num_fields() != 1 || schema->field_name(0) != "count" ||
	    std::strcmp(schema->field(0).format, "l") != 0 || schema->field(0).dictionary != nullptr ||
	    schema->field_nullable(0)) {
		throw IOException(
		    ErrorInfo(error_subtype::kProtocolViolation)
		        .Set(error_key::kField, "count")
		        .ExpectedActual("count: int64 not null", schema ? schema->ToString() : "<null>"),
		    "VGI worker emitted an incompatible count result: expected exactly 'count: int64 not null', got %s",
		    schema ? schema->ToString() : "<null>");
	}
	auto count_col = batch->column(0);
	if (!count_col) {
		throw IOException(ErrorInfo(error_subtype::kProtocolViolation).Set(error_key::kField, "count"),
		                  "VGI worker emitted an incompatible count result array");
	}
	idx_t total = 0;
	const int64_t n = count_col.length();
	for (int64_t i = 0; i < n; i++) {
		if (count_col.IsNull(i)) {
			throw IOException(ErrorInfo(error_subtype::kProtocolViolation).Set(error_key::kField, "count"),
			                  "VGI worker emitted a null count result");
		}
		total += NumericCast<idx_t>(count_col.GetInt(i));
	}
	return total;
}

// Helper: Finalize a write connection — read remaining responses, destroy connection.
//
// When ``gstate.has_finalize`` is true (worker declared has_finalize on the
// write function's metadata), invoke ``PerformFinalizeInit`` instead of a
// plain ``CloseInputWriter`` — that triggers the worker's
// ``cls.finalize(params)`` classmethod and lets it stream a final batch (or
// raise) for end-of-statement reconciliation. The standard ReadDataBatch
// loop below picks up the finalize-phase output the same way it picks up
// per-Sink response batches.
static idx_t FinalizeWriteConnection(VgiWriteGlobalState &gstate) {
	if (gstate.has_finalize) {
		// PerformFinalizeInit closes the input writer internally and re-runs
		// init on the worker with phase=FINALIZE.
		gstate.connection->PerformFinalizeInit(gstate.bind_result);
	} else {
		gstate.connection->CloseInputWriter();
	}

	idx_t total = 0;
	while (true) {
		auto batch = gstate.connection->ReadDataBatch();
		if (!batch) {
			break; // EOS = null batch from IPC reader
		}
		if (batch->num_rows() == 0) {
			continue; // 0-row batch = valid "no output", not EOS
		}
		total += ReadCountFromBatch(batch);
	}

	gstate.connection.reset();
	return total;
}

// Helper: Load an Arrow RecordBatch into an ArrowScanLocalState.
// Same pattern as LoadBatchIntoScanState in vgi_table_in_out_impl.cpp.
static void LoadResponseBatch(ArrowScanLocalState &scan_state, const VgiBatchPtr &batch) {
	auto chunk = make_uniq<ArrowArrayWrapper>();
	ExportRecordBatch(batch, *chunk);
	scan_state.chunk = shared_ptr<ArrowArrayWrapper>(chunk.release());
	scan_state.chunk_offset = 0;
	// Reset() clears owned_data in array_states — REQUIRED so ArrowToDuckDB
	// takes ownership of the new chunk's data
	scan_state.Reset();
}

// Helper: Produce count or RETURNING rows from source
static SourceResultType WriteGetDataInternal(VgiWriteGlobalState &gstate, VgiWriteSourceState &source_state,
                                             DataChunk &chunk, bool return_chunk) {
	if (!return_chunk) {
		if (source_state.returned) {
			return SourceResultType::FINISHED;
		}
		source_state.returned = true;
		chunk.SetCardinality(1);
		chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.changed_count)));
		return SourceResultType::FINISHED;
	}

	// RETURNING mode: scan from ColumnDataCollection (deep-copied in Sink)
	if (!source_state.returned) {
		gstate.return_collection.InitializeScan(source_state.scan_state);
		source_state.returned = true;
	}
	if (!gstate.return_collection.Scan(source_state.scan_state, chunk)) {
		return SourceResultType::FINISHED;
	}
	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

// DuckDB appends merge_action to a MERGE RETURNING result; the enclosing
// PhysicalMergeInto supplies that value after each action returns table rows.
static vector<LogicalType> WriteOutputTypes(const LogicalOperator &op, bool returning) {
	auto types = op.types;
	if (returning && op.type == LogicalOperatorType::LOGICAL_MERGE_INTO) {
		D_ASSERT(!types.empty());
		types.pop_back();
	}
	return types;
}

// ============================================================================
// INSERT Implementation
// ============================================================================

VgiPhysicalInsert::VgiPhysicalInsert(PhysicalPlan &plan, LogicalOperator &op, VgiTableEntry &table_p,
                                     bool return_chunk_p)
    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, WriteOutputTypes(op, return_chunk_p),
                       op.estimated_cardinality),
      insert_table(&table_p), return_chunk(return_chunk_p) {
}

VgiPhysicalInsert::VgiPhysicalInsert(PhysicalPlan &plan, LogicalOperator &op, SchemaCatalogEntry &schema_p,
                                     unique_ptr<BoundCreateTableInfo> create_info_p)
    : PhysicalOperator(plan, PhysicalOperatorType::CREATE_TABLE_AS, op.types, op.estimated_cardinality),
      schema(&schema_p), create_info(std::move(create_info_p)), return_chunk(false) {
}

InsertionOrderPreservingMap<string> VgiPhysicalInsert::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	if (insert_table) {
		result["Table"] = insert_table->name.GetIdentifierName();
	} else if (create_info) {
		result["Table"] = create_info->base->Cast<CreateTableInfo>().GetTableName().GetIdentifierName();
	}
	return result;
}

// Write-refusal split for multi-branch tables. INSERT is permitted when
// exactly one branch declares writable=true. UPDATE/DELETE/MERGE always
// refuse on multi-branch tables — the cross-arm-target semantics question
// has no clean answer (loud-error / silent-scope / refuse each carry UX
// trade-offs without engine precedent), and the conservative choice is
// to refuse and direct the user to per-branch dispatch via a
// catalog scan of the specific branch.
//
// Both helpers issue a fresh branches RPC (via catalog_table_scan_branches_get,
// which falls back to the legacy single-function path for older workers).
// For single-branch tables (size <= 1) they're no-ops; only multi-branch
// tables hit the refusal logic. Writes don't use AT (...) so empty strings
// are correct here.

// INSERT permits writes when at least one branch declares writable=true.
// (ParseScanBranchesResult already enforces at-most-one-writable, so the
// linear scan below sees 0 or 1 writable branches.) Short-circuits on
// the cheap multi-branch hint to avoid an RPC on every INSERT to a
// single-branch VGI table.
//
// Returns the writable arm's function_name when the table is multi-branch
// and a writable arm exists; std::nullopt when the table is single-branch
// (no disambiguation needed). The C++ caller forwards this value to the
// worker via the catalog_table_insert_function_get RPC so the worker can
// dispatch the INSERT against a verified key, instead of re-resolving the
// writable arm internally and hoping the C++ side and the worker agree.
static std::optional<std::string> RefuseInsertIfMultiBranchUnwritable(ClientContext &context,
                                                                      VgiTableEntry &table_entry) {
	if (table_entry.IsKnownSingleBranchNoAT()) {
		return std::nullopt; // cheap path: no RPC, no multi-branch refusal applies
	}
	auto branches_result = table_entry.FetchScanBranches(context, /*at_unit=*/"", /*at_value=*/"");
	if (branches_result.branches.size() <= 1) {
		return std::nullopt; // single-branch path, unchanged (hint now populated)
	}
	for (const auto &branch : branches_result.branches) {
		if (branch.writable) {
			return branch.function_name; // writable arm exists; INSERT proceeds
		}
	}
	throw BinderException(
	    ErrorInfo(error_subtype::kUnsupported)
	        .Set(error_key::kTable,
	             table_entry.ParentSchema().name.GetIdentifierName() + "." + table_entry.name.GetIdentifierName())
	        .Set(error_key::kOperation, "INSERT"),
	    StringUtil::Format("Multi-branch VGI table '%s.%s' is read-only — no branch declared writable=true. "
	                       "INSERT is not supported. (Set writable=true on exactly one branch in the worker's "
	                       "ScanBranchesResult to enable INSERT routing to that arm.)",
	                       table_entry.ParentSchema().name.GetIdentifierName(), table_entry.name.GetIdentifierName()));
}

// UPDATE/DELETE/MERGE always refuse on multi-branch tables, regardless of
// any branch's writable flag. The cross-arm-target UX question is open
// (loud-error vs. silent-scope vs. refuse) and refusing is the
// conservative answer until concrete customer requirements arrive.
// Short-circuits on the cheap multi-branch hint to avoid an RPC on every
// write to a single-branch VGI table.
static void RefuseUpdateDeleteIfMultiBranch(ClientContext &context, VgiTableEntry &table_entry, const char *operation) {
	if (table_entry.IsKnownSingleBranchNoAT()) {
		return; // cheap path: no RPC, no multi-branch refusal applies
	}
	auto branches_result = table_entry.FetchScanBranches(context, /*at_unit=*/"", /*at_value=*/"");
	if (branches_result.branches.size() <= 1) {
		return; // single-branch path, unchanged (hint now populated)
	}
	throw BinderException(ErrorInfo(error_subtype::kUnsupported)
	                          .Set(error_key::kTable, table_entry.ParentSchema().name.GetIdentifierName() + "." +
	                                                      table_entry.name.GetIdentifierName())
	                          .Set(error_key::kOperation, operation),
	                      StringUtil::Format("%s is not supported on multi-branch VGI table '%s.%s' (%d branches). "
	                                         "Issue the %s directly against the writable arm's underlying VGI table "
	                                         "(declare it as a single-branch VGI table for write access). "
	                                         "(UPDATE/DELETE/MERGE on multi-branch tables is not supported pending "
	                                         "concrete customer requirements for cross-arm semantics — see "
	                                         "docs/multi_branch.md.)",
	                                         operation, table_entry.ParentSchema().name.GetIdentifierName(),
	                                         table_entry.name.GetIdentifierName(),
	                                         static_cast<int>(branches_result.branches.size()), operation));
}

unique_ptr<GlobalSinkState> VgiPhysicalInsert::GetGlobalSinkState(ClientContext &context) const {
	optional_ptr<VgiTableEntry> table;
	if (create_info) {
		// CREATE TABLE AS: create the table now (inside the transaction)
		auto &schema_ref = *schema.get_mutable();
		auto result = schema_ref.CreateTable(schema_ref.GetCatalogTransaction(context), *create_info);
		if (!result) {
			// IF NOT EXISTS found an existing table (catalog_table_create
			// replied created=false): CTAS inserts nothing into it.
			auto gstate = make_uniq<VgiWriteGlobalState>(context, GetTypes(), false);
			gstate->changed_count = 0;
			return unique_ptr<GlobalSinkState>(gstate.release());
		}
		table = &result->Cast<VgiTableEntry>();
		// This statement created the table. If the CTAS then fails on a
		// non-transactional attach, rollback drops it again (best effort,
		// VgiTransaction::Rollback); Finalize clears the mark on success.
		auto &create_tx = VgiTransaction::Get(context, table->GetCatalog());
		if (create_tx.GetTransactionOpaqueData().empty()) {
			create_tx.AddPendingCtasTable(table->GetTableInfo().schema_name, table->GetTableInfo().name);
		}
	} else {
		D_ASSERT(insert_table);
		table = insert_table;
	}

	auto &catalog = table->GetCatalog().Cast<VgiCatalog>();
	auto &params = catalog.attach_parameters();
	auto &attach_result = catalog.attach_result();

	auto writable_branch_function_name = RefuseInsertIfMultiBranchUnwritable(context, *table);

	auto &vgi_tx = VgiTransaction::Get(context, table->GetCatalog());
	CatalogRpcContext rpc_ctx {params, attach_result->attach_opaque_data, vgi_tx.GetTransactionOpaqueData()};
	auto write_func = ResolveWriteFunction(context, *table, "insert", table->GetTableInfo().insert_function, [&]() {
		return InvokeCatalogTableInsertFunctionGet(rpc_ctx, table->GetTableInfo().schema_name,
		                                           table->GetTableInfo().name, context, writable_branch_function_name);
	});

	// Build input schema (table columns minus row_id)
	auto &table_info = table->GetTableInfo();
	// Catalog defaults have already been validated and bound by the table
	// adapter. Value streams carry their physical types, not SQL definitions.
	auto input_schema = NativeColumnTypeSchema(table_info.arrow_schema, table_info.schema_name);
	if (table_info.row_id_column >= 0) {
		// A fresh schema of the remaining fields (no schema-level metadata).
		input_schema = input_schema->RemoveField(table_info.row_id_column)->WithMetadata(VgiMetadata());
	}

	auto write_options = BuildWriteOptions(return_chunk);

	auto gstate = make_uniq<VgiWriteGlobalState>(context, GetTypes(), return_chunk);
	if (return_chunk) {
		// RETURNING expects the worker to emit batches with the table's
		// user-column schema (sans rowid). Same shape used by GetLocalSinkState.
		gstate->expected_returning_schema = input_schema;
	}
	input_schema = NullableWriteInputSchema(input_schema);
	gstate->send_schema = input_schema;
	const auto resolved_write = ResolveWriteFunctionEntry(context, *table, "insert", write_func);
	gstate->has_finalize = resolved_write.has_finalize;
	{
		auto setup = SetupWriteConnection(context, *table, write_func, resolved_write, input_schema, write_options,
		                                  vgi_tx.GetTransactionOpaqueData());
		gstate->connection = std::move(setup.connection);
		gstate->bind_result = std::move(setup.bind_result);
	}
	if (create_info) {
		gstate->ctas_table = table;
	}

	return unique_ptr<GlobalSinkState>(gstate.release());
}

unique_ptr<LocalSinkState> VgiPhysicalInsert::GetLocalSinkState(ExecutionContext &context) const {
	// Build response schema (table columns sans row_id) for RETURNING Arrow→DuckDB conversion
	// For CTAS, insert_table may be null (table created in GetGlobalSinkState), but
	// return_chunk is always false so the response schema is only used for count output.
	if (insert_table) {
		auto &table_info = insert_table->GetTableInfo();
		auto response_schema = NativeColumnTypeSchema(table_info.arrow_schema, table_info.schema_name);
		if (table_info.row_id_column >= 0) {
			response_schema = response_schema->RemoveField(table_info.row_id_column)->WithMetadata(VgiMetadata());
		}
		return make_uniq<VgiWriteLocalSinkState>(context.client, response_schema, GetTypes());
	}
	// CTAS: use a minimal schema — RETURNING is never used for CTAS
	auto empty_schema = VgiSchema::Empty();
	return make_uniq<VgiWriteLocalSinkState>(context.client, empty_schema, GetTypes());
}

SinkResultType VgiPhysicalInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<VgiWriteGlobalState>();
	if (!gstate.connection) {
		// CTAS with IF NOT EXISTS and table already exists — discard input
		return SinkResultType::FINISHED;
	}
	// Defaults are already resolved by DuckDB's PhysicalProjection child
	// (inserted by ResolveDefaultsProjection in PlanInsert). The chunk
	// arriving here has the full table column set with defaults filled in.
	auto batch = DataChunkToArrow(context.client, chunk, gstate.send_schema);

	{
		lock_guard<mutex> guard(gstate.write_lock);
		gstate.connection->WriteInputBatch(batch);

		auto response = gstate.connection->ReadDataBatch();
		if (response && response->num_rows() > 0) {
			if (gstate.return_chunk) {
				// For DO NOTHING, response may have fewer rows than input (conflicts skipped)
				D_ASSERT(NumericCast<idx_t>(response->num_rows()) <= chunk.size());
				D_ASSERT(NumericCast<idx_t>(response->num_rows()) <= STANDARD_VECTOR_SIZE);

				ValidateReturningSchema(
				    gstate.expected_returning_schema, response->schema(), "INSERT",
				    insert_table ? insert_table->name.GetIdentifierName()
				                 : create_info->base->Cast<CreateTableInfo>().GetTableName().GetIdentifierName());

				auto &lstate = input.local_state.Cast<VgiWriteLocalSinkState>();

				LoadResponseBatch(lstate.arrow_state, response);

				lstate.returning_chunk.Reset();
				lstate.returning_chunk.SetCardinality(NumericCast<idx_t>(response->num_rows()));
				ArrowTableFunction::ArrowToDuckDB(lstate.arrow_state, lstate.response_arrow_table.GetColumns(),
				                                  lstate.returning_chunk, false);

				gstate.return_collection.Append(lstate.returning_chunk);
				gstate.changed_count += NumericCast<idx_t>(response->num_rows());
			} else {
				gstate.changed_count += ReadCountFromBatch(response);
			}
		}
	}

	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType VgiPhysicalInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                             OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<VgiWriteGlobalState>();
	if (gstate.connection) {
		auto finalize_count = FinalizeWriteConnection(gstate);
		D_ASSERT(!gstate.return_chunk || finalize_count == 0);
		gstate.changed_count += finalize_count;
	}
	if (gstate.ctas_table) {
		// The CTAS completed: the created table stays even if a later
		// statement of the same transaction fails.
		VgiTransaction::Get(context, gstate.ctas_table->GetCatalog())
		    .RemovePendingCtasTable(gstate.ctas_table->GetTableInfo().schema_name,
		                            gstate.ctas_table->GetTableInfo().name);
	}
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSourceState> VgiPhysicalInsert::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<VgiWriteSourceState>();
}

SourceResultType VgiPhysicalInsert::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                    OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<VgiWriteGlobalState>();
	auto &source_state = input.global_state.Cast<VgiWriteSourceState>();
	return WriteGetDataInternal(gstate, source_state, chunk, return_chunk);
}

// ============================================================================
// DELETE Implementation
// ============================================================================

VgiPhysicalDelete::VgiPhysicalDelete(PhysicalPlan &plan, LogicalDelete &op, VgiTableEntry &table_p, bool return_chunk_p,
                                     idx_t rowid_index_p)
    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, WriteOutputTypes(op, return_chunk_p),
                       op.estimated_cardinality),
      table(table_p), return_chunk(return_chunk_p), rowid_index(rowid_index_p) {
}

VgiPhysicalDelete::VgiPhysicalDelete(PhysicalPlan &plan, LogicalOperator &op, VgiTableEntry &table_p,
                                     bool return_chunk_p, idx_t rowid_index_p)
    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, WriteOutputTypes(op, return_chunk_p),
                       op.estimated_cardinality),
      table(table_p), return_chunk(return_chunk_p), rowid_index(rowid_index_p) {
}

InsertionOrderPreservingMap<string> VgiPhysicalDelete::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table"] = table.name.GetIdentifierName();
	return result;
}

unique_ptr<GlobalSinkState> VgiPhysicalDelete::GetGlobalSinkState(ClientContext &context) const {
	auto &catalog = table.GetCatalog().Cast<VgiCatalog>();
	auto &params = catalog.attach_parameters();
	auto &attach_result = catalog.attach_result();

	RefuseUpdateDeleteIfMultiBranch(context, const_cast<VgiTableEntry &>(table), "DELETE");

	auto &vgi_tx = VgiTransaction::Get(context, table.GetCatalog());
	CatalogRpcContext rpc_ctx {params, attach_result->attach_opaque_data, vgi_tx.GetTransactionOpaqueData()};
	auto write_func = ResolveWriteFunction(context, table, "delete", table.GetTableInfo().delete_function, [&]() {
		return InvokeCatalogTableDeleteFunctionGet(rpc_ctx, table.GetTableInfo().schema_name, table.GetTableInfo().name,
		                                           context);
	});

	auto &table_info = table.GetTableInfo();
	auto value_schema = NativeColumnTypeSchema(table_info.arrow_schema, table_info.schema_name);
	auto input_schema =
	    NullableWriteInputSchema(value_schema->Select({table_info.row_id_column})->WithMetadata(VgiMetadata()));

	auto write_options = BuildWriteOptions(return_chunk);

	auto gstate = make_uniq<VgiWriteGlobalState>(context, GetTypes(), return_chunk);
	gstate->send_schema = input_schema;
	if (return_chunk) {
		// DELETE RETURNING surfaces the deleted rows in the table's user-column
		// schema (sans rowid). Same shape used by GetLocalSinkState below.
		auto response_schema = value_schema;
		if (table_info.row_id_column >= 0) {
			response_schema = response_schema->RemoveField(table_info.row_id_column)->WithMetadata(VgiMetadata());
		}
		gstate->expected_returning_schema = response_schema;
	}
	const auto resolved_write = ResolveWriteFunctionEntry(context, table, "delete", write_func);
	gstate->has_finalize = resolved_write.has_finalize;
	{
		auto setup = SetupWriteConnection(context, table, write_func, resolved_write, input_schema, write_options,
		                                  vgi_tx.GetTransactionOpaqueData());
		gstate->connection = std::move(setup.connection);
		gstate->bind_result = std::move(setup.bind_result);
	}

	return unique_ptr<GlobalSinkState>(gstate.release());
}

unique_ptr<LocalSinkState> VgiPhysicalDelete::GetLocalSinkState(ExecutionContext &context) const {
	auto &table_info = table.GetTableInfo();
	auto response_schema = NativeColumnTypeSchema(table_info.arrow_schema, table_info.schema_name);
	if (table_info.row_id_column >= 0) {
		response_schema = response_schema->RemoveField(table_info.row_id_column)->WithMetadata(VgiMetadata());
	}
	return make_uniq<VgiWriteLocalSinkState>(context.client, response_schema, GetTypes());
}

SinkResultType VgiPhysicalDelete::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<VgiWriteGlobalState>();

	DataChunk rowid_chunk;
	rowid_chunk.Initialize(context.client, {chunk.data[rowid_index].GetType()});
	rowid_chunk.data[0].Reference(chunk.data[rowid_index]);
	rowid_chunk.SetCardinality(chunk.size());

	{
		lock_guard<mutex> guard(gstate.write_lock);
		// DuckDB does not de-duplicate the row ids of a DELETE on a non-native
		// table: DELETE ... USING with several matching source rows sends a
		// target row once per match. Send each row id once per write call, so
		// the worker counts it once and RETURNING lists it once.
		const auto kept = gstate.deleted_rowids.Filter(rowid_chunk);
		if (kept == 0) {
			return SinkResultType::NEED_MORE_INPUT;
		}
		auto batch = DataChunkToArrow(context.client, rowid_chunk, gstate.send_schema);
		gstate.connection->WriteInputBatch(batch);

		auto response = gstate.connection->ReadDataBatch();
		if (response && response->num_rows() > 0) {
			if (gstate.return_chunk) {
				ValidateReturningSchema(gstate.expected_returning_schema, response->schema(), "DELETE",
				                        table.name.GetIdentifierName());
				auto &lstate = input.local_state.Cast<VgiWriteLocalSinkState>();
				LoadResponseBatch(lstate.arrow_state, response);
				lstate.returning_chunk.Reset();
				lstate.returning_chunk.SetCardinality(NumericCast<idx_t>(response->num_rows()));
				ArrowTableFunction::ArrowToDuckDB(lstate.arrow_state, lstate.response_arrow_table.GetColumns(),
				                                  lstate.returning_chunk, false);
				gstate.return_collection.Append(lstate.returning_chunk);
				gstate.changed_count += NumericCast<idx_t>(response->num_rows());
			} else {
				gstate.changed_count += ReadCountFromBatch(response);
			}
		}
	}

	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType VgiPhysicalDelete::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                             OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<VgiWriteGlobalState>();
	auto finalize_count = FinalizeWriteConnection(gstate);
	D_ASSERT(!gstate.return_chunk || finalize_count == 0);
	gstate.changed_count += finalize_count;
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSourceState> VgiPhysicalDelete::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<VgiWriteSourceState>();
}

SourceResultType VgiPhysicalDelete::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                    OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<VgiWriteGlobalState>();
	auto &source_state = input.global_state.Cast<VgiWriteSourceState>();
	return WriteGetDataInternal(gstate, source_state, chunk, return_chunk);
}

// ============================================================================
// UPDATE Implementation
// ============================================================================

VgiPhysicalUpdate::VgiPhysicalUpdate(PhysicalPlan &plan, LogicalOperator &op, VgiTableEntry &table_p,
                                     bool return_chunk_p, vector<PhysicalIndex> columns_p,
                                     vector<unique_ptr<Expression>> expressions_p,
                                     const vector<unique_ptr<Expression>> &bound_defaults_p)
    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, WriteOutputTypes(op, return_chunk_p),
                       op.estimated_cardinality),
      table(table_p), return_chunk(return_chunk_p), columns(std::move(columns_p)),
      expressions(std::move(expressions_p)) {
	D_ASSERT(columns.size() == expressions.size());
	for (auto &def : bound_defaults_p) {
		bound_defaults.push_back(def ? def->Copy() : nullptr);
	}
}

InsertionOrderPreservingMap<string> VgiPhysicalUpdate::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table"] = table.name.GetIdentifierName();
	return result;
}

unique_ptr<GlobalSinkState> VgiPhysicalUpdate::GetGlobalSinkState(ClientContext &context) const {
	auto &catalog = table.GetCatalog().Cast<VgiCatalog>();
	auto &params = catalog.attach_parameters();
	auto &attach_result = catalog.attach_result();

	RefuseUpdateDeleteIfMultiBranch(context, const_cast<VgiTableEntry &>(table), "UPDATE");

	auto &vgi_tx = VgiTransaction::Get(context, table.GetCatalog());
	CatalogRpcContext rpc_ctx {params, attach_result->attach_opaque_data, vgi_tx.GetTransactionOpaqueData()};
	auto write_func = ResolveWriteFunction(context, table, "update", table.GetTableInfo().update_function, [&]() {
		return InvokeCatalogTableUpdateFunctionGet(rpc_ctx, table.GetTableInfo().schema_name, table.GetTableInfo().name,
		                                           context);
	});

	auto &table_info = table.GetTableInfo();
	auto &table_columns = table.GetColumns();
	auto value_schema = NativeColumnTypeSchema(table_info.arrow_schema, table_info.schema_name);

	// For each updated column, look the field up by name in the worker's
	// declared table schema rather than re-deriving via DuckDB's Arrow
	// converter, so the worker sees its declared types. Every field is then
	// marked nullable (NullableWriteInputSchema): NOT NULL is the worker's to
	// enforce. The C++ ReconcileBatchToSchema helper reshapes incoming
	// batches to match this schema before they hit the IPC writer.
	VgiSchemaPtr input_schema = VgiSchema::Empty();
	for (auto &col_idx : columns) {
		auto &col = table_columns.GetColumn(col_idx);
		auto declared_idx = value_schema->GetFieldIndex(col.Name().GetIdentifierName());
		if (declared_idx >= 0) {
			input_schema = input_schema->AddField(input_schema->num_fields(), value_schema->field(declared_idx));
		} else {
			auto arrow_type = BuildArrowSchemaFromDuckDB(context, {col.Type()}, {col.Name()});
			input_schema = input_schema->AddField(input_schema->num_fields(), arrow_type->field(0));
		}
	}
	input_schema = input_schema->AddField(input_schema->num_fields(), value_schema->field(table_info.row_id_column));
	input_schema = NullableWriteInputSchema(input_schema);

	auto write_options = BuildWriteOptions(return_chunk);

	auto gstate = make_uniq<VgiWriteGlobalState>(context, GetTypes(), return_chunk);
	gstate->send_schema = input_schema;
	if (return_chunk) {
		// UPDATE RETURNING surfaces the updated rows in the table's user-column
		// schema (sans rowid). Same shape used by GetLocalSinkState below.
		auto response_schema = value_schema;
		if (table_info.row_id_column >= 0) {
			response_schema = response_schema->RemoveField(table_info.row_id_column)->WithMetadata(VgiMetadata());
		}
		gstate->expected_returning_schema = response_schema;
	}
	const auto resolved_write = ResolveWriteFunctionEntry(context, table, "update", write_func);
	gstate->has_finalize = resolved_write.has_finalize;
	{
		auto setup = SetupWriteConnection(context, table, write_func, resolved_write, input_schema, write_options,
		                                  vgi_tx.GetTransactionOpaqueData());
		gstate->connection = std::move(setup.connection);
		gstate->bind_result = std::move(setup.bind_result);
	}

	return unique_ptr<GlobalSinkState>(gstate.release());
}

unique_ptr<LocalSinkState> VgiPhysicalUpdate::GetLocalSinkState(ExecutionContext &context) const {
	auto &table_info = table.GetTableInfo();
	auto response_schema = NativeColumnTypeSchema(table_info.arrow_schema, table_info.schema_name);
	if (table_info.row_id_column >= 0) {
		response_schema = response_schema->RemoveField(table_info.row_id_column)->WithMetadata(VgiMetadata());
	}
	return make_uniq<VgiWriteLocalSinkState>(context.client, response_schema, GetTypes());
}

SinkResultType VgiPhysicalUpdate::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<VgiWriteGlobalState>();

	// Like DuckDB's PhysicalUpdate: the child emits the new values followed by
	// the row id (last column). Each update expression is a reference into the
	// child's columns, or DEFAULT. A plain UPDATE gets this layout from the
	// binder's projection, a MERGE UPDATE action from the action projection
	// VgiCatalog::PlanMergeInto builds.
	const idx_t num_update_cols = columns.size();
	const idx_t rowid_col = chunk.ColumnCount() - 1;

	DataChunk update_chunk;
	vector<LogicalType> update_types;
	for (idx_t i = 0; i < num_update_cols; i++) {
		update_types.push_back(expressions[i]->GetReturnType());
	}
	update_types.push_back(chunk.data[rowid_col].GetType());

	update_chunk.Initialize(context.client, update_types);
	for (idx_t i = 0; i < num_update_cols; i++) {
		auto &expr = *expressions[i];
		if (expr.GetExpressionType() == ExpressionType::BOUND_REF) {
			update_chunk.data[i].Reference(chunk.data[expr.Cast<BoundReferenceExpression>().Index()]);
			continue;
		}
		if (expr.GetExpressionType() != ExpressionType::VALUE_DEFAULT) {
			throw InternalException("VGI UPDATE expects column references or DEFAULT, got %s", expr.ToString());
		}
		// SET col = DEFAULT: evaluate the column's bound default for every row.
		const auto storage_idx = table.GetColumns().GetColumn(columns[i]).StorageOid();
		if (storage_idx < bound_defaults.size() && bound_defaults[storage_idx]) {
			ExpressionExecutor executor(context.client, *bound_defaults[storage_idx]);
			executor.ExecuteExpression(chunk, update_chunk.data[i]);
		} else {
			// No declared default: NULL.
			update_chunk.data[i].SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(update_chunk.data[i], true);
		}
	}
	update_chunk.data[num_update_cols].Reference(chunk.data[rowid_col]);
	update_chunk.SetCardinality(chunk.size());

	auto batch = DataChunkToArrow(context.client, update_chunk, gstate.send_schema);

	{
		lock_guard<mutex> guard(gstate.write_lock);
		gstate.connection->WriteInputBatch(batch);

		auto response = gstate.connection->ReadDataBatch();
		if (response && response->num_rows() > 0) {
			if (gstate.return_chunk) {
				ValidateReturningSchema(gstate.expected_returning_schema, response->schema(), "UPDATE",
				                        table.name.GetIdentifierName());
				auto &lstate = input.local_state.Cast<VgiWriteLocalSinkState>();
				LoadResponseBatch(lstate.arrow_state, response);
				lstate.returning_chunk.Reset();
				lstate.returning_chunk.SetCardinality(NumericCast<idx_t>(response->num_rows()));
				ArrowTableFunction::ArrowToDuckDB(lstate.arrow_state, lstate.response_arrow_table.GetColumns(),
				                                  lstate.returning_chunk, false);
				gstate.return_collection.Append(lstate.returning_chunk);
				gstate.changed_count += NumericCast<idx_t>(response->num_rows());
			} else {
				gstate.changed_count += ReadCountFromBatch(response);
			}
		}
	}

	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType VgiPhysicalUpdate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                             OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<VgiWriteGlobalState>();
	auto finalize_count = FinalizeWriteConnection(gstate);
	D_ASSERT(!gstate.return_chunk || finalize_count == 0);
	gstate.changed_count += finalize_count;
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSourceState> VgiPhysicalUpdate::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<VgiWriteSourceState>();
}

SourceResultType VgiPhysicalUpdate::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                    OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<VgiWriteGlobalState>();
	auto &source_state = input.global_state.Cast<VgiWriteSourceState>();
	return WriteGetDataInternal(gstate, source_state, chunk, return_chunk);
}

} // namespace duckdb
