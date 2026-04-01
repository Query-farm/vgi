#include "storage/vgi_physical_write.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "storage/vgi_transaction.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_function_connection.hpp"
#include "vgi_rpc_types.hpp"
#include "vgi_transport.hpp"
#include "vgi_worker_pool.hpp"

namespace duckdb {

using namespace vgi;

// ============================================================================
// Helper: Set up a write connection to a VGI worker function
// ============================================================================

// Build a one-row Arrow RecordBatch of write options to pass to the worker.
// The batch is self-describing and extensible — new options are just new fields.
static std::shared_ptr<arrow::RecordBatch> BuildWriteOptions(bool return_chunk,
                                                              OnConflictAction action_type,
                                                              const vector<string> &conflict_columns) {
	auto return_chunks_arr = arrow::MakeArrayFromScalar(*arrow::MakeScalar(return_chunk), 1).ValueOrDie();

	string action_str;
	switch (action_type) {
	case OnConflictAction::THROW:
		action_str = "throw";
		break;
	case OnConflictAction::NOTHING:
		action_str = "nothing";
		break;
	default:
		throw InternalException("Unsupported OnConflictAction for VGI tables");
	}
	auto action_arr = arrow::MakeArrayFromScalar(*arrow::MakeScalar(action_str), 1).ValueOrDie();

	// Build conflict columns as list<string>
	arrow::StringBuilder col_builder;
	auto list_builder = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(), std::make_shared<arrow::StringBuilder>());
	auto &col_value_builder = dynamic_cast<arrow::StringBuilder &>(*list_builder->value_builder());
	(void)list_builder->Append();
	for (auto &col : conflict_columns) {
		(void)col_value_builder.Append(col);
	}
	auto conflict_cols_arr = list_builder->Finish().ValueOrDie();

	auto schema = arrow::schema({
	    arrow::field("return_chunks", arrow::boolean()),
	    arrow::field("on_conflict", arrow::utf8()),
	    arrow::field("on_conflict_columns", arrow::list(arrow::utf8())),
	});
	return arrow::RecordBatch::Make(schema, 1, {return_chunks_arr, action_arr, conflict_cols_arr});
}

static std::unique_ptr<IFunctionConnection> SetupWriteConnection(ClientContext &context, VgiTableEntry &table,
                                                                  const VgiWriteFunctionResult &write_func,
                                                                  const std::shared_ptr<arrow::Schema> &input_schema,
                                                                  const std::shared_ptr<arrow::RecordBatch> &write_options,
                                                                  const std::vector<uint8_t> &transaction_id) {
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
		auto pooled = VgiWorkerPool::Instance().TryAcquire(params->worker_path());
		if (pooled) {
			connection = CreateFunctionConnectionFromPool(std::move(pooled), write_func.function_name, arguments,
			                                              attach_result->attach_id, transaction_id, context,
			                                              "TABLE", {}, params->worker_debug());
		}
	}
	if (!connection) {
		connection = CreateFunctionConnection(params->worker_path(), write_func.function_name, arguments,
		                                      attach_result->attach_id, transaction_id, context, "TABLE", {},
		                                      params->worker_debug());
	}

	connection->SetInputSchema(input_schema);
	connection->PerformBindFull();
	connection->PerformInit({}, nullptr, {}, "INPUT");
	connection->OpenInputWriter();

	return connection;
}

// Helper: Read the count from a response batch (count column)
static idx_t ReadCountFromBatch(const std::shared_ptr<arrow::RecordBatch> &batch) {
	if (!batch || batch->num_rows() == 0) {
		return 0;
	}
	auto count_col = batch->GetColumnByName("count");
	if (!count_col) {
		return NumericCast<idx_t>(batch->num_rows());
	}
	auto int_array = std::static_pointer_cast<arrow::Int64Array>(count_col);
	idx_t total = 0;
	for (int64_t i = 0; i < int_array->length(); i++) {
		total += NumericCast<idx_t>(int_array->Value(i));
	}
	return total;
}

// Helper: Finalize a write connection — read remaining responses, destroy connection
static idx_t FinalizeWriteConnection(VgiWriteGlobalState &gstate) {
	gstate.connection->CloseInputWriter();

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
static void LoadResponseBatch(ArrowScanLocalState &scan_state,
                               const std::shared_ptr<arrow::RecordBatch> &batch) {
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

// ============================================================================
// INSERT Implementation
// ============================================================================

VgiPhysicalInsert::VgiPhysicalInsert(PhysicalPlan &plan, LogicalInsert &op, VgiTableEntry &table_p,
                                     bool return_chunk_p, OnConflictAction action_type_p,
                                     unordered_set<column_t> on_conflict_filter_p)
    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, op.types, op.estimated_cardinality),
      insert_table(&table_p),
      return_chunk(return_chunk_p), action_type(action_type_p), on_conflict_filter(std::move(on_conflict_filter_p)) {
}

VgiPhysicalInsert::VgiPhysicalInsert(PhysicalPlan &plan, LogicalOperator &op, VgiTableEntry &table_p,
                                     bool return_chunk_p)
    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, op.types, op.estimated_cardinality),
      insert_table(&table_p),
      return_chunk(return_chunk_p), action_type(OnConflictAction::THROW) {
}

VgiPhysicalInsert::VgiPhysicalInsert(PhysicalPlan &plan, LogicalOperator &op, SchemaCatalogEntry &schema_p,
                                     unique_ptr<BoundCreateTableInfo> create_info_p)
    : PhysicalOperator(plan, PhysicalOperatorType::CREATE_TABLE_AS, op.types, op.estimated_cardinality),
      schema(&schema_p), create_info(std::move(create_info_p)),
      return_chunk(false), action_type(OnConflictAction::THROW) {
}

InsertionOrderPreservingMap<string> VgiPhysicalInsert::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	if (insert_table) {
		result["Table"] = insert_table->name;
	} else if (create_info) {
		result["Table"] = create_info->base->Cast<CreateTableInfo>().table;
	}
	return result;
}

unique_ptr<GlobalSinkState> VgiPhysicalInsert::GetGlobalSinkState(ClientContext &context) const {
	optional_ptr<VgiTableEntry> table;
	if (create_info) {
		// CREATE TABLE AS: create the table now (inside the transaction)
		auto &schema_ref = *schema.get_mutable();
		auto result = schema_ref.CreateTable(schema_ref.GetCatalogTransaction(context), *create_info);
		if (!result) {
			// IF NOT EXISTS and table already exists — return empty result
			auto gstate = make_uniq<VgiWriteGlobalState>(context, GetTypes(), false);
			gstate->changed_count = 0;
			return unique_ptr<GlobalSinkState>(gstate.release());
		}
		table = &result->Cast<VgiTableEntry>();
	} else {
		D_ASSERT(insert_table);
		table = insert_table;
	}

	auto &catalog = table->GetCatalog().Cast<VgiCatalog>();
	auto &params = catalog.attach_parameters();
	auto &attach_result = catalog.attach_result();

	auto &vgi_tx = VgiTransaction::Get(context, table->GetCatalog());
	auto write_func = InvokeCatalogTableInsertFunctionGet(params->worker_path(), attach_result->attach_id,
	                                                      table->GetTableInfo().schema_name, table->GetTableInfo().name,
	                                                      context, vgi_tx.GetTransactionId(),
	                                                      params->worker_debug(), params->use_pool());

	// Build input schema (table columns minus row_id)
	auto &table_info = table->GetTableInfo();
	auto input_schema = table_info.arrow_schema;
	if (table_info.row_id_column >= 0) {
		auto fields = input_schema->fields();
		fields.erase(fields.begin() + table_info.row_id_column);
		input_schema = arrow::schema(fields);
	}

	// Build conflict target column names from column IDs
	vector<string> conflict_columns;
	for (auto col_id : on_conflict_filter) {
		conflict_columns.push_back(table->GetColumns().GetColumn(PhysicalIndex(col_id)).Name());
	}

	auto write_options = BuildWriteOptions(return_chunk, action_type, conflict_columns);

	auto gstate = make_uniq<VgiWriteGlobalState>(context, GetTypes(), return_chunk);
	gstate->send_schema = input_schema;
	gstate->connection = SetupWriteConnection(context, *table, write_func, input_schema, write_options,
	                                           vgi_tx.GetTransactionId());

	return unique_ptr<GlobalSinkState>(gstate.release());
}

unique_ptr<LocalSinkState> VgiPhysicalInsert::GetLocalSinkState(ExecutionContext &context) const {
	// Build response schema (table columns sans row_id) for RETURNING Arrow→DuckDB conversion
	// For CTAS, insert_table may be null (table created in GetGlobalSinkState), but
	// return_chunk is always false so the response schema is only used for count output.
	if (insert_table) {
		auto &table_info = insert_table->GetTableInfo();
		auto response_schema = table_info.arrow_schema;
		if (table_info.row_id_column >= 0) {
			auto fields = response_schema->fields();
			fields.erase(fields.begin() + table_info.row_id_column);
			response_schema = arrow::schema(fields);
		}
		return make_uniq<VgiWriteLocalSinkState>(context.client, response_schema, GetTypes());
	}
	// CTAS: use a minimal schema — RETURNING is never used for CTAS
	auto empty_schema = arrow::schema({});
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

VgiPhysicalDelete::VgiPhysicalDelete(PhysicalPlan &plan, LogicalDelete &op, VgiTableEntry &table_p,
                                     bool return_chunk_p, idx_t rowid_index_p)
    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, op.types, op.estimated_cardinality), table(table_p),
      return_chunk(return_chunk_p), rowid_index(rowid_index_p) {
}

VgiPhysicalDelete::VgiPhysicalDelete(PhysicalPlan &plan, LogicalOperator &op, VgiTableEntry &table_p,
                                     bool return_chunk_p, idx_t rowid_index_p)
    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, op.types, op.estimated_cardinality), table(table_p),
      return_chunk(return_chunk_p), rowid_index(rowid_index_p) {
}

InsertionOrderPreservingMap<string> VgiPhysicalDelete::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table"] = table.name;
	return result;
}

unique_ptr<GlobalSinkState> VgiPhysicalDelete::GetGlobalSinkState(ClientContext &context) const {
	auto &catalog = table.GetCatalog().Cast<VgiCatalog>();
	auto &params = catalog.attach_parameters();
	auto &attach_result = catalog.attach_result();

	auto &vgi_tx = VgiTransaction::Get(context, table.GetCatalog());
	auto write_func = InvokeCatalogTableDeleteFunctionGet(params->worker_path(), attach_result->attach_id,
	                                                      table.GetTableInfo().schema_name, table.GetTableInfo().name,
	                                                      context, vgi_tx.GetTransactionId(),
	                                                      params->worker_debug(), params->use_pool());

	auto &table_info = table.GetTableInfo();
	auto rowid_field = table_info.arrow_schema->field(table_info.row_id_column);
	auto input_schema = arrow::schema({rowid_field});

	auto write_options = BuildWriteOptions(return_chunk, OnConflictAction::THROW, {});

	auto gstate = make_uniq<VgiWriteGlobalState>(context, GetTypes(), return_chunk);
	gstate->send_schema = input_schema;
	gstate->connection = SetupWriteConnection(context, table, write_func, input_schema, write_options,
	                                           vgi_tx.GetTransactionId());

	return unique_ptr<GlobalSinkState>(gstate.release());
}

unique_ptr<LocalSinkState> VgiPhysicalDelete::GetLocalSinkState(ExecutionContext &context) const {
	auto &table_info = table.GetTableInfo();
	auto response_schema = table_info.arrow_schema;
	if (table_info.row_id_column >= 0) {
		auto fields = response_schema->fields();
		fields.erase(fields.begin() + table_info.row_id_column);
		response_schema = arrow::schema(fields);
	}
	return make_uniq<VgiWriteLocalSinkState>(context.client, response_schema, GetTypes());
}

SinkResultType VgiPhysicalDelete::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<VgiWriteGlobalState>();

	DataChunk rowid_chunk;
	rowid_chunk.Initialize(context.client, {chunk.data[rowid_index].GetType()});
	rowid_chunk.data[0].Reference(chunk.data[rowid_index]);
	rowid_chunk.SetCardinality(chunk.size());

	auto batch = DataChunkToArrow(context.client, rowid_chunk, gstate.send_schema);

	{
		lock_guard<mutex> guard(gstate.write_lock);
		gstate.connection->WriteInputBatch(batch);

		auto response = gstate.connection->ReadDataBatch();
		if (response && response->num_rows() > 0) {
			if (gstate.return_chunk) {
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

VgiPhysicalUpdate::VgiPhysicalUpdate(PhysicalPlan &plan, LogicalUpdate &op, VgiTableEntry &table_p,
                                     bool return_chunk_p)
    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, op.types, op.estimated_cardinality), table(table_p),
      return_chunk(return_chunk_p), columns(op.columns) {
	for (auto &expr : op.expressions) {
		expressions.push_back(expr->Copy());
	}
}

VgiPhysicalUpdate::VgiPhysicalUpdate(PhysicalPlan &plan, LogicalOperator &op, VgiTableEntry &table_p,
                                     bool return_chunk_p, vector<PhysicalIndex> columns_p,
                                     vector<unique_ptr<Expression>> expressions_p)
    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, op.types, op.estimated_cardinality), table(table_p),
      return_chunk(return_chunk_p), columns(std::move(columns_p)), expressions(std::move(expressions_p)) {
}

InsertionOrderPreservingMap<string> VgiPhysicalUpdate::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table"] = table.name;
	return result;
}

unique_ptr<GlobalSinkState> VgiPhysicalUpdate::GetGlobalSinkState(ClientContext &context) const {
	auto &catalog = table.GetCatalog().Cast<VgiCatalog>();
	auto &params = catalog.attach_parameters();
	auto &attach_result = catalog.attach_result();

	auto &vgi_tx = VgiTransaction::Get(context, table.GetCatalog());
	auto write_func = InvokeCatalogTableUpdateFunctionGet(params->worker_path(), attach_result->attach_id,
	                                                      table.GetTableInfo().schema_name, table.GetTableInfo().name,
	                                                      context, vgi_tx.GetTransactionId(),
	                                                      params->worker_debug(), params->use_pool());

	auto &table_info = table.GetTableInfo();
	auto &table_columns = table.GetColumns();

	std::vector<std::shared_ptr<arrow::Field>> send_fields;
	for (auto &col_idx : columns) {
		auto &col = table_columns.GetColumn(col_idx);
		auto arrow_type = BuildArrowSchemaFromDuckDB(context, {col.Type()}, {col.Name()});
		send_fields.push_back(arrow_type->field(0));
	}
	auto rowid_field = table_info.arrow_schema->field(table_info.row_id_column);
	send_fields.push_back(rowid_field);

	auto input_schema = arrow::schema(send_fields);

	auto write_options = BuildWriteOptions(return_chunk, OnConflictAction::THROW, {});

	auto gstate = make_uniq<VgiWriteGlobalState>(context, GetTypes(), return_chunk);
	gstate->send_schema = input_schema;
	gstate->connection = SetupWriteConnection(context, table, write_func, input_schema, write_options,
	                                           vgi_tx.GetTransactionId());

	return unique_ptr<GlobalSinkState>(gstate.release());
}

unique_ptr<LocalSinkState> VgiPhysicalUpdate::GetLocalSinkState(ExecutionContext &context) const {
	auto &table_info = table.GetTableInfo();
	auto response_schema = table_info.arrow_schema;
	if (table_info.row_id_column >= 0) {
		auto fields = response_schema->fields();
		fields.erase(fields.begin() + table_info.row_id_column);
		response_schema = arrow::schema(fields);
	}
	return make_uniq<VgiWriteLocalSinkState>(context.client, response_schema, GetTypes());
}

SinkResultType VgiPhysicalUpdate::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<VgiWriteGlobalState>();

	idx_t num_update_cols = columns.size();
	idx_t rowid_col = chunk.ColumnCount() - 1;

	DataChunk update_chunk;
	vector<LogicalType> update_types;
	for (idx_t i = 0; i < num_update_cols; i++) {
		update_types.push_back(chunk.data[i].GetType());
	}
	update_types.push_back(chunk.data[rowid_col].GetType());

	update_chunk.Initialize(context.client, update_types);
	for (idx_t i = 0; i < num_update_cols; i++) {
		update_chunk.data[i].Reference(chunk.data[i]);
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
