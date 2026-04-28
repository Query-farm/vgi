//===----------------------------------------------------------------------===//
//                         VGI
//
// storage/vgi_physical_write.hpp
//
// Physical operators for INSERT, UPDATE, and DELETE on VGI catalog tables.
// These operators use the table-in-out exchange protocol to send batches
// to a VGI worker function that processes the write operation.
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "storage/vgi_catalog.hpp"
#include "storage/vgi_table_entry.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_ifunction_connection.hpp"

namespace duckdb {

// ============================================================================
// Shared State for all write operations
// ============================================================================

struct VgiWriteGlobalState : public GlobalSinkState {
	VgiWriteGlobalState(ClientContext &context, const vector<LogicalType> &return_types, bool return_chunk_p)
	    : return_chunk(return_chunk_p), return_collection(context, return_types), changed_count(0) {
	}

	std::unique_ptr<vgi::IFunctionConnection> connection;
	mutex write_lock;
	bool return_chunk;
	ColumnDataCollection return_collection; // deep-copies RETURNING rows in Sink
	idx_t changed_count;

	// Arrow schema for converting DuckDB DataChunks → Arrow batches (input to worker)
	std::shared_ptr<arrow::Schema> send_schema;

	// True if the write function declared has_finalize=true on its catalog
	// metadata. When set, ``FinalizeWriteConnection`` invokes
	// ``PerformFinalizeInit`` so the worker's ``cls.finalize(params)``
	// runs and gets one statement-final pass to drain async state and
	// surface partial-failure errors. False: the legacy close-and-drain
	// path runs (no finalize callback fires on the worker).
	bool has_finalize = false;
};

// Local sink state for RETURNING — holds persistent ArrowScanLocalState so that
// ArrowToDuckDB auxiliary data stays alive during ColumnDataCollection::Append.
struct VgiWriteLocalSinkState : public LocalSinkState {
	VgiWriteLocalSinkState(ClientContext &context, const std::shared_ptr<arrow::Schema> &response_schema,
	                       const vector<LogicalType> &return_types)
	    : arrow_state(make_uniq<ArrowArrayWrapper>(), context) {
		vector<LogicalType> resp_types;
		vector<string> resp_names;
		vgi::ArrowSchemaToDuckDBTypes(context, response_schema, response_c_schema, response_arrow_table, resp_types,
		                              resp_names);
		returning_chunk.Initialize(context, return_types);
	}

	ArrowScanLocalState arrow_state;
	ArrowSchemaWrapper response_c_schema;
	ArrowTableSchema response_arrow_table;
	DataChunk returning_chunk;
};

// Source state for producing output (changed count or RETURNING rows).
struct VgiWriteSourceState : public GlobalSourceState {
	bool returned = false;
	ColumnDataScanState scan_state;
};

// ============================================================================
// INSERT Physical Operator
// ============================================================================

class VgiPhysicalInsert : public PhysicalOperator {
public:
	VgiPhysicalInsert(PhysicalPlan &plan, LogicalInsert &op, VgiTableEntry &table, bool return_chunk,
	                  OnConflictAction action_type, unordered_set<column_t> on_conflict_filter);
	// Merge-compatible constructor (accepts any LogicalOperator)
	VgiPhysicalInsert(PhysicalPlan &plan, LogicalOperator &op, VgiTableEntry &table, bool return_chunk);
	// CREATE TABLE AS constructor — table is created during execution, not planning
	VgiPhysicalInsert(PhysicalPlan &plan, LogicalOperator &op, SchemaCatalogEntry &schema,
	                  unique_ptr<BoundCreateTableInfo> create_info);

	bool IsSink() const override {
		return true;
	}
	bool IsSource() const override {
		return true;
	}

	string GetName() const override {
		return "VGI_INSERT";
	}
	InsertionOrderPreservingMap<string> ParamsToString() const override;

	// Sink interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

private:
	optional_ptr<VgiTableEntry> insert_table;   // set for regular INSERT
	optional_ptr<SchemaCatalogEntry> schema;     // set for CTAS
	unique_ptr<BoundCreateTableInfo> create_info; // set for CTAS
	bool return_chunk;
	OnConflictAction action_type;
	unordered_set<column_t> on_conflict_filter; // conflict target column IDs
};

// ============================================================================
// DELETE Physical Operator
// ============================================================================

class VgiPhysicalDelete : public PhysicalOperator {
public:
	VgiPhysicalDelete(PhysicalPlan &plan, LogicalDelete &op, VgiTableEntry &table, bool return_chunk,
	                  idx_t rowid_index);
	// Merge-compatible constructor
	VgiPhysicalDelete(PhysicalPlan &plan, LogicalOperator &op, VgiTableEntry &table, bool return_chunk,
	                  idx_t rowid_index);

	bool IsSink() const override {
		return true;
	}
	bool IsSource() const override {
		return true;
	}

	string GetName() const override {
		return "VGI_DELETE";
	}
	InsertionOrderPreservingMap<string> ParamsToString() const override;

	// Sink interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

private:
	VgiTableEntry &table;
	bool return_chunk;
	idx_t rowid_index;
};

// ============================================================================
// UPDATE Physical Operator
// ============================================================================

class VgiPhysicalUpdate : public PhysicalOperator {
public:
	VgiPhysicalUpdate(PhysicalPlan &plan, LogicalUpdate &op, VgiTableEntry &table, bool return_chunk);
	// Merge-compatible constructor (no columns/expressions — they come from the merge action)
	VgiPhysicalUpdate(PhysicalPlan &plan, LogicalOperator &op, VgiTableEntry &table, bool return_chunk,
	                  vector<PhysicalIndex> columns, vector<unique_ptr<Expression>> expressions);

	bool IsSink() const override {
		return true;
	}
	bool IsSource() const override {
		return true;
	}

	string GetName() const override {
		return "VGI_UPDATE";
	}
	InsertionOrderPreservingMap<string> ParamsToString() const override;

	// Sink interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

private:
	VgiTableEntry &table;
	bool return_chunk;
	vector<PhysicalIndex> columns;
	vector<unique_ptr<Expression>> expressions;
};

} // namespace duckdb
