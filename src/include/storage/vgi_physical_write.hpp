// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
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

#include "vgi_ipc.hpp"

#include "duckdb.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "storage/vgi_catalog.hpp"
#include "storage/vgi_table_entry.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_metadata.hpp"
#include "vgi_ifunction_connection.hpp"

#include <set>
#include <unordered_set>

namespace duckdb {

// ============================================================================
// Shared State for all write operations
// ============================================================================

//! The row ids one DELETE write call has sent. DuckDB hands a non-native
//! table's DELETE a target row once per matching source row (DELETE ...
//! USING); a write call sends each row id once.
class VgiRowIdSet {
public:
	//! Keep only the rows of `rowids` (one column) not seen before in this call,
	//! slicing it in place; returns the number kept. NULL row ids are kept.
	idx_t Filter(DataChunk &rowids) {
		auto &vec = rowids.data[0];
		const idx_t n = rowids.size();
		UnifiedVectorFormat format;
		vec.ToUnifiedFormat(n, format);
		SelectionVector keep(n);
		idx_t kept = 0;
		const bool is_bigint = vec.GetType().InternalType() == PhysicalType::INT64;
		for (idx_t i = 0; i < n; i++) {
			const auto idx = format.sel->get_index(i);
			bool fresh = true;
			if (format.validity.RowIsValid(idx)) {
				if (is_bigint) {
					fresh = bigint_ids_.insert(UnifiedVectorFormat::GetData<int64_t>(format)[idx]).second;
				} else {
					fresh = other_ids_.insert(vec.GetValue(i)).second;
				}
			}
			if (fresh) {
				keep.set_index(kept++, i);
			}
		}
		if (kept < n) {
			rowids.Slice(keep, kept);
		}
		return kept;
	}

private:
	std::unordered_set<int64_t> bigint_ids_;
	std::set<Value> other_ids_;
};

struct VgiWriteGlobalState : public GlobalSinkState {
	VgiWriteGlobalState(ClientContext &context, const vector<LogicalType> &return_types, bool return_chunk_p)
	    : return_chunk(return_chunk_p), return_collection(context, return_types), changed_count(0) {
	}

	std::unique_ptr<vgi::IFunctionConnection> connection;
	// BindResult from the bind RPC sent at GetWriteConnection time. Held so
	// the FINALIZE phase (PerformFinalizeInit) can reference the same bind.
	vgi::BindResult bind_result;
	mutex write_lock;
	bool return_chunk;
	ColumnDataCollection return_collection; // deep-copies RETURNING rows in Sink
	idx_t changed_count;

	// Arrow schema for converting DuckDB DataChunks → Arrow batches (input to worker)
	vgi::VgiSchemaPtr send_schema;

	// Expected schema for RETURNING-mode response batches (table user-column
	// schema, sans rowid). Sink validates each non-empty worker response against
	// this before handing it to ArrowToDuckDB — a worker that emits a count
	// batch instead of the table-row schema would otherwise read past the end
	// of the conversion column metadata and crash.
	// Only set when return_chunk is true.
	vgi::VgiSchemaPtr expected_returning_schema;

	// True if the write function declared has_finalize=true on its catalog
	// metadata. When set, ``FinalizeWriteConnection`` invokes
	// ``PerformFinalizeInit`` so the worker's ``cls.finalize(params)``
	// runs and gets one statement-final pass to drain async state and
	// surface partial-failure errors. False: the legacy close-and-drain
	// path runs (no finalize callback fires on the worker).
	bool has_finalize = false;

	//! DELETE: the row ids this write call already sent.
	VgiRowIdSet deleted_rowids;

	//! CTAS: the table this statement created (catalog_table_create replied
	//! created=true). Finalize clears its rollback-drop mark.
	optional_ptr<VgiTableEntry> ctas_table;
};

// Local sink state for RETURNING — holds persistent ArrowScanLocalState so that
// ArrowToDuckDB auxiliary data stays alive during ColumnDataCollection::Append.
struct VgiWriteLocalSinkState : public LocalSinkState {
	VgiWriteLocalSinkState(ClientContext &context, const vgi::VgiSchemaPtr &response_schema,
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
	//! INSERT, and a MERGE's INSERT action (whose projection supplies the full
	//! row). ON CONFLICT never reaches here: DuckDB plans it as a MERGE.
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
	//! UPDATE, and a MERGE's UPDATE action. `expressions[i]` (one per entry of
	//! `columns`) references the child column holding the new value, or is
	//! DEFAULT (evaluated from `bound_defaults`, indexed by storage oid); the
	//! child's last column is the row id.
	VgiPhysicalUpdate(PhysicalPlan &plan, LogicalOperator &op, VgiTableEntry &table, bool return_chunk,
	                  vector<PhysicalIndex> columns, vector<unique_ptr<Expression>> expressions,
	                  const vector<unique_ptr<Expression>> &bound_defaults);

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
	vector<unique_ptr<Expression>> bound_defaults;
};

} // namespace duckdb
