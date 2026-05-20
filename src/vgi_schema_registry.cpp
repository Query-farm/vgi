// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_schema_registry.hpp"

#include "duckdb/common/exception.hpp"
#include "generated/vgi_protocol_schemas.hpp"

#include <sstream>
#include <unordered_map>

namespace duckdb {
namespace vgi {

namespace {

// All schemas this registry references live in
// `src/generated/vgi_protocol_schemas.hpp`, emitted by vgi-python's
// `vgi-gen-cpp-schemas`. This file owns **policy** (void vs dynamic vs strict
// classification, per-method `dynamic_reason` strings, which methods share a
// `List<Binary>` outer + inner-item schema) — not contract.

using namespace generated;

// ============================================================================
// Registry
// ============================================================================

struct DynamicMethod {
	const char *name;
	const char *reason;
};

const std::unordered_map<std::string, ResponseSchema> &Registry() {
	// Meyers singleton: thread-safe static init, no heap, no destructor ordering.
	static const std::unordered_map<std::string, ResponseSchema> kRegistry = []() {
		std::unordered_map<std::string, ResponseSchema> m;

		// --- List-of-binary responses with per-item inner schemas ---------
		// All catalog discovery methods wrap IPC-serialized info batches in
		// {items: List<Binary>}. Outer + inner schemas generated from the
		// vgi-python Protocol; see src/generated/vgi_protocol_schemas.hpp.
		struct ItemsMethod {
			const char *name;
			const std::shared_ptr<arrow::Schema> &(*outer)();
			const std::shared_ptr<arrow::Schema> &(*item)();
		};
		const ItemsMethod kItemsResponses[] = {
		    {"catalog_catalogs", &CatalogCatalogsResultSchema, &CatalogInfoSchema},
		    {"catalog_schemas", &CatalogSchemasResultSchema, &SchemaInfoSchema},
		    {"catalog_schema_get", &CatalogSchemaGetResultSchema, &SchemaInfoSchema},
		    {"catalog_schema_contents_tables", &CatalogSchemaContentsTablesResultSchema, &TableInfoSchema},
		    {"catalog_schema_contents_views", &CatalogSchemaContentsViewsResultSchema, &ViewInfoSchema},
		    {"catalog_schema_contents_functions", &CatalogSchemaContentsFunctionsResultSchema,
		     &FunctionInfoSchema},
		    {"catalog_schema_contents_macros", &CatalogSchemaContentsMacrosResultSchema, &MacroInfoSchema},
		    {"catalog_schema_contents_indexes", &CatalogSchemaContentsIndexesResultSchema, &IndexInfoSchema},
		    {"catalog_table_get", &CatalogTableGetResultSchema, &TableInfoSchema},
		    {"catalog_view_get", &CatalogViewGetResultSchema, &ViewInfoSchema},
		    {"catalog_macro_get", &CatalogMacroGetResultSchema, &MacroInfoSchema},
		    {"catalog_index_get", &CatalogIndexGetResultSchema, &IndexInfoSchema},
		};
		for (const auto &e : kItemsResponses) {
			m[e.name] = ResponseSchema{e.outer(), e.item(), false, nullptr};
		}

		// --- Void responses -----------------------------------------------
		// Mutation methods return None; a non-empty batch is a protocol violation.
		const char *kVoidResponses[] = {
		    "catalog_detach",
		    "catalog_create",
		    "catalog_drop",
		    "catalog_transaction_commit",
		    "catalog_transaction_rollback",
		    "catalog_schema_create",
		    "catalog_schema_drop",
		    "catalog_table_create",
		    "catalog_table_drop",
		    "catalog_table_rename",
		    "catalog_table_comment_set",
		    "catalog_table_column_comment_set",
		    "catalog_table_column_add",
		    "catalog_table_column_drop",
		    "catalog_table_column_rename",
		    "catalog_table_column_default_set",
		    "catalog_table_column_default_drop",
		    "catalog_table_column_type_change",
		    "catalog_table_not_null_set",
		    "catalog_table_not_null_drop",
		    "catalog_view_create",
		    "catalog_view_drop",
		    "catalog_view_rename",
		    "catalog_view_comment_set",
		    "catalog_macro_create",
		    "catalog_macro_drop",
		    "catalog_index_create",
		    "catalog_index_drop",
		};
		for (const char *name : kVoidResponses) {
			m[name] = ResponseSchema{nullptr, nullptr, false, nullptr};
		}

		// --- Strictly-typed single-struct responses -----------------------
		m["catalog_attach"] = ResponseSchema{CatalogAttachResultSchema(), nullptr, false, nullptr};
		m["catalog_version"] = ResponseSchema{CatalogVersionResultSchema(), nullptr, false, nullptr};
		m["catalog_transaction_begin"] =
		    ResponseSchema{CatalogTransactionBeginResultSchema(), nullptr, false, nullptr};
		m["table_function_cardinality"] =
		    ResponseSchema{TableFunctionCardinalityResultSchema(), nullptr, false, nullptr};
		m["table_function_dynamic_to_string"] =
		    ResponseSchema{TableFunctionDynamicToStringResultSchema(), nullptr, false, nullptr};
		m["bind"] = ResponseSchema{BindResultSchema(), nullptr, false, nullptr};
		m["aggregate_bind"] = ResponseSchema{AggregateBindResultSchema(), nullptr, false, nullptr};
		m["aggregate_finalize"] = ResponseSchema{AggregateFinalizeResultSchema(), nullptr, false, nullptr};
		m["table_buffering_process"] =
		    ResponseSchema{TableBufferingProcessResultSchema(), nullptr, false, nullptr};
		m["table_buffering_combine"] =
		    ResponseSchema{TableBufferingCombineResultSchema(), nullptr, false, nullptr};
		m["aggregate_window"] = ResponseSchema{AggregateWindowResultSchema(), nullptr, false, nullptr};
		m["aggregate_window_batch"] =
		    ResponseSchema{AggregateWindowBatchResultSchema(), nullptr, false, nullptr};
		m["aggregate_streaming_open"] =
		    ResponseSchema{AggregateStreamingOpenResultSchema(), nullptr, false, nullptr};
		m["aggregate_streaming_chunk"] =
		    ResponseSchema{AggregateStreamingChunkResultSchema(), nullptr, false, nullptr};
		m["catalog_table_scan_function_get"] =
		    ResponseSchema{ScanFunctionResultSchema(), nullptr, false, nullptr};
		m["catalog_table_scan_branches_get"] =
		    ResponseSchema{ScanBranchesResultSchema(), nullptr, false, nullptr};
		m["catalog_table_insert_function_get"] =
		    ResponseSchema{ScanFunctionResultSchema(), nullptr, false, nullptr};
		m["catalog_table_update_function_get"] =
		    ResponseSchema{ScanFunctionResultSchema(), nullptr, false, nullptr};
		m["catalog_table_delete_function_get"] =
		    ResponseSchema{ScanFunctionResultSchema(), nullptr, false, nullptr};

		// --- Zero-field ("empty struct") ack responses --------------------
		// These are concrete 0-column batches on the wire, distinct from void.
		m["aggregate_update"] = ResponseSchema{AggregateUpdateResultSchema(), nullptr, false, nullptr};
		m["aggregate_combine"] = ResponseSchema{AggregateCombineResultSchema(), nullptr, false, nullptr};
		m["aggregate_destructor"] =
		    ResponseSchema{AggregateDestructorResultSchema(), nullptr, false, nullptr};
		m["table_buffering_destructor"] =
		    ResponseSchema{TableBufferingDestructorResultSchema(), nullptr, false, nullptr};
		m["aggregate_window_init"] =
		    ResponseSchema{AggregateWindowInitResultSchema(), nullptr, false, nullptr};
		m["aggregate_window_destructor"] =
		    ResponseSchema{AggregateWindowDestructorResultSchema(), nullptr, false, nullptr};
		m["aggregate_streaming_close"] =
		    ResponseSchema{AggregateStreamingCloseResultSchema(), nullptr, false, nullptr};

		// --- Dynamic / genuinely per-call responses -----------------------
		// Only methods whose response shape is not expressible as a single
		// fixed Arrow schema belong here.
		const DynamicMethod kDynamicResponses[] = {
		    {"catalog_table_column_statistics_get",
		     "raw IPC sparse-union min/max batch; schema varies per column type"},
		    {"table_function_statistics",
		     "Optional[bytes] of per-function IPC statistics; schema is function-specific"},
		    {"init",
		     "stream method; response framed as a stream header plus data frames, "
		     "not a single unary result batch"},
		};
		for (const auto &entry : kDynamicResponses) {
			m[entry.name] = ResponseSchema{nullptr, nullptr, true, entry.reason};
		}

		// --- Per-method request schemas -----------------------------------
		// Every method in the registry also has an outgoing request-schema
		// contract generated from the Protocol. Attaching it to the existing
		// entry keeps validation symmetric (ValidateRequestSchema uses this).
		struct RequestSchemaEntry {
			const char *name;
			const std::shared_ptr<arrow::Schema> &(*request)();
		};
		const RequestSchemaEntry kRequestSchemas[] = {
		    {"catalog_catalogs", &CatalogCatalogsParamsSchema},
		    {"catalog_attach", &CatalogAttachParamsSchema},
		    {"catalog_detach", &CatalogDetachParamsSchema},
		    {"catalog_create", &CatalogCreateParamsSchema},
		    {"catalog_drop", &CatalogDropParamsSchema},
		    {"catalog_version", &CatalogVersionParamsSchema},
		    {"catalog_transaction_begin", &CatalogTransactionBeginParamsSchema},
		    {"catalog_transaction_commit", &CatalogTransactionCommitParamsSchema},
		    {"catalog_transaction_rollback", &CatalogTransactionRollbackParamsSchema},
		    {"catalog_schemas", &CatalogSchemasParamsSchema},
		    {"catalog_schema_get", &CatalogSchemaGetParamsSchema},
		    {"catalog_schema_create", &CatalogSchemaCreateParamsSchema},
		    {"catalog_schema_drop", &CatalogSchemaDropParamsSchema},
		    {"catalog_schema_contents_tables", &CatalogSchemaContentsTablesParamsSchema},
		    {"catalog_schema_contents_views", &CatalogSchemaContentsViewsParamsSchema},
		    {"catalog_schema_contents_functions", &CatalogSchemaContentsFunctionsParamsSchema},
		    {"catalog_schema_contents_macros", &CatalogSchemaContentsMacrosParamsSchema},
		    {"catalog_schema_contents_indexes", &CatalogSchemaContentsIndexesParamsSchema},
		    {"catalog_table_get", &CatalogTableGetParamsSchema},
		    {"catalog_table_create", &CatalogTableCreateParamsSchema},
		    {"catalog_table_drop", &CatalogTableDropParamsSchema},
		    {"catalog_table_rename", &CatalogTableRenameParamsSchema},
		    {"catalog_table_scan_function_get", &CatalogTableScanFunctionGetParamsSchema},
		    {"catalog_table_scan_branches_get", &CatalogTableScanBranchesGetParamsSchema},
		    {"catalog_table_insert_function_get", &CatalogTableInsertFunctionGetParamsSchema},
		    {"catalog_table_update_function_get", &CatalogTableUpdateFunctionGetParamsSchema},
		    {"catalog_table_delete_function_get", &CatalogTableDeleteFunctionGetParamsSchema},
		    {"catalog_table_column_statistics_get", &CatalogTableColumnStatisticsGetParamsSchema},
		    {"catalog_table_comment_set", &CatalogTableCommentSetParamsSchema},
		    {"catalog_table_column_comment_set", &CatalogTableColumnCommentSetParamsSchema},
		    {"catalog_table_column_add", &CatalogTableColumnAddParamsSchema},
		    {"catalog_table_column_drop", &CatalogTableColumnDropParamsSchema},
		    {"catalog_table_column_rename", &CatalogTableColumnRenameParamsSchema},
		    {"catalog_table_column_default_set", &CatalogTableColumnDefaultSetParamsSchema},
		    {"catalog_table_column_default_drop", &CatalogTableColumnDefaultDropParamsSchema},
		    {"catalog_table_column_type_change", &CatalogTableColumnTypeChangeParamsSchema},
		    {"catalog_table_not_null_set", &CatalogTableNotNullSetParamsSchema},
		    {"catalog_table_not_null_drop", &CatalogTableNotNullDropParamsSchema},
		    {"catalog_view_get", &CatalogViewGetParamsSchema},
		    {"catalog_view_create", &CatalogViewCreateParamsSchema},
		    {"catalog_view_drop", &CatalogViewDropParamsSchema},
		    {"catalog_view_rename", &CatalogViewRenameParamsSchema},
		    {"catalog_view_comment_set", &CatalogViewCommentSetParamsSchema},
		    {"catalog_macro_get", &CatalogMacroGetParamsSchema},
		    {"catalog_macro_create", &CatalogMacroCreateParamsSchema},
		    {"catalog_macro_drop", &CatalogMacroDropParamsSchema},
		    {"catalog_index_get", &CatalogIndexGetParamsSchema},
		    {"catalog_index_create", &CatalogIndexCreateParamsSchema},
		    {"catalog_index_drop", &CatalogIndexDropParamsSchema},
		    {"bind", &BindParamsSchema},
		    {"init", &InitParamsSchema},
		    {"table_function_cardinality", &TableFunctionCardinalityParamsSchema},
		    {"table_function_dynamic_to_string", &TableFunctionDynamicToStringParamsSchema},
		    {"table_function_statistics", &TableFunctionStatisticsParamsSchema},
		    {"aggregate_bind", &AggregateBindParamsSchema},
		    {"aggregate_update", &AggregateUpdateParamsSchema},
		    {"aggregate_combine", &AggregateCombineParamsSchema},
		    {"aggregate_finalize", &AggregateFinalizeParamsSchema},
		    {"aggregate_destructor", &AggregateDestructorParamsSchema},
		    {"aggregate_window_init", &AggregateWindowInitParamsSchema},
		    {"aggregate_window", &AggregateWindowParamsSchema},
		    {"aggregate_window_destructor", &AggregateWindowDestructorParamsSchema},
		    {"aggregate_window_batch", &AggregateWindowBatchParamsSchema},
		    {"aggregate_streaming_open", &AggregateStreamingOpenParamsSchema},
		    {"aggregate_streaming_chunk", &AggregateStreamingChunkParamsSchema},
		    {"aggregate_streaming_close", &AggregateStreamingCloseParamsSchema},
		    {"table_buffering_process", &TableBufferingProcessParamsSchema},
		    {"table_buffering_combine", &TableBufferingCombineParamsSchema},
		    {"table_buffering_destructor", &TableBufferingDestructorParamsSchema},
		};
		for (const auto &e : kRequestSchemas) {
			auto it = m.find(e.name);
			if (it != m.end()) {
				it->second.request_schema = e.request();
			}
		}

		return m;
	}();
	return kRegistry;
}

// ============================================================================
// Diff rendering
// ============================================================================

std::string DescribeField(const arrow::Field &f) {
	std::ostringstream s;
	s << f.name() << ": " << f.type()->ToString() << (f.nullable() ? " nullable" : " not null");
	return s.str();
}

std::string DiffSchemas(const arrow::Schema &expected, const arrow::Schema &actual) {
	std::ostringstream out;
	const auto nE = expected.num_fields();
	const auto nA = actual.num_fields();
	if (nE != nA) {
		out << "  (field count differs: expected " << nE << ", actual " << nA << ")\n"
		    << "  expected schema:\n"
		    << expected.ToString() << "\n"
		    << "  actual schema:\n"
		    << actual.ToString() << "\n";
		return out.str();
	}
	for (int i = 0; i < nE; i++) {
		const auto &e = *expected.field(i);
		const auto &a = *actual.field(i);
		if (e.Equals(a, /*check_metadata=*/false)) {
			continue;
		}
		out << "  [" << i << "] expected: " << DescribeField(e) << "\n"
		    << "      actual:   " << DescribeField(a) << "\n";
	}
	return out.str();
}

} // namespace

const ResponseSchema *LookupResponseSchema(const std::string &method_name) {
	const auto &reg = Registry();
	auto it = reg.find(method_name);
	return it == reg.end() ? nullptr : &it->second;
}

void ValidateResponseSchema(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &method_name,
                            const std::string &worker_path) {
	const auto *entry = LookupResponseSchema(method_name);
	if (!entry) {
		throw IOException("RPC method '%s' has no registered response schema [worker: %s]. "
		                  "Add an entry to vgi_schema_registry.cpp.",
		                  method_name, worker_path);
	}
	if (entry->dynamic) {
		return;
	}
	if (!entry->schema) {
		if (batch && batch->num_rows() > 0) {
			throw IOException("RPC method '%s' returned a non-empty result batch, but the wire contract "
			                  "specifies no response payload [worker: %s]. Actual schema: %s",
			                  method_name, worker_path, batch->schema()->ToString());
		}
		return;
	}
	if (!batch) {
		throw IOException("RPC method '%s' returned an empty response, but the wire contract requires "
		                  "a result batch with schema: %s [worker: %s]",
		                  method_name, entry->schema->ToString(), worker_path);
	}
	if (!batch->schema()->Equals(*entry->schema, /*check_metadata=*/false)) {
		throw IOException("Worker returned an out-of-date Apache Arrow schema. "
		                  "RPC response schema mismatch for '%s' [worker: %s]\n%s",
		                  method_name, worker_path, DiffSchemas(*entry->schema, *batch->schema()));
	}
}

void ValidateRequestSchema(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &method_name,
                           const std::string &worker_path) {
	const auto *entry = LookupResponseSchema(method_name);
	if (!entry) {
		throw IOException("RPC method '%s' has no registered response schema [worker: %s]. "
		                  "Add an entry to vgi_schema_registry.cpp.",
		                  method_name, worker_path);
	}
	if (!entry->request_schema) {
		// Method registered but no request schema captured — registry bug.
		throw IOException(
		    "RPC method '%s' has no registered request schema [worker: %s]. "
		    "Add the method name to kRequestSchemas in vgi_schema_registry.cpp.",
		    method_name, worker_path);
	}
	const auto &expected = *entry->request_schema;
	// An empty schema means the method takes no params (e.g. catalog_catalogs).
	// The C++ callers pass `nullptr` in that case; accept either nullptr or an
	// empty batch.
	if (expected.num_fields() == 0) {
		if (batch && batch->schema()->num_fields() > 0) {
			throw IOException(
			    "RPC method '%s' request has %d fields, but the wire contract is parameterless [worker: %s]",
			    method_name, batch->schema()->num_fields(), worker_path);
		}
		return;
	}
	if (!batch) {
		throw IOException("RPC method '%s' called with no request batch, but the wire contract requires "
		                  "schema: %s [worker: %s]",
		                  method_name, expected.ToString(), worker_path);
	}
	if (!batch->schema()->Equals(expected, /*check_metadata=*/false)) {
		throw IOException("Outgoing request batch does not match the wire contract — likely a bug in the "
		                  "C++ request builder. RPC request schema mismatch for '%s' [worker: %s]\n%s",
		                  method_name, worker_path, DiffSchemas(expected, *batch->schema()));
	}
}

void ValidateItemSchema(const std::shared_ptr<arrow::RecordBatch> &item_batch, const std::string &method_name,
                        const std::string &worker_path, size_t item_index) {
	const auto *entry = LookupResponseSchema(method_name);
	if (!entry || !entry->item_schema) {
		return; // No per-item contract registered for this method.
	}
	if (!item_batch) {
		throw IOException("RPC method '%s' returned a null item at index %llu; expected an IPC-serialized "
		                  "batch with schema: %s [worker: %s]",
		                  method_name, (unsigned long long)item_index, entry->item_schema->ToString(),
		                  worker_path);
	}
	if (!item_batch->schema()->Equals(*entry->item_schema, /*check_metadata=*/false)) {
		throw IOException("Worker returned an out-of-date Apache Arrow schema. "
		                  "RPC item schema mismatch for '%s' item[%llu] [worker: %s]\n%s",
		                  method_name, (unsigned long long)item_index, worker_path,
		                  DiffSchemas(*entry->item_schema, *item_batch->schema()));
	}
}

} // namespace vgi
} // namespace duckdb
