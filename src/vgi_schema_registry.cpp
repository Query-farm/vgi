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

using generated::AggregateBindResultSchema;
using generated::AggregateCombineResultSchema;
using generated::AggregateDestructorResultSchema;
using generated::AggregateFinalizeResultSchema;
using generated::AggregateUpdateResultSchema;
using generated::AggregateWindowBatchResultSchema;
using generated::AggregateWindowDestructorResultSchema;
using generated::AggregateWindowInitResultSchema;
using generated::AggregateWindowResultSchema;
using generated::BindResultSchema;
using generated::CatalogAttachResultSchema;
using generated::CatalogCatalogsResultSchema;
using generated::CatalogInfoSchema;
using generated::CatalogIndexGetResultSchema;
using generated::CatalogMacroGetResultSchema;
using generated::CatalogSchemaContentsFunctionsResultSchema;
using generated::CatalogSchemaContentsIndexesResultSchema;
using generated::CatalogSchemaContentsMacrosResultSchema;
using generated::CatalogSchemaContentsTablesResultSchema;
using generated::CatalogSchemaContentsViewsResultSchema;
using generated::CatalogSchemaGetResultSchema;
using generated::CatalogSchemasResultSchema;
using generated::CatalogTableGetResultSchema;
using generated::CatalogTransactionBeginResultSchema;
using generated::CatalogVersionResultSchema;
using generated::CatalogViewGetResultSchema;
using generated::FunctionInfoSchema;
using generated::IndexInfoSchema;
using generated::MacroInfoSchema;
using generated::ScanFunctionResultSchema;
using generated::SchemaInfoSchema;
using generated::TableFunctionCardinalityResultSchema;
using generated::TableInfoSchema;
using generated::ViewInfoSchema;

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
		m["bind"] = ResponseSchema{BindResultSchema(), nullptr, false, nullptr};
		m["aggregate_bind"] = ResponseSchema{AggregateBindResultSchema(), nullptr, false, nullptr};
		m["aggregate_finalize"] = ResponseSchema{AggregateFinalizeResultSchema(), nullptr, false, nullptr};
		m["aggregate_window"] = ResponseSchema{AggregateWindowResultSchema(), nullptr, false, nullptr};
		m["aggregate_window_batch"] =
		    ResponseSchema{AggregateWindowBatchResultSchema(), nullptr, false, nullptr};
		m["catalog_table_scan_function_get"] =
		    ResponseSchema{ScanFunctionResultSchema(), nullptr, false, nullptr};
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
		m["aggregate_window_init"] =
		    ResponseSchema{AggregateWindowInitResultSchema(), nullptr, false, nullptr};
		m["aggregate_window_destructor"] =
		    ResponseSchema{AggregateWindowDestructorResultSchema(), nullptr, false, nullptr};

		// --- Dynamic / genuinely per-call responses -----------------------
		// Only methods whose response shape is not expressible as a single
		// fixed Arrow schema belong here.
		const DynamicMethod kDynamicResponses[] = {
		    {"catalog_table_column_statistics_get",
		     "raw IPC sparse-union min/max batch; schema varies per column type"},
		    {"table_function_statistics",
		     "Optional[bytes] of per-function IPC statistics; schema is function-specific"},
		};
		for (const auto &entry : kDynamicResponses) {
			m[entry.name] = ResponseSchema{nullptr, nullptr, true, entry.reason};
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
		    << "  expected schema: " << expected.ToString() << "\n"
		    << "  actual schema:   " << actual.ToString();
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
		throw IOException("RPC response schema mismatch for '%s' [worker: %s]\n%s"
		                  "This usually indicates the worker returned an out-of-date response shape.",
		                  method_name, worker_path, DiffSchemas(*entry->schema, *batch->schema()));
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
		throw IOException("RPC item schema mismatch for '%s' item[%llu] [worker: %s]\n%s"
		                  "This usually indicates the worker returned an out-of-date info shape.",
		                  method_name, (unsigned long long)item_index, worker_path,
		                  DiffSchemas(*entry->item_schema, *item_batch->schema()));
	}
}

} // namespace vgi
} // namespace duckdb
