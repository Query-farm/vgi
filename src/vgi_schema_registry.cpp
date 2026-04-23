#include "vgi_schema_registry.hpp"

#include "duckdb/common/exception.hpp"

#include <sstream>
#include <unordered_map>

namespace duckdb {
namespace vgi {

namespace {

// ============================================================================
// Schema helpers
// ============================================================================
//
// The authoritative definitions live in vgi-python. To audit or regenerate
// these schemas, run:
//
//   .venv/bin/python -c "from vgi.<module> import <Class>; print(<Class>.ARROW_SCHEMA)"
//
// See `/Users/rusty/Development/vgi-python/vgi/protocol.py`,
// `/Users/rusty/Development/vgi-python/vgi/catalog/catalog_interface.py`, and
// `/Users/rusty/Development/vgi-python/vgi/invocation.py`.

// Common sub-type factories --------------------------------------------------

std::shared_ptr<arrow::DataType> Utf8StringMap() {
	// pyarrow `pa.map_(pa.string(), pa.string())` — default child struct name
	// "entries", keys non-null, values nullable. arrow::map matches.
	return arrow::map(arrow::utf8(), arrow::utf8());
}

// Enum serialization (vgi-rpc _infer_arrow_type):
// Enum subclasses become dictionary<int16, string>.
std::shared_ptr<arrow::DataType> EnumType() {
	return arrow::dictionary(arrow::int16(), arrow::utf8());
}

// CatalogObject inheritance: parent's dataclass fields come FIRST.
// CatalogObject = {comment: str|None, tags: dict[str,str]}
// CatalogSchemaObject(CatalogObject) adds {name, schema_name}.

// ============================================================================
// Inner info-batch schemas (per-item in {items: List<Binary>} responses)
// ============================================================================

std::shared_ptr<arrow::Schema> CatalogInfoSchema() {
	static const auto schema = arrow::schema({
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("implementation_version", arrow::utf8(), true),
	    arrow::field("data_version_spec", arrow::utf8(), true),
	});
	return schema;
}

std::shared_ptr<arrow::Schema> SchemaInfoSchema() {
	static const auto schema = arrow::schema({
	    arrow::field("comment", arrow::utf8(), true),
	    arrow::field("tags", Utf8StringMap(), false),
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("name", arrow::utf8(), false),
	});
	return schema;
}

std::shared_ptr<arrow::Schema> TableInfoSchema() {
	static const auto schema = arrow::schema({
	    arrow::field("comment", arrow::utf8(), true),
	    arrow::field("tags", Utf8StringMap(), false),
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("schema_name", arrow::utf8(), false),
	    arrow::field("columns", arrow::binary(), false),
	    arrow::field("not_null_constraints", arrow::list(arrow::int32()), false),
	    arrow::field("unique_constraints", arrow::list(arrow::list(arrow::int32())), false),
	    arrow::field("check_constraints", arrow::list(arrow::utf8()), false),
	    arrow::field("primary_key_constraints", arrow::list(arrow::list(arrow::int32())), false),
	    arrow::field("foreign_key_constraints", arrow::list(arrow::binary()), false),
	    arrow::field("supports_insert", arrow::boolean(), false),
	    arrow::field("supports_update", arrow::boolean(), false),
	    arrow::field("supports_delete", arrow::boolean(), false),
	    arrow::field("supports_column_statistics", arrow::boolean(), false),
	});
	return schema;
}

std::shared_ptr<arrow::Schema> ViewInfoSchema() {
	static const auto schema = arrow::schema({
	    arrow::field("comment", arrow::utf8(), true),
	    arrow::field("tags", Utf8StringMap(), false),
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("schema_name", arrow::utf8(), false),
	    arrow::field("definition", arrow::utf8(), false),
	});
	return schema;
}

std::shared_ptr<arrow::Schema> MacroInfoSchema() {
	static const auto schema = arrow::schema({
	    arrow::field("comment", arrow::utf8(), true),
	    arrow::field("tags", Utf8StringMap(), false),
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("schema_name", arrow::utf8(), false),
	    arrow::field("macro_type", EnumType(), false),
	    arrow::field("parameters", arrow::list(arrow::utf8()), false),
	    arrow::field("parameter_default_values", arrow::binary(), false),
	    arrow::field("definition", arrow::utf8(), false),
	});
	return schema;
}

std::shared_ptr<arrow::Schema> IndexInfoSchema() {
	static const auto schema = arrow::schema({
	    arrow::field("comment", arrow::utf8(), true),
	    arrow::field("tags", Utf8StringMap(), false),
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("schema_name", arrow::utf8(), false),
	    arrow::field("table_name", arrow::utf8(), false),
	    arrow::field("index_type", arrow::utf8(), false),
	    arrow::field("constraint_type", EnumType(), false),
	    arrow::field("expressions", arrow::list(arrow::utf8()), false),
	    arrow::field("options", Utf8StringMap(), false),
	});
	return schema;
}

std::shared_ptr<arrow::Schema> FunctionInfoSchema() {
	// CatalogExample struct: {sql: Utf8, description: Utf8, expected_output: Utf8?}
	auto example_struct =
	    arrow::struct_({arrow::field("sql", arrow::utf8(), false),
	                    arrow::field("description", arrow::utf8(), false),
	                    arrow::field("expected_output", arrow::utf8(), true)});
	// SecretLookupEntry struct: {secret_type: Utf8, scope: Utf8?, secret_name: Utf8?}
	auto secret_struct =
	    arrow::struct_({arrow::field("secret_type", arrow::utf8(), false),
	                    arrow::field("scope", arrow::utf8(), true),
	                    arrow::field("secret_name", arrow::utf8(), true)});

	static const auto schema = arrow::schema({
	    arrow::field("comment", arrow::utf8(), true),
	    arrow::field("tags", Utf8StringMap(), false),
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("schema_name", arrow::utf8(), false),
	    arrow::field("function_type", EnumType(), false),
	    arrow::field("arguments", arrow::binary(), false),
	    arrow::field("output_schema", arrow::binary(), false),
	    arrow::field("stability", EnumType(), true),
	    arrow::field("null_handling", EnumType(), true),
	    arrow::field("description", arrow::utf8(), false),
	    arrow::field("examples", arrow::list(example_struct), false),
	    arrow::field("categories", arrow::list(arrow::utf8()), false),
	    arrow::field("projection_pushdown", arrow::boolean(), true),
	    arrow::field("filter_pushdown", arrow::boolean(), true),
	    arrow::field("sampling_pushdown", arrow::boolean(), true),
	    arrow::field("supported_expression_filters", arrow::list(arrow::utf8()), false),
	    arrow::field("order_preservation", EnumType(), true),
	    arrow::field("max_workers", arrow::int32(), false),
	    arrow::field("order_dependent", EnumType(), false),
	    arrow::field("distinct_dependent", EnumType(), false),
	    arrow::field("supports_window", arrow::boolean(), false),
	    arrow::field("required_settings", arrow::list(arrow::utf8()), false),
	    arrow::field("required_secrets", arrow::list(secret_struct), false),
	});
	return schema;
}

// ============================================================================
// Outer response schemas
// ============================================================================

// Shared: `{items: List<Binary>}` — every *Response wrapper from
// vgi-python's `_catalog_items_response`.
std::shared_ptr<arrow::Schema> ListBinaryItemsSchema() {
	static const auto schema =
	    arrow::schema({arrow::field("items", arrow::list(arrow::binary()), false)});
	return schema;
}

// CatalogAttachResult — catalog_interface.py:114-157.
std::shared_ptr<arrow::Schema> CatalogAttachResultSchema() {
	static const auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("supports_transactions", arrow::boolean(), false),
	    arrow::field("supports_time_travel", arrow::boolean(), false),
	    arrow::field("catalog_version_frozen", arrow::boolean(), false),
	    arrow::field("catalog_version", arrow::int64(), false),
	    arrow::field("attach_id_required", arrow::boolean(), false),
	    arrow::field("default_schema", arrow::utf8(), false),
	    arrow::field("settings", arrow::list(arrow::binary()), false),
	    arrow::field("secret_types", arrow::list(arrow::binary()), false),
	    arrow::field("comment", arrow::utf8(), true),
	    arrow::field("tags", Utf8StringMap(), false),
	    arrow::field("supports_column_statistics", arrow::boolean(), false),
	    arrow::field("resolved_data_version", arrow::utf8(), true),
	    arrow::field("resolved_implementation_version", arrow::utf8(), true),
	});
	return schema;
}

// CatalogVersionResponse — protocol.py:249-252.
std::shared_ptr<arrow::Schema> CatalogVersionSchema() {
	static const auto schema = arrow::schema({arrow::field("version", arrow::int64(), false)});
	return schema;
}

// TransactionBeginResponse — protocol.py:255-259.
std::shared_ptr<arrow::Schema> TransactionBeginSchema() {
	static const auto schema =
	    arrow::schema({arrow::field("transaction_id", arrow::binary(), true)});
	return schema;
}

// TableCardinality — table_function.py:60-74.
std::shared_ptr<arrow::Schema> TableCardinalitySchema() {
	static const auto schema = arrow::schema({
	    arrow::field("estimate", arrow::int64(), true),
	    arrow::field("max", arrow::int64(), true),
	});
	return schema;
}

// BindResponse — invocation.py:49 (scalar/table/table-in-out function bind).
std::shared_ptr<arrow::Schema> BindResponseSchema() {
	static const auto schema = arrow::schema({
	    arrow::field("output_schema", arrow::binary(), false),
	    arrow::field("opaque_data", arrow::binary(), false),
	    arrow::field("lookup_secret_types", arrow::list(arrow::utf8()), false),
	    arrow::field("lookup_scopes", arrow::list(arrow::utf8()), false),
	    arrow::field("lookup_names", arrow::list(arrow::utf8()), false),
	});
	return schema;
}

// AggregateBindResponse — protocol.py:854.
std::shared_ptr<arrow::Schema> AggregateBindResponseSchema() {
	static const auto schema = arrow::schema({
	    arrow::field("output_schema", arrow::binary(), false),
	    arrow::field("execution_id", arrow::binary(), false),
	});
	return schema;
}

// Empty-struct ack: AggregateUpdateResponse, AggregateCombineResponse,
// AggregateDestructorResponse, AggregateWindowInitResponse,
// AggregateWindowDestructorResponse — all zero-field schemas.
std::shared_ptr<arrow::Schema> EmptyStructSchema() {
	static const auto schema = arrow::schema({});
	return schema;
}

// AggregateFinalizeResponse / AggregateWindowResponse / AggregateWindowBatchResponse.
std::shared_ptr<arrow::Schema> ResultBatchSchema() {
	static const auto schema =
	    arrow::schema({arrow::field("result_batch", arrow::binary(), false)});
	return schema;
}

// ScanFunctionResult / WriteFunctionResult — catalog_interface.py:369-398.
std::shared_ptr<arrow::Schema> ScanFunctionResultSchema() {
	static const auto schema = arrow::schema({
	    arrow::field("function_name", arrow::utf8(), false),
	    arrow::field("arguments", arrow::binary(), false),
	    arrow::field("required_extensions", arrow::list(arrow::utf8()), false),
	});
	return schema;
}

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
		struct ItemsMethod {
			const char *name;
			std::shared_ptr<arrow::Schema> item_schema;
		};
		const ItemsMethod kItemsResponses[] = {
		    {"catalog_catalogs", CatalogInfoSchema()},
		    {"catalog_schemas", SchemaInfoSchema()},
		    {"catalog_schema_get", SchemaInfoSchema()},
		    {"catalog_schema_contents_tables", TableInfoSchema()},
		    {"catalog_schema_contents_views", ViewInfoSchema()},
		    {"catalog_schema_contents_functions", FunctionInfoSchema()},
		    {"catalog_schema_contents_macros", MacroInfoSchema()},
		    {"catalog_schema_contents_indexes", IndexInfoSchema()},
		    {"catalog_table_get", TableInfoSchema()},
		    {"catalog_view_get", ViewInfoSchema()},
		    {"catalog_macro_get", MacroInfoSchema()},
		    {"catalog_index_get", IndexInfoSchema()},
		};
		for (const auto &e : kItemsResponses) {
			m[e.name] = ResponseSchema{ListBinaryItemsSchema(), e.item_schema, false, nullptr};
		}

		// --- Void responses -----------------------------------------------
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
		m["catalog_version"] = ResponseSchema{CatalogVersionSchema(), nullptr, false, nullptr};
		m["catalog_transaction_begin"] =
		    ResponseSchema{TransactionBeginSchema(), nullptr, false, nullptr};
		m["table_function_cardinality"] =
		    ResponseSchema{TableCardinalitySchema(), nullptr, false, nullptr};
		m["bind"] = ResponseSchema{BindResponseSchema(), nullptr, false, nullptr};
		m["aggregate_bind"] = ResponseSchema{AggregateBindResponseSchema(), nullptr, false, nullptr};
		m["aggregate_finalize"] = ResponseSchema{ResultBatchSchema(), nullptr, false, nullptr};
		m["aggregate_window"] = ResponseSchema{ResultBatchSchema(), nullptr, false, nullptr};
		m["aggregate_window_batch"] = ResponseSchema{ResultBatchSchema(), nullptr, false, nullptr};
		m["catalog_table_scan_function_get"] =
		    ResponseSchema{ScanFunctionResultSchema(), nullptr, false, nullptr};
		m["catalog_table_insert_function_get"] =
		    ResponseSchema{ScanFunctionResultSchema(), nullptr, false, nullptr};
		m["catalog_table_update_function_get"] =
		    ResponseSchema{ScanFunctionResultSchema(), nullptr, false, nullptr};
		m["catalog_table_delete_function_get"] =
		    ResponseSchema{ScanFunctionResultSchema(), nullptr, false, nullptr};

		// --- Zero-field ("empty struct") ack responses --------------------
		// These ARE concrete 0-column batches on the wire, distinct from void
		// (which has no batch at all). Mismatch = worker drift.
		const char *kEmptyStructResponses[] = {
		    "aggregate_update",
		    "aggregate_combine",
		    "aggregate_destructor",
		    "aggregate_window_init",
		    "aggregate_window_destructor",
		};
		for (const char *name : kEmptyStructResponses) {
			m[name] = ResponseSchema{EmptyStructSchema(), nullptr, false, nullptr};
		}

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
