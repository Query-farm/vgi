// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_schema_registry.hpp"

#include "duckdb/common/exception.hpp"
#include "generated/vgi_protocol_records.hpp"
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
//
// Every VgiProtocol method's request schema, and its result schema when the
// result is a record, come from the generated method table
// (generated::AllMethodSchemas()). Only the policy below is hand-written: which
// methods return `{items: List<Binary>}` and what each item is, which return raw
// bytes of a known record, and which are genuinely dynamic (and why).

struct DynamicMethod {
	const char *name;
	const char *reason;
};

struct ItemsMethod {
	const char *name;
	const VgiSchemaPtr &(*item)();
};

struct BlobMethod {
	const char *name;
	const VgiSchemaPtr &(*result)();
};

const std::unordered_map<std::string, ResponseSchema> &Registry() {
	// Meyers singleton: thread-safe static init, no heap, no destructor ordering.
	static const std::unordered_map<std::string, ResponseSchema> kRegistry = []() {
		// Catalog discovery wraps IPC-serialized info batches in {items: List<Binary>}.
		static const ItemsMethod kItemsResponses[] = {
		    {"catalog_catalogs", &CatalogInfoSchema},
		    {"catalog_schemas", &SchemaInfoSchema},
		    {"catalog_schema_get", &SchemaInfoSchema},
		    {"catalog_schema_contents_tables", &TableInfoSchema},
		    {"catalog_schema_contents_views", &ViewInfoSchema},
		    {"catalog_schema_contents_functions", &FunctionInfoSchema},
		    {"catalog_schema_contents_macros", &MacroInfoSchema},
		    {"catalog_copy_from_formats", &CopyFromFormatInfoSchema},
		    {"catalog_schema_contents_indexes", &IndexInfoSchema},
		    {"catalog_table_get", &TableInfoSchema},
		    {"catalog_view_get", &ViewInfoSchema},
		    {"catalog_macro_get", &MacroInfoSchema},
		    {"catalog_index_get", &IndexInfoSchema},
		};
		// Methods typed `bytes` in the Protocol whose bytes are a known record.
		static const BlobMethod kBlobResponses[] = {
		    {"catalog_table_scan_function_get", &ScanFunctionResultSchema},
		    {"catalog_table_insert_function_get", &ScanFunctionResultSchema},
		    {"catalog_table_update_function_get", &ScanFunctionResultSchema},
		    {"catalog_table_delete_function_get", &ScanFunctionResultSchema},
		    {"catalog_table_scan_branches_get", &ScanBranchesResultSchema},
		};
		// Only methods whose response shape is not a single fixed Arrow schema.
		static const DynamicMethod kDynamicResponses[] = {
		    {"catalog_table_column_statistics_get",
		     "raw IPC sparse-union min/max batch; schema varies per column type"},
		    {"table_function_statistics",
		     "Optional[bytes] of per-function IPC statistics; schema is function-specific"},
		    {"init",
		     "stream method; response framed as a stream header plus data frames, "
		     "not a single unary result batch"},
		};

		std::unordered_map<std::string, ResponseSchema> m;
		for (const auto &method : AllMethodSchemas()) {
			ResponseSchema entry;
			entry.request_schema = method.params;
			if (method.result) {
				// Strictly-typed record, including zero-field acks (a concrete 0-column
				// batch, distinct from void).
				entry.schema = method.result;
			} else if (method.returns_value) {
				entry.dynamic = true; // classified below
			}
			// else: void — the method returns None; a non-empty batch is a violation.
			m.emplace(method.method, std::move(entry));
		}
		auto classify = [&](const char *name) -> ResponseSchema & {
			auto it = m.find(name);
			if (it == m.end()) {
				throw InternalException("VGI schema registry: '%s' is not a VgiProtocol method", name);
			}
			return it->second;
		};
		for (const auto &e : kItemsResponses) {
			auto &entry = classify(e.name);
			entry.item_schema = e.item();
			entry.dynamic = false;
		}
		for (const auto &e : kBlobResponses) {
			auto &entry = classify(e.name);
			entry.schema = e.result();
			entry.dynamic = false;
		}
		for (const auto &e : kDynamicResponses) {
			auto &entry = classify(e.name);
			entry.schema = nullptr;
			entry.dynamic = true;
			entry.dynamic_reason = e.reason;
		}
		for (const auto &entry : m) {
			if (entry.second.dynamic && !entry.second.dynamic_reason) {
				throw InternalException("VGI schema registry: method '%s' returns bytes but is not classified as "
				                        "a known record or dynamic in vgi_schema_registry.cpp",
				                        entry.first);
			}
		}
		return m;
	}();
	return kRegistry;
}

// ============================================================================
// Diff rendering
// ============================================================================

std::string DescribeField(const VgiSchema &schema, int i) {
	std::ostringstream s;
	s << schema.field_name(i) << ": " << TypeToString(schema.field(i))
	  << (schema.field_nullable(i) ? " nullable" : " not null");
	return s.str();
}

std::string DiffSchemas(const VgiSchema &expected, const VgiSchema &actual) {
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
		// Field equality: name, nullability and type, metadata ignored.
		if (TypesEqual(expected.field(i), actual.field(i), /*check_names=*/true, /*check_metadata=*/false)) {
			continue;
		}
		out << "  [" << i << "] expected: " << DescribeField(expected, i) << "\n"
		    << "      actual:   " << DescribeField(actual, i) << "\n";
	}
	return out.str();
}

} // namespace

const ResponseSchema *LookupResponseSchema(const std::string &method_name) {
	const auto &reg = Registry();
	auto it = reg.find(method_name);
	return it == reg.end() ? nullptr : &it->second;
}

void ValidateResponseSchema(const VgiBatchPtr &batch, const std::string &method_name,
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

void ValidateRequestSchema(const VgiBatchPtr &batch, const std::string &method_name,
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

void ValidateItemSchema(const VgiBatchPtr &item_batch, const std::string &method_name,
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
