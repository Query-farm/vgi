#pragma once

#include <arrow/api.h>
#include <memory>
#include <string>

namespace duckdb {
namespace vgi {

// Expected shape of a unary RPC method's inner response batch — i.e., the batch
// obtained after ExtractAndDeserializeResult unwraps the {result: Binary}
// envelope.
//
// - schema != nullptr, dynamic == false: strict validation against `schema`.
// - schema == nullptr, dynamic == false: void response; a non-empty batch is
//   treated as a protocol violation.
// - dynamic == true: validation deferred (response schema varies per call,
//   e.g. bind's output_schema is per-function, or response is raw IPC bytes
//   that are not a standard result envelope).
struct ResponseSchema {
	std::shared_ptr<arrow::Schema> schema;
	// For methods whose response is `{items: List<Binary>}`: the schema each
	// inner IPC-serialized item decodes to. null when the method's response
	// doesn't follow that pattern.
	std::shared_ptr<arrow::Schema> item_schema = nullptr;
	bool dynamic = false;
	// For dynamic methods: human-readable reason the registry doesn't strictly
	// validate this method's response, and which component owns validation
	// instead. nullptr for non-dynamic entries.
	const char *dynamic_reason = nullptr;
	// Outer wire schema for this method's request batch (what C++ writes to
	// the worker's stdin). Matches the `params_schema` vgi-rpc expects.
	// Empty schema for parameter-less methods.
	std::shared_ptr<arrow::Schema> request_schema = nullptr;
};

// Look up a method's registered response contract. Returns nullptr when the
// method has no entry. Callers should treat "no entry" as a programmer error;
// `ValidateResponseSchema` will throw in that case.
const ResponseSchema *LookupResponseSchema(const std::string &method_name);

// Validate a deserialized response batch against the registered contract for
// `method_name`. Throws IOException with a readable expected-vs-actual diff on
// mismatch, method missing from the registry, or void/non-void inversion.
// Passing a null batch is legal when the registered contract is void.
void ValidateResponseSchema(const std::shared_ptr<arrow::RecordBatch> &batch,
                            const std::string &method_name,
                            const std::string &worker_path);

// Validate a single deserialized item from a {items: List<Binary>} response.
// The item is itself an IPC-serialized RecordBatch whose schema must match the
// registered `item_schema` for `method_name`. No-op if the method has no
// item_schema registered (e.g. method isn't a list-of-items response, or the
// registry deliberately leaves inner items unvalidated).
void ValidateItemSchema(const std::shared_ptr<arrow::RecordBatch> &item_batch,
                        const std::string &method_name,
                        const std::string &worker_path,
                        size_t item_index);

// Validate an outgoing request batch against the registered per-method params
// schema before it's written to the wire. Catches encoder drift (wrong field
// order, wrong nullability, missing fields) in C++ BuildXxxParams functions at
// the boundary where it happens — before the worker has to diagnose it.
//
// `batch` may be null for methods that take no params (e.g. catalog_catalogs,
// whose request has an empty schema). If `method_name` has no registered
// params schema, throws — every invoked method must be registered.
void ValidateRequestSchema(const std::shared_ptr<arrow::RecordBatch> &batch,
                           const std::string &method_name,
                           const std::string &worker_path);

} // namespace vgi
} // namespace duckdb
