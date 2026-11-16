// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"

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
	VgiSchemaPtr schema;
	// For methods whose response is `{items: List<Binary>}`: the schema each
	// inner IPC-serialized item decodes to. null when the method's response
	// doesn't follow that pattern.
	VgiSchemaPtr item_schema = nullptr;
	bool dynamic = false;
	// For dynamic methods: human-readable reason the registry doesn't strictly
	// validate this method's response, and which component owns validation
	// instead. nullptr for non-dynamic entries.
	const char *dynamic_reason = nullptr;
	// Outer wire schema for this method's request batch (what C++ writes to
	// the worker's stdin). Matches the `params_schema` vgi-rpc expects.
	// Empty schema for parameter-less methods.
	VgiSchemaPtr request_schema = nullptr;
};

// A method's registered response contract, or nullptr when unregistered (a
// programmer error; `ValidateResponseSchema` throws for it).
const ResponseSchema *LookupResponseSchema(const std::string &method_name);

// Validate a deserialized response batch against the registered contract for
// `method_name`. Throws IOException with a readable expected-vs-actual diff on
// mismatch, method missing from the registry, or void/non-void inversion.
// Passing a null batch is legal when the registered contract is void.
void ValidateResponseSchema(const VgiBatchPtr &batch,
                            const std::string &method_name,
                            const std::string &worker_path);

// Validate one deserialized item of a {items: List<Binary>} response against
// the method's registered `item_schema`; no-op when none is registered.
void ValidateItemSchema(const VgiBatchPtr &item_batch,
                        const std::string &method_name,
                        const std::string &worker_path,
                        size_t item_index);

// Validate an outgoing request batch against the method's registered params
// schema, catching encoder drift (field order, nullability, missing fields)
// before the worker has to diagnose it. `batch` may be null for parameter-less
// methods (e.g. catalog_catalogs). Throws for an unregistered method.
void ValidateRequestSchema(const VgiBatchPtr &batch,
                           const std::string &method_name,
                           const std::string &worker_path);

} // namespace vgi
} // namespace duckdb
