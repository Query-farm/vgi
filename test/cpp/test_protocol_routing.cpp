// © Copyright 2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Every request this client emits must name the protocol it addresses.
//
// A vgi-rpc server dispatches on the pair (protocol, method). One server may
// co-host several protocols, method names are allowed to collide between them,
// and on the raw transports — subprocess, AF_UNIX, TCP, stdio, SAB, Iroh — the
// `vgi_rpc.protocol` metadata key is the ONLY carrier of that routing decision.
// A request that omits it is unroutable: the server raises
// ProtocolNotSpecifiedError rather than guessing, precisely so a co-hosting
// server can never silently pick the wrong binding.
//
// The integration suite already proves the main protocol's key is present and
// correct — without it not even ATTACH completes. It does NOT cover the two
// cases below, which is why they are pinned here:
//
//   * The secret protocol. `secret_lookup` addresses Orchard's standalone
//     secret service, a DIFFERENT protocol with its own independent version.
//     The integration suite resolves secrets from DuckDB's own secret manager
//     and never dials that service, so a hardcoded "main protocol" routing key
//     on that path would pass every test here and misroute in production the
//     moment a server co-hosts both.
//   * Reserved, server-level methods (`__transport_options__`,
//     `__upload_url__`). These belong to no protocol: the server resolves them
//     from a built-in table BEFORE routing, and over HTTP mounts them flat at
//     {prefix}/{method}. Stamping a routing key on one is not harmlessly
//     redundant — over HTTP the server compares the key against the resolved
//     method's (empty) protocol name and rejects the mismatch.
//
// ---------------------------------------------------------------------------
// What this does NOT check, and why that is fine
// ---------------------------------------------------------------------------
//
// It does not check that the name literals are correct. They are generated:
// `vgi.codegen.cpp_protocol_name` emits `vgi_protocol_names.hpp` from the same
// `_protocol_wire_name` the dispatcher routes on, and vgi-python's
// `tests/test_generated_cpp_protocol_name.py` fails if the checked-in header
// drifts from it. Asserting a literal here would just be a second, weaker copy
// of the contract — exactly the hand-spelling that caused the break these tests
// were written for.
//
// What is checked here is everything the generator cannot see: that the value
// actually reaches the wire, on the right method, and is absent where its
// presence would be rejected.

#include "catch.hpp"

#include "vgi_rpc_client.hpp"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <memory>
#include <string>
#include <vector>

using namespace duckdb::vgi;

namespace {

// Read back the custom_metadata this client stamped on a serialized request.
std::shared_ptr<arrow::KeyValueMetadata> RequestMetadata(const std::vector<uint8_t> &body) {
	auto buffer = arrow::Buffer::Wrap(body.data(), body.size());
	auto input = std::make_shared<arrow::io::BufferReader>(buffer);
	auto reader = arrow::ipc::RecordBatchStreamReader::Open(input).ValueOrDie();
	auto with_metadata = reader->ReadNext().ValueOrDie();
	REQUIRE(with_metadata.batch != nullptr);
	return with_metadata.custom_metadata;
}

std::shared_ptr<arrow::RecordBatch> EmptyParams() {
	return arrow::RecordBatch::Make(arrow::schema({}), 1, std::vector<std::shared_ptr<arrow::Array>> {});
}

std::string MetadataValue(const std::shared_ptr<arrow::KeyValueMetadata> &md, const char *key) {
	REQUIRE(md != nullptr);
	auto found = md->Get(key);
	REQUIRE(found.ok());
	return found.ValueUnsafe();
}

} // namespace

TEST_CASE("a worker-protocol request names the worker protocol", "[protocol-routing]") {
	auto md = RequestMetadata(SerializeRpcRequest("bind", EmptyParams()));

	REQUIRE(MetadataValue(md, RPC_PROTOCOL_KEY) == std::string(generated::VGI_PROTOCOL_NAME));
	REQUIRE(MetadataValue(md, RPC_PROTOCOL_VERSION_KEY) ==
	        std::string(generated::VGI_PROTOCOL_VERSION));
	REQUIRE(MetadataValue(md, RPC_METHOD_KEY) == "bind");
}

TEST_CASE("a secret-service request names the secret protocol, not the worker protocol",
          "[protocol-routing]") {
	auto md = RequestMetadata(SerializeRpcRequest("secret_lookup", EmptyParams(), VGI_SECRET_PROTOCOL));

	REQUIRE(MetadataValue(md, RPC_PROTOCOL_KEY) == std::string(generated::VGI_SECRET_PROTOCOL_NAME));
	// The two protocols are versioned independently — the secret service's 1.x
	// has nothing to do with the worker protocol's 2.x. Name and version travel
	// together so a request can never carry one protocol's name and another's
	// version, which routes to a binding that then rejects it.
	REQUIRE(MetadataValue(md, RPC_PROTOCOL_VERSION_KEY) ==
	        std::string(generated::VGI_SECRET_PROTOCOL_VERSION));
	REQUIRE(VGI_SECRET_PROTOCOL.name != VGI_MAIN_PROTOCOL.name);
}

TEST_CASE("reserved server-level methods carry no routing key", "[protocol-routing]") {
	// __transport_options__ is resolved from the server's built-in table before
	// routing; it is owned by no protocol. Over HTTP the server compares any
	// routing key it finds against the resolved method's empty protocol name,
	// so sending one turns a working capability handshake into a hard rejection.
	auto md = RequestMetadata(SerializeRpcRequest(TRANSPORT_OPTIONS_METHOD, EmptyParams()));

	REQUIRE(md != nullptr);
	REQUIRE_FALSE(md->Contains(RPC_PROTOCOL_KEY));
	REQUIRE(MetadataValue(md, RPC_METHOD_KEY) == std::string(TRANSPORT_OPTIONS_METHOD));

	REQUIRE(IsReservedRpcMethod(TRANSPORT_OPTIONS_METHOD));
	// The upload-URL endpoint is addressed with its /init suffix attached, so
	// the carve-out keys off the leading "__" rather than the dunder shape.
	REQUIRE(IsReservedRpcMethod("__upload_url__/init"));
	REQUIRE_FALSE(IsReservedRpcMethod("bind"));
	REQUIRE_FALSE(IsReservedRpcMethod("catalog_attach"));
}
