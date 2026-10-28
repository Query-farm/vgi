// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_reflection.hpp"

#include <mutex>
#include <unordered_map>

#include "nanoarrow/nanoarrow.h"

#include "duckdb.hpp"
#include "storage/vgi_catalog.hpp"
#include "vgi_catalog_rpc.hpp"
#include "vgi_exception.hpp"
#include "vgi_logging.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// Decoding (tolerant: by name, unknown columns ignored, defaults applied)
// ============================================================================

namespace {

[[noreturn]] void DecodeFailure(const std::string &what, const std::string &detail, const std::string &worker_path) {
	throw IOException("Could not decode a '%s' %s: %s [worker: %s]. Check that "
	                  "the worker hosts a compatible "
	                  "major version of the reflection protocol.",
	                  REFLECTION_PROTOCOL_NAME, what, detail, worker_path);
}

// A column with its type parsed once (VgiColumn::type() parses the format string).
struct TypedColumn {
	TypedColumn() = default;
	explicit TypedColumn(VgiColumn column_p) : column(column_p), type(column_p ? column_p.type().type : 0) {
	}
	VgiColumn column;
	int type = 0;
};

// The child of struct `parent` called `name`; empty when there is none.
VgiColumn ChildByName(const VgiColumn &parent, const char *name) {
	for (int64_t k = 0; k < parent.num_children(); k++) {
		auto child = parent.child(k);
		if (child.name() == name) {
			return child;
		}
	}
	return VgiColumn();
}

// The string at `i`, or nullopt for a null or an absent column. Accepts every
// Arrow string layout a port may emit.
std::optional<std::string> StringAt(const TypedColumn &array, int64_t i) {
	if (!array.column || array.column.IsNull(i)) {
		return std::nullopt;
	}
	switch (array.type) {
	case NANOARROW_TYPE_STRING:
	case NANOARROW_TYPE_LARGE_STRING:
	case NANOARROW_TYPE_STRING_VIEW:
		return array.column.GetString(i);
	default:
		return std::nullopt;
	}
}

std::optional<bool> BoolAt(const TypedColumn &array, int64_t i) {
	if (!array.column || array.column.IsNull(i) || array.type != NANOARROW_TYPE_BOOL) {
		return std::nullopt;
	}
	return array.column.GetBool(i);
}

std::string RequiredString(const TypedColumn &array, int64_t i, const char *field, const char *what,
                           const std::string &worker_path) {
	auto value = StringAt(array, i);
	if (!value) {
		DecodeFailure(what, StringUtil::Format("it carries no string '%s', and that field has no default", field),
		              worker_path);
	}
	return std::move(*value);
}

VgiProtocolListing DecodeProtocolList(const VgiBatchPtr &batch, const std::string &worker_path) {
	const char *what = "protocol listing";
	if (!batch || batch->num_rows() < 1) {
		DecodeFailure(what, "the payload carries no rows", worker_path);
	}
	auto column = [&](const char *name) {
		return TypedColumn(batch->GetColumnByName(name));
	};
	VgiProtocolListing listing;
	listing.server_id = RequiredString(column("server_id"), 0, "server_id", what, worker_path);
	listing.server_version = RequiredString(column("server_version"), 0, "server_version", what, worker_path);
	listing.request_version = RequiredString(column("request_version"), 0, "request_version", what, worker_path);

	auto protocols = column("protocols");
	if (!protocols.column) {
		DecodeFailure(what, "the payload has no 'protocols' column", worker_path);
	}
	if (protocols.column.IsNull(0)) {
		DecodeFailure(what, "its 'protocols' column is null", worker_path);
	}
	if (protocols.type != NANOARROW_TYPE_LIST && protocols.type != NANOARROW_TYPE_LARGE_LIST) {
		DecodeFailure(what, "its 'protocols' column is not a list", worker_path);
	}
	int64_t start = 0, end = 0;
	protocols.column.ListRange(0, start, end);
	auto summaries = protocols.column.child(0);
	if (!summaries || summaries.type().type != NANOARROW_TYPE_STRUCT) {
		DecodeFailure(what, "its 'protocols' elements are not structs", worker_path);
	}
	const TypedColumn name_col(ChildByName(summaries, "protocol"));
	const TypedColumn version_col(ChildByName(summaries, "protocol_version"));
	const TypedColumn hash_col(ChildByName(summaries, "protocol_hash"));
	const TypedColumn deprecated_col(ChildByName(summaries, "deprecated"));
	const TypedColumn message_col(ChildByName(summaries, "deprecation_message"));
	// `features` is reserved and MUST be ignored by clients.
	for (int64_t i = start; i < end; i++) {
		VgiHostedProtocol p;
		p.name = RequiredString(name_col, i, "protocol", "protocol summary", worker_path);
		p.version = RequiredString(version_col, i, "protocol_version", "protocol summary", worker_path);
		p.hash = RequiredString(hash_col, i, "protocol_hash", "protocol summary", worker_path);
		p.deprecated = BoolAt(deprecated_col, i).value_or(false);
		p.deprecation_message = StringAt(message_col, i).value_or("");
		listing.protocols.push_back(std::move(p));
	}
	return listing;
}

} // namespace

// ============================================================================
// RPC
// ============================================================================

bool IsReflectionNotHostedError(const std::exception &e) {
	if (auto *rpc = dynamic_cast<const VgiRpcException *>(&e)) {
		const auto &kind = rpc->GetErrorKind();
		if (kind == error_kind::kProtocolNotSupported || kind == error_kind::kMethodNotImplemented) {
			return true;
		}
		if (kind.empty() && rpc->GetErrorCode() == "UNIMPLEMENTED") {
			return true;
		}
		return false;
	}
	// A server with no route for the protocol at all answers a bare 404 that
	// carries no Arrow error batch. The path is the reflection route, so the
	// message names it.
	std::string message = e.what();
	return message.find("(HTTP 404)") != std::string::npos &&
	       message.find(REFLECTION_PROTOCOL_NAME) != std::string::npos;
}

VgiProtocolListing InvokeListProtocols(const CatalogRpcContext &ctx, ClientContext &context,
                                       bool tolerate_pre_reflection) {
	VgiBatchPtr payload;
	try {
		payload = InvokeReflectionListProtocols(ctx, context);
	} catch (const std::exception &e) {
		if (!tolerate_pre_reflection || !IsReflectionNotHostedError(e)) {
			throw;
		}
		VGI_LOG(context, "reflection.pre_reflection_worker",
		        {{"worker_path", ctx.params->worker_path()}, {"error", e.what()}});
		return PreReflectionListing();
	}
	return DecodeProtocolList(payload, ctx.params->worker_path());
}

std::shared_ptr<const VgiProtocolListing> GetHostedProtocols(ClientContext &context,
                                                             const std::shared_ptr<VgiAttachParameters> &params) {
	if (!params) {
		throw InternalException("vgi: GetHostedProtocols called without attach parameters");
	}
	auto generation = WorkerConnectionGeneration(params->worker_path());
	return params->hosted_protocols().GetOrFetch(generation, [&]() {
		// list_protocols needs no attach or transaction state: the catalog's
		// transport, auth, cookies and HTTP pool are all on `params`.
		CatalogRpcContext ctx {params, {}, {}};
		auto listing = InvokeListProtocols(ctx, context, /*tolerate_pre_reflection=*/true);
		VGI_LOG(context, "reflection.list_protocols",
		        {{"worker_path", params->worker_path()},
		         {"catalog", params->catalog_name()},
		         {"num_protocols", std::to_string(listing.protocols.size())},
		         {"predates_reflection", listing.predates_reflection ? "true" : "false"}});
		return listing;
	});
}

std::shared_ptr<const VgiProtocolListing> GetHostedProtocols(ClientContext &context, const VgiCatalog &catalog) {
	return GetHostedProtocols(context, catalog.attach_parameters());
}

std::optional<std::string> HostsProtocol(ClientContext &context, const std::shared_ptr<VgiAttachParameters> &params,
                                         const std::string &protocol_name) {
	auto listing = GetHostedProtocols(context, params);
	if (auto *p = listing->Find(protocol_name)) {
		return p->version;
	}
	return std::nullopt;
}

std::optional<std::string> HostsProtocol(ClientContext &context, const VgiCatalog &catalog,
                                         const std::string &protocol_name) {
	return HostsProtocol(context, catalog.attach_parameters(), protocol_name);
}

} // namespace vgi
} // namespace duckdb
