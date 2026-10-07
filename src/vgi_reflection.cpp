// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_reflection.hpp"

#include <mutex>
#include <unordered_map>

#include <arrow/api.h>

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

// The string at `i`, or nullopt for a null or an absent column. Accepts every
// Arrow string layout a port may emit.
std::optional<std::string> StringAt(const std::shared_ptr<arrow::Array> &array, int64_t i) {
	if (!array || array->IsNull(i)) {
		return std::nullopt;
	}
	switch (array->type_id()) {
	case arrow::Type::STRING:
		return std::static_pointer_cast<arrow::StringArray>(array)->GetString(i);
	case arrow::Type::LARGE_STRING:
		return std::static_pointer_cast<arrow::LargeStringArray>(array)->GetString(i);
	case arrow::Type::STRING_VIEW:
		return std::string(std::static_pointer_cast<arrow::StringViewArray>(array)->GetView(i));
	default:
		return std::nullopt;
	}
}

std::optional<bool> BoolAt(const std::shared_ptr<arrow::Array> &array, int64_t i) {
	if (!array || array->IsNull(i) || array->type_id() != arrow::Type::BOOL) {
		return std::nullopt;
	}
	return std::static_pointer_cast<arrow::BooleanArray>(array)->Value(i);
}

std::string RequiredString(const std::shared_ptr<arrow::Array> &array, int64_t i, const char *field, const char *what,
                           const std::string &worker_path) {
	auto value = StringAt(array, i);
	if (!value) {
		DecodeFailure(what, StringUtil::Format("it carries no string '%s', and that field has no default", field),
		              worker_path);
	}
	return std::move(*value);
}

VgiProtocolListing DecodeProtocolList(const std::shared_ptr<arrow::RecordBatch> &batch,
                                      const std::string &worker_path) {
	const char *what = "protocol listing";
	if (!batch || batch->num_rows() < 1) {
		DecodeFailure(what, "the payload carries no rows", worker_path);
	}
	VgiProtocolListing listing;
	listing.server_id = RequiredString(batch->GetColumnByName("server_id"), 0, "server_id", what, worker_path);
	listing.server_version =
	    RequiredString(batch->GetColumnByName("server_version"), 0, "server_version", what, worker_path);
	listing.request_version =
	    RequiredString(batch->GetColumnByName("request_version"), 0, "request_version", what, worker_path);

	auto protocols = batch->GetColumnByName("protocols");
	if (!protocols) {
		DecodeFailure(what, "the payload has no 'protocols' column", worker_path);
	}
	if (protocols->IsNull(0)) {
		DecodeFailure(what, "its 'protocols' column is null", worker_path);
	}
	std::shared_ptr<arrow::Array> values;
	int64_t start = 0, end = 0;
	if (protocols->type_id() == arrow::Type::LIST) {
		auto list = std::static_pointer_cast<arrow::ListArray>(protocols);
		values = list->values();
		start = list->value_offset(0);
		end = list->value_offset(1);
	} else if (protocols->type_id() == arrow::Type::LARGE_LIST) {
		auto list = std::static_pointer_cast<arrow::LargeListArray>(protocols);
		values = list->values();
		start = list->value_offset(0);
		end = list->value_offset(1);
	} else {
		DecodeFailure(what, "its 'protocols' column is not a list", worker_path);
	}
	if (!values || values->type_id() != arrow::Type::STRUCT) {
		DecodeFailure(what, "its 'protocols' elements are not structs", worker_path);
	}
	auto summaries = std::static_pointer_cast<arrow::StructArray>(values);
	auto name_col = summaries->GetFieldByName("protocol");
	auto version_col = summaries->GetFieldByName("protocol_version");
	auto hash_col = summaries->GetFieldByName("protocol_hash");
	auto deprecated_col = summaries->GetFieldByName("deprecated");
	auto message_col = summaries->GetFieldByName("deprecation_message");
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
	std::shared_ptr<arrow::RecordBatch> payload;
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
