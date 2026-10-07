// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_attach_tickets.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <vector>

#include <arrow/api.h>

#include "duckdb.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "storage/vgi_catalog.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_rpc.hpp"
#include "vgi_exception.hpp"
#include "vgi_function_docs.hpp"
#include "vgi_logging.hpp"
#include "vgi_reflection.hpp"

namespace duckdb {
namespace vgi {

namespace {

std::shared_ptr<arrow::Array> Utf8Scalar(const std::string &value) {
	arrow::StringBuilder builder;
	if (!builder.Append(value).ok()) {
		throw IOException("vgi: failed to build a string request field");
	}
	return builder.Finish().ValueOrDie();
}

std::shared_ptr<arrow::Array> Int64Scalar(int64_t value) {
	arrow::Int64Builder builder;
	if (!builder.Append(value).ok()) {
		throw IOException("vgi: failed to build an int64 request field");
	}
	return builder.Finish().ValueOrDie();
}

// A required string field of a one-row reply payload, read by name.
std::string ReplyString(const std::shared_ptr<arrow::RecordBatch> &payload, const char *field, const char *what) {
	auto column = payload->GetColumnByName(field);
	if (!column || column->IsNull(0) || column->type_id() != arrow::Type::STRING) {
		throw IOException("vgi: the %s reply carries no string '%s'", what, field);
	}
	return std::static_pointer_cast<arrow::StringArray>(column)->GetString(0);
}

double ReplyDouble(const std::shared_ptr<arrow::RecordBatch> &payload, const char *field, const char *what) {
	auto column = payload->GetColumnByName(field);
	if (!column || column->IsNull(0)) {
		throw IOException("vgi: the %s reply carries no '%s'", what, field);
	}
	switch (column->type_id()) {
	case arrow::Type::DOUBLE:
		return std::static_pointer_cast<arrow::DoubleArray>(column)->Value(0);
	case arrow::Type::INT64:
		return static_cast<double>(std::static_pointer_cast<arrow::Int64Array>(column)->Value(0));
	default:
		throw IOException("vgi: the %s reply's '%s' is not a number", what, field);
	}
}

} // namespace

VgiIssuedGrant InvokeIssueGrant(const CatalogRpcContext &ctx, ClientContext &context,
                                const std::string &identity_version, const std::string &purpose, int64_t ttl_seconds) {
	// issue_grant(purpose: utf8, scopes: list<utf8>, ttl_seconds: int64), all non-null.
	auto scopes_type = arrow::list(arrow::utf8());
	auto schema =
	    arrow::schema({arrow::field("purpose", arrow::utf8(), false), arrow::field("scopes", scopes_type, false),
	                   arrow::field("ttl_seconds", arrow::int64(), false)});
	arrow::ListBuilder scopes_builder(arrow::default_memory_pool(), std::make_shared<arrow::StringBuilder>(),
	                                  scopes_type);
	if (!scopes_builder.Append().ok()) { // one empty list
		throw IOException("vgi: failed to build issue_grant scopes");
	}
	auto scopes = scopes_builder.Finish().ValueOrDie();
	auto params = arrow::RecordBatch::Make(schema, 1, {Utf8Scalar(purpose), scopes, Int64Scalar(ttl_seconds)});

	const VgiProtocolId identity {IDENTITY_PROTOCOL_NAME, identity_version};
	auto payload = InvokeProtocolUnary(ctx, context, identity, "issue_grant", params);
	VgiIssuedGrant grant;
	grant.token = ReplyString(payload, "token", "issue_grant");
	grant.expires_at = ReplyDouble(payload, "expires_at", "issue_grant");
	auto grant_id = payload->GetColumnByName("grant_id");
	if (grant_id && grant_id->type_id() == arrow::Type::STRING && !grant_id->IsNull(0)) {
		grant.grant_id = std::static_pointer_cast<arrow::StringArray>(grant_id)->GetString(0);
	}
	return grant;
}

VgiAttachTicket InvokeSealAttach(const CatalogRpcContext &ctx, ClientContext &context,
                                 const VgiRetainedAttach &retained, int64_t ttl_seconds) {
	// SealAttachRequest, serialized as the one-row batch carried in the
	// `request` binary parameter (spec §5.2). `options` is the same Arrow IPC
	// options record catalog_attach carries.
	std::shared_ptr<arrow::Array> options;
	{
		arrow::BinaryBuilder builder;
		arrow::Status status;
		if (retained.options.empty()) {
			status = builder.AppendNull();
		} else {
			auto bytes = SerializeToIpcBytes(BuildSettingsBatch(context, retained.options));
			status = builder.Append(bytes.data(), static_cast<int32_t>(bytes.size()));
		}
		if (!status.ok()) {
			throw IOException("vgi: failed to build seal_attach options");
		}
		options = builder.Finish().ValueOrDie();
	}
	auto request_schema = arrow::schema({arrow::field("catalog_name", arrow::utf8(), false),
	                                     arrow::field("options", arrow::binary(), true),
	                                     arrow::field("data_version_spec", arrow::utf8(), false),
	                                     arrow::field("implementation_version", arrow::utf8(), false),
	                                     arrow::field("ttl_seconds", arrow::int64(), false)});
	auto request =
	    arrow::RecordBatch::Make(request_schema, 1,
	                             {Utf8Scalar(retained.catalog_name), options, Utf8Scalar(retained.data_version_spec),
	                              Utf8Scalar(retained.implementation_version), Int64Scalar(ttl_seconds)});
	auto request_bytes = SerializeToIpcBytes(request);

	arrow::BinaryBuilder request_builder;
	if (!request_builder.Append(request_bytes.data(), static_cast<int32_t>(request_bytes.size())).ok()) {
		throw IOException("vgi: failed to build the seal_attach request");
	}
	auto params = arrow::RecordBatch::Make(arrow::schema({arrow::field("request", arrow::binary(), false)}), 1,
	                                       {request_builder.Finish().ValueOrDie()});

	static constexpr VgiProtocolId kTickets {ATTACH_TICKETS_PROTOCOL_NAME, ATTACH_TICKETS_PROTOCOL_VERSION};
	auto payload = InvokeProtocolUnary(ctx, context, kTickets, "seal_attach", params);
	VgiAttachTicket ticket;
	ticket.ticket = ReplyString(payload, "ticket", "seal_attach");
	ticket.expires_at = ReplyDouble(payload, "expires_at", "seal_attach");
	return ticket;
}

// ============================================================================
// vgi_export_session(aliases := NULL, ttl_seconds := NULL)
// ============================================================================

namespace {

// A NULL ttl_seconds asks for "as long as the worker allows". A grant needs a
// positive request (the worker caps it at its maximum); a ticket takes 0.
constexpr int64_t kGrantTtlUnbounded = 365LL * 24 * 3600;

struct ExportRow {
	std::string alias;
	Value location = Value(LogicalType::VARCHAR);
	Value catalog_name = Value(LogicalType::VARCHAR);
	Value grant = Value(LogicalType::VARCHAR);
	Value ticket = Value(LogicalType::VARCHAR);
	Value expires_at = Value(LogicalType::TIMESTAMP_TZ);
	std::string status;
	Value message = Value(LogicalType::VARCHAR);
};

struct VgiExportSessionBindData : public TableFunctionData {
	bool all_catalogs = true;
	std::vector<std::string> aliases;
	std::optional<int64_t> ttl_seconds;
};

struct VgiExportSessionState : public GlobalTableFunctionState {
	std::vector<ExportRow> rows;
	idx_t next = 0;
	idx_t MaxThreads() const override {
		return 1;
	}
};

unique_ptr<FunctionData> VgiExportSessionBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto data = make_uniq<VgiExportSessionBindData>();
	for (auto &kv : input.named_parameters) {
		auto name = StringUtil::Lower(kv.first);
		if (name == "aliases" && !kv.second.IsNull()) {
			data->all_catalogs = false;
			for (auto &child : ListValue::GetChildren(kv.second)) {
				if (child.IsNull()) {
					throw BinderException("vgi_export_session: aliases must not contain NULL");
				}
				data->aliases.push_back(child.GetValue<string>());
			}
		} else if (name == "ttl_seconds" && !kv.second.IsNull()) {
			auto ttl = kv.second.GetValue<int64_t>();
			if (ttl <= 0) {
				throw BinderException("vgi_export_session: ttl_seconds must be positive (or NULL for the "
				                      "longest the worker allows)");
			}
			data->ttl_seconds = ttl;
		}
	}
	names = {"alias", "location", "catalog_name", "grant", "ticket", "expires_at", "status", "message"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR,      LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::TIMESTAMP_TZ, LogicalType::VARCHAR, LogicalType::VARCHAR};
	return std::move(data);
}

// The worker's own message, without DuckDB's exception framing.
std::string ErrorMessage(const std::exception &e) {
	ErrorData error(e);
	return error.RawMessage();
}

Value ExpiryValue(double seconds) {
	if (!std::isfinite(seconds) || seconds <= 0) {
		return Value(LogicalType::TIMESTAMP_TZ);
	}
	return Value::TIMESTAMPTZ(timestamp_tz_t(static_cast<int64_t>(seconds * 1000000.0)));
}

void ExportCatalog(ClientContext &context, VgiCatalog &catalog, const VgiExportSessionBindData &bind, ExportRow &row) {
	const auto &params = catalog.attach_parameters();
	if (!params) {
		row.status = "error";
		row.message = Value("the catalog has no connection parameters");
		return;
	}
	const auto &retained = params->retained_attach();
	row.location = Value(retained ? retained->location : params->worker_path());
	row.catalog_name = Value(retained ? retained->catalog_name : params->catalog_name());

	auto listing = GetHostedProtocols(context, params);
	const auto *tickets = listing->Find(ATTACH_TICKETS_PROTOCOL_NAME);
	const auto *identity = listing->Find(IDENTITY_PROTOCOL_NAME);
	if (!tickets || !identity) {
		row.status = "not_supported";
		row.message = Value(StringUtil::Format("the worker does not host %s",
		                                       !tickets ? ATTACH_TICKETS_PROTOCOL_NAME : IDENTITY_PROTOCOL_NAME));
		return;
	}
	if (!retained || retained->via_ticket) {
		row.status = "not_supported";
		row.message = Value("the catalog was attached with an attach_ticket; its original options are not known "
		                    "to this session, so there is nothing to re-seal");
		return;
	}

	CatalogRpcContext ctx {params, {}, {}};
	VgiIssuedGrant grant;
	try {
		grant = InvokeIssueGrant(ctx, context, identity->version, UNATTENDED_GRANT_PURPOSE,
		                         bind.ttl_seconds.value_or(kGrantTtlUnbounded));
	} catch (const VgiRpcException &e) {
		const auto &kind = e.GetErrorKind();
		if (kind == "stale_auth") {
			row.status = "stale_login";
		} else if (kind == error_kind::kMethodNotImplemented || kind == error_kind::kProtocolNotSupported) {
			row.status = "not_supported";
		} else {
			row.status = "refused";
		}
		row.message = Value(ErrorMessage(e));
		return;
	}

	VgiAttachTicket ticket;
	try {
		ticket = InvokeSealAttach(ctx, context, *retained, bind.ttl_seconds.value_or(0));
	} catch (const VgiRpcException &e) {
		row.status = "refused";
		row.message = Value(ErrorMessage(e));
		return;
	}

	row.grant = Value(grant.token);
	row.ticket = Value(ticket.ticket);
	// The entry is usable until the first half expires.
	double expires = std::numeric_limits<double>::infinity();
	for (double candidate : {grant.expires_at, ticket.expires_at}) {
		if (std::isfinite(candidate) && candidate > 0) {
			expires = std::min(expires, candidate);
		}
	}
	row.expires_at = ExpiryValue(expires);
	row.status = "ok";
}

unique_ptr<GlobalTableFunctionState> VgiExportSessionInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<VgiExportSessionBindData>();
	auto state = make_uniq<VgiExportSessionState>();

	std::vector<std::string> aliases = bind.aliases;
	if (bind.all_catalogs) {
		for (auto &db : DatabaseManager::Get(context).GetDatabases(context)) {
			if (db->IsSystem() || db->IsTemporary()) {
				continue;
			}
			aliases.push_back(db->GetName());
		}
		std::sort(aliases.begin(), aliases.end());
	}

	for (const auto &alias : aliases) {
		ExportRow row;
		row.alias = alias;
		// Per-row failures never abort the export.
		try {
			auto catalog = Catalog::GetCatalogEntry(context, alias);
			if (!catalog) {
				row.status = "error";
				row.message = Value(StringUtil::Format("no attached catalog named '%s'", alias));
			} else if (catalog->GetCatalogType() != "vgi") {
				row.status = "not_vgi";
				row.message = Value(StringUtil::Format("not a VGI catalog (type %s)", catalog->GetCatalogType()));
			} else {
				ExportCatalog(context, catalog->Cast<VgiCatalog>(), bind, row);
			}
		} catch (const std::exception &e) {
			row.grant = Value(LogicalType::VARCHAR);
			row.ticket = Value(LogicalType::VARCHAR);
			row.expires_at = Value(LogicalType::TIMESTAMP_TZ);
			row.status = "error";
			row.message = Value(ErrorMessage(e));
		}
		// Status only: the grant and ticket are credentials and never logged.
		VGI_LOG(context, "export_session.row", {{"alias", alias}, {"status", row.status}});
		state->rows.push_back(std::move(row));
	}
	return std::move(state);
}

void VgiExportSessionScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<VgiExportSessionState>();
	idx_t count = 0;
	while (state.next < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		const auto &row = state.rows[state.next++];
		output.data[0].SetValue(count, Value(row.alias));
		output.data[1].SetValue(count, row.location);
		output.data[2].SetValue(count, row.catalog_name);
		output.data[3].SetValue(count, row.grant);
		output.data[4].SetValue(count, row.ticket);
		output.data[5].SetValue(count, row.expires_at);
		output.data[6].SetValue(count, Value(row.status));
		output.data[7].SetValue(count, row.message);
		count++;
	}
	output.SetCardinality(count);
}

} // namespace

void RegisterVgiExportSessionFunction(ExtensionLoader &loader) {
	TableFunction func("vgi_export_session", {}, VgiExportSessionScan, VgiExportSessionBind, VgiExportSessionInit);
	func.named_parameters["aliases"] = LogicalType::LIST(LogicalType::VARCHAR);
	func.named_parameters["ttl_seconds"] = LogicalType::BIGINT;
	CreateTableFunctionInfo info(func);
	info.descriptions.push_back(MakeFunctionDescription(
	    "Export each attached catalog (or each alias given) for unattended reattach: a sealed grant (who, from "
	    "vgi_rpc.Identity.v1 issue_grant) and an attach ticket (what, from vgi.attach_tickets.v1 seal_attach, "
	    "carrying the original ATTACH options including secret ones). Reattach with bearer_token = grant and "
	    "attach_ticket = ticket. One row per catalog: alias, location, catalog_name, grant, ticket, expires_at "
	    "(the earlier of the two), status (ok, not_vgi, not_supported, stale_login, refused, error) and message. "
	    "Per-row failures never abort the export. The grant and ticket are credentials: treat the result as secret.",
	    {}, {},
	    {"SELECT alias, status FROM vgi_export_session();",
	     "SELECT * FROM vgi_export_session(aliases := ['sales'], ttl_seconds := 86400);"}));
	loader.RegisterFunction(std::move(info));
}

} // namespace vgi
} // namespace duckdb
