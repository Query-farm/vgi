// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_attach_tickets.hpp"

#include <algorithm>
#include <vector>

#include "nanoarrow/nanoarrow.h"

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

// A required string field of a one-row reply payload, read by name.
std::string ReplyString(const VgiBatchPtr &payload, const char *field, const char *what) {
	auto column = payload->GetColumnByName(field);
	if (!column || column.IsNull(0) || column.type().type != NANOARROW_TYPE_STRING) {
		throw IOException("vgi: the %s reply carries no string '%s'", what, field);
	}
	return column.GetString(0);
}

// A timestamp[us, tz=UTC] field of a one-row reply payload, read by name; nullopt when null.
std::optional<int64_t> ReplyTimestampMicros(const VgiBatchPtr &payload, const char *field, const char *what) {
	auto column = payload->GetColumnByName(field);
	if (!column) {
		throw IOException("vgi: the %s reply carries no '%s'", what, field);
	}
	const auto type = column.type();
	if (type.type != NANOARROW_TYPE_TIMESTAMP || type.time_unit != NANOARROW_TIME_UNIT_MICRO ||
	    type.timezone != "UTC") {
		throw IOException("vgi: the %s reply's '%s' is %s, expected timestamp[us, tz=UTC]", what, field,
		                  column.type_string());
	}
	if (column.IsNull(0)) {
		return std::nullopt;
	}
	return column.GetInt(0);
}

} // namespace

VgiIssuedGrant InvokeIssueGrant(const CatalogRpcContext &ctx, ClientContext &context,
                                const std::string &identity_version, const std::string &purpose, int64_t ttl_seconds) {
	// issue_grant(purpose: utf8, scopes: list<utf8>, ttl_seconds: int64), all non-null.
	static const VgiSchemaPtr schema =
	    MakeSchema({type::Utf8().Named("purpose", false), type::List(type::Utf8()).Named("scopes", false),
	                type::Int64().Named("ttl_seconds", false)});
	VgiBatchBuilder builder(schema);
	append::String(builder.column(0), purpose);
	append::FinishElement(builder.column(1)); // one empty list
	append::Int(builder.column(2), ttl_seconds);
	builder.FinishRow();
	auto params = builder.Finish();

	const VgiProtocolId identity {IDENTITY_PROTOCOL_NAME, identity_version};
	auto payload = InvokeProtocolUnary(ctx, context, identity, "issue_grant", params);
	VgiIssuedGrant grant;
	grant.token = ReplyString(payload, "token", "issue_grant");
	auto expires_at = ReplyTimestampMicros(payload, "expires_at", "issue_grant");
	if (!expires_at) {
		throw IOException("vgi: the issue_grant reply carries no 'expires_at'");
	}
	grant.expires_at_us = *expires_at;
	auto grant_id = payload->GetColumnByName("grant_id");
	if (grant_id && grant_id.type().type == NANOARROW_TYPE_STRING && !grant_id.IsNull(0)) {
		grant.grant_id = grant_id.GetString(0);
	}
	return grant;
}

VgiAttachTicket InvokeSealAttach(const CatalogRpcContext &ctx, ClientContext &context,
                                 const VgiRetainedAttach &retained, int64_t ttl_seconds) {
	// SealAttachRequest, serialized as the one-row batch carried in the
	// `request` binary parameter (spec §5.2). `options` is the same Arrow IPC
	// options record catalog_attach carries.
	static const VgiSchemaPtr request_schema = MakeSchema(
	    {type::Utf8().Named("catalog_name", false), type::Binary().Named("options", true),
	     type::Utf8().Named("data_version_spec", false), type::Utf8().Named("implementation_version", false),
	     type::Int64().Named("ttl_seconds", false)});
	VgiBatchBuilder request_builder(request_schema);
	append::String(request_builder.column(0), retained.catalog_name);
	if (retained.options.empty()) {
		append::Null(request_builder.column(1));
	} else {
		auto bytes = SerializeToIpcBytes(BuildSettingsBatch(context, retained.options));
		append::Bytes(request_builder.column(1), bytes.data(), static_cast<int64_t>(bytes.size()));
	}
	append::String(request_builder.column(2), retained.data_version_spec);
	append::String(request_builder.column(3), retained.implementation_version);
	append::Int(request_builder.column(4), ttl_seconds);
	request_builder.FinishRow();
	auto request_bytes = SerializeToIpcBytes(request_builder.Finish());

	static const VgiSchemaPtr params_schema = MakeSchema({type::Binary().Named("request", false)});
	VgiBatchBuilder params_builder(params_schema);
	append::Bytes(params_builder.column(0), request_bytes.data(), static_cast<int64_t>(request_bytes.size()));
	params_builder.FinishRow();
	auto params = params_builder.Finish();

	static constexpr VgiProtocolId kTickets {ATTACH_TICKETS_PROTOCOL_NAME, ATTACH_TICKETS_PROTOCOL_VERSION};
	auto payload = InvokeProtocolUnary(ctx, context, kTickets, "seal_attach", params);
	VgiAttachTicket ticket;
	ticket.ticket = ReplyString(payload, "ticket", "seal_attach");
	ticket.expires_at_us = ReplyTimestampMicros(payload, "expires_at", "seal_attach");
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

Value ExpiryValue(std::optional<int64_t> micros) {
	if (!micros) {
		return Value(LogicalType::TIMESTAMP_TZ);
	}
	return Value::TIMESTAMPTZ(timestamp_tz_t(*micros));
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
	std::optional<int64_t> expires = grant.expires_at_us;
	if (ticket.expires_at_us) {
		expires = std::min(*expires, *ticket.expires_at_us);
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
	    "vgi_rpc.Identity.v2 issue_grant) and an attach ticket (what, from vgi.attach_tickets.v1 seal_attach, "
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
