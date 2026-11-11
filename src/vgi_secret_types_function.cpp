// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
// =============================================================================
// vgi_secret_types() — discovery table function. One row per secret type each
// attached VGI catalog declares.
//
// A worker's SecretTypeSpec carries a description and an Arrow parameters
// schema; ATTACH registers the type with DuckDB's SecretManager, whose
// duckdb_secret_types() exposes only the name. This surfaces the rest so a
// client (a directory listing, an agent, vgi-lint) can render a CREATE SECRET
// template and say which functions need it:
//   description  — the SecretTypeSpec description
//   doc_url      — vgi_doc_url schema metadata (how to obtain the credentials)
//   parameters   — per key: type, required (vgi_required), redact, description (vgi_doc)
//   required_by  — schema-qualified functions declaring a requirement on the type
//
// Reads the attach result already held by the catalog; required_by reuses the
// schemas' cached function inventories (only for catalogs that declare types).
// =============================================================================

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "storage/vgi_catalog.hpp"
#include "storage/vgi_schema_entry.hpp"
#include "storage/vgi_transaction.hpp"
#include "vgi_attach_credentials.hpp"
#include "vgi_catalog_rpc.hpp"
#include "vgi_function_docs.hpp"
#include "vgi_logging.hpp"
#include "vgi_secret_types_function.hpp"

namespace duckdb {
namespace vgi {

namespace {

struct SecretTypeRow {
	std::string catalog_name;
	VgiSecretType secret_type;
	std::vector<std::string> required_by;
};

struct VgiSecretTypesData : public TableFunctionData {
	std::vector<SecretTypeRow> rows;
	mutable idx_t current_idx = 0;
};

LogicalType ParameterStructType() {
	child_list_t<LogicalType> fields;
	fields.emplace_back("name", LogicalType::VARCHAR);
	fields.emplace_back("type", LogicalType::VARCHAR);
	fields.emplace_back("required", LogicalType::BOOLEAN);
	fields.emplace_back("redact", LogicalType::BOOLEAN);
	fields.emplace_back("description", LogicalType::VARCHAR);
	return LogicalType::STRUCT(std::move(fields));
}

Value NullableText(const std::string &s) {
	return s.empty() ? Value(LogicalType::VARCHAR) : Value(s);
}

//! Functions in `vgi_catalog` that declare a requirement, keyed by secret type
//! (case-insensitive, matching DuckDB's secret type lookup).
case_insensitive_map_t<std::vector<std::string>> CollectRequirements(ClientContext &context, VgiCatalog &vgi_catalog) {
	case_insensitive_map_t<std::vector<std::string>> by_type;
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();
	const auto catalog_name = vgi_catalog.GetName().GetIdentifierName();
	auto &vgi_tx = VgiTransaction::Get(context, vgi_catalog);
	const auto transaction_opaque = vgi_tx.GetTransactionOpaqueData();
	static const CatalogType kFunctionTypes[] = {CatalogType::SCALAR_FUNCTION_ENTRY, CatalogType::TABLE_FUNCTION_ENTRY,
	                                             CatalogType::AGGREGATE_FUNCTION_ENTRY};
	vgi_catalog.ScanSchemas(context, [&](SchemaCatalogEntry &schema) {
		const auto schema_name = schema.name.GetIdentifierName();
		auto &vgi_schema = schema.Cast<VgiSchemaEntry>();
		for (const auto function_type : kFunctionTypes) {
			try {
				CatalogRpcContext rpc_ctx {attach_params, attach_result->attach_opaque_data, transaction_opaque};
				rpc_ctx.entity_kind = "schema";
				rpc_ctx.entity_qualifier = schema_name;
				for (auto &func : vgi_schema.GetFunctionInventory(rpc_ctx, function_type, context)) {
					// A table_only function (a table's hidden scan or a write
					// function) cannot be called from SQL, so it is not listed as
					// requiring the secret type; its table is the user-facing name.
					if (func.visibility == VgiFunctionVisibility::TableOnly) {
						continue;
					}
					for (auto &req : func.required_secrets) {
						auto &names = by_type[req.secret_type];
						auto qualified = schema_name + "." + func.name;
						if (std::find(names.begin(), names.end(), qualified) == names.end()) {
							names.push_back(std::move(qualified));
						}
					}
				}
			} catch (const std::exception &e) {
				VGI_LOG(context, "vgi.secret_types.skip_failed",
				        {{"catalog", catalog_name},
				         {"schema", schema_name},
				         {"function_type", EnumUtil::ToString(function_type)},
				         {"error", e.what()}});
			}
		}
	});
	return by_type;
}

static unique_ptr<FunctionData> VgiSecretTypesBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<Identifier> &names) {
	return_types = {LogicalType::VARCHAR,
	                LogicalType::VARCHAR,
	                LogicalType::VARCHAR,
	                LogicalType::VARCHAR,
	                LogicalType::LIST(ParameterStructType()),
	                LogicalType::LIST(LogicalType::VARCHAR)};
	names = {"catalog_name", "secret_type", "description", "doc_url", "parameters", "required_by"};

	auto data = make_uniq<VgiSecretTypesData>();
	const auto selected_catalog = input.named_parameters.find("catalog");
	for (auto &db : DatabaseManager::Get(context).GetDatabases(context)) {
		auto &catalog = db->GetCatalog();
		if (catalog.GetCatalogType() != "vgi") {
			continue;
		}
		if (selected_catalog != input.named_parameters.end() && !selected_catalog->second.IsNull() &&
		    !StringUtil::CIEquals(catalog.GetName().GetIdentifierName(), selected_catalog->second.ToString())) {
			continue;
		}
		auto &vgi_catalog = catalog.Cast<VgiCatalog>();
		auto &attach_result = vgi_catalog.attach_result();
		if (!vgi_catalog.attach_parameters() || !attach_result || attach_result->secret_types.empty()) {
			continue;
		}
		auto requirements = CollectRequirements(context, vgi_catalog);
		for (const auto &st : attach_result->secret_types) {
			// ATTACH refuses extension-reserved names (iroh); don't advertise them.
			if (IsReservedSecretTypeName(st.name)) {
				continue;
			}
			SecretTypeRow row;
			row.catalog_name = vgi_catalog.GetName().GetIdentifierName();
			row.secret_type = st;
			auto it = requirements.find(st.name);
			if (it != requirements.end()) {
				row.required_by = it->second;
			}
			data->rows.push_back(std::move(row));
		}
	}
	return std::move(data);
}

static void VgiSecretTypesScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.bind_data->Cast<VgiSecretTypesData>();
	const auto param_type = ParameterStructType();
	idx_t count = 0;
	while (data.current_idx < data.rows.size() && count < STANDARD_VECTOR_SIZE) {
		const auto &row = data.rows[data.current_idx++];
		const auto &st = row.secret_type;
		vector<Value> params;
		for (const auto &p : st.parameters) {
			child_list_t<Value> fields;
			fields.emplace_back("name", Value(p.name));
			fields.emplace_back("type", Value(p.type.ToString()));
			fields.emplace_back("required", Value::BOOLEAN(p.required));
			fields.emplace_back("redact", Value::BOOLEAN(p.redact));
			fields.emplace_back("description", NullableText(p.description));
			params.push_back(Value::STRUCT(std::move(fields)));
		}
		vector<Value> required_by;
		for (const auto &f : row.required_by) {
			required_by.emplace_back(f);
		}
		output.SetValue(0, count, Value(row.catalog_name));
		output.SetValue(1, count, Value(st.name));
		output.SetValue(2, count, NullableText(st.description));
		output.SetValue(3, count, NullableText(st.doc_url));
		output.SetValue(4, count, Value::LIST(param_type, std::move(params)));
		output.SetValue(5, count, Value::LIST(LogicalType::VARCHAR, std::move(required_by)));
		count++;
	}
	output.SetCardinality(count);
}

} // anonymous namespace

void RegisterVgiSecretTypesFunction(ExtensionLoader &loader) {
	TableFunction func("vgi_secret_types", {}, VgiSecretTypesScan, VgiSecretTypesBind);
	AddVgiFunctionOption(func, "catalog", LogicalType::VARCHAR);
	CreateTableFunctionInfo info(func);
	info.descriptions.push_back(MakeFunctionDescription(
	    "List the secret types every attached VGI catalog declares, one row per type: its description, "
	    "doc_url (where to obtain the credentials), each CREATE SECRET parameter (type, required, redact, "
	    "description), and required_by, the schema-qualified functions that need it. duckdb_secret_types() "
	    "shows only the type name. Pass catalog := '...' to inspect one attachment.",
	    {}, {},
	    {"SELECT * FROM vgi_secret_types();",
	     "SELECT p.name, p.required, p.redact, p.description FROM vgi_secret_types(catalog := 'example'), "
	     "unnest(parameters) AS t(p);"}));
	loader.RegisterFunction(std::move(info));
}

} // namespace vgi
} // namespace duckdb
