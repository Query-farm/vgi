// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_catalog_contents_function.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "storage/vgi_catalog.hpp"
#include "vgi_catalog_rpc.hpp"
#include "vgi_function_docs.hpp"

namespace duckdb {
namespace vgi {

namespace {

struct VgiCatalogContentsData : public TableFunctionData {
	std::string catalog_name;
	std::optional<std::string> if_none_match;
	bool finished = false;
};

unique_ptr<FunctionData> VgiCatalogContentsBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto data = make_uniq<VgiCatalogContentsData>();
	if (input.inputs[0].IsNull()) {
		throw BinderException("vgi_catalog_contents: catalog name must not be NULL");
	}
	data->catalog_name = input.inputs[0].GetValue<std::string>();
	auto it = input.named_parameters.find("if_none_match");
	if (it != input.named_parameters.end() && !it->second.IsNull()) {
		data->if_none_match = it->second.GetValue<std::string>();
	}
	names = {"catalog_version", "etag", "not_modified", "schemas", "schema_info_names"};
	return_types = {LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::BOOLEAN,
	                LogicalType::LIST(LogicalType::VARCHAR), LogicalType::LIST(LogicalType::VARCHAR)};
	return std::move(data);
}

void VgiCatalogContentsScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<VgiCatalogContentsData>();
	if (data.finished) {
		return;
	}
	data.finished = true;

	auto &catalog = Catalog::GetCatalog(context, data.catalog_name);
	if (catalog.GetCatalogType() != "vgi") {
		throw InvalidInputException("vgi_catalog_contents: '%s' is not a VGI catalog", data.catalog_name);
	}
	auto &vgi_catalog = catalog.Cast<VgiCatalog>();
	auto &attach_result = vgi_catalog.attach_result();
	if (!attach_result || !attach_result->supports_catalog_contents) {
		return; // Not advertised: no rows (the RPC must not be sent).
	}
	CatalogRpcContext rpc_ctx{vgi_catalog.attach_parameters(), attach_result->attach_opaque_data, {}};
	auto snapshot = InvokeCatalogContents(rpc_ctx, context, data.if_none_match);

	vector<Value> schema_names;
	vector<Value> info_names;
	for (auto &schema : snapshot.schemas) {
		schema_names.emplace_back(schema.name);
		// Decoding checks SchemaInfo.path == SchemaContents.path.
		info_names.emplace_back(DecodeContentsSchemaInfo(schema).name);
	}
	output.SetValue(0, 0, Value::BIGINT(snapshot.catalog_version));
	output.SetValue(1, 0, snapshot.etag ? Value(*snapshot.etag) : Value(LogicalType::VARCHAR));
	output.SetValue(2, 0, Value::BOOLEAN(snapshot.not_modified));
	output.SetValue(3, 0, Value::LIST(LogicalType::VARCHAR, std::move(schema_names)));
	output.SetValue(4, 0, Value::LIST(LogicalType::VARCHAR, std::move(info_names)));
	output.SetCardinality(1);
}

} // namespace

void RegisterVgiCatalogContentsFunction(ExtensionLoader &loader) {
	TableFunction func("vgi_catalog_contents", {LogicalType::VARCHAR}, VgiCatalogContentsScan, VgiCatalogContentsBind);
	func.named_parameters["if_none_match"] = LogicalType::VARCHAR;
	CreateTableFunctionInfo info(func);
	info.descriptions.push_back(MakeFunctionDescription(
	    "Issue one catalog_contents RPC against an attached VGI catalog and return the answer: the snapshot's "
	    "catalog_version and etag, whether the worker answered not_modified to if_none_match, and the schema "
	    "names (from SchemaContents.path, and decoded from each SchemaInfo). No rows when the worker does not "
	    "advertise catalog_contents. A diagnostic; the catalog caches are not touched.",
	    {"catalog"}, {LogicalType::VARCHAR},
	    {"SELECT * FROM vgi_catalog_contents('my_catalog');",
	     "SELECT not_modified FROM vgi_catalog_contents('my_catalog', if_none_match := 'abc');"}));
	loader.RegisterFunction(std::move(info));
}

} // namespace vgi
} // namespace duckdb
