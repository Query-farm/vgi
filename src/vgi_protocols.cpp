// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// vgi_protocols(): which vgi-rpc protocols a worker hosts, from its
// vgi_rpc.Reflection.v1/list_protocols. Two forms:
//
//   vgi_protocols('<location>', <vgi_catalogs() auth/iroh options>)
//       Asks the worker at a LOCATION, exactly as vgi_catalogs() addresses it.
//       A worker that does not host reflection is an error naming the cause.
//
//   vgi_protocols(catalog := '<attached alias>')
//       Reads the attached catalog's cached listing (vgi_reflection.hpp), the
//       same answer the extension's own capability checks use. A worker that
//       predates reflection lists just vgi.v2, with no version or hash.
#include "vgi_protocols.hpp"

#include <string>
#include <vector>

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "storage/vgi_catalog.hpp"
#include "vgi_catalog_rpc.hpp"
#include "vgi_catalogs.hpp"
#include "vgi_function_docs.hpp"
#include "vgi_iroh_config.hpp"
#include "vgi_location_policy.hpp"
#include "vgi_logging.hpp"
#include "vgi_oauth.hpp"
#include "vgi_reflection.hpp"

namespace duckdb {

namespace {

constexpr const char *kCatalogParam = "catalog";

struct VgiProtocolsBindData : public TableFunctionData {
	// Exactly one of the two is set.
	std::string worker_path;
	std::string catalog_alias;
	std::shared_ptr<vgi::CatalogAuth> auth;
	std::shared_ptr<vgi::IrohClientConfig> iroh;
};

struct VgiProtocolsGlobalState : public GlobalTableFunctionState {
	std::shared_ptr<const vgi::VgiProtocolListing> listing;
	idx_t current_idx = 0;

	idx_t MaxThreads() const override {
		return 1;
	}
};

// The attached VGI catalog named `alias`, or a BinderException.
VgiCatalog &ResolveVgiCatalog(ClientContext &context, const std::string &alias) {
	auto catalog = Catalog::GetCatalogEntry(context, alias);
	if (!catalog) {
		throw BinderException("vgi_protocols: no attached catalog named '%s'", alias);
	}
	if (catalog->GetCatalogType() != "vgi") {
		throw BinderException("vgi_protocols: catalog '%s' is not a VGI catalog (it is of type '%s')", alias,
		                      catalog->GetCatalogType());
	}
	return catalog->Cast<VgiCatalog>();
}

unique_ptr<FunctionData> VgiProtocolsBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<VgiProtocolsBindData>();

	Value catalog_value;
	for (auto &kv : input.named_parameters) {
		if (StringUtil::Lower(kv.first) == kCatalogParam) {
			catalog_value = kv.second;
		}
	}
	const bool has_location = !input.inputs.empty();
	const bool has_catalog = !catalog_value.IsNull();

	if (has_location && has_catalog) {
		throw BinderException("vgi_protocols: pass either a worker location or "
		                      "catalog := '<alias>', not both");
	}
	if (has_location) {
		if (input.inputs[0].IsNull()) {
			throw BinderException("vgi_protocols: worker location must not be NULL");
		}
		auto target =
		    vgi::BindDiscoveryTarget(context, input.inputs[0].GetValue<string>(), input.named_parameters,
		                             "vgi_protocols()", vgi::LocationEntryPoint::VGI_PROTOCOLS, {kCatalogParam});
		bind_data->worker_path = std::move(target.worker_path);
		bind_data->auth = std::move(target.auth);
		bind_data->iroh = std::move(target.iroh);
		VGI_LOG(context, "vgi_protocols.bind", {{"worker_path", bind_data->worker_path}});
	} else {
		if (!has_catalog) {
			throw BinderException("vgi_protocols: pass a worker location, vgi_protocols('<location>'), "
			                      "or an "
			                      "attached VGI catalog, vgi_protocols(catalog := '<alias>')");
		}
		// The attached catalog's own connection and auth are used; options that
		// configure a connection belong to the location form only.
		for (auto &kv : input.named_parameters) {
			if (StringUtil::Lower(kv.first) != kCatalogParam) {
				throw BinderException("vgi_protocols: %s is only valid with a worker location; catalog "
				                      ":= uses "
				                      "the attached catalog's connection and authentication",
				                      kv.first);
			}
		}
		bind_data->catalog_alias = catalog_value.GetValue<string>();
		ResolveVgiCatalog(context, bind_data->catalog_alias);
		VGI_LOG(context, "vgi_protocols.bind", {{"catalog", bind_data->catalog_alias}});
	}

	names = {"position", "protocol_name", "protocol_version", "protocol_hash"};
	return_types = {LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
	return std::move(bind_data);
}

unique_ptr<GlobalTableFunctionState> VgiProtocolsInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<VgiProtocolsBindData>();
	auto state = make_uniq<VgiProtocolsGlobalState>();

	if (!bind_data.catalog_alias.empty()) {
		// Re-resolved: the catalog may have been detached since a PREPARE.
		auto &catalog = ResolveVgiCatalog(context, bind_data.catalog_alias);
		state->listing = vgi::GetHostedProtocols(context, catalog);
		return std::move(state);
	}

	vgi::CheckLocationPolicy(context, bind_data.worker_path, vgi::LocationEntryPoint::VGI_PROTOCOLS);
	// Addressed exactly as vgi_catalogs() addresses a location (InvokeCatalogs).
	vgi::VgiAttachParametersConfig cfg;
	cfg.worker_path = bind_data.worker_path;
	cfg.use_pool = true;
	cfg.auth = bind_data.auth;
	cfg.iroh = bind_data.iroh;
	auto params = std::make_shared<vgi::VgiAttachParameters>(std::move(cfg));
	vgi::CatalogRpcContext ctx {params, {}, {}};
	try {
		state->listing = std::make_shared<const vgi::VgiProtocolListing>(
		    vgi::InvokeListProtocols(ctx, context,
		                             /*tolerate_pre_reflection=*/false));
	} catch (const std::exception &e) {
		if (!vgi::IsReflectionNotHostedError(e)) {
			throw;
		}
		throw InvalidInputException("vgi_protocols: the worker at '%s' does not host %s, so it cannot list "
		                            "its protocols: it predates "
		                            "vgi-rpc reflection (vgi-rpc < 0.46). Upgrade the worker's VGI SDK. "
		                            "Worker error: %s",
		                            bind_data.worker_path, vgi::REFLECTION_PROTOCOL_NAME, e.what());
	}
	VGI_LOG(
	    context, "vgi_protocols.init",
	    {{"worker_path", bind_data.worker_path}, {"num_protocols", std::to_string(state->listing->protocols.size())}});
	return std::move(state);
}

Value NullIfEmpty(const std::string &s) {
	return s.empty() ? Value(LogicalType::VARCHAR) : Value(s);
}

void VgiProtocolsScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<VgiProtocolsGlobalState>();
	const auto &protocols = state.listing->protocols;
	idx_t count = 0;
	while (state.current_idx < protocols.size() && count < STANDARD_VECTOR_SIZE) {
		const auto &p = protocols[state.current_idx];
		output.data[0].SetValue(count, Value::INTEGER(static_cast<int32_t>(state.current_idx)));
		output.data[1].SetValue(count, Value(p.name));
		output.data[2].SetValue(count, NullIfEmpty(p.version));
		output.data[3].SetValue(count, NullIfEmpty(p.hash));
		count++;
		state.current_idx++;
	}
	output.SetCardinality(count);
}

InsertionOrderPreservingMap<string> VgiProtocolsToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind_data = input.bind_data->Cast<VgiProtocolsBindData>();
	if (!bind_data.catalog_alias.empty()) {
		result["Catalog"] = bind_data.catalog_alias;
	} else {
		result["Worker"] = bind_data.worker_path;
	}
	return result;
}

TableFunction MakeVgiProtocolsFunction(vector<LogicalType> arguments) {
	TableFunction func("vgi_protocols", std::move(arguments), VgiProtocolsScan, VgiProtocolsBind,
	                   VgiProtocolsInitGlobal);
	func.named_parameters[kCatalogParam] = LogicalType::VARCHAR;
	vgi::AddDiscoveryNamedParameters(func);
	func.to_string = VgiProtocolsToString;
	return func;
}

} // anonymous namespace

void RegisterVgiProtocolsFunction(ExtensionLoader &loader) {
	TableFunctionSet set("vgi_protocols");
	set.AddFunction(MakeVgiProtocolsFunction({LogicalType::VARCHAR}));
	set.AddFunction(MakeVgiProtocolsFunction({}));

	CreateTableFunctionInfo info(std::move(set));
	const char *description = "List the vgi-rpc protocols a VGI worker hosts, from its "
	                          "vgi_rpc.Reflection.v1 list_protocols, one row "
	                          "per protocol in the worker's order (vgi.v2 first): position, "
	                          "protocol_name, protocol_version (NULL when "
	                          "the protocol declares none) and protocol_hash. Pass a worker location "
	                          "with the same authentication and "
	                          "iroh_* options as vgi_catalogs(), or catalog := an attached VGI "
	                          "catalog's alias to use its connection "
	                          "and cached answer. A worker that predates reflection is an error by "
	                          "location, and lists only vgi.v2 "
	                          "(NULL version and hash) by catalog.";
	info.descriptions.push_back(vgi::MakeFunctionDescription(description, {"worker_path"}, {LogicalType::VARCHAR},
	                                                         {"SELECT * FROM vgi_protocols('./worker');"}));
	info.descriptions.push_back(
	    vgi::MakeFunctionDescription(description, {}, {}, {"SELECT * FROM vgi_protocols(catalog := 'my_catalog');"}));
	loader.RegisterFunction(std::move(info));
}

} // namespace duckdb
