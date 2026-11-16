// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "storage/vgi_aggregate_function_set.hpp"

#include <algorithm>

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"

#include "storage/vgi_catalog.hpp"
#include "storage/vgi_schema_entry.hpp"
#include "storage/vgi_transaction.hpp"
#include "vgi_aggregate_function_impl.hpp"
#include "vgi_aggregate_window_impl.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_function_docs.hpp"
#include "vgi_catalog_rpc.hpp"
#include "vgi_exception.hpp"
#include "vgi_global_functions.hpp"
#include "vgi_logging.hpp"

namespace duckdb {

VgiAggregateFunctionSet::VgiAggregateFunctionSet(Catalog &catalog, VgiSchemaEntry &schema)
    : VgiCatalogSet(catalog, &schema), schema_(schema) {
}

namespace vgi {

AggregateFunctionSet BuildVgiAggregateFunctionSet(ClientContext &context, const std::string &registered_name,
                                                  const std::vector<vgi::VgiFunctionInfo> &overloads,
                                                  const VgiFunctionRegistrationTarget &target,
                                                  vector<FunctionDescription> &descriptions) {
	AggregateFunctionSet func_set {Identifier(registered_name)};

	for (const auto &func_info : overloads) {
		vgi::FunctionArgumentTypes arg_types;
		if (func_info.arguments_schema) {
			arg_types = vgi::ParseFunctionArgumentSchema(context, func_info.arguments_schema);
		}

		if (!func_info.output_schema) {
			throw InvalidInputException("Aggregate function '%s' missing required output_schema", func_info.name);
		}
		if (func_info.output_schema->num_fields() != 1) {
			throw InvalidInputException("Aggregate function '%s' output_schema must have exactly 1 field, got %d",
			                            func_info.name, func_info.output_schema->num_fields());
		}

		auto metadata = func_info.output_schema->field_metadata(0);
		bool is_any_output = false;
		if (!metadata.empty()) {
			auto type_idx = metadata.FindKey("vgi:any");
			if (type_idx >= 0) {
				is_any_output = true;
			}
			type_idx = metadata.FindKey("vgi_type");
			if (type_idx >= 0 && metadata.value(type_idx) == "any") {
				is_any_output = true;
			}
		}

		LogicalType return_type;
		if (is_any_output) {
			return_type = LogicalType::ANY;
		} else {
			ArrowSchemaWrapper c_schema;
			ArrowTableSchema arrow_table;
			vector<LogicalType> output_types;
			vector<string> output_names;
			vgi::ArrowSchemaToDuckDBTypes(context, func_info.output_schema, c_schema, arrow_table, output_types,
			                              output_names);
			return_type = output_types[0];
		}

		FunctionNullHandling null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING;
		if (func_info.null_handling.has_value()) {
			null_handling = func_info.null_handling.value();
		}

		AggregateFunction agg_func(Identifier(registered_name),   // name
		                           arg_types.positional_types,    // argument types
		                           return_type,                   // return type
		                           vgi::VgiAggregateStateSize,    // state_size
		                           vgi::VgiAggregateInitialize,   // initialize
		                           vgi::VgiAggregateUpdate,       // update
		                           vgi::VgiAggregateCombine,      // combine
		                           vgi::VgiAggregateFinalize,     // finalize
		                           null_handling,                 // null_handling
		                           nullptr,                       // simple_update (not used)
		                           vgi::VgiAggregateFunctionBind, // bind
		                           vgi::VgiAggregateDestroy       // destructor
		);

		if (arg_types.has_varargs) {
			agg_func.GetSignature().AddArgs("args", arg_types.varargs_type);
		}

		agg_func.SetOrderDependent(func_info.order_dependent);
		agg_func.SetDistinctDependent(func_info.distinct_dependent);

		// DuckDB's WindowCustomAggregator picks the window callbacks for OVER
		// queries; GROUP BY keeps the update/combine/finalize path.
		if (func_info.supports_window) {
			agg_func.SetWindowInitCallback(vgi::VgiAggregateWindowInit);
			// Both engines (Haybarn and upstream v2.0.0) prefer the batch callback
			// when set: one RPC per Evaluate() chunk instead of one per row.
			agg_func.SetWindowBatchCallback(vgi::VgiAggregateWindowBatch);
			agg_func.SetWindowCallback(vgi::VgiAggregateWindow);
		}

		auto agg_func_info = make_shared_ptr<vgi::VgiAggregateFunctionInfo>();
		agg_func_info->attach_params = target.attach_params;
		agg_func_info->attach_opaque_data = target.attach_opaque_data;
		agg_func_info->catalog = target.catalog;
		agg_func_info->catalog_name = target.catalog_name;
		agg_func_info->global = target.IsGlobal();
		// Dispatch always uses the worker's own name, never the (possibly
		// prefixed) name the function is registered under.
		agg_func_info->function_name = func_info.name;
		// Every aggregate RPC names the owning schema: the same name in two
		// schemas is two implementations, and a bare-name lookup could pick
		// the wrong one.
		agg_func_info->schema_name = target.DispatchSchema(func_info);
		agg_func_info->output_schema = func_info.output_schema;
		agg_func_info->positional_is_const = arg_types.positional_is_const;
		agg_func_info->positional_names = arg_types.positional_names;
		agg_func_info->setting_names = target.setting_names;
		agg_func_info->required_secrets = func_info.required_secrets;
		agg_func_info->supports_window = func_info.supports_window;
		agg_func_info->streaming_partitioned = func_info.streaming_partitioned;
		if (func_info.streaming_partitioned) {
			VGI_STDERR_DEBUG("[VGI] streaming_partitioned aggregate registered: %s\n", func_info.name.c_str());
		}

		agg_func.SetExtraFunctionInfo(agg_func_info);

		vgi::SetVgiFunctionArgumentNames(agg_func, arg_types.positional_names);
		func_set.AddFunction(agg_func);

		FunctionDescription desc;
		desc.parameter_types = arg_types.positional_types;
		desc.parameter_names = arg_types.positional_names;
		desc.description = func_info.description;
		for (const auto &ex : func_info.examples) {
			desc.examples.push_back(ex);
		}
		for (const auto &cat : func_info.categories) {
			desc.categories.push_back(cat);
		}
		descriptions.push_back(std::move(desc));
	}

	return func_set;
}

} // namespace vgi

void VgiAggregateFunctionSet::LoadEntries(ClientContext &context, const std::lock_guard<std::mutex> & /*_load_lock*/) {
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();

	if (!attach_params || !attach_result) {
		return;
	}

	std::vector<std::string> setting_names;
	for (const auto &setting : attach_result->settings) {
		setting_names.push_back(setting.name);
	}

	auto &vgi_tx_load = VgiTransaction::Get(context, catalog_);
	vgi::CatalogRpcContext rpc_ctx {attach_params, attach_result->attach_opaque_data,
	                                vgi_tx_load.GetTransactionOpaqueData()};
	rpc_ctx.entity_kind = "schema";
	rpc_ctx.entity_qualifier = schema_.name.GetIdentifierName();

	auto function_list = schema_.GetFunctionInventory(rpc_ctx, CatalogType::AGGREGATE_FUNCTION_ENTRY, context);

	// Group overloads by name; std::map for a stable registration order (see
	// VgiTableFunctionSet::LoadEntries).
	std::map<std::string, std::vector<vgi::VgiFunctionInfo>> functions_by_name;
	for (auto &func_info : function_list) {
		if (func_info.function_type != vgi::VgiFunctionType::Aggregate) {
			throw IOException(vgi::ErrorInfo(vgi::error_subtype::kProtocolViolation)
			                      .Function(func_info.name, "AGGREGATE")
			                      .Worker(attach_params->worker_path())
			                      .ExpectedActual("aggregate", vgi::VgiFunctionTypeToString(func_info.function_type)),
			                  "VGI worker returned '%s' function_type when 'aggregate' was requested (function: %s)",
			                  vgi::VgiFunctionTypeToString(func_info.function_type), func_info.name);
		}
		// table_only applies to table functions a table names; a table_only
		// aggregate is unreachable.
		if (func_info.visibility == vgi::VgiFunctionVisibility::TableOnly) {
			continue;
		}
		functions_by_name[func_info.name].push_back(std::move(func_info));
	}

	// schema_name stays empty so DispatchSchema() uses each function's own
	// declared schema.
	vgi::VgiFunctionRegistrationTarget target;
	target.catalog = &catalog_;
	target.catalog_name = catalog_.GetName().GetIdentifierName();
	target.attach_params = attach_params;
	target.attach_opaque_data = attach_result->attach_opaque_data;
	target.setting_names = setting_names;

	for (const auto &pair : functions_by_name) {
		vector<FunctionDescription> descriptions;
		auto func_set = vgi::BuildVgiAggregateFunctionSet(context, pair.first, pair.second, target, descriptions);

		CreateAggregateFunctionInfo info(func_set);
		info.SetQualifiedName(schema_.GetQualifiedName(info.GetFunctionName()));
		info.internal = true;
		info.descriptions = std::move(descriptions);

		auto function_entry = make_uniq_base<StandardEntry, AggregateFunctionCatalogEntry>(
		    catalog_, schema_, info.Cast<CreateAggregateFunctionInfo>());

		// DuckDB's FunctionEntry constructor does not copy comment/tags from the
		// CreateInfo, so set them on the entry or duckdb_functions() never shows
		// them. Both are entry-level: union tags across overloads, take the first
		// non-empty comment.
		for (const auto &func_info : pair.second) {
			for (const auto &[key, val] : func_info.tags) {
				function_entry->tags[key] = val;
			}
			if (function_entry->comment.IsNull() && !func_info.comment.empty()) {
				function_entry->comment = Value(func_info.comment);
			}
		}

		{
			std::lock_guard<std::mutex> __entry_lk(entry_lock_);
			CreateEntryLocked(std::move(function_entry));
		}
	}
}

} // namespace duckdb
