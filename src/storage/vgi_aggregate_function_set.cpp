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
#include "vgi_catalog_api.hpp"
#include "vgi_logging.hpp"

namespace duckdb {

VgiAggregateFunctionSet::VgiAggregateFunctionSet(Catalog &catalog, VgiSchemaEntry &schema)
    : VgiCatalogSet(catalog, &schema), schema_(schema) {
}

void VgiAggregateFunctionSet::LoadEntries(ClientContext &context, const std::lock_guard<std::mutex> &/*_load_lock*/) {
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
	vgi::CatalogRpcContext rpc_ctx{attach_params, attach_result->attach_opaque_data, vgi_tx_load.GetTransactionOpaqueData()};
	rpc_ctx.entity_kind = "schema";
	rpc_ctx.entity_qualifier = schema_.name;

	auto function_list = vgi::InvokeCatalogSchemaContentsFunctions(rpc_ctx, schema_.name,
	                                                               "AGGREGATE_FUNCTION", context);

	// Group functions by name (overloads). std::map (not unordered_map)
	// for stable iteration order — see B43 note in vgi_table_function_set.cpp.
	std::map<std::string, std::vector<vgi::VgiFunctionInfo>> functions_by_name;
	for (auto &func_info : function_list) {
		if (func_info.function_type != vgi::VgiFunctionType::Aggregate) {
			throw IOException("VGI worker returned '%s' function_type when 'aggregate' was requested (function: %s)",
			                  vgi::VgiFunctionTypeToString(func_info.function_type), func_info.name);
		}
		functions_by_name[func_info.name].push_back(std::move(func_info));
	}

	for (const auto &pair : functions_by_name) {
		AggregateFunctionSet func_set(pair.first);
		vector<FunctionDescription> descriptions;

		for (const auto &func_info : pair.second) {
			// Parse argument types
			vgi::FunctionArgumentTypes arg_types;
			if (func_info.arguments_schema) {
				arg_types = vgi::ParseFunctionArgumentSchema(context, func_info.arguments_schema);
			}

			// Determine return type (must have exactly 1 field)
			if (!func_info.output_schema) {
				throw InvalidInputException("Aggregate function '%s' missing required output_schema", func_info.name);
			}
			if (func_info.output_schema->num_fields() != 1) {
				throw InvalidInputException("Aggregate function '%s' output_schema must have exactly 1 field, got %d",
				                            func_info.name, func_info.output_schema->num_fields());
			}

			// Check if output type is dynamic (ANY)
			auto output_field = func_info.output_schema->field(0);
			bool is_any_output = false;
			if (output_field->HasMetadata()) {
				auto metadata = output_field->metadata();
				auto type_idx = metadata->FindKey("vgi:any");
				if (type_idx >= 0) {
					is_any_output = true;
				}
				type_idx = metadata->FindKey("vgi_type");
				if (type_idx >= 0 && metadata->value(type_idx) == "any") {
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

			// Determine null handling
			FunctionNullHandling null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING;
			if (func_info.null_handling.has_value()) {
				null_handling = func_info.null_handling.value();
			}

			// Create AggregateFunction with all VGI callbacks
			AggregateFunction agg_func(
			    pair.first,                          // name
			    arg_types.positional_types,           // argument types
			    return_type,                          // return type
			    vgi::VgiAggregateStateSize,           // state_size
			    vgi::VgiAggregateInitialize,          // initialize
			    vgi::VgiAggregateUpdate,              // update
			    vgi::VgiAggregateCombine,             // combine
			    vgi::VgiAggregateFinalize,            // finalize
			    null_handling,                         // null_handling
			    nullptr,                              // simple_update (not used)
			    vgi::VgiAggregateFunctionBind,        // bind
			    vgi::VgiAggregateDestroy              // destructor
			);

			// Set varargs if this function accepts variable arguments
			if (arg_types.has_varargs) {
				agg_func.varargs = arg_types.varargs_type;
			}

			// Set order/distinct dependence from metadata
			agg_func.SetOrderDependent(func_info.order_dependent);
			agg_func.SetDistinctDependent(func_info.distinct_dependent);

			// Wire the optional window callbacks when the worker advertises
			// supports_window. DuckDB's WindowCustomAggregator::CanAggregate
			// picks the window path automatically for OVER queries; the
			// standard update/combine/finalize path remains for GROUP BY.
			if (func_info.supports_window) {
				agg_func.SetWindowInitCallback(vgi::VgiAggregateWindowInit);
				// SetWindowBatchCallback is a post-v1.5.2 addition (DuckDB main
				// commit 9677176756 "Add aggregate_window_batch_t"). On v1.5.2
				// we fall back to the per-row SetWindowCallback path.
#ifdef DUCKDB_HAS_AGGREGATE_WINDOW_BATCH
				agg_func.SetWindowBatchCallback(vgi::VgiAggregateWindowBatch);
#endif
				agg_func.SetWindowCallback(vgi::VgiAggregateWindow);
			}

			// Create and attach VgiAggregateFunctionInfo
			auto agg_func_info = make_shared_ptr<vgi::VgiAggregateFunctionInfo>();
			agg_func_info->attach_params = attach_params;
			agg_func_info->attach_opaque_data = attach_result->attach_opaque_data;
			agg_func_info->catalog = &catalog_;
			agg_func_info->function_name = func_info.name;
			agg_func_info->output_schema = func_info.output_schema;
			agg_func_info->positional_is_const = arg_types.positional_is_const;
			agg_func_info->positional_names = arg_types.positional_names;
			agg_func_info->setting_names = setting_names;
			agg_func_info->required_secrets = func_info.required_secrets;
			agg_func_info->supports_window = func_info.supports_window;
			agg_func_info->streaming_partitioned = func_info.streaming_partitioned;
			if (func_info.streaming_partitioned) {
				VGI_STDERR_DEBUG("[VGI] streaming_partitioned aggregate registered: %s\n",
				                 func_info.name.c_str());
			}

			agg_func.function_info = agg_func_info;

			func_set.AddFunction(agg_func);

			// Create function description
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

		CreateAggregateFunctionInfo info(func_set);
		info.schema = schema_.name;
		info.internal = true;
		info.descriptions = std::move(descriptions);

		auto function_entry = make_uniq_base<StandardEntry, AggregateFunctionCatalogEntry>(
		    catalog_, schema_, info.Cast<CreateAggregateFunctionInfo>());

		{ std::lock_guard<std::mutex> __entry_lk(entry_lock_); CreateEntryLocked(std::move(function_entry)); }
	}
}

} // namespace duckdb
