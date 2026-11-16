// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "storage/vgi_table_function_set.hpp"
#include "vgi_engine_compat.hpp"
#include "vgi_function_docs.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "storage/vgi_catalog.hpp"
#include "storage/vgi_schema_entry.hpp"
#include "storage/vgi_transaction.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_rpc.hpp"
#include "vgi_exception.hpp"
#include "vgi_table_function_impl.hpp"
#include "vgi_table_in_out_impl.hpp"

#include <algorithm>
#include <cstdint>

namespace duckdb {

namespace {

bool SupportsVgiFilterV2(const vgi::VgiFunctionInfo &info) {
	return info.filter_pushdown.value_or(false) &&
	       std::find(info.filter_semantic_profiles.begin(), info.filter_semantic_profiles.end(),
	                 "vgi.duckdb.standard.v1") != info.filter_semantic_profiles.end();
}

//! Read the client-side ``vgi_split_scans`` kill switch. Defaults to true.
bool VgiSplitScansEnabled(ClientContext &context) {
	Value v;
	if (context.TryGetCurrentSetting("vgi_split_scans", v) && !v.IsNull()) {
		return v.GetValue<bool>();
	}
	return true;
}

} // namespace

namespace {

// Current values of the settings the catalog registered at ATTACH.
std::map<std::string, Value> ExtractVgiSettings(ClientContext &context, const std::vector<std::string> &setting_names) {
	std::map<std::string, Value> settings;

	for (const auto &name : setting_names) {
		Value value;
		if (context.TryGetCurrentSetting(Identifier(name), value)) {
			settings[name] = value;
		}
	}

	return settings;
}

// Unset maps to INSERTION_ORDER, DuckDB's TableFunction default.
OrderPreservationType MapOrderPreservation(std::optional<vgi::VgiOrderPreservation> v) {
	if (!v) {
		return OrderPreservationType::INSERTION_ORDER;
	}
	switch (*v) {
	case vgi::VgiOrderPreservation::PreservesOrder:
		return OrderPreservationType::INSERTION_ORDER;
	case vgi::VgiOrderPreservation::NoOrderGuarantee:
		return OrderPreservationType::NO_ORDER;
	case vgi::VgiOrderPreservation::FixedOrder:
		return OrderPreservationType::FIXED_ORDER;
	}
	return OrderPreservationType::INSERTION_ORDER;
}

} // anonymous namespace

VgiTableFunctionSet::VgiTableFunctionSet(Catalog &catalog, VgiSchemaEntry &schema)
    : VgiCatalogSet(catalog, &schema), schema_(schema) {
}

static unique_ptr<FunctionData> VgiCatalogTableFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types,
                                                            vector<Identifier> &names) {
	auto &vgi_info = input.info->Cast<vgi::VgiTableFunctionInfo>();

	auto bind_data = make_uniq<vgi::VgiTableFunctionBindData>();

	// A function published into system.main outlives DETACH, so its live state
	// is resolved from the attached catalog (see ResolveVgiFunctionBinding).
	auto binding =
	    vgi::ResolveVgiFunctionBinding(context, vgi_info.target(), input.table_function.GetName().GetIdentifierName());
	bind_data->attach_params = binding.attach_params;
	bind_data->attach_opaque_data = binding.attach_opaque_data;
	auto &vgi_tx = VgiTransaction::Get(context, *binding.catalog);
	bind_data->transaction_opaque_data = vgi_tx.GetTransactionOpaqueData();
	bind_data->function_name = vgi_info.function_info().name;
	bind_data->schema_name = vgi_info.function_info().schema_name;
	bind_data->projection_pushdown = vgi_info.function_info().projection_pushdown.value_or(false);
	bind_data->filter_semantic_profiles = vgi_info.function_info().filter_semantic_profiles;
	bind_data->additional_filter_functions = vgi_info.function_info().additional_filter_functions;
	// InstallBatch requires vgi_batch_index batch metadata when this is set.
	bind_data->supports_batch_index = vgi_info.function_info().supports_batch_index;

	// The setting is read at BIND, not init: a ``SET vgi_split_scans`` between
	// bind and execute must not pair a bind that assumed splits with an init
	// that did not.
	bind_data->supports_splits = vgi_info.function_info().supports_splits && VgiSplitScansEnabled(context);
	bind_data->split_token_ttl_seconds = vgi_info.function_info().split_token_ttl_seconds;
	// fixed_order clamps MaxThreads to 1, which is what actually serialises a
	// FIXED_ORDER source (order_preservation_type only informs the planner).
	// batch_index functions skip the clamp: ordered sinks reassemble their
	// output via get_partition_data, so the source can stay parallel.
	if (vgi_info.function_info().supports_batch_index) {
		bind_data->fixed_order = false;
	} else {
		bind_data->fixed_order =
		    MapOrderPreservation(vgi_info.function_info().order_preservation) == OrderPreservationType::FIXED_ORDER;
	}

	// partition_column_indices is resolved after PerformVgiTableFunctionBind,
	// once the output schema's field metadata is available.
	bind_data->partition_kind = vgi_info.function_info().partition_kind;

	vector<Value> positional_args;
	for (auto &val : input.inputs) {
		positional_args.push_back(val);
	}

	vector<std::pair<string, Value>> named_args;
	for (auto &[name, value] : input.named_parameters) {
		named_args.emplace_back(name.GetIdentifierName(), value);
	}

	bind_data->arguments = vgi::BuildArgumentsFromValues(context, positional_args, named_args);

	bind_data->settings = ExtractVgiSettings(context, binding.setting_names);

	bind_data->required_secrets = vgi_info.function_info().required_secrets;

	const auto &required_settings = vgi_info.function_info().required_settings;
	if (!required_settings.empty()) {
		std::vector<std::string> missing_settings;
		for (const auto &setting_name : required_settings) {
			if (bind_data->settings.find(setting_name) == bind_data->settings.end()) {
				missing_settings.push_back(setting_name);
			}
		}
		if (!missing_settings.empty()) {
			std::string missing_list;
			for (size_t i = 0; i < missing_settings.size(); i++) {
				if (i > 0) {
					missing_list += ", ";
				}
				missing_list += missing_settings[i];
			}
			throw BinderException(vgi::ErrorInfo(vgi::error_subtype::kMissingSettings)
			                          .Set(vgi::error_key::kFunctionName, bind_data->function_name)
			                          .Set(vgi::error_key::kMissingSettings, missing_list),
			                      StringUtil::Format("Function '%s' requires the following settings to be set: %s. "
			                                         "Use SET <setting_name> = <value> before calling this function.",
			                                         bind_data->function_name, missing_list));
		}
	}

	vgi::PerformVgiTableFunctionBind(context, *bind_data, return_types, names);

	// Resolved once here so VgiGetPartitionInfo needs no schema walk per
	// planner call.
	if (bind_data->partition_kind != vgi::VgiPartitionKind::NotPartitioned) {
		const auto &out_schema = bind_data->bind_result.output_schema;
		if (out_schema) {
			for (int i = 0; i < out_schema->num_fields(); ++i) {
				const auto meta = out_schema->field_metadata(i);
				const auto idx = meta.FindKey("vgi.partition_column");
				if (idx >= 0 && meta.value(idx) == "true") {
					bind_data->partition_column_indices.push_back(static_cast<idx_t>(i));
				}
			}
		}
		// The worker SDK enforces this too; checked here against version skew.
		if (bind_data->partition_column_indices.empty()) {
			throw BinderException("VGI function '%s' declares partition_kind but no bind-schema "
			                      "field carries vgi.partition_column metadata; mark partition "
			                      "columns with vgi.schema_utils.partition_field()",
			                      bind_data->function_name);
		}
	}

	input.table_function.projection_pushdown = bind_data->projection_pushdown;

	return bind_data;
}

static unique_ptr<FunctionData> VgiCatalogTableInOutFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                                                 vector<LogicalType> &return_types,
                                                                 vector<Identifier> &names) {
	auto &vgi_info = input.info->Cast<vgi::VgiTableFunctionInfo>();

	vgi::VgiTableInOutBindParams params;
	// See VgiCatalogTableFunctionBind: resolved live because of DETACH.
	auto binding =
	    vgi::ResolveVgiFunctionBinding(context, vgi_info.target(), input.table_function.GetName().GetIdentifierName());
	params.attach_params = binding.attach_params;
	params.function_name = vgi_info.function_info().name;
	params.schema_name = vgi_info.function_info().schema_name;
	params.attach_opaque_data = binding.attach_opaque_data;
	auto &tio_tx = VgiTransaction::Get(context, *binding.catalog);
	params.transaction_opaque_data = tio_tx.GetTransactionOpaqueData();
	params.settings = ExtractVgiSettings(context, binding.setting_names);
	params.required_secrets = vgi_info.function_info().required_secrets;
	params.table_buffering = vgi_info.function_info().function_type == vgi::VgiFunctionType::TableBuffering;
	params.source_order_dependent = vgi_info.function_info().source_order_dependent;
	params.sink_order_dependent = vgi_info.function_info().sink_order_dependent;
	params.requires_input_batch_index = vgi_info.function_info().requires_input_batch_index;
	params.projection_pushdown = vgi_info.function_info().projection_pushdown.value_or(false);
	params.filter_pushdown = SupportsVgiFilterV2(vgi_info.function_info());
	params.filter_semantic_profiles = vgi_info.function_info().filter_semantic_profiles;
	params.additional_filter_functions = vgi_info.function_info().additional_filter_functions;
	// max_workers=1 keeps a streaming table-in-out on one shared worker.
	params.max_workers = vgi_info.function_info().max_workers.value_or(0);
	// Blended: the declared positional arg names are the worker's input column
	// names, so bind builds the input schema from the arguments schema.
	params.input_from_args = vgi_info.function_info().input_from_args;
	params.input_rows_per_call = vgi_info.function_info().input_rows_per_call;
	params.stability = vgi_info.function_info().stability; // for the map-operator dedup gate
	if (params.input_from_args && vgi_info.function_info().arguments_schema) {
		auto blended_args = vgi::ParseFunctionArgumentSchema(context, vgi_info.function_info().arguments_schema);
		params.positional_input_names = blended_args.positional_names;
		params.positional_input_types = blended_args.positional_types;
		params.varargs_input_type = blended_args.varargs_type;
		params.has_varargs = blended_args.has_varargs;
	}

	const auto &required_settings = vgi_info.function_info().required_settings;
	if (!required_settings.empty()) {
		std::vector<std::string> missing_settings;
		for (const auto &setting_name : required_settings) {
			if (params.settings.find(setting_name) == params.settings.end()) {
				missing_settings.push_back(setting_name);
			}
		}
		if (!missing_settings.empty()) {
			std::string missing_list;
			for (size_t i = 0; i < missing_settings.size(); i++) {
				if (i > 0) {
					missing_list += ", ";
				}
				missing_list += missing_settings[i];
			}
			throw BinderException(vgi::ErrorInfo(vgi::error_subtype::kMissingSettings)
			                          .Set(vgi::error_key::kFunctionName, params.function_name)
			                          .Set(vgi::error_key::kMissingSettings, missing_list),
			                      StringUtil::Format("Function '%s' requires the following settings to be set: %s. "
			                                         "Use SET <setting_name> = <value> before calling this function.",
			                                         params.function_name, missing_list));
		}
	}

	return vgi::VgiTableInOutBind(context, input, return_types, names, params);
}

namespace vgi {

TableFunctionSet BuildVgiTableFunctionSet(ClientContext &context, const std::string &registered_name,
                                          const std::vector<vgi::VgiFunctionInfo> &overloads,
                                          const VgiFunctionRegistrationTarget &target,
                                          vector<FunctionDescription> &descriptions) {
	TableFunctionSet func_set {Identifier(registered_name)};

	for (const auto &func_info : overloads) {
		vgi::FunctionArgumentTypes arg_types;
		if (func_info.arguments_schema) {
			arg_types = vgi::ParseFunctionArgumentSchema(context, func_info.arguments_schema);
		}

		// Table-in-out: a TABLE argument, or a blended function whose positional
		// args are its per-row input columns (input_from_args). Blended
		// positional_types are value types, so f(52,13) and FROM t, f(t.x,t.y) bind.
		if (arg_types.HasTableInput() || func_info.input_from_args) {
			// Blended functions are map-shaped; the SDK rejects a finalize too,
			// this guards hand-rolled or old workers.
			if (func_info.input_from_args && func_info.has_finalize) {
				throw InvalidInputException(
				    "Function '%s' advertises input_from_args (blended) AND has_finalize; a blended "
				    "RowTransformFunction is map-shaped and cannot have a finalize.",
				    func_info.name);
			}
			TableFunction table_func(arg_types.positional_types, nullptr, VgiCatalogTableInOutFunctionBind,
			                         vgi::VgiTableInOutInitGlobal, vgi::VgiTableInOutInitLocal);

			table_func.in_out_function = vgi::VgiTableInOutFunction;
			// Register the finalize only when the worker declares one:
			// PhysicalTableInOutFunction throws "FinalExecute not supported for
			// project_input" when both it and projected_input (correlated
			// LATERAL) are set. TableBuffering omits it: VgiTableBufferingRewriter
			// moves finalize into PhysicalVgiTableBufferingFunction, and if the
			// rewrite does not fire VgiTableInOutFunction throws.
			const bool is_buffering = func_info.function_type == vgi::VgiFunctionType::TableBuffering;
			if (func_info.has_finalize && !is_buffering) {
				table_func.in_out_function_final = vgi::VgiTableInOutFinalize;
			}
			// Buffered (Sink+Source) supports projection and filter pushdown:
			// VgiTableBufferingRewriter captures the pushed-down state at plan time.
			// Streaming InOut supports only projection pushdown. For InOut,
			// DuckDB's plan_get.cpp hands table_filters only to the function's init
			// and adds no FILTER above PhysicalTableInOutFunction, so advertising
			// filter (or expression) pushdown would make the predicates the worker's
			// sole responsibility. Leaving it off keeps a FILTER above the operator.
			table_func.projection_pushdown = func_info.projection_pushdown.value_or(false);
			VgiDisableRuntimeFilterPushdown(table_func);
			if (is_buffering) {
				table_func.filter_pushdown = SupportsVgiFilterV2(func_info);
				if (table_func.filter_pushdown) {
					table_func.pushdown_expression = vgi::VgiTableInOutPushdownExpression;
				}
			}
			table_func.order_preservation_type = MapOrderPreservation(func_info.order_preservation);

			// Varargs lets DuckDB accept a blended VARARGS call
			// (row_sum(1,2,3) -> N input columns of varargs_type).
			if (arg_types.has_varargs) {
				table_func.GetSignature().AddArgs("args", arg_types.varargs_type);
			}

			for (const auto &option : arg_types.named_parameters) {
				vgi::AddVgiFunctionOption(table_func, Identifier(option.first), option.second);
			}

			table_func.function_info = make_uniq<vgi::VgiTableFunctionInfo>(target, func_info);

			vgi::SetVgiFunctionArgumentNames(table_func, arg_types.positional_names);
			func_set.AddFunction(table_func);
		} else {
			// Only positional arguments go in the signature; named ones are options.
			TableFunction table_func(arg_types.positional_types, vgi::VgiTableFunctionScan, VgiCatalogTableFunctionBind,
			                         vgi::VgiTableFunctionInitGlobal, vgi::VgiTableFunctionInitLocal);
			table_func.projection_pushdown = func_info.projection_pushdown.value_or(false);
			table_func.filter_pushdown = SupportsVgiFilterV2(func_info);
			VgiDisableRuntimeFilterPushdown(table_func);
			table_func.sampling_pushdown = func_info.sampling_pushdown.value_or(false);
			table_func.order_preservation_type = MapOrderPreservation(func_info.order_preservation);
			if (table_func.filter_pushdown) {
				table_func.pushdown_expression = vgi::VgiPushdownExpression;
			}
			table_func.cardinality = vgi::VgiTableFunctionCardinality;
			table_func.statistics = vgi::VgiTableFunctionStatistics;
			table_func.table_scan_progress = vgi::VgiTableFunctionProgress;
			table_func.to_string = vgi::VgiTableFunctionToString;
			table_func.get_metrics = vgi::VgiTableFunctionGetMetrics;
			table_func.set_scan_order = vgi::VgiSetScanOrder;
			// batch_index: ordered sinks reassemble parallel output from
			// get_partition_data (the matching fixed_order clamp skip is in
			// VgiCatalogTableFunctionBind).
			if (func_info.supports_batch_index) {
				table_func.get_partition_data = vgi::VgiGetPartitionData;
			}
			// PartitionColumns: get_partition_info reports the partition shape over
			// the GROUP BY columns; get_partition_data supplies per-column (min, max).
			if (func_info.partition_kind != vgi::VgiPartitionKind::NotPartitioned) {
				table_func.get_partition_info = vgi::VgiGetPartitionInfo;
				table_func.get_partition_data = vgi::VgiGetPartitionData;
			}
			// Deliberately not INITIALIZE_ON_SCHEDULE: it would run all init_globals
			// in parallel at scheduling time (max(RTT) instead of sum(RTT) across
			// many scans), but before a hash join's build side has filled
			// PhysicalTableScan::dynamic_filters, so a probe-side VGI scan would
			// never receive the join-key InFilter (join_keys_pushdown.test).
			// Restoring it needs init split into "establish session" + "set filters".

			if (arg_types.has_varargs) {
				table_func.GetSignature().AddArgs("args", arg_types.varargs_type);
			}

			for (const auto &option : arg_types.named_parameters) {
				vgi::AddVgiFunctionOption(table_func, Identifier(option.first), option.second);
			}

			table_func.function_info = make_uniq<vgi::VgiTableFunctionInfo>(target, func_info);

			vgi::SetVgiFunctionArgumentNames(table_func, arg_types.positional_names);
			func_set.AddFunction(table_func);
		}

		// Named parameters are appended so duckdb_functions() lists them.
		FunctionDescription desc;
		desc.parameter_types = arg_types.positional_types;
		desc.parameter_names = arg_types.positional_names;

		for (const auto &[name, type] : arg_types.named_parameters) {
			desc.parameter_types.push_back(type);
			desc.parameter_names.push_back(name);
		}
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

void VgiTableFunctionSet::LoadEntries(ClientContext &context, const std::lock_guard<std::mutex> & /*_load_lock*/) {
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

	auto worker_path = attach_params->worker_path();
	auto &vgi_tx_load = VgiTransaction::Get(context, catalog_);
	vgi::CatalogRpcContext rpc_ctx {attach_params, attach_result->attach_opaque_data,
	                                vgi_tx_load.GetTransactionOpaqueData()};
	rpc_ctx.entity_kind = "schema";
	rpc_ctx.entity_qualifier = schema_.name.GetIdentifierName();

	auto function_list = schema_.GetFunctionInventory(rpc_ctx, CatalogType::TABLE_FUNCTION_ENTRY, context);

	// Group overloads by name; each name's overloads keep the worker's order
	// (DuckDB's overload resolution is order-sensitive for ambiguous matches).
	// std::map keeps entry creation order deterministic.
	//
	// table_only functions (protocol 3.3) are built the same way but go into the
	// hidden set, which Scan()/LookupEntry() never return. A name listed both
	// ways gets one entry in each set.
	std::map<std::string, std::vector<vgi::VgiFunctionInfo>> functions_by_name;
	std::map<std::string, std::vector<vgi::VgiFunctionInfo>> hidden_functions_by_name;
	for (auto &func_info : function_list) {
		// Table and TableBuffering share the table-function namespace.
		if (func_info.function_type != vgi::VgiFunctionType::Table &&
		    func_info.function_type != vgi::VgiFunctionType::TableBuffering) {
			throw IOException(vgi::ErrorInfo(vgi::error_subtype::kProtocolViolation)
			                      .Function(func_info.name, "TABLE")
			                      .Worker(attach_params->worker_path())
			                      .ExpectedActual("table", vgi::VgiFunctionTypeToString(func_info.function_type)),
			                  "VGI worker returned '%s' function_type when 'table' was requested (function: %s)",
			                  vgi::VgiFunctionTypeToString(func_info.function_type), func_info.name);
		}
		auto &group = func_info.visibility == vgi::VgiFunctionVisibility::TableOnly ? hidden_functions_by_name
		                                                                             : functions_by_name;
		group[func_info.name].push_back(std::move(func_info));
	}

	vgi::VgiFunctionRegistrationTarget target;
	target.catalog = &catalog_;
	target.catalog_name = catalog_.GetName().GetIdentifierName();
	target.attach_params = attach_params;
	target.attach_opaque_data = attach_result->attach_opaque_data;
	target.setting_names = setting_names;
	target.schema_name = schema_.name.GetIdentifierName();

	auto build_entries = [&](const std::map<std::string, std::vector<vgi::VgiFunctionInfo>> &groups, bool hidden) {
		for (const auto &pair : groups) {
			vector<FunctionDescription> descriptions;
			auto func_set = vgi::BuildVgiTableFunctionSet(context, pair.first, pair.second, target, descriptions);

			CreateTableFunctionInfo info(func_set);
			info.SetQualifiedName(schema_.GetQualifiedName(info.GetFunctionName()));
			info.internal = true;
			info.descriptions = std::move(descriptions);

			auto function_entry = make_uniq_base<StandardEntry, TableFunctionCatalogEntry>(
			    catalog_, schema_, info.Cast<CreateTableFunctionInfo>());

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

			if (hidden) {
				AddHiddenEntry(std::move(function_entry));
			} else {
				std::lock_guard<std::mutex> __entry_lk(entry_lock_);
				CreateEntryLocked(std::move(function_entry));
			}
		}
	};
	build_entries(functions_by_name, /*hidden=*/false);
	build_entries(hidden_functions_by_name, /*hidden=*/true);
}

} // namespace duckdb
