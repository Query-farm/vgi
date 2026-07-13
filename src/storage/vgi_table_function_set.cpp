// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "storage/vgi_table_function_set.hpp"

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
#include "vgi_table_function_impl.hpp"
#include "vgi_table_in_out_impl.hpp"

#include <cstdint>

namespace duckdb {

namespace {

// Extract settings from client context given a list of setting names
// This is used for catalog-bound functions where we know which settings the catalog registered
std::map<std::string, Value> ExtractVgiSettings(ClientContext &context,
                                                const std::vector<std::string> &setting_names) {
	std::map<std::string, Value> settings;

	for (const auto &name : setting_names) {
		Value value;
		if (context.TryGetCurrentSetting(name, value)) {
			settings[name] = value;
		}
	}

	return settings;
}

// Map the wire-format VgiOrderPreservation to DuckDB's OrderPreservationType.
// Unset (nullopt) collapses to INSERTION_ORDER, matching DuckDB's TableFunction
// default; FixedOrder forces Pipeline::IsOrderDependent() -> true so the planner
// serialises the pipeline (single worker emits all rows).
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

// ============================================================================
// Bind Function - For catalog-based VGI table functions
// ============================================================================

static unique_ptr<FunctionData> VgiCatalogTableFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types, vector<string> &names) {
	// Access the VgiTableFunctionInfo attached to this function
	auto &vgi_info = input.info->Cast<vgi::VgiTableFunctionInfo>();

	auto bind_data = make_uniq<vgi::VgiTableFunctionBindData>();

	// Copy connection information from the catalog function info
	bind_data->attach_params = vgi_info.attach_params();
	bind_data->attach_opaque_data = vgi_info.attach_opaque_data();
	auto &vgi_tx = VgiTransaction::Get(context, vgi_info.catalog());
	bind_data->transaction_opaque_data = vgi_tx.GetTransactionOpaqueData();
	bind_data->function_name = vgi_info.function_info().name;
	bind_data->projection_pushdown = vgi_info.function_info().projection_pushdown.value_or(false);
	bind_data->supported_expression_filters = vgi_info.function_info().supported_expression_filters;
	// Carry the wire flag onto bind_data so InstallBatch knows whether to
	// require/parse vgi_batch_index from each Arrow record-batch's
	// KeyValueMetadata.
	bind_data->supports_batch_index = vgi_info.function_info().supports_batch_index;
	// FIXED_ORDER: clamps MaxThreads to 1 so DuckDB schedules the source
	// onto a single thread. ``TableFunction::order_preservation_type`` is
	// already set at function-registration time (see ``AddFunction`` calls
	// below) and drives the planner's ``Pipeline::IsOrderDependent()``;
	// the boolean here is what actually serialises the source.
	//
	// EXCEPT when the function opts in to batch_index: ordered sinks
	// (BatchCollector, BatchInsert, BatchCopyToFile, Limit) reassemble
	// output via ``TableFunction::get_partition_data``, so the source
	// can stay parallel. Skipping the clamp here is the entire point of
	// the batch_index feature.
	if (vgi_info.function_info().supports_batch_index) {
		bind_data->fixed_order = false;
	} else {
		bind_data->fixed_order =
		    MapOrderPreservation(vgi_info.function_info().order_preservation) ==
		    OrderPreservationType::FIXED_ORDER;
	}

	// PartitionColumns mode: carry the wire ``partition_kind`` flag onto
	// bind_data. ``partition_column_indices`` is resolved later, after
	// ``PerformVgiTableFunctionBind`` populates the bind result's output
	// schema (the per-field metadata we need is only available then).
	bind_data->partition_kind = vgi_info.function_info().partition_kind;

	// Build Arrow arguments from the function call inputs
	// input.inputs contains positional arguments passed to the function
	vector<Value> positional_args;
	for (auto &val : input.inputs) {
		positional_args.push_back(val);
	}

	// Extract named arguments from input.named_parameters
	vector<std::pair<string, Value>> named_args;
	for (auto &[name, value] : input.named_parameters) {
		named_args.emplace_back(name, value);
	}

	// Build Arrow arguments struct from both positional and named args
	bind_data->arguments = vgi::BuildArgumentsFromValues(context, positional_args, named_args);

	// Extract settings registered by this catalog
	bind_data->settings = ExtractVgiSettings(context, vgi_info.setting_names());

	// Copy required secrets from function metadata
	bind_data->required_secrets = vgi_info.function_info().required_secrets;

	// Validate that all required settings for this function are present
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
			throw BinderException("Function '%s' requires the following settings to be set: %s. "
			                      "Use SET <setting_name> = <value> before calling this function.",
			                      bind_data->function_name, missing_list);
		}
	}

	// Perform the common bind handshake
	vgi::PerformVgiTableFunctionBind(context, *bind_data, return_types, names);

	// Resolve partition_column_indices by walking the bind output schema
	// for fields whose metadata carries ``vgi.partition_column = "true"``.
	// Per-field Arrow metadata round-trips losslessly through
	// ``pa.Schema.serialize() -> arrow::ipc::ReadSchema``, which is what
	// the bind RPC uses today. Doing this once at bind keeps
	// ``VgiGetPartitionInfo`` cheap (O(P) membership check per planner
	// call, no schema walk).
	if (bind_data->partition_kind != vgi::VgiPartitionKind::NotPartitioned) {
		const auto &out_schema = bind_data->bind_result.output_schema;
		if (out_schema) {
			for (int i = 0; i < out_schema->num_fields(); ++i) {
				const auto &field = out_schema->field(i);
				const auto &meta = field->metadata();
				if (meta) {
					int idx = meta->FindKey("vgi.partition_column");
					if (idx >= 0 && meta->value(idx) == "true") {
						bind_data->partition_column_indices.push_back(static_cast<idx_t>(i));
					}
				}
			}
		}
		// Registration-time invariant: ``partition_kind != NotPartitioned``
		// requires at least one annotated field. The worker library also
		// enforces this at resolve_metadata; we check here as defense in
		// depth (catches partial-deployment / version-skew bugs).
		if (bind_data->partition_column_indices.empty()) {
			throw BinderException(
			    "VGI function '%s' declares partition_kind but no bind-schema "
			    "field carries vgi.partition_column metadata; mark partition "
			    "columns with vgi.schema_utils.partition_field()",
			    bind_data->function_name);
		}
	}

	input.table_function.projection_pushdown = bind_data->projection_pushdown;

	return bind_data;
}

// ============================================================================
// Bind Function - For catalog-based VGI table-in-out functions
// ============================================================================

static unique_ptr<FunctionData> VgiCatalogTableInOutFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                                                 vector<LogicalType> &return_types,
                                                                 vector<string> &names) {
	// Access the VgiTableFunctionInfo attached to this function
	auto &vgi_info = input.info->Cast<vgi::VgiTableFunctionInfo>();

	// Build parameters for the table-in-out bind
	vgi::VgiTableInOutBindParams params;
	params.attach_params = vgi_info.attach_params();
	params.function_name = vgi_info.function_info().name;
	params.attach_opaque_data = vgi_info.attach_opaque_data();
	auto &tio_tx = VgiTransaction::Get(context, vgi_info.catalog());
	params.transaction_opaque_data = tio_tx.GetTransactionOpaqueData();
	params.settings = ExtractVgiSettings(context, vgi_info.setting_names());
	params.required_secrets = vgi_info.function_info().required_secrets;
	params.table_buffering =
	    vgi_info.function_info().function_type == vgi::VgiFunctionType::TableBuffering;
	params.source_order_dependent = vgi_info.function_info().source_order_dependent;
	params.sink_order_dependent = vgi_info.function_info().sink_order_dependent;
	params.requires_input_batch_index = vgi_info.function_info().requires_input_batch_index;
	params.projection_pushdown = vgi_info.function_info().projection_pushdown.value_or(false);
	params.filter_pushdown = vgi_info.function_info().filter_pushdown.value_or(false);
	// A3 serial opt-out: a worker that declares Meta.max_workers=1 keeps its
	// streaming table-in-out on a single shared worker (MaxThreads()=1).
	params.max_workers = vgi_info.function_info().max_workers.value_or(0);
	// Blended (Phase B): the DECLARED positional arg names are the worker's input
	// column names (it reads batch.column("<name>")). Re-parse the arguments schema
	// to recover them (+ varargs flag) so bind can build the input schema by name.
	params.input_from_args = vgi_info.function_info().input_from_args;
	if (params.input_from_args && vgi_info.function_info().arguments_schema) {
		auto blended_args =
		    vgi::ParseFunctionArgumentSchema(context, vgi_info.function_info().arguments_schema);
		params.positional_input_names = blended_args.positional_names;
		params.positional_input_types = blended_args.positional_types;
		params.varargs_input_type = blended_args.varargs_type;
		params.has_varargs = blended_args.has_varargs;
	}

	// Validate required settings
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
			throw BinderException("Function '%s' requires the following settings to be set: %s. "
			                      "Use SET <setting_name> = <value> before calling this function.",
			                      params.function_name, missing_list);
		}
	}

	// Use the table-in-out bind function
	return vgi::VgiTableInOutBind(context, input, return_types, names, params);
}

void VgiTableFunctionSet::LoadEntries(ClientContext &context, const std::lock_guard<std::mutex> &/*_load_lock*/) {
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();

	if (!attach_params || !attach_result) {
		return;
	}

	// Extract setting names from the attach result for passing to function bind
	std::vector<std::string> setting_names;
	for (const auto &setting : attach_result->settings) {
		setting_names.push_back(setting.name);
	}

	// Call catalog_schema_contents_functions via RPC for table functions
	auto worker_path = attach_params->worker_path();
	auto &vgi_tx_load = VgiTransaction::Get(context, catalog_);
	vgi::CatalogRpcContext rpc_ctx{attach_params, attach_result->attach_opaque_data, vgi_tx_load.GetTransactionOpaqueData()};
	rpc_ctx.entity_kind = "schema";
	rpc_ctx.entity_qualifier = schema_.name;

	auto function_list = vgi::InvokeCatalogSchemaContentsFunctions(rpc_ctx, schema_.name,
	                                                               "TABLE_FUNCTION", context);

	// Group functions by name (overloads). std::map (not unordered_map) so
	// the iteration order over `functions_by_name` is stable across runs
	// and stdlib versions — overload resolution treats the order in which
	// signatures are added to a TableFunctionSet as significant for
	// ambiguous matches, and we don't want non-determinism from libstdc++
	// hash-bucket order leaking into user-visible function dispatch.
	std::map<std::string, std::vector<vgi::VgiFunctionInfo>> functions_by_name;
	for (auto &func_info : function_list) {
		// Streaming Table and buffered TableBuffering both live in the
		// table-function namespace; the distinction drives bind-time
		// dispatch (streaming in_out_function vs the optimizer rewrite to
		// PhysicalVgiTableBufferingFunction) but not registration.
		if (func_info.function_type != vgi::VgiFunctionType::Table &&
		    func_info.function_type != vgi::VgiFunctionType::TableBuffering) {
			throw IOException("VGI worker returned '%s' function_type when 'table' was requested (function: %s)",
			                  vgi::VgiFunctionTypeToString(func_info.function_type), func_info.name);
		}
		functions_by_name[func_info.name].push_back(std::move(func_info));
	}

	// Create function entries
	for (const auto &pair : functions_by_name) {
		TableFunctionSet func_set(pair.first);

		// Collect function descriptions for all overloads
		vector<FunctionDescription> descriptions;

		for (const auto &func_info : pair.second) {
			// Parse argument types, distinguishing positional from named arguments
			// Named arguments have metadata "vgi_arg: named" in the Arrow schema
			vgi::FunctionArgumentTypes arg_types;
			if (func_info.arguments_schema) {
				arg_types = vgi::ParseFunctionArgumentSchema(context, func_info.arguments_schema);
			}

			// Check if this is a table-in-out function: either it has a TABLE input
			// argument (classic), or it is a blended ("UNNEST-style") function whose
			// positional args ARE its per-row input columns (input_from_args). Both
			// register through the in_out_function path — a blended function's
			// positional_types are real value types (no TABLE marker), so it also
			// serves the literal f(52,13) and column FROM t, f(t.x,t.y) shapes.
			if (arg_types.HasTableInput() || func_info.input_from_args) {
				// A blended function must not also declare a finalize (map-shaped).
				// The worker's resolve_metadata already rejects this; assert as a
				// defense against a hand-rolled/old worker that advertises both.
				if (func_info.input_from_args && func_info.has_finalize) {
					throw InvalidInputException(
					    "Function '%s' advertises input_from_args (blended) AND has_finalize; a blended "
					    "RowTransformFunction is map-shaped and cannot have a finalize.", func_info.name);
				}
				// Create a table-in-out function
				// Table-in-out functions use LogicalType::TABLE in args and have an in_out_function
				TableFunction table_func(arg_types.positional_types, nullptr, VgiCatalogTableInOutFunctionBind,
				                         vgi::VgiTableInOutInitGlobal, vgi::VgiTableInOutInitLocal);

				// Set the in_out_function for processing streaming input.
				table_func.in_out_function = vgi::VgiTableInOutFunction;
				// Only register the finalize callback when the worker declares
				// a finalize/finish stage. DuckDB's PhysicalTableInOutFunction
				// throws "FinalExecute not supported for project_input" when
				// both in_out_function_final and projected_input (LATERAL with
				// correlated columns) are set, so leaving it unset for
				// stateless transforms lets those functions participate in
				// lateral joins.
				//
				// When the function is TableBuffering, we deliberately *omit*
				// in_out_function_final — the OptimizerExtension rewrites the
				// LogicalGet into PhysicalVgiTableBufferingFunction, which
				// handles finalize through its Sink+Source lifecycle. If the
				// rewrite is somehow missed, the operator falls back to
				// VgiTableInOutFunction, which asserts loudly on bind_data
				// with table_buffering=true.
				const bool is_buffering =
				    func_info.function_type == vgi::VgiFunctionType::TableBuffering;
				if (func_info.has_finalize && !is_buffering) {
					table_func.in_out_function_final = vgi::VgiTableInOutFinalize;
				}
				// Advertise pushdown capability to DuckDB's optimizer.
				//
				// **Buffered (Sink+Source)** supports BOTH projection
				// and filter pushdown. ``VgiTableBufferingRewriter`` in
				// vgi_extension.cpp captures pushed-down state at plan
				// time, narrows the Logical op's return_types, threads
				// pushdown through PerformInit, and the Source's
				// ArrowToDuckDB call uses arrow_scan_is_projected=true
				// to read narrow worker batches positionally.
				//
				// **Streaming InOut** supports ONLY projection pushdown.
				// DuckDB's planner discards ``table_filters`` for the
				// InOut path (see duckdb/src/execution/physical_plan/
				// plan_get.cpp:37-83 — the early-return InOut branch
				// constructs ``PhysicalTableInOutFunction`` with no
				// table_filters parameter and doesn't add a FILTER node
				// above the operator). Advertising filter_pushdown=true
				// causes the filter optimizer to push predicates into
				// ``LogicalGet.table_filters``, where they then get
				// silently dropped — query returns unfiltered rows.
				// Don't advertise filter_pushdown for InOut. Filters
				// always run via a separate FILTER node above the
				// operator (correct behavior, just no wire-bandwidth
				// savings). Expression pushdown is intentionally
				// skipped for the same reason.
				table_func.projection_pushdown = func_info.projection_pushdown.value_or(false);
				if (is_buffering) {
					table_func.filter_pushdown = func_info.filter_pushdown.value_or(false);
					if (!func_info.supported_expression_filters.empty()) {
						table_func.pushdown_expression = vgi::VgiPushdownExpression;
					}
				}
				table_func.order_preservation_type = MapOrderPreservation(func_info.order_preservation);

				// Register named parameters
				table_func.named_parameters = arg_types.named_parameters;

				// Register varargs so DuckDB accepts a blended VARARGS call
				// (row_sum(1,2,3) -> N input columns of varargs_type). Without this
				// the signature would carry only the named params and DuckDB would
				// reject any positional args.
				if (arg_types.has_varargs) {
					table_func.varargs = arg_types.varargs_type;
				}

				// Attach function info
				table_func.function_info = make_uniq<vgi::VgiTableFunctionInfo>(
				    catalog_, attach_params, attach_result->attach_opaque_data, func_info, setting_names);

				func_set.AddFunction(table_func);
			} else {
				// Create a regular table function
				// Only positional arguments go in the function signature
				TableFunction table_func(arg_types.positional_types, vgi::VgiTableFunctionScan,
				                         VgiCatalogTableFunctionBind, vgi::VgiTableFunctionInitGlobal,
				                         vgi::VgiTableFunctionInitLocal);
				table_func.projection_pushdown = func_info.projection_pushdown.value_or(false);
				table_func.filter_pushdown = func_info.filter_pushdown.value_or(false);
				table_func.sampling_pushdown = func_info.sampling_pushdown.value_or(false);
				table_func.order_preservation_type = MapOrderPreservation(func_info.order_preservation);
				if (!func_info.supported_expression_filters.empty()) {
					table_func.pushdown_expression = vgi::VgiPushdownExpression;
				}
				table_func.cardinality = vgi::VgiTableFunctionCardinality;
				table_func.statistics = vgi::VgiTableFunctionStatistics;
				table_func.table_scan_progress = vgi::VgiTableFunctionProgress;
				table_func.to_string = vgi::VgiTableFunctionToString;
				table_func.dynamic_to_string = vgi::VgiTableFunctionDynamicToString;
				table_func.set_scan_order = vgi::VgiSetScanOrder;
				// batch_index opt-in: install the partition-data callback so
				// ordered sinks (BatchCollector, BatchInsert,
				// BatchCopyToFile, Limit) can reassemble parallel output via
				// ``OperatorPartitionData{batch_index}``. The matching skip
				// of the ``fixed_order -> MaxThreads=1`` clamp lives in
				// VgiCatalogTableFunctionBind above. Each emitted Arrow
				// batch carries ``vgi_batch_index`` in KeyValueMetadata —
				// see ``InstallBatch`` for the parse + monotonicity check.
				if (func_info.supports_batch_index) {
					table_func.get_partition_data = vgi::VgiGetPartitionData;
				}
				// PartitionColumns opt-in: install the partition-info
				// callback so the planner can ask whether the source is
				// SINGLE_VALUE / OVERLAPPING / DISJOINT over the GROUP BY
				// columns. Also installs get_partition_data (idempotent if
				// supports_batch_index already did so) — PartitionColumns
				// mode populates the ``partition_data`` half of
				// OperatorPartitionData with per-column (min, max).
				if (func_info.partition_kind != vgi::VgiPartitionKind::NotPartitioned) {
					table_func.get_partition_info = vgi::VgiGetPartitionInfo;
					table_func.get_partition_data = vgi::VgiGetPartitionData;
				}
				// INITIALIZE_ON_SCHEDULE would move init_global into
				// `Executor::ScheduleEventsInternal`'s eager-init loop so the
				// pipelines' init_globals all fire from the same site at
				// scheduling time — combined with the async kickoff in
				// VgiTableFunctionInitGlobal, this collapses N sequential
				// metadata RTTs into one parallel batch (the Ducklake
				// `SELECT *` 25-table planner case).
				//
				// However, INITIALIZE_ON_SCHEDULE fires init_global *before*
				// any sibling build pipeline has populated
				// `PhysicalTableScan::dynamic_filters` (the hash join's
				// `JoinFilterPushdownInfo::PushInFilter` only runs at build
				// finalize). With the flag on, a VGI scan sitting on the
				// probe side of a hash join sees an empty
				// `op.dynamic_filters` at init and never receives the
				// build-side InFilter — breaking join key pushdown
				// (join_keys_pushdown.test). DuckDB's
				// `TableScanGlobalSourceState` only combines dynamic_filters
				// into the init_input when `HasFilters()` is already true,
				// so there is no in-DuckDB hook to refresh the captured
				// filter set later.
				//
				// Correctness wins over the Ducklake fan-out optimization.
				// Leaving the flag off means each `Pipeline::Schedule` does
				// kickoff + block sequentially, so 25-table metadata reads
				// pay sum(RTT) instead of max(RTT). The async kickoff
				// inside `VgiTableFunctionInitGlobal` still lets the
				// pre-init connection acquire run on a background thread.
				// TODO: a deferred-filter-capture refactor (split init
				// into "establish session" + "set filters") could restore
				// INITIALIZE_ON_SCHEDULE without sacrificing join pushdown.

				// Register named parameters so DuckDB knows how to handle them
				table_func.named_parameters = arg_types.named_parameters;

				// Register varargs support if the function accepts additional positional arguments
				if (arg_types.has_varargs) {
					table_func.varargs = arg_types.varargs_type;
				}

				// Attach VgiTableFunctionInfo so the bind function can access worker_path and function metadata
				table_func.function_info = make_uniq<vgi::VgiTableFunctionInfo>(
				    catalog_, attach_params, attach_result->attach_opaque_data, func_info, setting_names);

				func_set.AddFunction(table_func);
			}

			// Create function description with full metadata
			// Include both positional and named parameters so duckdb_functions() shows them correctly
			FunctionDescription desc;
			desc.parameter_types = arg_types.positional_types;
			desc.parameter_names = arg_types.positional_names;

			// Append named parameters to the description
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

		// Create function info and entry
		CreateTableFunctionInfo info(func_set);
		info.schema = schema_.name;
		info.internal = true;
		info.descriptions = std::move(descriptions);

		auto function_entry = make_uniq_base<StandardEntry, TableFunctionCatalogEntry>(
		    catalog_, schema_, info.Cast<CreateTableFunctionInfo>());

		// DuckDB's FunctionEntry constructor does not copy comment/tags from the
		// CreateInfo the way schema/table/view/macro entries do, so the worker's
		// tags must be set on the constructed entry directly — otherwise they
		// never reach duckdb_functions().tags. Tags are an entry-level concept
		// (not per-overload), so union them across overloads.
		for (const auto &func_info : pair.second) {
			for (const auto &[key, val] : func_info.tags) {
				function_entry->tags[key] = val;
			}
		}

		{ std::lock_guard<std::mutex> __entry_lk(entry_lock_); CreateEntryLocked(std::move(function_entry)); }
	}
}

} // namespace duckdb
