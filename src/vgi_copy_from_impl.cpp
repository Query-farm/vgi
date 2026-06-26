// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_copy_from_impl.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"

#include "vgi_arrow_utils.hpp"
#include "vgi_attach_parameters.hpp"
#include "vgi_rpc_types.hpp"
#include "vgi_table_function_impl.hpp"

namespace duckdb {
namespace vgi {

TableFunction MakeVgiCopyFromTableFunction() {
	// Empty argument list — the copy_from_function is never SQL-callable directly;
	// its bind data is produced by VgiCopyFromBind. Reuse the producer-mode scan
	// + init callbacks verbatim so there is no new streaming code.
	TableFunction func("vgi_copy_from", {}, vgi::VgiTableFunctionScan, /*bind=*/nullptr,
	                   vgi::VgiTableFunctionInitGlobal, vgi::VgiTableFunctionInitLocal);
	// Mirror the diagnostic hooks the direct vgi_table_function() path installs so
	// EXPLAIN / EXPLAIN ANALYZE / progress annotate the COPY scan.
	func.cardinality = vgi::VgiTableFunctionCardinality;
	func.table_scan_progress = vgi::VgiTableFunctionProgress;
	func.to_string = vgi::VgiTableFunctionToString;
	func.dynamic_to_string = vgi::VgiTableFunctionDynamicToString;
	func.statistics = vgi::VgiTableFunctionStatistics;
	// COPY FROM always projects all target columns (identity), so no projection
	// pushdown is needed; leave projection_pushdown at its default (false).
	return func;
}

// Resolve the format's option schema into a case-insensitive name -> (orig_name,
// LogicalType) map for validation + coercion.
namespace {

struct OptionSpec {
	std::string name;
	LogicalType type;
};

std::map<std::string, OptionSpec> ResolveOptionSpecs(ClientContext &context,
                                                      const std::shared_ptr<arrow::Schema> &options_schema) {
	std::map<std::string, OptionSpec> specs;
	if (!options_schema || options_schema->num_fields() == 0) {
		return specs;
	}
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
	vector<LogicalType> types;
	vector<string> names;
	ArrowSchemaToDuckDBTypes(context, options_schema, c_schema, arrow_table, types, names);
	for (idx_t i = 0; i < names.size(); i++) {
		specs[StringUtil::Lower(names[i])] = OptionSpec {names[i], types[i]};
	}
	return specs;
}

} // namespace

unique_ptr<FunctionData> VgiCopyFromBind(ClientContext &context, CopyFromFunctionBindInput &info,
                                         vector<string> &expected_names, vector<LogicalType> &expected_types) {
	auto &table_info = info.tf.function_info;
	if (!table_info) {
		throw InternalException("VgiCopyFromBind: copy_from_function has no function_info carrier");
	}
	auto &carrier = table_info->Cast<VgiCopyFromFunctionInfo>();
	const auto &copy_info = info.info;

	// --- 1. Validate + coerce COPY options against the format's option schema ---
	// Worker-side validation (required / choices / ranges) still runs when the
	// worker parses the arguments; here we reject unknown options and coerce each
	// value to the declared option type. FORMAT itself arrives on copy_info.format,
	// not in the options map.
	auto specs = ResolveOptionSpecs(context, carrier.options_schema);
	vector<std::pair<string, Value>> named_args;
	for (auto &entry : copy_info.options) {
		const auto &key = entry.first;
		const auto &values = entry.second;
		auto key_lower = StringUtil::Lower(key);
		if (key_lower == "format") {
			continue; // FORMAT is carried separately, never a worker option
		}
		auto spec_it = specs.find(key_lower);
		if (spec_it == specs.end()) {
			throw BinderException("Unsupported option '%s' for COPY ... FROM (FORMAT %s)", key,
			                      carrier.format_name);
		}
		// COPY options are a list of Values; scalar options carry exactly one.
		Value raw;
		if (values.empty()) {
			raw = Value(LogicalType::BOOLEAN); // valueless flag option -> NULL placeholder
		} else if (values.size() == 1) {
			raw = values[0];
		} else {
			raw = Value::LIST(values);
		}
		Value coerced;
		string cast_error;
		if (!raw.DefaultTryCastAs(spec_it->second.type, coerced, &cast_error)) {
			throw BinderException("COPY ... FROM (FORMAT %s): option '%s' value %s is not coercible to %s%s%s",
			                      carrier.format_name, spec_it->second.name, raw.ToString(),
			                      spec_it->second.type.ToString(), cast_error.empty() ? "" : ": ", cast_error);
		}
		named_args.emplace_back(spec_it->second.name, std::move(coerced));
	}

	// --- 2. Build bind data targeting the worker handler ---
	auto bind_data = make_uniq<VgiTableFunctionBindData>();
	bind_data->attach_params = carrier.attach_params;
	bind_data->attach_opaque_data = carrier.attach_opaque_data;
	// COPY runs outside a catalog read transaction; the direct vgi_table_function()
	// path also binds with no transaction_opaque_data.
	bind_data->function_name = carrier.handler;
	bind_data->arguments = vgi::BuildArgumentsFromValues(context, /*positional=*/{}, named_args);
	bind_data->settings = ExtractVgiSettings(context, carrier.setting_names);

	// --- 3. Attach the COPY FROM context (format + path + target schema) ---
	auto expected_schema = BuildArrowSchemaFromDuckDB(context, expected_types, expected_names);
	CopyFromBindContext copy_ctx;
	copy_ctx.format = carrier.format_name;
	copy_ctx.file_path = copy_info.file_path;
	copy_ctx.expected_schema_bytes = SerializeSchemaToIpcBytes(expected_schema);
	bind_data->copy_from = std::move(copy_ctx);

	// --- 4. Bind against the worker (discovers + validates the output schema) ---
	vector<LogicalType> worker_types;
	vector<string> worker_names;
	vgi::PerformVgiTableFunctionBind(context, *bind_data, worker_types, worker_names);

	// --- 5. Hard-validate: DuckDB inserts NO cast between the scan and the INSERT,
	// so the worker's output types must match the COPY target exactly. ---
	if (worker_types.size() != expected_types.size()) {
		throw BinderException(
		    "COPY ... FROM (FORMAT %s): worker '%s' produces %llu columns but the COPY target expects %llu",
		    carrier.format_name, carrier.handler, (unsigned long long)worker_types.size(),
		    (unsigned long long)expected_types.size());
	}
	for (idx_t i = 0; i < expected_types.size(); i++) {
		if (worker_types[i] != expected_types[i]) {
			throw BinderException(
			    "COPY ... FROM (FORMAT %s): column %llu type mismatch — worker '%s' produces %s but the COPY "
			    "target column '%s' is %s (DuckDB inserts no cast for COPY FROM)",
			    carrier.format_name, (unsigned long long)i, carrier.handler, worker_types[i].ToString(),
			    i < expected_names.size() ? expected_names[i] : std::to_string(i), expected_types[i].ToString());
		}
	}

	// Mirror table-function configuration onto the bound copy_from_function, as
	// the direct path does (vgi_table_function.cpp).
	info.tf.projection_pushdown = bind_data->projection_pushdown;

	return std::move(bind_data);
}

} // namespace vgi
} // namespace duckdb
