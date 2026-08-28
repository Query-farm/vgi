// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
// =============================================================================
// vgi_table_branches() — diagnostic table function. One row per
// (catalog, schema, table, branch_index) across every attached VGI catalog.
// Surfaces the multi-branch shape that VgiMultiScanRewriter consumes, so
// users can debug plan trees, branch_filter declarations, and worker
// metadata without reading EXPLAIN output.
//
// Single-branch tables surface as one row with branch_index=0; multi-branch
// tables surface as N rows with stable ordinal indices.
//
// Performance: this issues a fresh catalog_table_scan_branches_get RPC per
// table. Acceptable for a diagnostic; do not call from a hot path.
// =============================================================================

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "storage/vgi_catalog.hpp"
#include "storage/vgi_table_entry.hpp"
#include "vgi_function_docs.hpp"
#include "vgi_logging.hpp"

namespace duckdb {
namespace vgi {

namespace {

struct BranchesRow {
	std::string catalog_name;
	std::string schema_name;
	std::string table_name;
	int64_t branch_index;
	std::string branch_kind;  // "function" | "catalog_table" | "format" — see VgiScanBranch's discriminator
	std::string function_name;
	std::string positional_arguments_json;
	std::string named_arguments_json;
	std::string branch_filter;  // empty == NULL
	bool branch_filter_present;
	std::vector<std::string> table_required_extensions;
	bool writable;  // INSERT target declaration for multi-branch tables
	// Catalog-table branch fields (empty unless branch_kind == "catalog_table").
	std::string source_catalog;
	std::string source_schema;
	std::string source_table;
	// Format branch fields (empty unless branch_kind == "format").
	std::string format_name;
	std::vector<std::string> format_locations;
	std::string format_options_json;
};

struct VgiTableBranchesData : public TableFunctionData {
	std::vector<BranchesRow> rows;
	mutable idx_t current_idx = 0;
};

// Crude JSON-array encoder: emits a JSON array string with each scalar
// formatted via Value::ToString(). Strings get quoted; numbers / bools
// stay raw. Workable for diagnostics — not a full JSON serialiser.
static std::string EncodeValuesAsJsonArray(const duckdb::vector<Value> &values) {
	std::string out = "[";
	for (size_t i = 0; i < values.size(); i++) {
		if (i > 0) {
			out += ",";
		}
		const auto &v = values[i];
		if (v.IsNull()) {
			out += "null";
		} else if (v.type().IsNumeric() || v.type() == LogicalType::BOOLEAN) {
			out += v.ToString();
		} else {
			// String / timestamp / blob / anything else: quote it.
			std::string s = v.ToString();
			// Escape embedded quotes minimally.
			std::string escaped;
			escaped.reserve(s.size() + 2);
			escaped += '"';
			for (char c : s) {
				if (c == '"' || c == '\\') {
					escaped += '\\';
				}
				escaped += c;
			}
			escaped += '"';
			out += escaped;
		}
	}
	out += "]";
	return out;
}

static std::string EncodeNamedArgsAsJson(const std::map<std::string, Value> &named) {
	std::string out = "{";
	bool first = true;
	for (const auto &kv : named) {
		if (!first) {
			out += ",";
		}
		first = false;
		out += "\"" + kv.first + "\":";
		duckdb::vector<Value> single{kv.second};
		std::string single_arr = EncodeValuesAsJsonArray(single);
		// Strip the surrounding [ ] from the single-element array.
		out += single_arr.substr(1, single_arr.size() - 2);
	}
	out += "}";
	return out;
}

static unique_ptr<FunctionData> VgiTableBranchesBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	return_types = {
	    LogicalType::VARCHAR,                          // catalog_name
	    LogicalType::VARCHAR,                          // schema_name
	    LogicalType::VARCHAR,                          // table_name
	    LogicalType::BIGINT,                           // branch_index
	    LogicalType::VARCHAR,                          // branch_kind ("function" | "catalog_table" | "format")
	    LogicalType::VARCHAR,                          // function_name (NULL unless branch_kind = 'function')
	    LogicalType::JSON(),                           // positional_arguments
	    LogicalType::JSON(),                           // named_arguments
	    LogicalType::VARCHAR,                          // branch_filter (NULL when unset)
	    LogicalType::LIST(LogicalType::VARCHAR),       // table_required_extensions
	    LogicalType::BOOLEAN,                          // writable (INSERT target declaration for multi-branch)
	    LogicalType::VARCHAR,                          // source_catalog (NULL unless branch_kind = 'catalog_table')
	    LogicalType::VARCHAR,                          // source_schema (NULL unless branch_kind = 'catalog_table')
	    LogicalType::VARCHAR,                          // source_table (NULL unless branch_kind = 'catalog_table')
	    LogicalType::VARCHAR,                          // format_name (NULL unless branch_kind = 'format')
	    LogicalType::LIST(LogicalType::VARCHAR),       // format_locations
	    LogicalType::JSON(),                           // format_options
	};
	names = {"catalog_name",   "schema_name",       "table_name",  "branch_index",         "branch_kind",
	         "function_name",  "positional_arguments", "named_arguments", "branch_filter",  "table_required_extensions",
	         "writable",       "source_catalog",    "source_schema", "source_table",       "format_name",
	         "format_locations", "format_options"};

	auto data = make_uniq<VgiTableBranchesData>();

	// Walk every attached VGI catalog. For each, walk schemas → tables;
	// for each table, fetch its branches via the catalog API (one RPC per
	// table — acceptable for a diagnostic).
	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db : databases) {
		auto &catalog = db->GetCatalog();
		if (catalog.GetCatalogType() != "vgi") {
			continue;
		}
		auto &vgi_catalog = catalog.Cast<VgiCatalog>();
		const auto catalog_name = vgi_catalog.GetName();
		vgi_catalog.ScanSchemas(context, [&](SchemaCatalogEntry &schema) {
			const auto schema_name = schema.name;
			schema.Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
				if (entry.type != CatalogType::TABLE_ENTRY) {
					return;
				}
				auto *table_entry = dynamic_cast<VgiTableEntry *>(&entry);
				if (!table_entry) {
					return;  // Not a VGI table (defensive — shouldn't happen in a VGI catalog).
				}
				// Fetch branches with empty AT — diagnostic doesn't honour time travel
				// (no clean way to vary it through a 0-arg table function call).
				VgiScanBranchesResult result;
				try {
					result = table_entry->FetchScanBranches(context, /*at_unit=*/"", /*at_value=*/"");
				} catch (const std::exception &e) {
					// Skip tables whose branches RPC fails (e.g., transient
					// worker error, parse-time rejection like at-most-one-
					// writable). The diagnostic should not block on
					// individual table failures, but the skip is observable
					// via duckdb_logs so a hidden bad entry doesn't go
					// unnoticed.
					VGI_LOG(context, "vgi.table_branches.skip_failed_table",
					        {{"catalog", catalog_name},
					         {"schema", schema_name},
					         {"table", table_entry->name},
					         {"error", e.what()}});
					return;
				}
				for (size_t i = 0; i < result.branches.size(); i++) {
					const auto &branch = result.branches[i];
					BranchesRow row;
					row.catalog_name = catalog_name;
					row.schema_name = schema_name;
					row.table_name = table_entry->name;
					row.branch_index = static_cast<int64_t>(i);
					row.function_name = branch.function_name;
					row.positional_arguments_json = EncodeValuesAsJsonArray(branch.positional_arguments);
					row.named_arguments_json = EncodeNamedArgsAsJson(branch.named_arguments);
					row.branch_filter = branch.branch_filter;
					row.branch_filter_present = !branch.branch_filter.empty();
					row.table_required_extensions = result.required_extensions;
					row.writable = branch.writable;
					// The three branch kinds are mutually exclusive at parse time
					// (VgiScanBranch's own comment) — dispatch on the same
					// discriminators the rewriter uses so this diagnostic never
					// silently blanks out a catalog-table or format branch.
					if (branch.IsCatalogTable()) {
						row.branch_kind = "catalog_table";
						row.source_catalog = branch.source_catalog;
						row.source_schema = branch.source_schema;
						row.source_table = branch.source_table;
					} else if (branch.IsFormatBranch()) {
						row.branch_kind = "format";
						row.format_name = branch.format_name;
						row.format_locations = branch.format_locations;
						row.format_options_json = EncodeNamedArgsAsJson(branch.format_options);
					} else {
						row.branch_kind = "function";
					}
					data->rows.push_back(std::move(row));
				}
			});
		});
	}

	return data;
}

static void VgiTableBranchesScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.bind_data->Cast<VgiTableBranchesData>();
	idx_t count = 0;
	while (data.current_idx < data.rows.size() && count < STANDARD_VECTOR_SIZE) {
		const auto &row = data.rows[data.current_idx++];
		output.SetValue(0, count, Value(row.catalog_name));
		output.SetValue(1, count, Value(row.schema_name));
		output.SetValue(2, count, Value(row.table_name));
		output.SetValue(3, count, Value::BIGINT(row.branch_index));
		output.SetValue(4, count, Value(row.branch_kind));
		output.SetValue(5, count, row.function_name.empty() ? Value() : Value(row.function_name));
		// JSON columns: emit as VARCHAR; DuckDB's JSON type is a thin alias.
		output.SetValue(6, count, Value(row.positional_arguments_json));
		output.SetValue(7, count, Value(row.named_arguments_json));
		output.SetValue(8, count, row.branch_filter_present ? Value(row.branch_filter) : Value());
		// LIST(VARCHAR) for required_extensions.
		duckdb::vector<Value> ext_values;
		ext_values.reserve(row.table_required_extensions.size());
		for (const auto &ext : row.table_required_extensions) {
			ext_values.emplace_back(ext);
		}
		output.SetValue(9, count, Value::LIST(LogicalType::VARCHAR, std::move(ext_values)));
		output.SetValue(10, count, Value::BOOLEAN(row.writable));
		output.SetValue(11, count, row.source_catalog.empty() ? Value() : Value(row.source_catalog));
		output.SetValue(12, count, row.source_schema.empty() ? Value() : Value(row.source_schema));
		output.SetValue(13, count, row.source_table.empty() ? Value() : Value(row.source_table));
		output.SetValue(14, count, row.format_name.empty() ? Value() : Value(row.format_name));
		duckdb::vector<Value> format_loc_values;
		format_loc_values.reserve(row.format_locations.size());
		for (const auto &loc : row.format_locations) {
			format_loc_values.emplace_back(loc);
		}
		output.SetValue(15, count, Value::LIST(LogicalType::VARCHAR, std::move(format_loc_values)));
		output.SetValue(16, count, row.format_options_json.empty() ? Value() : Value(row.format_options_json));
		count++;
	}
	output.SetCardinality(count);
}

} // anonymous namespace

void RegisterVgiTableBranchesFunction(ExtensionLoader &loader) {
	TableFunction func("vgi_table_branches", {}, VgiTableBranchesScan, VgiTableBranchesBind);
	CreateTableFunctionInfo info(func);
	info.descriptions.push_back(MakeFunctionDescription(
	    "Inspect how each attached VGI table is composed from its underlying branches, one row per branch per "
	    "table across every attached VGI catalog. branch_kind discriminates the three mutually-exclusive shapes "
	    "a branch can take: 'function' (function_name, positional/named arguments — a worker table function), "
	    "'catalog_table' (source_catalog/source_schema/source_table — a companion-catalog base table, e.g. "
	    "DuckLake/Iceberg federation), or 'format' (format_name/format_locations/format_options — a COPY-style "
	    "format reader). Also shows branch_filter, required extensions, and writable (INSERT target "
	    "declaration) for every branch. Single-branch tables surface as one row with branch_index=0.",
	    {}, {}, {"SELECT * FROM vgi_table_branches();"}));
	loader.RegisterFunction(std::move(info));
}

} // namespace vgi
} // namespace duckdb
