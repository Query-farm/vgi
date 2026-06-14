// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_table_statistics_function.hpp"
#include "vgi_function_docs.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/geometry_stats.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// vgi_table_statistics(catalog, schema, table) — diagnostic table function
// Returns per-column statistics with UNION-typed min/max
// ============================================================================

struct VgiTableStatisticsData : public TableFunctionData {
	struct ColumnStat {
		string column_name;
		LogicalType column_type;
		unique_ptr<BaseStatistics> stats; // nullptr = no stats for this column
	};
	vector<ColumnStat> columns;
	LogicalType union_type;
	child_list_t<LogicalType> union_members; // kept for Value::UNION construction
	mutable idx_t current_idx = 0;
};

// Extract min Value from BaseStatistics (type-aware)
static Value ExtractMinValue(const BaseStatistics &stats) {
	switch (stats.GetStatsType()) {
	case StatisticsType::NUMERIC_STATS:
		if (NumericStats::HasMin(stats)) {
			return NumericStats::Min(stats);
		}
		break;
	case StatisticsType::STRING_STATS:
		return Value(StringStats::Min(stats));
	case StatisticsType::GEOMETRY_STATS: {
		auto &extent = GeometryStats::GetExtent(stats);
		return Value(StringUtil::Format("BOX(%g %g, %g %g)", extent.x_min, extent.y_min, extent.x_max, extent.y_max));
	}
	default:
		break;
	}
	return Value();
}

// Extract max Value from BaseStatistics (type-aware)
static Value ExtractMaxValue(const BaseStatistics &stats) {
	switch (stats.GetStatsType()) {
	case StatisticsType::NUMERIC_STATS:
		if (NumericStats::HasMax(stats)) {
			return NumericStats::Max(stats);
		}
		break;
	case StatisticsType::STRING_STATS:
		return Value(StringStats::Max(stats));
	case StatisticsType::GEOMETRY_STATS: {
		auto &extent = GeometryStats::GetExtent(stats);
		return Value(StringUtil::Format("BOX(%g %g, %g %g)", extent.x_min, extent.y_min, extent.x_max, extent.y_max));
	}
	default:
		break;
	}
	return Value();
}

static unique_ptr<FunctionData> VgiTableStatisticsBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
	auto catalog_name = input.inputs[0].GetValue<string>();
	auto schema_name = input.inputs[1].GetValue<string>();
	auto table_name = input.inputs[2].GetValue<string>();

	// Look up the table entry
	auto &catalog = Catalog::GetCatalog(context, catalog_name);
	auto table_entry = catalog.GetEntry<TableCatalogEntry>(context, schema_name, table_name,
	                                                        OnEntryNotFound::THROW_EXCEPTION);

	auto data = make_uniq<VgiTableStatisticsData>();

	// Collect statistics for each column and track distinct types for the UNION
	auto &columns = table_entry->GetColumns();
	std::map<string, uint8_t> type_tag_map; // type name → UNION tag

	for (idx_t i = 0; i < columns.PhysicalColumnCount(); i++) {
		auto &col = columns.GetColumn(PhysicalIndex(i));
		VgiTableStatisticsData::ColumnStat cs;
		cs.column_name = col.GetName();
		cs.column_type = col.GetType();
		cs.stats = table_entry->GetStatistics(context, i);

		// Track type for UNION construction.
		// Geometry stats are displayed as VARCHAR (bounding box text), so map
		// GEOMETRY → VARCHAR in the union to avoid type issues.
		auto display_type = col.GetType();
		if (display_type.id() == LogicalTypeId::GEOMETRY) {
			display_type = LogicalType::VARCHAR;
		}
		auto type_name = display_type.ToString();
		std::transform(type_name.begin(), type_name.end(), type_name.begin(), ::tolower);
		if (type_tag_map.find(type_name) == type_tag_map.end()) {
			auto tag = static_cast<uint8_t>(type_tag_map.size());
			type_tag_map[type_name] = tag;
			data->union_members.push_back({type_name, display_type});
		}

		data->columns.push_back(std::move(cs));
	}

	// Build the UNION type (or use a single VARCHAR if no columns)
	if (data->union_members.empty()) {
		data->union_members.push_back({"varchar", LogicalType::VARCHAR});
	}
	data->union_type = LogicalType::UNION(data->union_members);

	// Return schema
	return_types = {
	    LogicalType::VARCHAR,     // column_name
	    LogicalType::VARCHAR,     // column_type
	    data->union_type,         // min
	    data->union_type,         // max
	    LogicalType::BOOLEAN,     // has_null
	    LogicalType::BOOLEAN,     // has_not_null
	    LogicalType::BIGINT,      // distinct_count
	    LogicalType::BOOLEAN,     // contains_unicode
	    LogicalType::UBIGINT,     // max_string_length
	};
	names = {"column_name", "column_type", "min", "max", "has_null", "has_not_null", "distinct_count",
	         "contains_unicode", "max_string_length"};

	return data;
}

static void VgiTableStatisticsScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.bind_data->Cast<VgiTableStatisticsData>();

	idx_t count = 0;
	while (data.current_idx < data.columns.size() && count < STANDARD_VECTOR_SIZE) {
		auto &cs = data.columns[data.current_idx++];

		if (!cs.stats) {
			// No stats for this column — skip
			continue;
		}

		output.SetValue(0, count, Value(cs.column_name));
		output.SetValue(1, count, Value(cs.column_type.ToString()));

		// Build UNION values for min/max
		// For geometry, use VARCHAR display type; for others, use column type
		auto display_type = cs.column_type;
		if (display_type.id() == LogicalTypeId::GEOMETRY) {
			display_type = LogicalType::VARCHAR;
		}
		auto type_name = display_type.ToString();
		std::transform(type_name.begin(), type_name.end(), type_name.begin(), ::tolower);
		uint8_t tag = 0;
		for (idx_t t = 0; t < data.union_members.size(); t++) {
			if (data.union_members[t].first == type_name) {
				tag = static_cast<uint8_t>(t);
				break;
			}
		}

		Value min_val = ExtractMinValue(*cs.stats);
		Value max_val = ExtractMaxValue(*cs.stats);

		if (!min_val.IsNull()) {
			output.SetValue(2, count, Value::UNION(data.union_members, tag, min_val.DefaultCastAs(display_type)));
		} else {
			output.SetValue(2, count, Value());
		}

		if (!max_val.IsNull()) {
			output.SetValue(3, count, Value::UNION(data.union_members, tag, max_val.DefaultCastAs(display_type)));
		} else {
			output.SetValue(3, count, Value());
		}

		// Null flags
		output.SetValue(4, count, Value::BOOLEAN(cs.stats->CanHaveNull()));
		output.SetValue(5, count, Value::BOOLEAN(cs.stats->CanHaveNoNull()));

		// Distinct count
		auto dc = cs.stats->GetDistinctCount();
		output.SetValue(6, count, Value::BIGINT(static_cast<int64_t>(dc)));

		// String-specific fields: contains_unicode and max_string_length
		if (cs.stats->GetStatsType() == StatisticsType::STRING_STATS) {
			output.SetValue(7, count, Value::BOOLEAN(StringStats::CanContainUnicode(*cs.stats)));
			if (StringStats::HasMaxStringLength(*cs.stats)) {
				output.SetValue(8, count, Value::UBIGINT(StringStats::MaxStringLength(*cs.stats)));
			} else {
				output.SetValue(8, count, Value());
			}
		} else {
			output.SetValue(7, count, Value());
			output.SetValue(8, count, Value());
		}

		count++;
	}
	output.SetCardinality(count);
}

void RegisterVgiTableStatisticsFunction(ExtensionLoader &loader) {
	TableFunction func("vgi_table_statistics",
	                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                   VgiTableStatisticsScan, VgiTableStatisticsBind);
	CreateTableFunctionInfo info(func);
	info.descriptions.push_back(MakeFunctionDescription(
	    "Show the per-column statistics DuckDB holds for a VGI table, identified by catalog, schema, and "
	    "table name. One row per column, exposing the worker-supplied min/max, null counts, and distinct "
	    "estimates the optimizer uses to plan queries.",
	    {"catalog", "schema", "table"},
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	    {"SELECT * FROM vgi_table_statistics('mycatalog', 'main', 'events');"}));
	loader.RegisterFunction(std::move(info));
}

} // namespace vgi
} // namespace duckdb
