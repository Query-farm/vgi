#include "storage/vgi_object_counts.hpp"

namespace duckdb {

VgiObjectCounts ObjectCountsFromMap(const std::map<std::string, int64_t> &m, int64_t default_value) {
	VgiObjectCounts counts;
	counts.table = default_value;
	counts.view = default_value;
	counts.index = default_value;
	counts.scalar_function = default_value;
	counts.aggregate_function = default_value;
	counts.table_function = default_value;
	counts.macro = default_value;
	for (auto &[k, v] : m) {
		if (k == "table") {
			counts.table = v;
		} else if (k == "view") {
			counts.view = v;
		} else if (k == "index") {
			counts.index = v;
		} else if (k == "scalar_function") {
			counts.scalar_function = v;
		} else if (k == "aggregate_function") {
			counts.aggregate_function = v;
		} else if (k == "table_function") {
			counts.table_function = v;
		} else if (k == "macro") {
			counts.macro = v;
		}
		// Unknown kinds are ignored — workers may emit forward-compatible keys
		// for object types this client doesn't yet model.
	}
	return counts;
}

int64_t ObjectCountFor(const VgiObjectCounts &counts, const std::string &kind) {
	if (kind == "table")
		return counts.table;
	if (kind == "view")
		return counts.view;
	if (kind == "index")
		return counts.index;
	if (kind == "scalar_function")
		return counts.scalar_function;
	if (kind == "aggregate_function")
		return counts.aggregate_function;
	if (kind == "table_function")
		return counts.table_function;
	if (kind == "macro")
		return counts.macro;
	return 1;
}

} // namespace duckdb
