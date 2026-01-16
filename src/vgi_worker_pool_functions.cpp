#include "vgi_worker_pool.hpp"

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// vgi_worker_pool_flush() - Scalar function to flush the worker pool
// ============================================================================

static void VgiWorkerPoolFlushFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = VgiWorkerPool::Instance().Flush();
	result.SetValue(0, Value::BIGINT(static_cast<int64_t>(count)));
}

void RegisterVgiWorkerPoolFlushFunction(ExtensionLoader &loader) {
	ScalarFunction func("vgi_worker_pool_flush", {}, LogicalType::BIGINT, VgiWorkerPoolFlushFunction);
	func.stability = FunctionStability::VOLATILE;
	loader.RegisterFunction(func);
}

// ============================================================================
// vgi_worker_pool() - Table function to show pool contents
// ============================================================================

struct VgiWorkerPoolData : public TableFunctionData {
	std::vector<VgiWorkerPool::PoolEntry> entries;
	mutable idx_t current_idx = 0;
};

static unique_ptr<FunctionData> VgiWorkerPoolBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	return_types = {LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::BIGINT};
	names = {"worker_path", "pid", "age_seconds"};

	auto data = make_uniq<VgiWorkerPoolData>();
	data->entries = VgiWorkerPool::Instance().GetPoolEntries();
	return data;
}

static void VgiWorkerPoolScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.bind_data->Cast<VgiWorkerPoolData>();

	idx_t count = 0;
	while (data.current_idx < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.current_idx++];
		output.SetValue(0, count, Value(entry.worker_path));
		output.SetValue(1, count, Value::INTEGER(static_cast<int32_t>(entry.pid)));
		output.SetValue(2, count, Value::BIGINT(entry.age_seconds));
		count++;
	}
	output.SetCardinality(count);
}

void RegisterVgiWorkerPoolFunction(ExtensionLoader &loader) {
	TableFunction func("vgi_worker_pool", {}, VgiWorkerPoolScan, VgiWorkerPoolBind);
	loader.RegisterFunction(func);
}

// ============================================================================
// vgi_worker_pool_stats() - Table function to show pool hit/miss statistics
// ============================================================================

struct VgiWorkerPoolStatsData : public TableFunctionData {
	std::vector<VgiWorkerPool::PoolStats> stats;
	mutable idx_t current_idx = 0;
};

static unique_ptr<FunctionData> VgiWorkerPoolStatsBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
	return_types = {LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::UBIGINT};
	names = {"worker_path", "hits", "misses"};

	auto data = make_uniq<VgiWorkerPoolStatsData>();
	data->stats = VgiWorkerPool::Instance().GetPoolStats();
	return data;
}

static void VgiWorkerPoolStatsScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.bind_data->Cast<VgiWorkerPoolStatsData>();

	idx_t count = 0;
	while (data.current_idx < data.stats.size() && count < STANDARD_VECTOR_SIZE) {
		auto &stat = data.stats[data.current_idx++];
		output.SetValue(0, count, Value(stat.worker_path));
		output.SetValue(1, count, Value::UBIGINT(stat.hits));
		output.SetValue(2, count, Value::UBIGINT(stat.misses));
		count++;
	}
	output.SetCardinality(count);
}

void RegisterVgiWorkerPoolStatsFunction(ExtensionLoader &loader) {
	TableFunction func("vgi_worker_pool_stats", {}, VgiWorkerPoolStatsScan, VgiWorkerPoolStatsBind);
	loader.RegisterFunction(func);
}

} // namespace vgi
} // namespace duckdb
