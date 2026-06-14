// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_worker_pool.hpp"

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "vgi_function_docs.hpp"

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
	CreateScalarFunctionInfo info(func);
	info.descriptions.push_back(MakeFunctionDescription(
	    "Clear all subprocess-pooled VGI workers, returning the count of workers flushed. Has no effect on "
	    "launch:/unix:// workers, which are pooled by the OS-level AF_UNIX socket rather than this process.",
	    {}, {}, {"SELECT vgi_worker_pool_flush();"}));
	loader.RegisterFunction(std::move(info));
}

// ============================================================================
// vgi_worker_subprocess_pool() - Table function to show pool contents
// ============================================================================

struct VgiWorkerPoolData : public TableFunctionData {
	std::vector<VgiWorkerPool::PoolEntry> entries;
	mutable idx_t current_idx = 0;
};

static unique_ptr<FunctionData> VgiWorkerPoolBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::BIGINT, LogicalType::BIGINT};
	names = {"worker_path", "data_version_spec", "implementation_version", "pid", "age_seconds"};

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
		output.SetValue(1, count, Value(entry.data_version_spec));
		output.SetValue(2, count, Value(entry.implementation_version));
		output.SetValue(3, count, Value::BIGINT(static_cast<int64_t>(entry.pid)));
		output.SetValue(4, count, Value::BIGINT(entry.age_seconds));
		count++;
	}
	output.SetCardinality(count);
}

void RegisterVgiWorkerPoolFunction(ExtensionLoader &loader) {
	TableFunction func("vgi_worker_subprocess_pool", {}, VgiWorkerPoolScan, VgiWorkerPoolBind);
	CreateTableFunctionInfo info(func);
	info.descriptions.push_back(MakeFunctionDescription(
	    "Diagnostic: list the subprocess-pooled VGI workers (worker_path, data_version_spec, "
	    "implementation_version, pid, age_seconds). Returns no rows for launch:/unix:// transports, whose "
	    "workers are pooled by the OS-level AF_UNIX socket rather than this DuckDB process.",
	    {}, {}, {"SELECT * FROM vgi_worker_subprocess_pool();"}));
	loader.RegisterFunction(std::move(info));
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
	CreateTableFunctionInfo info(func);
	info.descriptions.push_back(MakeFunctionDescription(
	    "Diagnostic: subprocess worker-pool hit/miss statistics by worker_path (worker_path, hits, misses). "
	    "Subprocess transport only.",
	    {}, {}, {"SELECT * FROM vgi_worker_pool_stats();"}));
	loader.RegisterFunction(std::move(info));
}

} // namespace vgi
} // namespace duckdb
