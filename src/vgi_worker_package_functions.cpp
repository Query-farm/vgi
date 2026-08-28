// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_worker_package_functions.hpp"

#include "duckdb/common/constants.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/table_macro_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"

#include "vgi_database_worker.hpp"
#include "vgi_function_docs.hpp"

namespace duckdb {
namespace vgi {

void RegisterVgiWorkerPackageMacro(ExtensionLoader &loader) {
	// Keep this as a macro over DuckDB primitives: read_blob participates in the
	// caller's transaction and works with local/remote filesystems already
	// supported by DuckDB. The aggregate guard makes globs deterministic: a
	// package is exactly one existing executable/archive, never an accidental set.
	const std::string definition = R"SQL(
WITH package_file AS MATERIALIZED (
    SELECT filename, content FROM read_blob(path)
), one_file AS (
    SELECT count(*) AS file_count, first(filename) AS filename, first(content) AS content
    FROM package_file
), normalized AS (
    SELECT file_count, filename, content,
           CASE
             WHEN lower(CAST(package_format AS VARCHAR)) <> 'auto' THEN lower(CAST(package_format AS VARCHAR))
             WHEN lower(filename) LIKE '%.tar.gz' THEN 'tar.gz'
             WHEN lower(filename) LIKE '%.tar.zst' THEN 'tar.zst'
             WHEN lower(filename) LIKE '%.zip' THEN 'zip'
             WHEN lower(filename) LIKE '%.tar' THEN 'tar'
             WHEN lower(filename) LIKE '%.gz' THEN 'gzip'
             WHEN lower(filename) LIKE '%.zst' THEN 'zstd'
             ELSE 'executable'
           END AS resolved_format
    FROM one_file
)
SELECT
    CAST(CASE WHEN worker_name IS NULL OR CAST(worker_name AS VARCHAR) = ''
              THEN error('vgi_worker_package: worker_name must be non-empty') ELSE worker_name END AS VARCHAR)
        AS worker_name,
    CAST(COALESCE(platform, (SELECT p.platform FROM pragma_platform() AS p)) AS VARCHAR) AS platform,
    CAST(CASE WHEN package_version IS NULL OR CAST(package_version AS VARCHAR) = ''
              THEN error('vgi_worker_package: package_version must be non-empty') ELSE package_version END AS VARCHAR)
        AS package_version,
    CAST(CASE WHEN resolved_format IN ('raw', 'binary') THEN 'executable'
              WHEN resolved_format = 'tgz' THEN 'tar.gz'
              WHEN resolved_format = 'gz' THEN 'gzip'
              WHEN resolved_format = 'zst' THEN 'zstd'
              WHEN resolved_format IN ('executable', 'zip', 'tar', 'tar.gz', 'tar.zst', 'gzip', 'zstd')
                THEN resolved_format
              ELSE error('vgi_worker_package: unsupported package_format') END AS VARCHAR) AS package_format,
    CAST(CASE
      WHEN file_count <> 1 THEN error('vgi_worker_package: path must match exactly one file')
      WHEN octet_length(content) = 0 THEN error('vgi_worker_package: package file must not be empty')
      WHEN resolved_format IN ('zip', 'tar', 'tar.gz', 'tar.zst', 'tgz')
        THEN CASE WHEN entrypoint IS NULL OR CAST(entrypoint AS VARCHAR) = ''
                  THEN error('vgi_worker_package: archive packages require entrypoint')
                  ELSE CAST(entrypoint AS VARCHAR) END
      WHEN entrypoint IS NOT NULL AND CAST(entrypoint AS VARCHAR) <> '' THEN CAST(entrypoint AS VARCHAR)
      WHEN resolved_format IN ('gzip', 'gz')
        THEN regexp_replace(parse_filename(filename, 'both_slash'), '\\.gz$', '', 'i')
      WHEN resolved_format IN ('zstd', 'zst')
        THEN regexp_replace(parse_filename(filename, 'both_slash'), '\\.zst$', '', 'i')
      ELSE parse_filename(filename, 'both_slash')
    END AS VARCHAR) AS entrypoint,
    CAST(content AS BLOB) AS contents,
    CAST(sha256(content) AS VARCHAR) AS sha256,
    current_timestamp AS created_at
FROM normalized
)SQL";

	Parser parser;
	parser.ParseQuery(definition);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw InternalException("vgi_worker_package macro definition did not parse as SELECT");
	}
	auto &select = parser.statements[0]->Cast<SelectStatement>();
	auto macro = make_uniq<TableMacroFunction>(std::move(select.node));
	for (const char *name : {"path", "worker_name", "package_version"}) {
		macro->parameters.push_back(make_uniq<ColumnRefExpression>(name));
	}
	for (const char *name : {"package_format", "entrypoint", "platform"}) {
		macro->parameters.push_back(make_uniq<ColumnRefExpression>(name));
	}
	macro->default_parameters["package_format"] = make_uniq<ConstantExpression>(Value("auto"));
	macro->default_parameters["entrypoint"] = make_uniq<ConstantExpression>(Value());
	macro->default_parameters["platform"] = make_uniq<ConstantExpression>(Value());

	CreateMacroInfo info(CatalogType::TABLE_MACRO_ENTRY);
	info.name = "vgi_worker_package";
	info.schema = DEFAULT_SCHEMA;
	info.internal = true;
	info.macros.push_back(std::move(macro));
	loader.RegisterFunction(info);
}

namespace {

struct WorkerCacheScanData : TableFunctionData {
	std::vector<WorkerCacheEntry> entries;
	idx_t offset = 0;
};

unique_ptr<FunctionData> WorkerCacheBind(ClientContext &context, TableFunctionBindInput &,
	                                      vector<LogicalType> &types, vector<string> &names) {
	types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	         LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::BIGINT,
	         LogicalType::BOOLEAN};
	names = {"artifact_id", "source", "digest", "package_format", "entrypoint", "directory",
	         "size_bytes", "age_seconds", "in_use"};
	auto data = make_uniq<WorkerCacheScanData>();
	data->entries = ListWorkerCache(context);
	return std::move(data);
}

void WorkerCacheScan(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.bind_data->CastNoConst<WorkerCacheScanData>();
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset++];
		output.SetValue(0, count, entry.artifact_id);
		output.SetValue(1, count, entry.source);
		output.SetValue(2, count, entry.digest);
		output.SetValue(3, count, entry.package_format);
		output.SetValue(4, count, entry.entrypoint);
		output.SetValue(5, count, entry.directory);
		output.SetValue(6, count, Value::UBIGINT(entry.size_bytes));
		output.SetValue(7, count, Value::BIGINT(entry.age_seconds));
		output.SetValue(8, count, Value::BOOLEAN(entry.in_use));
		count++;
	}
	output.SetCardinality(count);
}

struct WorkerCacheActionData : TableFunctionData {
	bool done = false;
	bool flush = false;
};

unique_ptr<FunctionData> WorkerCacheActionBind(bool flush, vector<LogicalType> &types, vector<string> &names) {
	types = {LogicalType::BIGINT, LogicalType::UBIGINT, LogicalType::BIGINT};
	names = {"removed", "bytes_removed", "skipped_in_use"};
	auto data = make_uniq<WorkerCacheActionData>();
	data->flush = flush;
	return std::move(data);
}

void WorkerCacheActionScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.bind_data->CastNoConst<WorkerCacheActionData>();
	if (data.done) return;
	auto result = PruneWorkerCache(context, data.flush);
	output.SetValue(0, 0, Value::BIGINT(result.removed));
	output.SetValue(1, 0, Value::UBIGINT(result.bytes_removed));
	output.SetValue(2, 0, Value::BIGINT(result.skipped_in_use));
	output.SetCardinality(1);
	data.done = true;
}

} // namespace

void RegisterVgiWorkerCacheFunctions(ExtensionLoader &loader) {
	{
		TableFunction function("vgi_worker_cache", {}, WorkerCacheScan, WorkerCacheBind);
		CreateTableFunctionInfo info(function);
		info.descriptions.push_back(MakeFunctionDescription(
		    "List immutable worker packages in VGI's local artifact cache.", {}, {},
		    {"SELECT * FROM vgi_worker_cache();"}));
		loader.RegisterFunction(std::move(info));
	}
	{
		TableFunction function("vgi_worker_cache_prune", {}, WorkerCacheActionScan,
		                       [](ClientContext &, TableFunctionBindInput &, vector<LogicalType> &types,
		                          vector<string> &names) { return WorkerCacheActionBind(false, types, names); });
		CreateTableFunctionInfo info(function);
		loader.RegisterFunction(std::move(info));
	}
	{
		TableFunction function("vgi_worker_cache_flush", {}, WorkerCacheActionScan,
		                       [](ClientContext &, TableFunctionBindInput &, vector<LogicalType> &types,
		                          vector<string> &names) { return WorkerCacheActionBind(true, types, names); });
		CreateTableFunctionInfo info(function);
		loader.RegisterFunction(std::move(info));
	}
}

} // namespace vgi
} // namespace duckdb
