// © Copyright 2026 Query Farm LLC - https://query.farm
#include "vgi_http_request_stats.hpp"

#include <algorithm>
#include <map>
#include <mutex>

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "vgi_function_docs.hpp"
#include "vgi_exception.hpp" // RedactCredentials

namespace duckdb {
namespace vgi {

namespace {

std::mutex &StatsMutex() {
	static std::mutex mu;
	return mu;
}

std::map<std::string, HttpRequestStats> &StatsMap() {
	static std::map<std::string, HttpRequestStats> stats;
	return stats;
}

// Callers hold StatsMutex(). Keyed without trailing slashes, so the unary and
// stream paths (which spell the base URL differently) share one row.
HttpRequestStats &Entry(const std::string &base_url) {
	auto key = base_url;
	while (!key.empty() && key.back() == '/') {
		key.pop_back();
	}
	auto &entry = StatsMap()[key];
	if (entry.worker_url.empty()) {
		entry.worker_url = key;
	}
	return entry;
}

} // namespace

void RecordHttpRequestBody(const std::string &base_url, uint64_t body_bytes) {
	std::lock_guard<std::mutex> lock(StatsMutex());
	auto &entry = Entry(base_url);
	entry.requests++;
	entry.total_request_bytes += body_bytes;
	entry.max_request_bytes = std::max(entry.max_request_bytes, body_bytes);
}

void RecordHttpRequestUpload(const std::string &base_url, uint64_t body_bytes) {
	std::lock_guard<std::mutex> lock(StatsMutex());
	auto &entry = Entry(base_url);
	entry.request_uploads++;
	entry.request_upload_bytes += body_bytes;
}

void RecordArgumentsRefUpload(const std::string &base_url, uint64_t argument_bytes) {
	std::lock_guard<std::mutex> lock(StatsMutex());
	auto &entry = Entry(base_url);
	entry.arguments_ref_uploads++;
	entry.arguments_ref_upload_bytes += argument_bytes;
}

void RecordArgumentsRefReuse(const std::string &base_url) {
	std::lock_guard<std::mutex> lock(StatsMutex());
	Entry(base_url).arguments_ref_reuses++;
}

std::vector<HttpRequestStats> SnapshotHttpRequestStats(bool reset) {
	std::lock_guard<std::mutex> lock(StatsMutex());
	std::vector<HttpRequestStats> out;
	out.reserve(StatsMap().size());
	for (const auto &entry : StatsMap()) {
		out.push_back(entry.second);
	}
	if (reset) {
		StatsMap().clear();
	}
	return out;
}

// ---- vgi_http_request_stats([reset := false]) ----------------------------

namespace {

struct VgiHttpRequestStatsData : public TableFunctionData {
	std::vector<HttpRequestStats> rows;
	idx_t offset = 0;
};

unique_ptr<FunctionData> VgiHttpRequestStatsBind(ClientContext &, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<Identifier> &names) {
	names = {"worker_url",         "requests",             "max_request_bytes",          "total_request_bytes",
	         "request_uploads",    "request_upload_bytes", "arguments_ref_uploads",      "arguments_ref_upload_bytes",
	         "arguments_ref_reuses"};
	return_types = {LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::UBIGINT,
	                LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT,
	                LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT};
	bool reset = false;
	auto it = input.named_parameters.find("reset");
	if (it != input.named_parameters.end() && !it->second.IsNull()) {
		reset = it->second.GetValue<bool>();
	}
	// Taken at bind, so a reset happens exactly once per call.
	auto data = make_uniq<VgiHttpRequestStatsData>();
	data->rows = SnapshotHttpRequestStats(reset);
	return std::move(data);
}

void VgiHttpRequestStatsScan(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.bind_data->CastNoConst<VgiHttpRequestStatsData>();
	idx_t count = 0;
	while (data.offset < data.rows.size() && count < STANDARD_VECTOR_SIZE) {
		const auto &row = data.rows[data.offset++];
		idx_t col = 0;
		output.SetValue(col++, count, Value(RedactCredentials(row.worker_url)));
		output.SetValue(col++, count, Value::UBIGINT(row.requests));
		output.SetValue(col++, count, Value::UBIGINT(row.max_request_bytes));
		output.SetValue(col++, count, Value::UBIGINT(row.total_request_bytes));
		output.SetValue(col++, count, Value::UBIGINT(row.request_uploads));
		output.SetValue(col++, count, Value::UBIGINT(row.request_upload_bytes));
		output.SetValue(col++, count, Value::UBIGINT(row.arguments_ref_uploads));
		output.SetValue(col++, count, Value::UBIGINT(row.arguments_ref_upload_bytes));
		output.SetValue(col++, count, Value::UBIGINT(row.arguments_ref_reuses));
		count++;
	}
	output.SetCardinality(count);
}

} // namespace

void RegisterVgiHttpRequestStatsFunction(ExtensionLoader &loader) {
	TableFunction func("vgi_http_request_stats", {}, VgiHttpRequestStatsScan, VgiHttpRequestStatsBind);
	AddVgiFunctionOption(func, Identifier("reset"), LogicalType::BOOLEAN);
	CreateTableFunctionInfo info(func);
	info.descriptions.push_back(MakeFunctionDescription(
	    "Process-wide counters of the request bodies sent to each HTTP worker URL: request count, largest and "
	    "total body bytes (uncompressed Arrow IPC, after any upload replaced a body with a pointer), whole-request "
	    "uploads, and argument-reference uploads and reuses. `reset => true` clears the counters after reading "
	    "them.",
	    {"reset"}, {LogicalType::BOOLEAN}, {"SELECT * FROM vgi_http_request_stats();"}));
	loader.RegisterFunction(std::move(info));
}

} // namespace vgi
} // namespace duckdb
