#include "vgi_transport.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {
namespace vgi {

TransportType DetectTransport(const std::string &worker_path) {
	if (IsHttpTransport(worker_path)) {
		return TransportType::HTTP;
	}
	return TransportType::SUBPROCESS;
}

bool IsHttpTransport(const std::string &worker_path) {
	auto lower = StringUtil::Lower(worker_path);
	return StringUtil::StartsWith(lower, "http://") || StringUtil::StartsWith(lower, "https://");
}

} // namespace vgi
} // namespace duckdb
