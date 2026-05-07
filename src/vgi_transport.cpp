#include "vgi_transport.hpp"

#include "duckdb/common/string_util.hpp"
#include "vgi_launcher_internal.hpp"

#include <stdexcept>

namespace duckdb {
namespace vgi {

bool IsHttpTransport(const std::string &worker_path) {
	auto lower = StringUtil::Lower(worker_path);
	return StringUtil::StartsWith(lower, "http://") || StringUtil::StartsWith(lower, "https://");
}

bool IsUnixLocation(const std::string &worker_path) {
	// Delegate to the launcher's pure helper so the scheme contract is
	// defined in exactly one place (and reused by the unit tests there).
	return launcher::IsUnixLocation(worker_path);
}

bool IsLaunchLocation(const std::string &worker_path) {
	return launcher::IsLaunchLocation(worker_path);
}

TransportType DetectTransport(const std::string &worker_path) {
	if (IsHttpTransport(worker_path)) {
		return TransportType::HTTP;
	}
	if (IsLaunchLocation(worker_path)) {
		return TransportType::LAUNCH;
	}
	if (IsUnixLocation(worker_path)) {
		return TransportType::UNIX;
	}
	return TransportType::SUBPROCESS;
}

std::string StripUnixScheme(const std::string &location) {
	const std::string prefix = "unix://";
	if (!IsUnixLocation(location)) {
		throw std::invalid_argument("StripUnixScheme: location is not a unix:// URL: " + location);
	}
	return location.substr(prefix.size());
}

std::string StripLaunchScheme(const std::string &location) {
	const std::string prefix = "launch:";
	if (!IsLaunchLocation(location)) {
		throw std::invalid_argument("StripLaunchScheme: location is not a launch: URL: " +
		                             location);
	}
	return location.substr(prefix.size());
}

} // namespace vgi
} // namespace duckdb
