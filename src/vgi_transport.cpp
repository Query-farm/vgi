// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
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

bool IsContainerLocation(const std::string &worker_path) {
	auto lower = StringUtil::Lower(worker_path);
	return StringUtil::StartsWith(lower, "oci://") || StringUtil::StartsWith(lower, "docker://");
}

bool IsContainerSharedLocation(const std::string &worker_path) {
	return StringUtil::StartsWith(worker_path, "container-shared:");
}

bool IsGithubLocation(const std::string &worker_path) {
	// Case-insensitive scheme match.  `github-auto://` does not start with
	// `github://` (position 6 is '-' vs ':'), so the two predicates are disjoint.
	auto lower = StringUtil::Lower(worker_path);
	return StringUtil::StartsWith(lower, "github://");
}

bool IsGithubAutoLocation(const std::string &worker_path) {
	auto lower = StringUtil::Lower(worker_path);
	return StringUtil::StartsWith(lower, "github-auto://");
}

bool IsTcpTransport(const std::string &worker_path) {
	auto lower = StringUtil::Lower(worker_path);
	return StringUtil::StartsWith(lower, "tcp://");
}

void ParseTcpLocation(const std::string &location, std::string &host, int &port) {
	if (!IsTcpTransport(location)) {
		throw std::invalid_argument("vgi: not a tcp:// location: " + location);
	}
	std::string rest = location.substr(6); // strip "tcp://"
	auto colon = rest.rfind(':');
	if (colon == std::string::npos || colon == 0 || colon + 1 >= rest.size()) {
		throw std::invalid_argument("vgi: tcp:// location must be tcp://host:port: " + location);
	}
	host = rest.substr(0, colon);
	try {
		port = std::stoi(rest.substr(colon + 1));
	} catch (...) {
		throw std::invalid_argument("vgi: tcp:// location has a non-numeric port: " + location);
	}
	if (port <= 0 || port > 65535) {
		throw std::invalid_argument("vgi: tcp:// port out of range: " + location);
	}
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
	if (IsContainerLocation(worker_path)) {
		return TransportType::CONTAINER;
	}
	if (IsTcpTransport(worker_path)) {
		return TransportType::TCP;
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

std::string StripContainerScheme(const std::string &location) {
	if (!IsContainerLocation(location)) {
		throw std::invalid_argument("StripContainerScheme: location is not an oci:// / docker:// URL: " +
		                             location);
	}
	// Case-insensitive scheme match, but preserve the original-case remainder
	// (image refs are case-sensitive on the path component for some registries).
	auto lower = StringUtil::Lower(location);
	size_t scheme_len = StringUtil::StartsWith(lower, "oci://") ? 6 : 9;  // "oci://" | "docker://"
	std::string rest = location.substr(scheme_len);
	// Drop the "#<hash>" pool-disambiguation suffix if present.
	auto hash_pos = rest.rfind('#');
	if (hash_pos != std::string::npos) {
		rest = rest.substr(0, hash_pos);
	}
	return rest;
}

std::string StripGithubScheme(const std::string &location) {
	if (!IsGithubLocation(location)) {
		throw std::invalid_argument("StripGithubScheme: location is not a github:// URL: " + location);
	}
	// "github://" is 9 chars.  Preserve the original-case remainder and keep any
	// "#sha256="/"#path=" fragment (the coordinate parser consumes it).
	return location.substr(9);
}

std::string StripGithubAutoScheme(const std::string &location) {
	if (!IsGithubAutoLocation(location)) {
		throw std::invalid_argument("StripGithubAutoScheme: location is not a github-auto:// URL: " + location);
	}
	// "github-auto://" is 14 chars.
	return location.substr(14);
}

} // namespace vgi
} // namespace duckdb
