// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_database_worker.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/keyword_helper.hpp"

#include "vgi_platform.hpp"
#include "vgi_sha256.hpp"
#include "vgi_worker_archive.hpp"

#include <atomic>
#include <algorithm>
#include <chrono>
#include <cctype>
#include <cerrno>
#include <cstdlib>
#include <ctime>
#include <cstring>
#include <map>
#include <mutex>
#include <thread>
#include <unordered_map>

#if VGI_POSIX_TRANSPORT
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#elif defined(_WIN32)
#include "duckdb/common/windows_util.hpp"
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#endif

namespace duckdb {
namespace vgi {

namespace {

constexpr uint64_t DEFAULT_MAX_PACKAGE_BYTES = 512ULL * 1024ULL * 1024ULL;
constexpr uint64_t DEFAULT_MAX_EXTRACTED_BYTES = 1024ULL * 1024ULL * 1024ULL;
constexpr uint64_t DEFAULT_MAX_PACKAGE_FILES = 10000;
constexpr uint64_t DEFAULT_CACHE_MAX_BYTES = 5ULL * 1024ULL * 1024ULL * 1024ULL;
constexpr int64_t DEFAULT_CACHE_TTL_SECONDS = 30LL * 24LL * 60LL * 60LL;

FileSystem &LocalFs() {
	static unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	return *fs;
}

std::string PercentDecode(const std::string &input, const char *component) {
	std::string out;
	out.reserve(input.size());
	for (size_t i = 0; i < input.size(); i++) {
		if (input[i] != '%') {
			out.push_back(input[i]);
			continue;
		}
		if (i + 2 >= input.size() || !std::isxdigit(static_cast<unsigned char>(input[i + 1])) ||
		    !std::isxdigit(static_cast<unsigned char>(input[i + 2]))) {
			throw InvalidInputException("vgi: malformed percent escape in database LOCATION %s", component);
		}
		auto hex = [](char c) -> unsigned char {
			if (c >= '0' && c <= '9') return static_cast<unsigned char>(c - '0');
			if (c >= 'a' && c <= 'f') return static_cast<unsigned char>(c - 'a' + 10);
			return static_cast<unsigned char>(c - 'A' + 10);
		};
		char decoded = static_cast<char>((hex(input[i + 1]) << 4) | hex(input[i + 2]));
		if (decoded == '\0') {
			throw InvalidInputException("vgi: NUL is not allowed in database LOCATION %s", component);
		}
		out.push_back(decoded);
		i += 2;
	}
	return out;
}

bool IsHexDigest(const std::string &value) {
	return value.size() == 64 &&
	       value.find_first_not_of("0123456789abcdefABCDEF") == std::string::npos;
}

std::string CacheDir(ClientContext &context) {
	Value value;
	if (context.TryGetCurrentSetting("vgi_worker_cache_dir", value) && !value.IsNull()) {
		auto configured = value.ToString();
		if (!configured.empty()) {
			return LocalFs().ExpandPath(configured);
		}
	}
	if (context.TryGetCurrentSetting("vgi_github_cache_dir", value) && !value.IsNull()) {
		auto configured = value.ToString();
		if (!configured.empty()) {
			return LocalFs().ExpandPath(configured);
		}
	}
	const char *xdg = std::getenv("XDG_CACHE_HOME");
	if (xdg && *xdg) {
		return LocalFs().ExpandPath(std::string(xdg) + "/vgi/releases");
	}
	auto home = LocalFs().GetHomeDirectory();
	if (!home.empty()) {
		return home + "/.cache/vgi/releases";
	}
#if defined(_WIN32)
	return LocalFs().GetWorkingDirectory() + "/.vgi/releases";
#else
	return "/tmp/vgi-releases";
#endif
}

uint64_t GetUnsignedSetting(ClientContext &context, const char *name, uint64_t default_value) {
	Value value;
	if (!context.TryGetCurrentSetting(name, value) || value.IsNull()) {
		return default_value;
	}
	return value.DefaultCastAs(LogicalType::UBIGINT).GetValue<uint64_t>();
}

void MkdirP(const std::string &path) {
	if (path.empty() || LocalFs().DirectoryExists(path)) {
		return;
	}
	auto slash = path.find_last_of("/\\");
	if (slash != std::string::npos && slash > 0) {
		MkdirP(path.substr(0, slash));
	}
	if (!LocalFs().DirectoryExists(path)) {
		LocalFs().CreateDirectory(path);
	}
#if VGI_POSIX_TRANSPORT
	(void)::chmod(path.c_str(), 0700);
#endif
}

void RmRf(const std::string &path) noexcept {
	try {
		if (LocalFs().DirectoryExists(path)) {
			LocalFs().RemoveDirectory(path);
		} else if (LocalFs().FileExists(path)) {
			LocalFs().RemoveFile(path);
		}
	} catch (...) {
	}
}

void WritePrivateFile(const std::string &path, const std::string &contents) {
	auto flags = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW;
	auto handle = LocalFs().OpenFile(path, flags);
	if (!contents.empty()) {
		handle->Write(QueryContext(), data_ptr_cast(const_cast<char *>(contents.data())), contents.size(), 0);
	}
#if VGI_POSIX_TRANSPORT
	(void)::chmod(path.c_str(), 0600);
#endif
}

std::string ReadFile(const std::string &path) {
	if (!LocalFs().FileExists(path)) {
		return {};
	}
	auto handle = LocalFs().OpenFile(path, FileFlags::FILE_FLAGS_READ);
	auto size = handle->GetFileSize();
	std::string result(size, '\0');
	if (size) {
		handle->Read(QueryContext(), data_ptr_cast(result.data()), size, 0);
	}
	return result;
}

std::map<std::string, std::string> ParseMeta(const std::string &contents) {
	std::map<std::string, std::string> out;
	size_t pos = 0;
	while (pos < contents.size()) {
		auto end = contents.find('\n', pos);
		auto line = contents.substr(pos, end == std::string::npos ? std::string::npos : end - pos);
		auto eq = line.find('=');
		if (eq != std::string::npos) {
			out[line.substr(0, eq)] = line.substr(eq + 1);
		}
		pos = end == std::string::npos ? contents.size() : end + 1;
	}
	return out;
}

std::string CleanMeta(std::string value) {
	value.erase(std::remove(value.begin(), value.end(), '\n'), value.end());
	value.erase(std::remove(value.begin(), value.end(), '\r'), value.end());
	return value;
}

class ArtifactLease {
public:
	explicit ArtifactLease(std::string path_p) : path(std::move(path_p)) {
	}
	~ArtifactLease() {
#if VGI_POSIX_TRANSPORT
		if (fd >= 0) {
			(void)::flock(fd, LOCK_UN);
			::close(fd);
		}
#elif defined(_WIN32)
		if (handle != INVALID_HANDLE_VALUE) {
			OVERLAPPED ov {};
			(void)::UnlockFileEx(handle, 0, MAXDWORD, MAXDWORD, &ov);
			::CloseHandle(handle);
		}
#endif
	}

	static std::shared_ptr<ArtifactLease> Acquire(const std::string &path, bool exclusive, bool nonblocking) {
		auto lease = std::shared_ptr<ArtifactLease>(new ArtifactLease(path));
#if VGI_POSIX_TRANSPORT
		lease->fd = ::open(path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0600);
		if (lease->fd < 0) {
			throw IOException("vgi: failed to open worker cache lease %s: %s", path, std::strerror(errno));
		}
		int op = exclusive ? LOCK_EX : LOCK_SH;
		if (nonblocking) op |= LOCK_NB;
		while (::flock(lease->fd, op) != 0) {
			if (nonblocking && (errno == EWOULDBLOCK || errno == EAGAIN)) {
				return nullptr;
			}
			if (errno == EINTR) continue;
			throw IOException("vgi: failed to lock worker cache lease %s: %s", path, std::strerror(errno));
		}
#elif defined(_WIN32)
		auto wide_path = WindowsUtil::UTF8ToUnicode(path.c_str());
		lease->handle = ::CreateFileW(wide_path.c_str(), GENERIC_READ | GENERIC_WRITE,
		                              FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr,
		                              OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
		if (lease->handle == INVALID_HANDLE_VALUE) {
			throw IOException("vgi: failed to open worker cache lease %s", path);
		}
		OVERLAPPED ov {};
		DWORD flags = exclusive ? LOCKFILE_EXCLUSIVE_LOCK : 0;
		if (nonblocking) flags |= LOCKFILE_FAIL_IMMEDIATELY;
		if (!::LockFileEx(lease->handle, flags, 0, MAXDWORD, MAXDWORD, &ov)) {
			if (nonblocking && (GetLastError() == ERROR_LOCK_VIOLATION || GetLastError() == ERROR_IO_PENDING)) {
				return nullptr;
			}
			throw IOException("vgi: failed to lock worker cache lease %s", path);
		}
#else
		(void)exclusive;
		(void)nonblocking;
		throw NotImplementedException("vgi: database worker cache requires a native filesystem");
#endif
		return lease;
	}

private:
	std::string path;
#if VGI_POSIX_TRANSPORT
	int fd = -1;
#elif defined(_WIN32)
	HANDLE handle = INVALID_HANDLE_VALUE;
#endif
};

struct RegisteredArtifact {
	std::string entrypoint;
	std::string lock_path;
	std::weak_ptr<ArtifactLease> lease;
};

std::mutex registry_mutex;
std::unordered_map<std::string, RegisteredArtifact> registry;
std::atomic<uint64_t> staging_counter {0};

uint64_t DirectorySize(const std::string &path) {
	uint64_t total = 0;
	if (!LocalFs().DirectoryExists(path)) return 0;
	LocalFs().ListFiles(path, [&](const std::string &name, bool is_dir) {
		auto child = path + "/" + name;
		if (is_dir) {
			total += DirectorySize(child);
		} else {
			try {
				auto handle = LocalFs().OpenFile(child, FileFlags::FILE_FLAGS_READ);
				total += static_cast<uint64_t>(handle->GetFileSize());
			} catch (...) {
			}
		}
	});
	return total;
}

std::string ManifestBody(const std::string &source, const std::string &artifact_id, const std::string &digest,
	                     const std::string &format, const std::string &entrypoint, uint64_t size_bytes) {
	auto now = static_cast<int64_t>(std::time(nullptr));
	return "manifest_version=1\nartifact_id=" + CleanMeta(artifact_id) + "\nsource=" + CleanMeta(source) +
	       "\ndigest=" + CleanMeta(digest) + "\npackage_format=" + CleanMeta(format) +
	       "\nentrypoint=" + CleanMeta(entrypoint) + "\nsize_bytes=" + std::to_string(size_bytes) +
	       "\ninstalled_at=" + std::to_string(now) + "\nlast_used=" + std::to_string(now) + "\n";
}

bool ManifestMatches(const std::string &manifest_path, const std::string &artifact_id,
                     const std::string &digest, const std::string &format,
                     const std::string &entrypoint) {
	if (!LocalFs().FileExists(manifest_path)) {
		return false;
	}
	auto meta = ParseMeta(ReadFile(manifest_path));
	return meta["manifest_version"] == "1" && meta["artifact_id"] == artifact_id &&
	       meta["digest"] == digest && meta["package_format"] == format &&
	       meta["entrypoint"] == entrypoint;
}

std::string CanonicalCacheRoot(const std::string &cache) {
#if VGI_POSIX_TRANSPORT
	auto resolved = ::realpath(cache.c_str(), nullptr);
	if (!resolved) {
		throw IOException("vgi: failed to canonicalize worker cache directory %s: %s", cache,
		                  std::strerror(errno));
	}
	std::string result(resolved);
	std::free(resolved);
	return result;
#elif defined(_WIN32)
	auto wide_cache = WindowsUtil::UTF8ToUnicode(cache.c_str());
	DWORD required = ::GetFullPathNameW(wide_cache.c_str(), 0, nullptr, nullptr);
	if (required == 0) {
		throw IOException("vgi: failed to canonicalize worker cache directory %s", cache);
	}
	std::vector<wchar_t> buffer(static_cast<size_t>(required));
	if (::GetFullPathNameW(wide_cache.c_str(), required, buffer.data(), nullptr) == 0) {
		throw IOException("vgi: failed to canonicalize worker cache directory %s", cache);
	}
	return WindowsUtil::UnicodeToUTF8(buffer.data());
#else
	return cache;
#endif
}

void TouchLastUsed(const std::string &manifest_path) noexcept {
	std::string tmp;
	try {
		auto body = ReadFile(manifest_path);
		const std::string key = "last_used=";
		auto start = body.find(key);
		if (start == std::string::npos) {
			return;
		}
		start += key.size();
		auto end = body.find('\n', start);
		body.replace(start, end == std::string::npos ? body.size() - start : end - start,
		             std::to_string(static_cast<int64_t>(std::time(nullptr))));
		tmp = manifest_path + ".touch-" +
		      std::to_string(static_cast<unsigned long long>(staging_counter.fetch_add(1)));
		WritePrivateFile(tmp, body);
		LocalFs().MoveFile(tmp, manifest_path);
	} catch (...) {
		if (!tmp.empty()) {
			RmRf(tmp);
		}
	}
}

DatabaseWorkerResolution Install(const std::string &source, const std::string &contents,
	                              const std::string &digest, const std::string &format,
	                              const std::string &entrypoint, ClientContext &context) {
	auto cache = CacheDir(context);
	auto artifacts = cache + "/artifacts";
	auto locks = cache + "/locks";
	auto staging = cache + "/staging";
	MkdirP(artifacts);
	MkdirP(locks);
	MkdirP(staging);

	std::string identity = "vgi-worker-artifact-v1";
	identity.push_back('\0');
	identity += digest;
	identity.push_back('\0');
	identity += format;
	identity.push_back('\0');
	identity += entrypoint;
	auto artifact_id = VgiSha256Hex(identity);
	auto final_dir = artifacts + "/" + artifact_id;
	auto lock_path = locks + "/" + artifact_id + ".lease";
	// Every filesystem mutation takes locks in install -> lifetime order. Cache
	// hits take the short install mutex and a shared lifetime lease; repair takes
	// the same mutex and an exclusive lifetime lease, so it cannot replace files
	// underneath a running or pooled worker.
	auto install_guard = ArtifactLease::Acquire(locks + "/" + artifact_id + ".install", true, false);
	auto manifest_path = final_dir + "/.vgi-manifest";
	auto local_entrypoint = final_dir + "/" + entrypoint;
	auto max_extracted_bytes =
	    GetUnsignedSetting(context, "vgi_worker_package_max_extracted_bytes", DEFAULT_MAX_EXTRACTED_BYTES);
	auto max_entries = GetUnsignedSetting(context, "vgi_worker_package_max_files", DEFAULT_MAX_PACKAGE_FILES);
	if (max_extracted_bytes == 0 || max_entries == 0) {
		throw InvalidInputException("vgi: worker package extraction limits must be greater than zero");
	}

	auto cache_valid = [&]() {
		return LocalFs().FileExists(local_entrypoint) &&
		       ManifestMatches(manifest_path, artifact_id, digest, format, entrypoint);
	};
	if (!cache_valid()) {
		auto repair_lease = ArtifactLease::Acquire(lock_path, true, false);
		// Another process may have completed repair while we waited for its
		// lifetime users to drain. Always validate again under exclusivity.
		if (!cache_valid()) {
			if (LocalFs().DirectoryExists(final_dir)) {
				RmRf(final_dir);
			}
			auto tmp = staging + "/" + artifact_id + "-" +
			           std::to_string(static_cast<unsigned long long>(staging_counter.fetch_add(1)));
			RmRf(tmp);
			MkdirP(tmp);
			try {
				auto extracted = ExtractWorkerPackage(contents, format, entrypoint, tmp, max_extracted_bytes,
				                                     max_entries);
				auto expected = tmp + "/" + entrypoint;
				if (extracted != expected || !LocalFs().FileExists(expected)) {
					throw IOException("vgi: extracted worker entrypoint does not match requested path '%s'", entrypoint);
				}
				auto size_bytes = DirectorySize(tmp);
				WritePrivateFile(tmp + "/.vgi-manifest",
				                 ManifestBody(source, artifact_id, digest, format, entrypoint, size_bytes));
				LocalFs().MoveFile(tmp, final_dir);
			} catch (...) {
				RmRf(tmp);
				throw;
			}
		}
		repair_lease.reset();
	}
	// Cleanup also takes install_guard before the exclusive lifetime lease, so
	// no remover can enter between this acquisition and releasing the mutex.
	auto lease = ArtifactLease::Acquire(lock_path, false, false);
	TouchLastUsed(manifest_path);

	if (!LocalFs().FileExists(local_entrypoint)) {
		throw IOException("vgi: cached worker entrypoint disappeared while acquiring its lease: %s",
		                  local_entrypoint);
	}
	auto token = "vgi-artifact:" + VgiSha256Hex(CanonicalCacheRoot(cache)) + ":" + artifact_id;
	{
		std::lock_guard<std::mutex> guard(registry_mutex);
		registry[token] = RegisteredArtifact {local_entrypoint, lock_path, lease};
	}
	auto resolution = DatabaseWorkerResolution {token, digest, local_entrypoint, lease};
	install_guard.reset();
	// Cleanup is synchronous and opportunistic: an ATTACH naturally reaps stale
	// entries without a background thread. The current artifact is protected by
	// its shared lease, as are catalogs and pooled/running workers in any process.
	try {
		(void)PruneWorkerCache(context, false);
	} catch (...) {
		// Cache maintenance must never turn a valid package resolution into an
		// ATTACH failure. Explicit prune/flush functions still surface results.
	}
	return resolution;
}

void CleanupStaging(const std::string &cache) noexcept {
	try {
		auto staging = cache + "/staging";
		if (!LocalFs().DirectoryExists(staging)) {
			return;
		}
		LocalFs().ListFiles(staging, [&](const std::string &name, bool) {
			std::string artifact_id;
			if (StringUtil::StartsWith(name, ".trash-")) {
				artifact_id = name.substr(7, 64);
			} else if (name.size() >= 65 && name[64] == '-') {
				artifact_id = name.substr(0, 64);
			}
			if (!IsHexDigest(artifact_id)) {
				return;
			}
			auto install_guard = ArtifactLease::Acquire(cache + "/locks/" + artifact_id + ".install", true, false);
			auto lease = ArtifactLease::Acquire(cache + "/locks/" + artifact_id + ".lease", true, true);
			if (lease) {
				RmRf(staging + "/" + name);
			}
		});
	} catch (...) {
	}
}

} // namespace

DatabaseWorkerCoordinates ParseDatabaseWorkerLocation(const std::string &location) {
	if (!StringUtil::StartsWith(StringUtil::Lower(location), "database://")) {
		throw InvalidInputException("vgi: not a database:// worker LOCATION: %s", location);
	}
	auto body = location.substr(11);
	std::string fragment;
	auto hash = body.find('#');
	if (hash != std::string::npos) {
		fragment = body.substr(hash + 1);
		body.resize(hash);
	}
	std::string query;
	auto question = body.find('?');
	if (question != std::string::npos) {
		query = body.substr(question + 1);
		body.resize(question);
	}

	std::vector<std::string> segments;
	size_t start = 0;
	while (start <= body.size()) {
		auto slash = body.find('/', start);
		segments.push_back(PercentDecode(body.substr(start, slash == std::string::npos ? std::string::npos : slash - start),
		                                 "path"));
		if (slash == std::string::npos) break;
		start = slash + 1;
	}
	if (segments.size() != 4 || std::any_of(segments.begin(), segments.end(), [](const std::string &s) { return s.empty(); })) {
		throw InvalidInputException(
		    "vgi: database LOCATION must be database://catalog/schema/table/worker?package_version=<version>");
	}
	if (StringUtil::CIEquals(segments[0], "temp")) {
		throw InvalidInputException("vgi: database worker packages cannot be read from the temporary catalog");
	}

	std::string package_version;
	start = 0;
	while (start <= query.size() && !query.empty()) {
		auto amp = query.find('&', start);
		auto item = query.substr(start, amp == std::string::npos ? std::string::npos : amp - start);
		auto eq = item.find('=');
		if (eq == std::string::npos) {
			throw InvalidInputException("vgi: malformed database LOCATION query parameter");
		}
		auto key = PercentDecode(item.substr(0, eq), "query key");
		auto value = PercentDecode(item.substr(eq + 1), "query value");
		if (key != "package_version" || !package_version.empty()) {
			throw InvalidInputException("vgi: database LOCATION only accepts one package_version query parameter");
		}
		package_version = value;
		if (amp == std::string::npos) break;
		start = amp + 1;
	}
	if (package_version.empty()) {
		throw InvalidInputException("vgi: database LOCATION requires a non-empty package_version query parameter");
	}

	std::string expected;
	if (!fragment.empty()) {
		const std::string prefix = "sha256=";
		if (!StringUtil::StartsWith(fragment, prefix)) {
			throw InvalidInputException("vgi: database LOCATION fragment must be #sha256=<64 hex characters>");
		}
		expected = StringUtil::Lower(PercentDecode(fragment.substr(prefix.size()), "digest pin"));
		if (!IsHexDigest(expected)) {
			throw InvalidInputException("vgi: database LOCATION SHA-256 pin must contain exactly 64 hex characters");
		}
	}
	return {segments[0], segments[1], segments[2], segments[3], package_version, expected};
}

DatabaseWorkerResolution ResolveDatabaseWorker(const std::string &location, ClientContext &context) {
#if !VGI_SUBPROCESS_TRANSPORT
	(void)context;
	(void)ParseDatabaseWorkerLocation(location);
	throw InvalidInputException("vgi: database:// LOCATIONs require a child-process transport");
#else
	auto coords = ParseDatabaseWorkerLocation(location);
	auto max_bytes = GetUnsignedSetting(context, "vgi_worker_package_max_bytes", DEFAULT_MAX_PACKAGE_BYTES);
	auto qualified = KeywordHelper::WriteOptionallyQuoted(coords.catalog) + "." +
	                 KeywordHelper::WriteOptionallyQuoted(coords.schema) + "." +
	                 KeywordHelper::WriteOptionallyQuoted(coords.table);
	auto sql = "SELECT CAST(worker_name AS VARCHAR), CAST(platform AS VARCHAR), "
	           "CAST(package_version AS VARCHAR), CAST(package_format AS VARCHAR), "
	           "CAST(entrypoint AS VARCHAR), "
	           "CAST(CASE WHEN ? = 0 OR octet_length(contents) <= ? THEN contents ELSE NULL END AS BLOB), "
	           "CAST(sha256 AS VARCHAR), octet_length(contents), count(*) OVER () FROM " +
	           qualified + " WHERE worker_name = ? AND platform = ? AND package_version = ? LIMIT 1";
	Connection connection(*context.db);
	auto result = connection.Query(sql, max_bytes, max_bytes, coords.worker_name, DuckDB::Platform(),
	                               coords.package_version);
	if (result->HasError()) {
		throw BinderException("vgi: failed to read database worker package from %s: %s", qualified, result->GetError());
	}
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		throw BinderException("vgi: no worker package '%s' for platform '%s' and package_version '%s' in %s",
		                      coords.worker_name, DuckDB::Platform(), coords.package_version, qualified);
	}
	if (chunk->GetValue(8, 0).GetValue<uint64_t>() != 1) {
		throw BinderException("vgi: multiple worker packages match '%s' for platform '%s' and package_version '%s' in %s",
		                      coords.worker_name, DuckDB::Platform(), coords.package_version, qualified);
	}
	for (idx_t col : {idx_t(0), idx_t(1), idx_t(2), idx_t(3), idx_t(4), idx_t(6), idx_t(7)}) {
		if (chunk->GetValue(col, 0).IsNull()) {
			throw BinderException("vgi: database worker package contains NULL in required column %llu",
			                      static_cast<unsigned long long>(col + 1));
		}
	}
	auto format = NormalizeWorkerPackageFormat(chunk->GetValue(3, 0).GetValue<std::string>());
	auto entrypoint = chunk->GetValue(4, 0).GetValue<std::string>();
	auto package_bytes = chunk->GetValue(7, 0).GetValue<uint64_t>();
	if (max_bytes && package_bytes > max_bytes) {
		throw BinderException("vgi: database worker package is %llu bytes, exceeding vgi_worker_package_max_bytes=%llu",
		                      static_cast<unsigned long long>(package_bytes),
		                      static_cast<unsigned long long>(max_bytes));
	}
	if (chunk->GetValue(5, 0).IsNull()) {
		throw BinderException("vgi: database worker package contents must be non-NULL");
	}
	// BLOB::GetValue<std::string>() formats bytes as a SQL-escaped VARCHAR
	// (e.g. newline becomes "\\x0A"). StringValue exposes the actual payload.
	auto contents_value = chunk->GetValue(5, 0);
	auto contents = StringValue::Get(contents_value);
	auto declared = StringUtil::Lower(chunk->GetValue(6, 0).GetValue<std::string>());
	if (entrypoint.empty() || contents.empty()) {
		throw BinderException("vgi: database worker package entrypoint and contents must be non-empty");
	}
	if (!IsHexDigest(declared)) {
		throw BinderException("vgi: database worker package sha256 must contain exactly 64 hex characters");
	}
	auto digest = VgiSha256Hex(contents);
	if (digest != declared) {
		throw IOException("vgi: database worker package SHA-256 mismatch: row declares %s, contents hash to %s",
		                  declared, digest);
	}
	if (!coords.expected_sha256.empty() && digest != coords.expected_sha256) {
		throw IOException("vgi: database worker package LOCATION pin mismatch: expected %s, got %s",
		                  coords.expected_sha256, digest);
	}
	return Install(location, contents, digest, format, entrypoint, context);
#endif
}

ResolvedWorkerLaunch AcquireResolvedWorkerLaunch(const std::string &token) {
	std::lock_guard<std::mutex> guard(registry_mutex);
	auto found = registry.find(token);
	if (found == registry.end()) {
		throw IOException("vgi: resolved worker token is not registered: %s", token);
	}
	auto lease = found->second.lease.lock();
	if (!lease) {
		lease = ArtifactLease::Acquire(found->second.lock_path, false, false);
		found->second.lease = lease;
	}
	if (!LocalFs().FileExists(found->second.entrypoint)) {
		throw IOException("vgi: cached worker entrypoint disappeared: %s", found->second.entrypoint);
	}
	return {found->second.entrypoint, lease};
}

std::vector<WorkerCacheEntry> ListWorkerCache(ClientContext &context) {
	std::vector<WorkerCacheEntry> out;
	auto cache = CacheDir(context);
	auto artifacts = cache + "/artifacts";
	if (!LocalFs().DirectoryExists(artifacts)) return out;
	auto now = static_cast<int64_t>(std::time(nullptr));
	LocalFs().ListFiles(artifacts, [&](const std::string &name, bool is_dir) {
		if (!is_dir || !IsHexDigest(name)) return;
		auto dir = artifacts + "/" + name;
		auto meta = ParseMeta(ReadFile(dir + "/.vgi-manifest"));
		if (meta["manifest_version"] != "1") return;
		WorkerCacheEntry entry;
		entry.artifact_id = name;
		entry.source = meta["source"];
		entry.digest = meta["digest"];
		entry.package_format = meta["package_format"];
		entry.entrypoint = meta["entrypoint"];
		entry.directory = dir;
		try { entry.size_bytes = std::stoull(meta["size_bytes"]); } catch (...) { entry.size_bytes = DirectorySize(dir); }
		int64_t last_used = 0;
		try { last_used = std::stoll(meta["last_used"]); } catch (...) { }
		entry.age_seconds = last_used > 0 ? std::max<int64_t>(0, now - last_used) : 0;
		auto lease = ArtifactLease::Acquire(cache + "/locks/" + name + ".lease", true, true);
		entry.in_use = !lease;
		out.push_back(std::move(entry));
	});
	return out;
}

WorkerCachePruneResult PruneWorkerCache(ClientContext &context, bool flush_all) {
	WorkerCachePruneResult result;
	auto cache = CacheDir(context);
	CleanupStaging(cache);
	auto entries = ListWorkerCache(context);
	auto max_bytes = GetUnsignedSetting(context, "vgi_worker_cache_max_bytes", DEFAULT_CACHE_MAX_BYTES);
	auto ttl = static_cast<int64_t>(GetUnsignedSetting(context, "vgi_worker_cache_ttl_seconds", DEFAULT_CACHE_TTL_SECONDS));
	uint64_t total = 0;
	for (auto &entry : entries) total += entry.size_bytes;
	std::sort(entries.begin(), entries.end(), [](const WorkerCacheEntry &a, const WorkerCacheEntry &b) {
		return a.age_seconds > b.age_seconds;
	});
	uint64_t low_water = max_bytes ? max_bytes - max_bytes / 5 : 0;
	bool reduce_to_low_water = max_bytes > 0 && total > max_bytes;
	for (auto &entry : entries) {
		bool expired = ttl > 0 && entry.age_seconds >= ttl;
		bool over_budget = reduce_to_low_water && total > low_water;
		if (!flush_all && !expired && !over_budget) continue;
		auto install_guard = ArtifactLease::Acquire(cache + "/locks/" + entry.artifact_id + ".install", true, false);
		auto lease = ArtifactLease::Acquire(cache + "/locks/" + entry.artifact_id + ".lease", true, true);
		if (!lease) {
			result.skipped_in_use++;
			continue;
		}
		auto trash = cache + "/staging/.trash-" + entry.artifact_id;
		RmRf(trash);
		try {
			LocalFs().MoveFile(entry.directory, trash);
			RmRf(trash);
			result.removed++;
			result.bytes_removed += entry.size_bytes;
			total = total > entry.size_bytes ? total - entry.size_bytes : 0;
			if (total <= low_water) {
				reduce_to_low_water = false;
			}
		} catch (...) {
		}
	}
	return result;
}

} // namespace vgi
} // namespace duckdb
