// © Copyright 2026 Query Farm LLC - https://query.farm

#include "vgi_oauth_store.hpp"

#include "duckdb/common/exception.hpp"
#include "vgi_exception.hpp"
#include "vgi_oauth.hpp"
#include "vgi_sha256.hpp"

#include <algorithm>
#include <cstdint>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <vector>

#if defined(__APPLE__)
#include <CoreFoundation/CoreFoundation.h>
#include <Security/Security.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#elif defined(_WIN32)
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <dpapi.h>
#include <filesystem>
#elif defined(__linux__) && !defined(__EMSCRIPTEN__)
#define VGI_OAUTH_LINUX_STORE 1
#include <dlfcn.h>
#include <fcntl.h>
#include <mutex>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#else
#include <unistd.h>
#endif

namespace duckdb {
namespace vgi {
namespace {

constexpr const char *kKeychainService = "com.queryfarm.vgi.oauth";

#if defined(VGI_OAUTH_LINUX_STORE)
enum class LinuxBackend { NONE, SECRET_SERVICE, FILE };
struct LinuxStoreState {
	LinuxBackend backend = LinuxBackend::NONE;
	std::string reason; // why NONE
	std::string dir;    // FILE
};
const LinuxStoreState &LinuxStore();
#endif

// `allow_linux`: the caller opted in to the Linux backends (Secret Service,
// else an owner-only file). Ordinary OAuth stays memory-only on Linux.
bool WantsPersistence(const std::string &mode, bool allow_linux) {
	if (mode == "none" || mode == "memory")
		return false;
#if defined(__APPLE__) || defined(_WIN32)
	return mode == "auto" || mode == "persistent";
#elif defined(VGI_OAUTH_LINUX_STORE)
	if (allow_linux) {
		const auto &store = LinuxStore();
		if (store.backend != LinuxBackend::NONE)
			return mode == "auto" || mode == "persistent";
		if (mode == "persistent") {
			throw IOException(ErrorInfo(error_subtype::kUnsupported).Set(error_key::kAttachOption, "oauth_cache"),
			                  "VGI OAuth: persistent credential storage is unavailable: %s", store.reason);
		}
		return false;
	}
	if (mode == "persistent") {
		throw IOException(ErrorInfo(error_subtype::kUnsupported).Set(error_key::kAttachOption, "oauth_cache"),
		                  "VGI OAuth: persistent credential storage is not available in this Linux build; "
		                  "use oauth_cache='memory' or install a build with Secret Service support");
	}
	return false; // conservative Linux auto: never fall back to plaintext
#else
	(void)allow_linux;
	if (mode == "persistent") {
		throw IOException(ErrorInfo(error_subtype::kUnsupported).Set(error_key::kAttachOption, "oauth_cache"),
		                  "VGI OAuth: persistent credential storage is not available on this platform; "
		                  "use oauth_cache='memory'");
	}
	return false;
#endif
}

#if defined(__APPLE__) || defined(VGI_OAUTH_LINUX_STORE)
// A directory this user owns, not a symlink, with no group/other access.
// Refuses anything else: in a shared /tmp another user could pre-create it.
void RequirePrivateDirectory(const std::string &dir, const char *what) {
	struct stat st;
	if (lstat(dir.c_str(), &st) != 0) {
		throw IOException(ErrorInfo(error_subtype::kCredentialStoreFailed), "VGI OAuth: %s %s is missing", what, dir);
	}
	if (!S_ISDIR(st.st_mode) || st.st_uid != getuid() || (st.st_mode & 077) != 0) {
		throw IOException(ErrorInfo(error_subtype::kCredentialStoreFailed),
		                  "VGI OAuth: refusing %s %s: it must be a directory owned by this user with mode 0700 "
		                  "(not a symlink)",
		                  what, dir);
	}
}

class PosixLease final : public OAuthCredentialLease {
public:
	explicit PosixLease(const std::string &key) {
		std::string dir = "/tmp/queryfarm-vgi-oauth-" + std::to_string(static_cast<unsigned long long>(getuid()));
		if (mkdir(dir.c_str(), 0700) != 0 && errno != EEXIST) {
			throw IOException(ErrorInfo(error_subtype::kCredentialStoreFailed), "VGI OAuth: could not create credential lock directory");
		}
		RequirePrivateDirectory(dir, "credential lock directory");
		std::string path = dir + "/" + VgiSha256Hex(key) + ".lock";
		fd_ = open(path.c_str(), O_CREAT | O_RDWR | O_NOFOLLOW | O_CLOEXEC, 0600);
		struct stat st;
		if (fd_ < 0 || fstat(fd_, &st) != 0 || !S_ISREG(st.st_mode) || st.st_uid != getuid() ||
		    flock(fd_, LOCK_EX) != 0) {
			if (fd_ >= 0)
				close(fd_);
			throw IOException(ErrorInfo(error_subtype::kCredentialStoreFailed), "VGI OAuth: could not acquire credential cache lease");
		}
	}
	~PosixLease() override {
		if (fd_ >= 0) {
			flock(fd_, LOCK_UN);
			close(fd_);
		}
	}

private:
	int fd_ = -1;
};
#endif

#if defined(__APPLE__)
CFMutableDictionaryRef KeychainQuery(const std::string &account) {
	auto query = CFDictionaryCreateMutable(kCFAllocatorDefault, 0, &kCFTypeDictionaryKeyCallBacks,
	                                       &kCFTypeDictionaryValueCallBacks);
	auto service = CFStringCreateWithCString(kCFAllocatorDefault, kKeychainService, kCFStringEncodingUTF8);
	auto acct = CFStringCreateWithCString(kCFAllocatorDefault, account.c_str(), kCFStringEncodingUTF8);
	CFDictionarySetValue(query, kSecClass, kSecClassGenericPassword);
	CFDictionarySetValue(query, kSecAttrService, service);
	CFDictionarySetValue(query, kSecAttrAccount, acct);
	CFRelease(service);
	CFRelease(acct);
	return query;
}

bool PlatformLoad(const std::string &key, std::string &data) {
	auto query = KeychainQuery(VgiSha256Hex(key));
	CFDictionarySetValue(query, kSecReturnData, kCFBooleanTrue);
	CFDictionarySetValue(query, kSecMatchLimit, kSecMatchLimitOne);
	CFTypeRef result = nullptr;
	auto status = SecItemCopyMatching(query, &result);
	CFRelease(query);
	if (status == errSecItemNotFound)
		return false;
	if (status != errSecSuccess || !result)
		throw IOException(ErrorInfo(error_subtype::kCredentialStoreFailed), "VGI OAuth: Keychain read failed (%d)", status);
	auto bytes = reinterpret_cast<CFDataRef>(result);
	data.assign(reinterpret_cast<const char *>(CFDataGetBytePtr(bytes)), CFDataGetLength(bytes));
	CFRelease(result);
	return true;
}

void PlatformStore(const std::string &key, const std::string &data) {
	auto query = KeychainQuery(VgiSha256Hex(key));
	auto value = CFDataCreate(kCFAllocatorDefault, reinterpret_cast<const UInt8 *>(data.data()), data.size());
	auto attrs = CFDictionaryCreateMutable(kCFAllocatorDefault, 0, &kCFTypeDictionaryKeyCallBacks,
	                                       &kCFTypeDictionaryValueCallBacks);
	CFDictionarySetValue(attrs, kSecValueData, value);
	auto status = SecItemUpdate(query, attrs);
	if (status == errSecItemNotFound) {
		CFDictionarySetValue(query, kSecValueData, value);
		status = SecItemAdd(query, nullptr);
	}
	CFRelease(attrs);
	CFRelease(value);
	CFRelease(query);
	if (status != errSecSuccess)
		throw IOException(ErrorInfo(error_subtype::kCredentialStoreFailed), "VGI OAuth: Keychain write failed (%d)", status);
}

void PlatformDelete(const std::string &key) {
	auto query = KeychainQuery(VgiSha256Hex(key));
	auto status = SecItemDelete(query);
	CFRelease(query);
	if (status != errSecSuccess && status != errSecItemNotFound) {
		throw IOException(ErrorInfo(error_subtype::kCredentialStoreFailed), "VGI OAuth: Keychain delete failed (%d)", status);
	}
}

void PlatformPrune() {
	auto query = CFDictionaryCreateMutable(kCFAllocatorDefault, 0, &kCFTypeDictionaryKeyCallBacks,
	                                       &kCFTypeDictionaryValueCallBacks);
	auto service = CFStringCreateWithCString(kCFAllocatorDefault, kKeychainService, kCFStringEncodingUTF8);
	CFDictionarySetValue(query, kSecClass, kSecClassGenericPassword);
	CFDictionarySetValue(query, kSecAttrService, service);
	CFDictionarySetValue(query, kSecReturnAttributes, kCFBooleanTrue);
	CFDictionarySetValue(query, kSecMatchLimit, kSecMatchLimitAll);
	CFTypeRef result = nullptr;
	auto status = SecItemCopyMatching(query, &result);
	CFRelease(service);
	CFRelease(query);
	if (status == errSecItemNotFound)
		return;
	if (status != errSecSuccess || !result || CFGetTypeID(result) != CFArrayGetTypeID()) {
		if (result)
			CFRelease(result);
		return;
	}
	auto items = reinterpret_cast<CFArrayRef>(result);
	struct Candidate {
		CFAbsoluteTime modified;
		std::string account;
	};
	std::vector<Candidate> candidates;
	for (CFIndex i = 0; i < CFArrayGetCount(items); ++i) {
		auto attrs = reinterpret_cast<CFDictionaryRef>(CFArrayGetValueAtIndex(items, i));
		auto account = reinterpret_cast<CFStringRef>(CFDictionaryGetValue(attrs, kSecAttrAccount));
		auto modified = reinterpret_cast<CFDateRef>(CFDictionaryGetValue(attrs, kSecAttrModificationDate));
		if (!account)
			continue;
		char buffer[256];
		if (!CFStringGetCString(account, buffer, sizeof(buffer), kCFStringEncodingUTF8))
			continue;
		candidates.push_back({modified ? CFDateGetAbsoluteTime(modified) : 0, buffer});
	}
	CFRelease(result);
	if (candidates.size() <= 128)
		return;
	std::sort(candidates.begin(), candidates.end(),
	          [](const Candidate &a, const Candidate &b) { return a.modified < b.modified; });
	for (size_t i = 0; i < candidates.size() - 128; ++i) {
		auto old = KeychainQuery(candidates[i].account);
		SecItemDelete(old);
		CFRelease(old);
	}
}
#elif defined(_WIN32)
class WindowsLease final : public OAuthCredentialLease {
public:
	explicit WindowsLease(const std::string &key) {
		std::string name = "Local\\QueryFarm.VGI.OAuth." + VgiSha256Hex(key);
		handle_ = CreateMutexA(nullptr, FALSE, name.c_str());
		const auto result = handle_ ? WaitForSingleObject(handle_, INFINITE) : WAIT_FAILED;
		// WAIT_ABANDONED grants ownership after the previous owner exits. Keep
		// the lease; the caller reloads and validates the protected record before
		// using it, and the destructor must release this ownership normally.
		if (result != WAIT_OBJECT_0 && result != WAIT_ABANDONED) {
			if (handle_)
				CloseHandle(handle_);
			throw IOException(ErrorInfo(error_subtype::kCredentialStoreFailed), "VGI OAuth: could not acquire credential cache lease");
		}
	}
	~WindowsLease() override {
		if (handle_) {
			ReleaseMutex(handle_);
			CloseHandle(handle_);
		}
	}

private:
	HANDLE handle_ = nullptr;
};

std::filesystem::path StorePath(const std::string &key) {
	const char *base = std::getenv("LOCALAPPDATA");
	if (!base || !*base)
		throw IOException(ErrorInfo(error_subtype::kCredentialStoreFailed), "VGI OAuth: LOCALAPPDATA is unavailable");
	auto dir = std::filesystem::path(base) / "QueryFarm" / "VGI" / "oauth";
	std::filesystem::create_directories(dir);
	return dir / (VgiSha256Hex(key) + ".bin");
}

bool PlatformLoad(const std::string &key, std::string &data) {
	std::ifstream in(StorePath(key), std::ios::binary);
	if (!in)
		return false;
	std::string encrypted((std::istreambuf_iterator<char>(in)), {});
	DATA_BLOB input {static_cast<DWORD>(encrypted.size()), reinterpret_cast<BYTE *>(encrypted.data())};
	DATA_BLOB output {};
	if (!CryptUnprotectData(&input, nullptr, nullptr, nullptr, nullptr, 0, &output)) {
		throw IOException(ErrorInfo(error_subtype::kCredentialStoreFailed), "VGI OAuth: DPAPI credential decrypt failed");
	}
	data.assign(reinterpret_cast<char *>(output.pbData), output.cbData);
	SecureZeroMemory(output.pbData, output.cbData);
	LocalFree(output.pbData);
	return true;
}

void PlatformStore(const std::string &key, const std::string &data) {
	DATA_BLOB input {static_cast<DWORD>(data.size()), reinterpret_cast<BYTE *>(const_cast<char *>(data.data()))};
	DATA_BLOB output {};
	if (!CryptProtectData(&input, L"Query Farm VGI OAuth", nullptr, nullptr, nullptr, CRYPTPROTECT_UI_FORBIDDEN,
	                      &output)) {
		throw IOException(ErrorInfo(error_subtype::kCredentialStoreFailed), "VGI OAuth: DPAPI credential encrypt failed");
	}
	auto path = StorePath(key);
	auto temp = path;
	temp += ".tmp." + std::to_string(GetCurrentProcessId());
	{
		std::ofstream out(temp, std::ios::binary | std::ios::trunc);
		out.write(reinterpret_cast<char *>(output.pbData), output.cbData);
		if (!out) {
			LocalFree(output.pbData);
			throw IOException(ErrorInfo(error_subtype::kCredentialStoreFailed), "VGI OAuth: DPAPI cache write failed");
		}
	}
	SecureZeroMemory(output.pbData, output.cbData);
	LocalFree(output.pbData);
	std::error_code ec;
	std::filesystem::rename(temp, path, ec);
	if (ec) {
		std::filesystem::remove(path, ec);
		ec.clear();
		std::filesystem::rename(temp, path, ec);
	}
	if (ec)
		throw IOException(ErrorInfo(error_subtype::kCredentialStoreFailed), "VGI OAuth: DPAPI cache replace failed");
}

void PlatformDelete(const std::string &key) {
	std::error_code ec;
	std::filesystem::remove(StorePath(key), ec);
	if (ec) {
		throw IOException(ErrorInfo(error_subtype::kCredentialStoreFailed), "VGI OAuth: DPAPI cache delete failed (Windows error %d)", ec.value());
	}
}

void PlatformPrune() {
	auto dir = StorePath("prune-sentinel").parent_path();
	std::vector<std::filesystem::directory_entry> files;
	for (const auto &entry : std::filesystem::directory_iterator(dir)) {
		if (entry.is_regular_file() && entry.path().extension() == ".bin")
			files.push_back(entry);
	}
	if (files.size() <= 128)
		return;
	std::sort(files.begin(), files.end(),
	          [](const auto &a, const auto &b) { return a.last_write_time() < b.last_write_time(); });
	std::error_code ec;
	for (size_t i = 0; i < files.size() - 128; ++i)
		std::filesystem::remove(files[i], ec);
}
#elif defined(VGI_OAUTH_LINUX_STORE)
//===--------------------------------------------------------------------===//
// Linux: the freedesktop Secret Service through libsecret, loaded with dlopen
// (no build or runtime dependency), else an owner-only file under
// $XDG_STATE_HOME/queryfarm-vgi (default ~/.local/state/queryfarm-vgi).
// The backend is chosen once per process. VGI_OAUTH_LINUX_STORE=file /
// secret-service / none forces one (test hook).
//===--------------------------------------------------------------------===//

// libsecret's ABI, declared here so nothing links against it.
struct SsAttribute {
	const char *name;
	int type; // SECRET_SCHEMA_ATTRIBUTE_STRING = 0
};
struct SsSchema {
	const char *name;
	int flags; // SECRET_SCHEMA_NONE = 0
	SsAttribute attributes[32];
	int reserved;
	void *reserved1, *reserved2, *reserved3, *reserved4, *reserved5, *reserved6, *reserved7;
};
struct SsError {
	uint32_t domain;
	int code;
	char *message;
};
using SsStoreFn = int (*)(const SsSchema *, const char *, const char *, const char *, void *, SsError **, ...);
using SsLookupFn = char *(*)(const SsSchema *, void *, SsError **, ...);
using SsClearFn = int (*)(const SsSchema *, void *, SsError **, ...);
using SsFreeFn = void (*)(char *);
using GErrorFreeFn = void (*)(SsError *);

struct SecretService {
	SsStoreFn store = nullptr;
	SsLookupFn lookup = nullptr;
	SsClearFn clear = nullptr;
	SsFreeFn free_password = nullptr;
	GErrorFreeFn free_error = nullptr;
	SsSchema schema {};
};

SecretService &Ss() {
	static SecretService ss;
	return ss;
}

std::string TakeError(SsError *err) {
	std::string msg = err && err->message ? err->message : "unknown error";
	if (err && Ss().free_error)
		Ss().free_error(err);
	return msg;
}

bool LoadSecretService(std::string &reason) {
	if (!std::getenv("DBUS_SESSION_BUS_ADDRESS")) {
		reason = "no D-Bus session (DBUS_SESSION_BUS_ADDRESS is unset)";
		return false;
	}
	void *lib = dlopen("libsecret-1.so.0", RTLD_NOW | RTLD_LOCAL);
	if (!lib) {
		reason = "libsecret-1.so.0 is not installed";
		return false;
	}
	auto &ss = Ss();
	ss.store = reinterpret_cast<SsStoreFn>(dlsym(lib, "secret_password_store_sync"));
	ss.lookup = reinterpret_cast<SsLookupFn>(dlsym(lib, "secret_password_lookup_sync"));
	ss.clear = reinterpret_cast<SsClearFn>(dlsym(lib, "secret_password_clear_sync"));
	ss.free_password = reinterpret_cast<SsFreeFn>(dlsym(lib, "secret_password_free"));
	ss.free_error = reinterpret_cast<GErrorFreeFn>(dlsym(lib, "g_error_free"));
	if (!ss.store || !ss.lookup || !ss.clear || !ss.free_password || !ss.free_error) {
		reason = "libsecret-1.so.0 lacks the password API";
		return false;
	}
	ss.schema.name = "com.queryfarm.vgi.oauth";
	ss.schema.attributes[0] = {"account", 0};
	// Probe: a lookup of an absent item succeeds with NULL when a Secret Service
	// is running, and errors (fast) when none answers on the bus.
	SsError *err = nullptr;
	char *found = ss.lookup(&ss.schema, nullptr, &err, "account", "vgi-probe", static_cast<const char *>(nullptr));
	if (found)
		ss.free_password(found);
	if (err) {
		reason = "the Secret Service is not available: " + TakeError(err);
		return false;
	}
	return true;
}

std::string StateDirectory(std::string &reason) {
	std::string base;
	const char *xdg = std::getenv("XDG_STATE_HOME");
	if (xdg && xdg[0] == '/') {
		base = xdg;
	} else {
		const char *home = std::getenv("HOME");
		if (!home || home[0] != '/') {
			reason = "neither XDG_STATE_HOME nor HOME is an absolute path";
			return std::string();
		}
		base = std::string(home) + "/.local/state";
	}
	// Create missing components owner-only (existing parents are left alone).
	for (size_t pos = 1; pos != std::string::npos;) {
		pos = base.find('/', pos + 1);
		auto part = base.substr(0, pos);
		if (mkdir(part.c_str(), 0700) != 0 && errno != EEXIST) {
			reason = "could not create " + part + ": " + std::strerror(errno);
			return std::string();
		}
	}
	auto dir = base + "/queryfarm-vgi";
	if (mkdir(dir.c_str(), 0700) != 0 && errno != EEXIST) {
		reason = "could not create " + dir + ": " + std::strerror(errno);
		return std::string();
	}
	try {
		RequirePrivateDirectory(dir, "credential directory");
	} catch (...) {
		reason = dir + " is not a private directory (owner-only, mode 0700, not a symlink)";
		return std::string();
	}
	return dir;
}

const LinuxStoreState &LinuxStore() {
	static LinuxStoreState state;
	static std::once_flag once;
	std::call_once(once, [] {
		const char *forced = std::getenv("VGI_OAUTH_LINUX_STORE");
		std::string force = forced ? forced : "";
		std::string ss_reason, file_reason;
		if (force == "none") {
			state.reason = "disabled by VGI_OAUTH_LINUX_STORE=none";
			return;
		}
		if (force != "file" && LoadSecretService(ss_reason)) {
			state.backend = LinuxBackend::SECRET_SERVICE;
			return;
		}
		if (force == "secret-service") {
			state.reason = ss_reason;
			return;
		}
		state.dir = StateDirectory(file_reason);
		if (!state.dir.empty()) {
			state.backend = LinuxBackend::FILE;
			return;
		}
		state.reason = (ss_reason.empty() ? std::string() : ss_reason + "; ") + file_reason;
	});
	return state;
}

std::string CredentialFile(const std::string &key) {
	return LinuxStore().dir + "/" + VgiSha256Hex(key) + ".cred";
}

bool PlatformLoad(const std::string &key, std::string &data) {
	const auto &store = LinuxStore();
	if (store.backend == LinuxBackend::SECRET_SERVICE) {
		auto &ss = Ss();
		SsError *err = nullptr;
		auto account = VgiSha256Hex(key);
		char *found = ss.lookup(&ss.schema, nullptr, &err, "account", account.c_str(), static_cast<const char *>(nullptr));
		if (err)
			throw IOException(ErrorInfo(error_subtype::kCredentialStoreFailed), "VGI OAuth: Secret Service read failed: %s",
			                  TakeError(err));
		if (!found)
			return false;
		// Stored hex-encoded: the Secret Service password API takes a C string.
		std::string hex(found);
		ss.free_password(found);
		data.clear();
		for (size_t i = 0; i + 1 < hex.size(); i += 2)
			data.push_back(static_cast<char>(std::stoi(hex.substr(i, 2), nullptr, 16)));
		return true;
	}
	int fd = open(CredentialFile(key).c_str(), O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
	if (fd < 0) {
		if (errno == ENOENT)
			return false;
		throw IOException(ErrorInfo(error_subtype::kCredentialStoreFailed), "VGI OAuth: credential file read failed: %s",
		                  std::strerror(errno));
	}
	struct stat st;
	if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_uid != getuid() || (st.st_mode & 077) != 0) {
		close(fd);
		throw IOException(ErrorInfo(error_subtype::kCredentialStoreFailed),
		                  "VGI OAuth: refusing credential file %s: it must be a regular file owned by this user "
		                  "with mode 0600",
		                  CredentialFile(key));
	}
	data.clear();
	char buf[4096];
	for (;;) {
		auto n = read(fd, buf, sizeof(buf));
		if (n < 0 && errno == EINTR)
			continue;
		if (n < 0) {
			close(fd);
			throw IOException(ErrorInfo(error_subtype::kCredentialStoreFailed), "VGI OAuth: credential file read failed");
		}
		if (n == 0)
			break;
		data.append(buf, static_cast<size_t>(n));
	}
	close(fd);
	return true;
}

void PlatformStore(const std::string &key, const std::string &data) {
	const auto &store = LinuxStore();
	if (store.backend == LinuxBackend::SECRET_SERVICE) {
		static const char *kHex = "0123456789abcdef";
		std::string hex;
		for (unsigned char c : data) {
			hex.push_back(kHex[c >> 4]);
			hex.push_back(kHex[c & 15]);
		}
		auto &ss = Ss();
		SsError *err = nullptr;
		auto account = VgiSha256Hex(key);
		ss.store(&ss.schema, "default", "Query.Farm VGI sign-in", hex.c_str(), nullptr, &err, "account",
		         account.c_str(), static_cast<const char *>(nullptr));
		if (err)
			throw IOException(ErrorInfo(error_subtype::kCredentialStoreFailed), "VGI OAuth: Secret Service write failed: %s",
			                  TakeError(err));
		return;
	}
	auto path = CredentialFile(key);
	auto temp = path + ".tmp." + std::to_string(static_cast<long long>(getpid()));
	unlink(temp.c_str());
	int fd = open(temp.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600);
	if (fd < 0)
		throw IOException(ErrorInfo(error_subtype::kCredentialStoreFailed), "VGI OAuth: credential file write failed: %s",
		                  std::strerror(errno));
	size_t off = 0;
	bool ok = true;
	while (off < data.size()) {
		auto n = write(fd, data.data() + off, data.size() - off);
		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0) {
			ok = false;
			break;
		}
		off += static_cast<size_t>(n);
	}
	ok = ok && fsync(fd) == 0;
	ok = close(fd) == 0 && ok;
	if (!ok || rename(temp.c_str(), path.c_str()) != 0) {
		unlink(temp.c_str());
		throw IOException(ErrorInfo(error_subtype::kCredentialStoreFailed), "VGI OAuth: credential file write failed");
	}
}

void PlatformDelete(const std::string &key) {
	const auto &store = LinuxStore();
	if (store.backend == LinuxBackend::SECRET_SERVICE) {
		auto &ss = Ss();
		SsError *err = nullptr;
		auto account = VgiSha256Hex(key);
		ss.clear(&ss.schema, nullptr, &err, "account", account.c_str(), static_cast<const char *>(nullptr));
		if (err)
			throw IOException(ErrorInfo(error_subtype::kCredentialStoreFailed), "VGI OAuth: Secret Service delete failed: %s",
			                  TakeError(err));
		return;
	}
	if (unlink(CredentialFile(key).c_str()) != 0 && errno != ENOENT) {
		throw IOException(ErrorInfo(error_subtype::kCredentialStoreFailed), "VGI OAuth: credential file delete failed: %s",
		                  std::strerror(errno));
	}
}

void PlatformPrune() {
	const auto &store = LinuxStore();
	if (store.backend != LinuxBackend::FILE)
		return;
	DIR *dir = opendir(store.dir.c_str());
	if (!dir)
		return;
	std::vector<std::pair<time_t, std::string>> files;
	while (auto *entry = readdir(dir)) {
		std::string name = entry->d_name;
		if (name.size() < 5 || name.compare(name.size() - 5, 5, ".cred") != 0)
			continue;
		struct stat st;
		auto path = store.dir + "/" + name;
		if (lstat(path.c_str(), &st) == 0 && S_ISREG(st.st_mode))
			files.emplace_back(st.st_mtime, path);
	}
	closedir(dir);
	if (files.size() <= 128)
		return;
	std::sort(files.begin(), files.end());
	for (size_t i = 0; i < files.size() - 128; ++i)
		unlink(files[i].second.c_str());
}
#endif

} // namespace

std::unique_ptr<OAuthCredentialLease> AcquireOAuthCredentialLease(const std::string &key, const std::string &mode,
                                                                  bool allow_linux) {
	if (!WantsPersistence(mode, allow_linux))
		return nullptr;
	try {
#if defined(__APPLE__) || defined(VGI_OAUTH_LINUX_STORE)
		return std::make_unique<PosixLease>(key);
#elif defined(_WIN32)
		return std::make_unique<WindowsLease>(key);
#else
		return nullptr;
#endif
	} catch (...) {
		if (mode == "auto")
			return nullptr;
		throw;
	}
}

bool LoadOAuthRefreshToken(const std::string &key, const OAuthRefreshContext &expected, const std::string &mode,
                           std::string &token, OAuthRefreshContext *issued, bool allow_linux) {
	if (!WantsPersistence(mode, allow_linux))
		return false;
#if defined(__APPLE__) || defined(_WIN32) || defined(VGI_OAUTH_LINUX_STORE)
	try {
		std::string data;
		if (!PlatformLoad(key, data))
			return false;
		OAuthRefreshContext resolved;
		if (!DecodeOAuthRefreshRecord(data, expected, token, resolved)) {
			PlatformDelete(key); // corrupt or rebound record: fail closed
			return false;
		}
		if (issued)
			*issued = std::move(resolved);
		return true;
	} catch (...) {
		if (mode == "auto")
			return false;
		throw;
	}
#else
	return false;
#endif
}

void StoreOAuthRefreshToken(const std::string &key, const OAuthRefreshContext &binding, const std::string &mode,
                            const std::string &token, bool allow_linux) {
	if (!WantsPersistence(mode, allow_linux) || token.empty())
		return;
#if defined(__APPLE__) || defined(_WIN32) || defined(VGI_OAUTH_LINUX_STORE)
	try {
		PlatformStore(key, EncodeOAuthRefreshRecord(binding, token));
		PlatformPrune();
	} catch (...) {
		if (mode != "auto")
			throw;
	}
#endif
}

void DeleteOAuthRefreshToken(const std::string &key, const std::string &mode, bool allow_linux) {
	if (!WantsPersistence(mode, allow_linux))
		return;
#if defined(__APPLE__) || defined(_WIN32) || defined(VGI_OAUTH_LINUX_STORE)
	// Forgetting an existing credential must report failure even in auto mode:
	// falling back to memory would leave the saved token available to reconnect.
	PlatformDelete(key);
#endif
}

std::string DescribeOAuthStore(const std::string &mode, bool allow_linux) {
	if (mode == "none" || mode == "memory")
		return "memory only (oauth_cache='" + mode + "')";
#if defined(__APPLE__)
	return "the macOS Keychain";
#elif defined(_WIN32)
	return "a DPAPI-protected file";
#elif defined(VGI_OAUTH_LINUX_STORE)
	if (!allow_linux)
		return "memory only (no Linux store for this credential)";
	const auto &store = LinuxStore();
	switch (store.backend) {
	case LinuxBackend::SECRET_SERVICE:
		return "the Secret Service (libsecret)";
	case LinuxBackend::FILE:
		return "an owner-only file in " + store.dir;
	default:
		return "memory only (" + store.reason + ")";
	}
#else
	return "memory only (no credential store on this platform)";
#endif
}

} // namespace vgi
} // namespace duckdb
