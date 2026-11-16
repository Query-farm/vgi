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
#else
#include <unistd.h>
#endif

namespace duckdb {
namespace vgi {
namespace {

constexpr const char *kKeychainService = "com.queryfarm.vgi.oauth";

bool WantsPersistence(const std::string &mode) {
	if (mode == "none" || mode == "memory")
		return false;
#if defined(__APPLE__) || defined(_WIN32)
	return mode == "auto" || mode == "persistent";
#else
	if (mode == "persistent") {
		throw IOException(ErrorInfo(error_subtype::kUnsupported).Set(error_key::kAttachOption, "oauth_cache"),
		                  "VGI OAuth: persistent credential storage is not available in this Linux build; "
		                  "use oauth_cache='memory' or install a build with Secret Service support");
	}
	return false; // conservative Linux auto: never fall back to plaintext
#endif
}

#if defined(__APPLE__)
class PosixLease final : public OAuthCredentialLease {
public:
	explicit PosixLease(const std::string &key) {
		std::string dir = "/tmp/queryfarm-vgi-oauth-" + std::to_string(static_cast<unsigned long long>(getuid()));
		if (mkdir(dir.c_str(), 0700) != 0 && errno != EEXIST) {
			throw IOException(ErrorInfo(error_subtype::kCredentialStoreFailed), "VGI OAuth: could not create credential lock directory");
		}
		std::string path = dir + "/" + VgiSha256Hex(key) + ".lock";
		fd_ = open(path.c_str(), O_CREAT | O_RDWR, 0600);
		if (fd_ < 0 || flock(fd_, LOCK_EX) != 0) {
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
#endif

} // namespace

std::unique_ptr<OAuthCredentialLease> AcquireOAuthCredentialLease(const std::string &key, const std::string &mode) {
	if (!WantsPersistence(mode))
		return nullptr;
	try {
#if defined(__APPLE__)
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
                           std::string &token, OAuthRefreshContext *issued) {
	if (!WantsPersistence(mode))
		return false;
#if defined(__APPLE__) || defined(_WIN32)
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
                            const std::string &token) {
	if (!WantsPersistence(mode) || token.empty())
		return;
#if defined(__APPLE__) || defined(_WIN32)
	try {
		PlatformStore(key, EncodeOAuthRefreshRecord(binding, token));
		PlatformPrune();
	} catch (...) {
		if (mode != "auto")
			throw;
	}
#endif
}

void DeleteOAuthRefreshToken(const std::string &key, const std::string &mode) {
	if (!WantsPersistence(mode))
		return;
#if defined(__APPLE__) || defined(_WIN32)
	// Forgetting an existing credential must report failure even in auto mode:
	// falling back to memory would leave the saved token available to reconnect.
	PlatformDelete(key);
#endif
}

} // namespace vgi
} // namespace duckdb
