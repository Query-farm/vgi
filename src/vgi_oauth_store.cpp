// © Copyright 2026 Query Farm LLC - https://query.farm

#include "vgi_oauth_store.hpp"

#include "duckdb/common/exception.hpp"
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

void AppendField(std::string &out, const std::string &value) {
	uint32_t n = static_cast<uint32_t>(value.size());
	for (int i = 0; i < 4; ++i)
		out.push_back(static_cast<char>((n >> (i * 8)) & 0xff));
	out.append(value);
}

bool ReadField(const std::string &in, size_t &offset, std::string &value) {
	if (offset + 4 > in.size())
		return false;
	uint32_t n = 0;
	for (int i = 0; i < 4; ++i)
		n |= static_cast<uint32_t>(static_cast<unsigned char>(in[offset++])) << (i * 8);
	if (n > in.size() - offset)
		return false;
	value.assign(in.data() + offset, n);
	offset += n;
	return true;
}

std::string EncodeRecord(const OAuthRefreshContext &ctx, const std::string &token) {
	std::string out("VGI1", 4);
	AppendField(out, ctx.issuer);
	AppendField(out, ctx.resource.empty() ? ctx.resource_metadata_url : ctx.resource);
	AppendField(out, ctx.client_id);
	AppendField(out, ctx.scope);
	AppendField(out, token);
	return out;
}

bool DecodeRecord(const std::string &data, const OAuthRefreshContext &expected, std::string &token) {
	if (data.size() < 4 || data.compare(0, 4, "VGI1") != 0)
		return false;
	size_t off = 4;
	std::string issuer, resource, client, scope;
	if (!ReadField(data, off, issuer) || !ReadField(data, off, resource) || !ReadField(data, off, client) ||
	    !ReadField(data, off, scope) || !ReadField(data, off, token) || off != data.size())
		return false;
	const auto expected_resource = expected.resource.empty() ? expected.resource_metadata_url : expected.resource;
	return issuer == expected.issuer && resource == expected_resource && client == expected.client_id &&
	       scope == expected.scope && !token.empty();
}

bool WantsPersistence(const std::string &mode) {
	if (mode == "none" || mode == "memory")
		return false;
#if defined(__APPLE__) || defined(_WIN32)
	return mode == "auto" || mode == "persistent";
#else
	if (mode == "persistent") {
		throw IOException("VGI OAuth: persistent credential storage is not available in this Linux build; "
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
			throw IOException("VGI OAuth: could not create credential lock directory");
		}
		std::string path = dir + "/" + VgiSha256Hex(key) + ".lock";
		fd_ = open(path.c_str(), O_CREAT | O_RDWR, 0600);
		if (fd_ < 0 || flock(fd_, LOCK_EX) != 0) {
			if (fd_ >= 0)
				close(fd_);
			throw IOException("VGI OAuth: could not acquire credential cache lease");
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
		throw IOException("VGI OAuth: Keychain read failed (%d)", status);
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
		throw IOException("VGI OAuth: Keychain write failed (%d)", status);
}

void PlatformDelete(const std::string &key) {
	auto query = KeychainQuery(VgiSha256Hex(key));
	auto status = SecItemDelete(query);
	CFRelease(query);
	if (status != errSecSuccess && status != errSecItemNotFound) {
		throw IOException("VGI OAuth: Keychain delete failed (%d)", status);
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
		if (!handle_ || WaitForSingleObject(handle_, INFINITE) != WAIT_OBJECT_0) {
			if (handle_)
				CloseHandle(handle_);
			throw IOException("VGI OAuth: could not acquire credential cache lease");
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
		throw IOException("VGI OAuth: LOCALAPPDATA is unavailable");
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
		throw IOException("VGI OAuth: DPAPI credential decrypt failed");
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
		throw IOException("VGI OAuth: DPAPI credential encrypt failed");
	}
	auto path = StorePath(key);
	auto temp = path;
	temp += ".tmp." + std::to_string(GetCurrentProcessId());
	{
		std::ofstream out(temp, std::ios::binary | std::ios::trunc);
		out.write(reinterpret_cast<char *>(output.pbData), output.cbData);
		if (!out) {
			LocalFree(output.pbData);
			throw IOException("VGI OAuth: DPAPI cache write failed");
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
		throw IOException("VGI OAuth: DPAPI cache replace failed");
}

void PlatformDelete(const std::string &key) {
	std::error_code ec;
	std::filesystem::remove(StorePath(key), ec);
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
                           std::string &token) {
	if (!WantsPersistence(mode))
		return false;
#if defined(__APPLE__) || defined(_WIN32)
	try {
		std::string data;
		if (!PlatformLoad(key, data))
			return false;
		if (!DecodeRecord(data, expected, token)) {
			PlatformDelete(key); // corrupt or rebound record: fail closed
			return false;
		}
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
		PlatformStore(key, EncodeRecord(binding, token));
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
	try {
		PlatformDelete(key);
	} catch (...) {
		if (mode != "auto")
			throw;
	}
#endif
}

} // namespace vgi
} // namespace duckdb
