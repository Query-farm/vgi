// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "vgi_location_policy.hpp"
#include "vgi_platform.hpp" // pid_t (real on POSIX, shim on Windows)

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/exception/catalog_exception.hpp"
#include "duckdb/common/exception.hpp"

#include "vgi_sha256.hpp"
#include "vgi_subprocess.hpp"

namespace duckdb {
namespace vgi {

// Convert binary bytes to hex string for logging/exceptions
// Uses lookup table for efficiency instead of snprintf per byte
inline std::string BytesToHex(const std::vector<uint8_t> &bytes) {
	static constexpr char hex_chars[] = "0123456789abcdef";
	if (bytes.empty()) {
		return "";
	}
	std::string result;
	result.resize(bytes.size() * 2);
	for (size_t i = 0; i < bytes.size(); i++) {
		result[i * 2] = hex_chars[bytes[i] >> 4];
		result[i * 2 + 1] = hex_chars[bytes[i] & 0x0F];
	}
	return result;
}

// The loggable form of an opaque value (attach_opaque_data,
// transaction_opaque_data): the first 12 hex characters of its SHA-256, ""
// when empty. The raw bytes, or their full hex, MUST NOT reach a log, trace,
// error message or telemetry attribute: some SDKs carry secret attach options
// in plaintext inside attach_opaque_data (vgi-opaque-data-sealing.md rule 7).
inline std::string OpaqueDigest(const std::vector<uint8_t> &bytes) {
	if (bytes.empty()) {
		return "";
	}
	return VgiSha256Hex(std::string(bytes.begin(), bytes.end())).substr(0, 12);
}

// Build the standard extra_info map for VGI exceptions
inline std::unordered_map<std::string, std::string> BuildExtraInfo(const std::string &worker_path,
                                                                    pid_t worker_pid = -1,
                                                                    const std::string &invocation_id_hex = "");

// ============================================================================
// Structured error context (extra_info)
// ============================================================================
//
// DuckDB exceptions carry an `extra_info` string map. Under
// `SET errors_as_json=true` every entry becomes a top-level JSON field next to
// `exception_type` / `exception_message`, so programmatic callers branch on
// fields instead of parsing English. The message keeps the same facts for the
// CLI, which does not print extra_info.
//
// The key names and `error_subtype` values below are a public contract,
// documented in docs/error_info.md. Add new ones there; never rename one.

// extra_info keys. Use these, not literals, so one fact has one spelling.
namespace error_key {
inline constexpr const char *kErrorSubtype = "error_subtype"; // DuckDB's own key
inline constexpr const char *kTransport = "transport";        // PolicyTransportName token
inline constexpr const char *kWorkerPath = "worker_path";
inline constexpr const char *kWorkerPid = "worker_pid";
inline constexpr const char *kInvocationId = "invocation_id";
inline constexpr const char *kUrl = "url";
inline constexpr const char *kHttpStatus = "http_status";
inline constexpr const char *kContentType = "content_type";
inline constexpr const char *kBodyPreview = "body_preview";
inline constexpr const char *kExitCode = "exit_code";
inline constexpr const char *kExitSignal = "exit_signal";
inline constexpr const char *kCatalog = "catalog";
inline constexpr const char *kRpcMethod = "rpc_method";
inline constexpr const char *kFunctionName = "function_name";
inline constexpr const char *kFunctionKind = "function_kind"; // SCALAR / TABLE / AGGREGATE / TABLE_IN_OUT / COPY
inline constexpr const char *kTable = "table";
inline constexpr const char *kOperation = "operation";
inline constexpr const char *kField = "field";
inline constexpr const char *kColumnIndex = "column_index";
inline constexpr const char *kColumnName = "column_name";
inline constexpr const char *kExpected = "expected";
inline constexpr const char *kActual = "actual";
inline constexpr const char *kLimit = "limit";
inline constexpr const char *kSetting = "setting";
inline constexpr const char *kAttachOption = "attach_option";
inline constexpr const char *kExceptionType = "worker_exception_type"; // the worker's own exception class
inline constexpr const char *kErrorKind = "error_kind";                // vgi_rpc.error_kind token
inline constexpr const char *kErrorCode = "error_code";                // vgi_rpc.error_code (gRPC code name)
inline constexpr const char *kTraceback = "traceback";
// catalog
inline constexpr const char *kEntityKind = "entity_kind"; // schema / table / function / ...
inline constexpr const char *kEntity = "entity";          // qualified name being resolved
inline constexpr const char *kBranchIndex = "branch_index";
inline constexpr const char *kMissingSettings = "missing_settings"; // comma-separated
// execution
inline constexpr const char *kPhase = "phase"; // INPUT / FINALIZE / ...
inline constexpr const char *kMetadataKey = "metadata_key";
inline constexpr const char *kValidationLevel = "validation_level";
// transports
inline constexpr const char *kSocketPath = "socket_path";
inline constexpr const char *kHost = "host";
inline constexpr const char *kPort = "port";
inline constexpr const char *kErrno = "errno_name"; // ECONNREFUSED, ENOENT, ...
inline constexpr const char *kContainerImage = "container_image";
inline constexpr const char *kContainerRuntime = "container_runtime";
inline constexpr const char *kContainerName = "container_name";
inline constexpr const char *kGithubRepo = "github_repo"; // owner/repo
inline constexpr const char *kGithubTag = "github_tag";
inline constexpr const char *kGithubAsset = "github_asset";
inline constexpr const char *kPackageName = "package_name";
inline constexpr const char *kPackageVersion = "package_version";
inline constexpr const char *kPlatform = "platform";
inline constexpr const char *kIrohStage = "iroh_stage";
inline constexpr const char *kIrohCategory = "iroh_category";
inline constexpr const char *kIrohDispatch = "iroh_dispatch"; // not_sent => safe to retry
// auth and policy
inline constexpr const char *kOAuthStage = "oauth_stage"; // discovery / device_code / device_poll / authorize / token_exchange / refresh
inline constexpr const char *kOAuthError = "oauth_error"; // RFC 6749 error code from the IdP
inline constexpr const char *kOAuthEndpoint = "oauth_endpoint";
inline constexpr const char *kAllowedTransports = "allowed_transports";
inline constexpr const char *kSecretType = "secret_type";
inline constexpr const char *kCompanion = "companion"; // companion catalog alias
} // namespace error_key

// `error_subtype` values. Transport-neutral: "unreachable", "not a VGI
// server", "protocol violation" mean the same on every transport.
namespace error_subtype {
// --- reaching the worker
// No response at all: connection refused, DNS, timeout, connection reset.
inline constexpr const char *kTransportFailure = "TRANSPORT_FAILURE";
// Something answered, but not a VGI server: a proxy or tunnel error page, a
// load balancer with no backend, or the wrong URL.
inline constexpr const char *kNotVgiServer = "NOT_VGI_SERVER";
// A VGI server that lacks a capability this client requires.
inline constexpr const char *kServerTooOld = "SERVER_TOO_OLD";
// A VGI HTTP server answered with a non-success status.
inline constexpr const char *kHttpError = "HTTP_ERROR";
// --- starting / running the worker
// The worker executable, image, release asset or package does not exist.
inline constexpr const char *kWorkerNotFound = "WORKER_NOT_FOUND";
// The worker exists but cannot be executed (permission denied).
inline constexpr const char *kWorkerNotExecutable = "WORKER_NOT_EXECUTABLE";
// The worker exited with a non-zero code (`exit_code`).
inline constexpr const char *kWorkerExited = "WORKER_EXITED";
// The worker was killed by a signal (`exit_signal`).
inline constexpr const char *kWorkerKilled = "WORKER_KILLED";
// The worker did not become ready before the deadline.
inline constexpr const char *kWorkerStartTimeout = "WORKER_START_TIMEOUT";
// The worker could not be connected to (refused, missing socket, timeout).
inline constexpr const char *kConnectFailed = "CONNECT_FAILED";
// The remote peer's identity did not match the one configured.
inline constexpr const char *kPeerIdentityMismatch = "PEER_IDENTITY_MISMATCH";
// No container runtime is available.
inline constexpr const char *kRuntimeNotFound = "RUNTIME_NOT_FOUND";
// The worker raised an exception in its own code (`worker_exception_type`,
// `error_kind`, `traceback`).
inline constexpr const char *kWorkerException = "WORKER_EXCEPTION";
// --- what the worker sent back
// The worker answered but broke the protocol: missing or malformed content,
// wrong row count, invalid metadata.
inline constexpr const char *kProtocolViolation = "PROTOCOL_VIOLATION";
// An Arrow batch failed validation or would not decode.
inline constexpr const char *kMalformedBatch = "MALFORMED_BATCH";
// A batch's columns differ from the schema declared at bind.
inline constexpr const char *kSchemaMismatch = "SCHEMA_MISMATCH";
inline constexpr const char *kChecksumMismatch = "CHECKSUM_MISMATCH";
// The response exceeded a client-side size bound.
inline constexpr const char *kResponseTooLarge = "RESPONSE_TOO_LARGE";
// The response exceeded a client-side count bound (pages, splits, ...).
inline constexpr const char *kLimitExceeded = "LIMIT_EXCEEDED";
// --- auth
inline constexpr const char *kAuthRequired = "AUTH_REQUIRED";
inline constexpr const char *kAuthFailed = "AUTH_FAILED";
// The OAuth identity provider rejected a request (`oauth_error`).
inline constexpr const char *kOAuthFailed = "OAUTH_FAILED";
// The device-code or browser wait expired.
inline constexpr const char *kOAuthTimeout = "OAUTH_TIMEOUT";
// OAuth resource metadata or provider configuration is missing or invalid.
inline constexpr const char *kOAuthDiscoveryFailed = "OAUTH_DISCOVERY_FAILED";
// The platform credential store (Keychain, DPAPI) failed.
inline constexpr const char *kCredentialStoreFailed = "CREDENTIAL_STORE_FAILED";
inline constexpr const char *kSecretLookupFailed = "SECRET_LOOKUP_FAILED";
// --- policy and configuration
// `vgi_allowed_transports` does not permit this LOCATION's transport.
inline constexpr const char *kTransportNotAllowed = "TRANSPORT_NOT_ALLOWED";
// A local transport was used with `enable_external_access=false`.
inline constexpr const char *kExternalAccessDisabled = "EXTERNAL_ACCESS_DISABLED";
inline constexpr const char *kInvalidAttachOption = "INVALID_ATTACH_OPTION";
// A function needs session settings that are unset.
inline constexpr const char *kMissingSettings = "MISSING_SETTINGS";
inline constexpr const char *kReadOnly = "READ_ONLY";
inline constexpr const char *kUnsupported = "UNSUPPORTED";
inline constexpr const char *kCompanionAttachFailed = "COMPANION_ATTACH_FAILED";
// A local resource could not be set up (directory, lock, shared memory).
inline constexpr const char *kLocalResource = "LOCAL_RESOURCE";
} // namespace error_subtype

// The HTTP client predates the transport-neutral names.
namespace http_error = error_subtype;

// Remove credentials from a URL or connection string before it is reported:
// URI userinfo (`scheme://user:pass@host` keeps `user`) and the value of any
// `password=` / `pwd=` / `secret=` / `token=` key=value pair.
inline std::string RedactCredentials(const std::string &text) {
	std::string out = text;
	// URI userinfo password.
	auto scheme_end = out.find("://");
	if (scheme_end != std::string::npos) {
		auto authority = scheme_end + 3;
		auto authority_end = out.find_first_of("/?#", authority);
		auto at = out.rfind('@', authority_end == std::string::npos ? std::string::npos : authority_end);
		if (at != std::string::npos && at >= authority) {
			auto colon = out.find(':', authority);
			if (colon != std::string::npos && colon < at) {
				out.replace(colon + 1, at - colon - 1, "***");
			}
		}
	}
	// key=value secrets (libpq / ODBC style, also query strings).
	static const char *const kSecretKeys[] = {"password", "pwd", "secret", "token"};
	std::string lower = out;
	std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c) { return std::tolower(c); });
	for (const char *key : kSecretKeys) {
		const std::string needle = std::string(key) + "=";
		size_t pos = 0;
		while ((pos = lower.find(needle, pos)) != std::string::npos) {
			const bool at_boundary = pos == 0 || !std::isalnum(static_cast<unsigned char>(lower[pos - 1]));
			const auto value_start = pos + needle.size();
			if (!at_boundary) {
				pos = value_start;
				continue;
			}
			auto value_end = out.find_first_of(" &;", value_start);
			if (value_end == std::string::npos) {
				value_end = out.size();
			}
			out.replace(value_start, value_end - value_start, "***");
			lower.replace(value_start, value_end - value_start, "***");
			pos = value_start + 3;
		}
	}
	return out;
}

// Fluent builder for extra_info. It IS the map, so it passes straight to any
// DuckDB exception constructor:
//   throw IOException(ErrorInfo(error_subtype::kProtocolViolation).Rpc(method).Worker(path), "...");
// Empty values are skipped, so callers need not guard optional context.
class ErrorInfo : public std::unordered_map<std::string, std::string> {
public:
	ErrorInfo() = default;
	explicit ErrorInfo(const char *subtype) {
		Set(error_key::kErrorSubtype, subtype);
	}

	ErrorInfo &Set(const char *key, const std::string &value) {
		if (!value.empty()) {
			(*this)[key] = value;
		}
		return *this;
	}
	ErrorInfo &Set(const char *key, int64_t value) {
		(*this)[key] = std::to_string(value);
		return *this;
	}
	ErrorInfo &Worker(const std::string &worker_path, pid_t worker_pid = -1,
	                  const std::string &invocation_id_hex = std::string()) {
		Set(error_key::kWorkerPath, RedactCredentials(worker_path));
		if (find(error_key::kTransport) == end()) {
			Set(error_key::kTransport, TransportNameForLocation(worker_path));
		}
		if (worker_pid > 0) {
			Set(error_key::kWorkerPid, static_cast<int64_t>(worker_pid));
		}
		return Set(error_key::kInvocationId, invocation_id_hex);
	}
	ErrorInfo &Url(const std::string &url) {
		return Set(error_key::kUrl, RedactCredentials(url));
	}
	ErrorInfo &HttpStatus(int status) {
		if (status >= 0) {
			Set(error_key::kHttpStatus, static_cast<int64_t>(status));
		}
		return *this;
	}
	ErrorInfo &Rpc(const std::string &method) {
		return Set(error_key::kRpcMethod, method);
	}
	ErrorInfo &Function(const std::string &name, const std::string &kind = std::string()) {
		Set(error_key::kFunctionName, name);
		return Set(error_key::kFunctionKind, kind);
	}
	ErrorInfo &ExpectedActual(const std::string &expected, const std::string &actual) {
		Set(error_key::kExpected, expected);
		return Set(error_key::kActual, actual);
	}
	// Fill keys this map lacks from `other`; existing (more specific) values win.
	ErrorInfo &Merge(const std::unordered_map<std::string, std::string> &other) {
		for (const auto &entry : other) {
			if (entry.first != "stack_trace_pointers") {
				emplace(entry.first, entry.second);
			}
		}
		return *this;
	}
};

inline std::unordered_map<std::string, std::string> BuildExtraInfo(const std::string &worker_path, pid_t worker_pid,
                                                                    const std::string &invocation_id_hex) {
	return std::move(ErrorInfo().Worker(worker_path, worker_pid, invocation_id_hex));
}

// extra_info for an HTTP-transport error. `http_status` < 0 means no status
// (no response was received).
inline ErrorInfo BuildHttpExtraInfo(const char *subtype, const std::string &url, int http_status = -1) {
	return std::move(ErrorInfo(subtype).Set(error_key::kTransport, url.rfind("https", 0) == 0 ? "https" : "http")
	                     .Url(url)
	                     .HttpStatus(http_status));
}

// The message of any exception WITHOUT DuckDB's JSON envelope. DuckDB's
// what() is always the JSON form, so `"...: %s", e.what()` nests JSON inside
// the new message. Use this (and ExtraInfoOf) when wrapping.
inline std::string RawMessageOf(const std::exception &e) {
	return ErrorData(e).RawMessage();
}

// The extra_info an exception carries (empty for non-DuckDB exceptions).
inline ErrorInfo ExtraInfoOf(const std::exception &e) {
	ErrorInfo info;
	info.Merge(ErrorData(e).ExtraInfo());
	return info;
}

// Rethrow `type` with `extra_info` and `message`, as the concrete DuckDB
// exception class. ErrorData::Throw() raises the base `Exception`, which
// slips past `catch (const IOException &)` (the pool's retry handler) and
// every other typed catch, so enrichment must not use it.
[[noreturn]] inline void ThrowTypedException(ExceptionType type, const ErrorInfo &extra_info,
                                             const std::string &message) {
	switch (type) {
	case ExceptionType::IO:
		throw IOException(extra_info, message);
	case ExceptionType::INVALID_INPUT:
		throw InvalidInputException(extra_info, message);
	case ExceptionType::BINDER:
		throw BinderException(extra_info, message);
	case ExceptionType::CATALOG:
		throw CatalogException(extra_info, message);
	case ExceptionType::NOT_IMPLEMENTED:
		throw NotImplementedException(extra_info, message);
	case ExceptionType::INTERRUPT:
		throw InterruptException();
	default:
		// PermissionException, ConversionException, ... have no extra_info
		// constructor; nothing in VGI catches them by type.
		throw Exception(extra_info, type, message);
	}
}

// Build message with worker path context for CLI visibility
// The worker_path is included in the message since DuckDB CLI doesn't display extra_info
inline std::string BuildMessageWithContext(const std::string &msg, const std::string &worker_path) {
	if (worker_path.empty()) {
		return msg;
	}
	return msg + " [worker: " + worker_path + "]";
}

// Throw an IOException with worker context
// Usage: ThrowVgiIOException("Failed to do X: %s", worker_path, pid, invocation_id_hex, error_msg);
// Note: worker_path is included in the message for CLI visibility, plus stored in extra_info
template <typename... ARGS>
[[noreturn]] void ThrowVgiIOException(const std::string &msg, const std::string &worker_path, pid_t worker_pid,
                                      const std::string &invocation_id_hex, ARGS... params) {
	auto extra_info = BuildExtraInfo(worker_path, worker_pid, invocation_id_hex);
	auto full_msg = BuildMessageWithContext(msg, worker_path);
	throw IOException(extra_info, full_msg, params...);
}

// Throw an IOException with worker context (no format args)
[[noreturn]] inline void ThrowVgiIOException(const std::string &msg, const std::string &worker_path,
                                             pid_t worker_pid, const std::string &invocation_id_hex = "") {
	auto extra_info = BuildExtraInfo(worker_path, worker_pid, invocation_id_hex);
	auto full_msg = BuildMessageWithContext(msg, worker_path);
	throw IOException(extra_info, full_msg);
}

// Typed VGI RPC exception subclass — adds an `error_kind` field that callers
// can inspect at catch time without parsing the message text.
//
// `error_kind` is an open-enum string token mirrored from the
// `vgi_rpc.error_kind` metadata key on EXCEPTION-level batches (see
// vgi-rpc Python's metadata.ERROR_KIND_KEY). Empty string when the worker
// did not advertise a kind.
//
// Subclass of InvalidInputException so:
//   1. Existing `catch (const InvalidInputException &)` callers in the
//      retry-suppression path keep working unchanged.
//   2. Callers that DO want to pattern-match (e.g. capability-detection
//      fallback) catch `const VgiRpcException &` and read `GetErrorKind()`.
class VgiRpcException : public InvalidInputException {
public:
	VgiRpcException(const std::unordered_map<std::string, std::string> &extra_info,
	                const std::string &msg, std::string error_kind, std::string error_code = "")
	    : InvalidInputException(extra_info, msg), error_kind_(std::move(error_kind)),
	      error_code_(std::move(error_code)) {
	}

	const std::string &GetErrorKind() const noexcept {
		return error_kind_;
	}

	// Canonical code (`vgi_rpc.error_code`, a gRPC code name such as
	// "UNIMPLEMENTED"), or empty when the worker sent none.
	const std::string &GetErrorCode() const noexcept {
		return error_code_;
	}

private:
	std::string error_kind_;
	std::string error_code_;
};

// Throw an InvalidInputException (or VgiRpcException if `error_kind` is set)
// for user-code exceptions bubbling out of worker Python code. This is distinct
// from IOException (transport failures) so that retry logic in
// InvokePooledUnaryRpc does NOT retry user-code errors — retrying them on a
// fresh worker is unsafe for stateful operations (the fresh worker has no
// state populated by previous update/combine calls) and also masks the real
// user-code error behind a silent NULL result.
[[noreturn]] inline void ThrowVgiUserException(const std::string &msg, const std::string &worker_path,
                                                pid_t worker_pid, const std::string &invocation_id_hex = "",
                                                const std::string &error_kind = "",
                                                const std::string &error_code = "",
                                                const ErrorInfo &context = ErrorInfo()) {
	ErrorInfo extra_info = context;
	extra_info.Worker(worker_path, worker_pid, invocation_id_hex)
	    .Set(error_key::kErrorKind, error_kind)
	    .Set(error_key::kErrorCode, error_code);
	auto full_msg = BuildMessageWithContext(msg, worker_path);
	if (!error_kind.empty() || !error_code.empty()) {
		throw VgiRpcException(extra_info, full_msg, error_kind, error_code);
	}
	throw InvalidInputException(extra_info, full_msg);
}

// Well-known `error_kind` token values. Open enum — new tokens may appear
// at any time, callers should treat unknown values as "kind not recognised".
namespace error_kind {
inline constexpr const char *kMethodNotImplemented = "method_not_implemented";
// The worker does not host the protocol the request named (vgi-rpc WIRE_PROTOCOL
// §3.1). The capability-probe answer for an optional protocol.
inline constexpr const char *kProtocolNotSupported = "protocol_not_supported";
} // namespace error_kind

// Format captured worker stderr into a suffix appended to worker-failure
// messages, so the user sees *why* the worker died (Python traceback, missing
// module, "command not found", etc.). Returns "" when there is no stderr.
//
// Tail-capped: a worker can spew arbitrarily much before dying, but the useful
// part of a traceback is the bottom (the exception type/message), so we keep the
// last ~kMaxLines lines / ~kMaxBytes bytes and prepend an omitted-lines marker
// if we dropped anything. Each retained line is indented two spaces under a
// "worker stderr:" header.
inline std::string FormatWorkerStderrSuffix(const std::string &worker_stderr) {
	if (worker_stderr.empty()) {
		return "";
	}
	static constexpr size_t kMaxLines = 40;
	static constexpr size_t kMaxBytes = 4096;

	// Split into lines (no trailing-empty padding beyond what the input has).
	std::vector<std::string> lines;
	size_t start = 0;
	while (start <= worker_stderr.size()) {
		size_t nl = worker_stderr.find('\n', start);
		if (nl == std::string::npos) {
			lines.push_back(worker_stderr.substr(start));
			break;
		}
		lines.push_back(worker_stderr.substr(start, nl - start));
		start = nl + 1;
	}

	// Keep the tail by line count, then trim further by byte budget from the top.
	size_t first = lines.size() > kMaxLines ? lines.size() - kMaxLines : 0;
	size_t total_bytes = 0;
	for (size_t i = lines.size(); i > first; i--) {
		total_bytes += lines[i - 1].size() + 1; // +1 for the newline join
	}
	while (first < lines.size() && total_bytes > kMaxBytes) {
		total_bytes -= lines[first].size() + 1;
		first++;
	}

	std::string suffix = "\nworker stderr:";
	if (first > 0) {
		suffix += "\n  [... " + std::to_string(first) + " earlier line(s) omitted ...]";
	}
	for (size_t i = first; i < lines.size(); i++) {
		suffix += "\n  " + lines[i];
	}
	return suffix;
}

// Throw an IOException for a worker that exited with an error. The captured
// worker stderr (tail-capped) is appended *after* the "[worker: …]" context tag
// so the message reads:
//   VGI worker <ctx> (exit code N) [worker: <path>]
//   worker stderr:
//     <captured stderr tail>
[[noreturn]] inline void ThrowVgiWorkerExitException(const std::string &msg, const std::string &worker_path,
                                                     pid_t worker_pid, const std::string &invocation_id_hex,
                                                     const std::string &worker_stderr,
                                                     const ErrorInfo &context = ErrorInfo()) {
	ErrorInfo extra_info = context;
	extra_info.Worker(worker_path, worker_pid, invocation_id_hex);
	auto full_msg = BuildMessageWithContext(msg, worker_path) + FormatWorkerStderrSuffix(worker_stderr);
	throw IOException(extra_info, full_msg);
}

// Check if a worker process exited with an error and throw appropriate exception.
// Returns true if the process has exited, false if still running.
// Throws VgiIOException for exit codes 127 (not found), 126 (permission denied), or other non-zero.
// The error_context parameter customizes the message for non-special exit codes:
// - "failed to start" for errors during read attempts
// - "exited with status" for EOF/null batch cases
// worker_stderr (typically StderrDrainer::CaptureStderrSnapshot()) is appended to
// the message tail-capped so the user sees the worker's own error output.
inline bool CheckWorkerExitStatus(SubProcess &proc, const std::string &worker_path, const std::string &error_context,
                                  const std::string &invocation_id_hex = "", const std::string &worker_stderr = "") {
	int exit_status = 0;
	// Every caller reaches here from a *transport* failure (EPIPE on write, EOF /
	// error on read), i.e. the worker's pipe end is already closed. A dying child
	// closes its fds during process teardown, so the parent routinely observes
	// that EOF a few scheduler slices BEFORE the child becomes reapable — a bare
	// waitpid(WNOHANG) then reports "still running", the caller rethrows the raw
	// "RPC response stream EOF" and the user loses both the exit code and the
	// captured stderr that explains the crash. Give the child a bounded grace
	// window to be reaped instead. This costs nothing when it has already exited
	// (first TryWait wins) and is only ever paid on an error path that is about
	// to throw anyway.
	if (!proc.TryWait(&exit_status)) {
		constexpr int kExitGraceMs = 500;
		auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(kExitGraceMs);
		bool exited = false;
		int sleep_us = 200;
		while (std::chrono::steady_clock::now() < deadline) {
			std::this_thread::sleep_for(std::chrono::microseconds(sleep_us));
			if (proc.TryWait(&exit_status)) {
				exited = true;
				break;
			}
			sleep_us = std::min(sleep_us * 2, 5000);
		}
		if (!exited) {
			return false; // Genuinely still running
		}
	}

	// Build the messages by concatenation (no printf) so the stderr block lands
	// *after* the "[worker: …]" context tag that ThrowVgiWorkerExitException
	// appends, and so worker stderr containing '%' is never format-interpreted.
	if (exit_status == 127) {
		ThrowVgiWorkerExitException("VGI worker not found or not executable", worker_path, proc.GetPid(),
		                            invocation_id_hex, worker_stderr,
		                            ErrorInfo(error_subtype::kWorkerNotFound).Set(error_key::kExitCode, 127));
	} else if (exit_status == 126) {
		ThrowVgiWorkerExitException("VGI worker permission denied", worker_path, proc.GetPid(), invocation_id_hex,
		                            worker_stderr,
		                            ErrorInfo(error_subtype::kWorkerNotExecutable).Set(error_key::kExitCode, 126));
	}
#if !defined(_WIN32)
	// TryWait reports a signal death as the negated signal number. POSIX
	// only: a Windows NTSTATUS exit code (0xC0000005) is negative as an int.
	if (exit_status < 0 && exit_status != -1) {
		ThrowVgiWorkerExitException("VGI worker " + error_context + " (killed by signal " +
		                                std::to_string(-exit_status) + ")",
		                            worker_path, proc.GetPid(), invocation_id_hex, worker_stderr,
		                            ErrorInfo(error_subtype::kWorkerKilled)
		                                .Set(error_key::kExitSignal, static_cast<int64_t>(-exit_status)));
	}
#endif
	if (exit_status != 0) {
		ThrowVgiWorkerExitException("VGI worker " + error_context + " (exit code " + std::to_string(exit_status) + ")",
		                            worker_path, proc.GetPid(), invocation_id_hex, worker_stderr,
		                            ErrorInfo(error_subtype::kWorkerExited)
		                                .Set(error_key::kExitCode, static_cast<int64_t>(exit_status)));
	}

	return true; // Process exited normally (exit_status == 0)
}

} // namespace vgi
} // namespace duckdb
