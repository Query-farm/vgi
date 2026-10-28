// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_arrow_ipc.hpp"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstring>

#include "nanoarrow/nanoarrow.h"

#if VGI_POSIX_TRANSPORT
#include <sys/select.h>
#include <unistd.h>
#elif defined(_WIN32)
#include <io.h> // _read, _write
#endif

#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp" // ClientContext::interrupted
#include "vgi_exception.hpp"
#include "vgi_logging.hpp" // VGI_STDERR_DEBUG (one-shot codec-fallback warning)
#include "vgi_subprocess.hpp"

#if defined(_WIN32)
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h> // overlapped ReadFile / WaitForSingleObject
#endif

namespace duckdb {
namespace vgi {

// Largest byte count handed to a single read()/write()/ReadFile()/_write().
//
// A batch whose serialized body exceeds 2 GiB used to fail on macOS with
// "Read error: Invalid argument": Darwin's read(2)/write(2) reject a count
// above INT_MAX with EINVAL rather than transferring a prefix. Linux instead
// caps silently at 0x7ffff000 and returns a short count, which is why the
// ceiling only ever showed up on macOS. Windows' _write takes an unsigned
// int, so a count at or above 4 GiB truncates -- at exactly 4 GiB it
// truncates to zero and the loop spins forever.
//
// Every one of these loops already handles a short transfer, so clamping is
// purely a portability guard: it changes how many syscalls a large payload
// costs, never how many bytes move. 1 GiB is far below every platform limit
// and keeps the extra syscalls negligible (one per GiB).
static constexpr int64_t kMaxIoChunkBytes = 1LL << 30; // 1 GiB

// ============================================================================
// Protocol State Metadata
// ============================================================================

VgiMetadataPtr CreateProtocolStateMetadata(const char *state) {
	return VgiMetadata::Make({{PROTOCOL_STATE_KEY, state}});
}

std::string GetProtocolState(const VgiMetadataPtr &metadata) {
	if (!metadata) {
		return "";
	}
	auto result = metadata->Get(PROTOCOL_STATE_KEY);
	return result ? *result : std::string();
}

void ValidateProtocolState(const VgiMetadataPtr &metadata, const char *expected_state, const std::string &context,
                           const std::string &worker_path, pid_t worker_pid) {
	auto actual_state = GetProtocolState(metadata);
	if (actual_state.empty()) {
		ThrowVgiIOException("Protocol state missing in %s response (expected '%s')", worker_path, worker_pid, "",
		                    context, expected_state);
	}
	if (actual_state != expected_state) {
		ThrowVgiIOException("Protocol state mismatch in %s: expected '%s', got '%s'", worker_path, worker_pid, "",
		                    context, expected_state, actual_state);
	}
}

// ============================================================================
// Serialization Functions
// ============================================================================

std::string ResolveDiskCompressionCodec(const std::string &requested) {
	std::string c;
	c.reserve(requested.size());
	for (char ch : requested) {
		c.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
	}
	if (c.empty() || c == "none") {
		return "none";
	}
	if (c != "zstd" && c != "lz4") {
		static std::atomic<bool> warned {false};
		if (!warned.exchange(true)) {
			VGI_STDERR_DEBUG("[VGI] result cache: unknown disk compression codec '%s' → uncompressed\n",
			                 requested.c_str());
		}
		return "none";
	}
	return c;
}

VgiBufferPtr TranscodeIpcWithCodec(const VgiBufferPtr &ipc, const std::string &codec, uint64_t level) {
	if (!ipc || codec.empty() || codec == "none") {
		return ipc;
	}
	try {
		auto batch = DeserializeBatch(ipc);
		if (!batch) {
			return ipc;
		}
		// Cache batches are single-batch streams with no per-batch custom_metadata
		// (vgi_meta lives in the blob framing), so none needs preserving.
		VgiMetadata none;
		auto out = SerializeRecordBatch(*batch, &none, codec, level);
		return out ? out : ipc;
	} catch (...) {
		return ipc; // best-effort: compression must never break caching
	}
}

VgiBufferPtr SerializeRecordBatch(const VgiBatch &batch, const VgiMetadata *custom_metadata, const std::string &codec,
                                  uint64_t level) {
	VgiIpcWriteOptions options;
	options.codec = ResolveIpcCodec(codec);
	options.level = static_cast<int>(level == 0 ? 1 : level);
	VgiMetadata none;
	return SerializeBatch(batch, custom_metadata ? custom_metadata : &none, options);
}

// Fd-based stream I/O is the subprocess/AF_UNIX data path
// (raw read/write on a pipe or socket fd). Compiled on POSIX and Windows; the
// per-syscall split is `#if VGI_POSIX_TRANSPORT … #else (Windows _read/_write)`.
// HTTP and catalog paths use the buffer-based serialization helpers above, which
// stay compiled everywhere. See vgi_platform.hpp.
#if VGI_SUBPROCESS_TRANSPORT

// FdInputStream implementation
FdInputStream::FdInputStream(int fd, ClientContext *context, bool require_complete_reads)
    : fd_(fd), position_(0), is_open_(true), context_(context), require_complete_reads_(require_complete_reads)
#if defined(_WIN32)
      ,
      read_event_(::CreateEvent(nullptr, TRUE, FALSE, nullptr))
#endif
{
}

FdInputStream::~FdInputStream() {
#if defined(_WIN32)
	if (read_event_) {
		::CloseHandle(read_event_);
		read_event_ = nullptr;
	}
#endif
}

void FdInputStream::Close() {
	is_open_ = false;
}

bool FdInputStream::closed() const {
	return !is_open_;
}

int64_t FdInputStream::Read(int64_t nbytes, void *out) {
	if (!is_open_) {
		throw IOException("Stream is closed");
	}

	// Loop until we have all requested bytes or hit EOF.
	// Pipes may return partial reads, so we must loop.
	uint8_t *buffer = static_cast<uint8_t *>(out);
	int64_t total_bytes_read = 0;

	while (total_bytes_read < nbytes) {
		// Clamped per-syscall count; see kMaxIoChunkBytes.
		const int64_t chunk = std::min<int64_t>(nbytes - total_bytes_read, kMaxIoChunkBytes);
#if VGI_POSIX_TRANSPORT
		ssize_t bytes_read = read(fd_, buffer + total_bytes_read, static_cast<size_t>(chunk));
#else
		// Windows: overlapped ReadFile so the engine thread blocks at 0% CPU and
		// wakes the instant bytes arrive (poll()-equivalent), instead of
		// busy-spinning and stealing a core from the worker. A non-overlapped
		// handle (e.g. a socket) completes synchronously and we use that result.
		// Cancellation: re-check context->IsInterrupted() every 250ms.
		int bytes_read;
		{
			DWORD want = static_cast<DWORD>(chunk);
			DWORD got = 0;
			HANDLE h = reinterpret_cast<HANDLE>(_get_osfhandle(fd_));
			OVERLAPPED ov = {};
			if (read_event_) {
				::ResetEvent(read_event_);
			}
			ov.hEvent = read_event_;
			if (::ReadFile(h, buffer + total_bytes_read, want, &got, &ov)) {
				bytes_read = static_cast<int>(got); // synchronous completion
			} else {
				DWORD e = ::GetLastError();
				if (e == ERROR_IO_PENDING) {
					// Cheap spin (no syscall — polls the OVERLAPPED completion flag)
					// to catch the common case where the next batch is already
					// in-flight and completes within nanoseconds, keeping the inline
					// RPC hot path fast. If it doesn't land quickly, fall through to a
					// blocking wait at 0% CPU so a slow worker (e.g. the shm-write
					// path) gets the core instead of our busy-spinning.
					bool completed = false;
					for (int spin = 0; spin < 8192; ++spin) {
						if (HasOverlappedIoCompleted(&ov)) {
							completed = true;
							break;
						}
						YieldProcessor();
					}
					bool cancelled = false;
					while (!completed) {
						DWORD w = ::WaitForSingleObject(read_event_, 250);
						if (w == WAIT_OBJECT_0) {
							break;
						}
						if (w == WAIT_TIMEOUT) {
							if (context_ && context_->IsInterrupted()) {
								::CancelIoEx(h, &ov);
								cancelled = true;
								break;
							}
							continue;
						}
						::CancelIoEx(h, &ov); // wait failed
						break;
					}
					if (cancelled) {
						throw IOException("VGI operation interrupted (query cancelled)");
					}
					if (::GetOverlappedResult(h, &ov, &got, FALSE)) {
						bytes_read = static_cast<int>(got);
					} else {
						DWORD ge = ::GetLastError();
						if (ge == ERROR_BROKEN_PIPE || ge == ERROR_HANDLE_EOF) {
							bytes_read = 0;
						} else {
							throw IOException("Overlapped read failed: " + std::to_string(ge));
						}
					}
				} else if (e == ERROR_BROKEN_PIPE || e == ERROR_HANDLE_EOF) {
					bytes_read = 0;
				} else {
					throw IOException("ReadFile failed: " + std::to_string(e));
				}
			}
		}
#endif
		if (bytes_read == -1) {
			if (errno == EINTR) {
				continue;
			}
			throw IOException(std::string("Read error: ") + strerror(errno));
		}
		if (bytes_read == 0) {
			// EOF reached partway through. Return the bytes we did read —
			// returning 0 here would discard them and surface as "stream
			// ended" to Arrow's IPC reader, after which any subsequent
			// reads start mid-frame and produce corrupt batches.
			break;
		}
		total_bytes_read += bytes_read;
	}

	position_ += total_bytes_read;
	if (require_complete_reads_ && total_bytes_read != nbytes) {
		throw IOException("Unexpected transport EOF before Arrow EOS marker: read %lld of %lld bytes",
		                  static_cast<long long>(total_bytes_read), static_cast<long long>(nbytes));
	}
	return total_bytes_read;
}

// FdOutputStream implementation
FdOutputStream::FdOutputStream(int fd) : fd_(fd), position_(0), is_open_(true) {
}

void FdOutputStream::Close() {
	is_open_ = false;
}

bool FdOutputStream::closed() const {
	return !is_open_;
}

void FdOutputStream::Write(const void *data, int64_t nbytes) {
	if (!is_open_) {
		throw IOException("Stream is closed");
	}

	const uint8_t *bytes = static_cast<const uint8_t *>(data);
	int64_t total_written = 0;

#if VGI_POSIX_TRANSPORT
	// Bound: an unresponsive worker that fills its kernel pipe buffer
	// would otherwise block write() forever. 60s is well above any
	// realistic RPC payload write time on a healthy worker. Mirrors the
	// fd-readiness wait (WaitForReadableUntilCancel) on the read side.
	constexpr int kWriteTimeoutSeconds = 60;
	const auto deadline =
	    std::chrono::steady_clock::now() + std::chrono::seconds(kWriteTimeoutSeconds);

	while (total_written < nbytes) {
		// Wait for the fd to become writable before each write call so
		// we never block indefinitely on a kernel-pipe-buffer-full
		// scenario where the worker is hung. If the deadline has passed
		// before select() returns writable, surface a clean IOError
		// instead of hanging the calling query forever.
		fd_set write_fds;
		FD_ZERO(&write_fds);
		FD_SET(fd_, &write_fds);

		auto remaining = std::chrono::duration_cast<std::chrono::microseconds>(
		    deadline - std::chrono::steady_clock::now());
		if (remaining.count() <= 0) {
			throw IOException("Write to worker timed out after " + std::to_string(kWriteTimeoutSeconds) +
			                  "s (worker unresponsive); " + std::to_string(total_written) + " of " +
			                  std::to_string(nbytes) + " bytes written");
		}
		struct timeval tv;
		tv.tv_sec = remaining.count() / 1000000;
		tv.tv_usec = remaining.count() % 1000000;

		int sel = select(fd_ + 1, nullptr, &write_fds, nullptr, &tv);
		if (sel < 0) {
			if (errno == EINTR) {
				continue;
			}
			throw IOException(std::string("select() error during write: ") + strerror(errno));
		}
		if (sel == 0) {
			throw IOException("Write to worker timed out after " + std::to_string(kWriteTimeoutSeconds) +
			                  "s (worker unresponsive); " + std::to_string(total_written) + " of " +
			                  std::to_string(nbytes) + " bytes written");
		}

		// Clamped per-syscall count; see kMaxIoChunkBytes.
		const int64_t chunk = std::min<int64_t>(nbytes - total_written, kMaxIoChunkBytes);
		ssize_t written = write(fd_, bytes + total_written, static_cast<size_t>(chunk));
		if (written < 0) {
			if (errno == EINTR) {
				continue;
			}
			if (errno == EPIPE) {
				throw IOException("Broken pipe writing to worker");
			}
			throw IOException(std::string("Write error: ") + strerror(errno));
		}
		total_written += written;
	}
#else // _WIN32: anonymous pipes are not selectable, so no select() pre-gate; a
      // blocking _write loop suffices for the lockstep RPC. (A hung-worker write
      // timeout would need overlapped I/O — deferred; the read side is bounded.)
	while (total_written < nbytes) {
		// Clamped per-syscall count; see kMaxIoChunkBytes. Without this a
		// count at or above 4 GiB truncates to zero in the unsigned int
		// conversion and this loop never advances.
		const int64_t chunk = std::min<int64_t>(nbytes - total_written, kMaxIoChunkBytes);
		int written = _write(fd_, bytes + total_written, static_cast<unsigned int>(chunk));
		if (written < 0) {
			if (errno == EINTR) {
				continue;
			}
			if (errno == EPIPE) {
				throw IOException("Broken pipe writing to worker");
			}
			throw IOException(std::string("Write error: ") + strerror(errno));
		}
		total_written += written;
	}
#endif

	position_ += total_written;
}

#endif // VGI_SUBPROCESS_TRANSPORT

// Extract string values from a result batch column.
// Returns empty vector if batch is null (protocol error) or has no rows (empty result).
// Callers should distinguish these cases if needed by checking the batch before calling.
std::vector<std::string> ExtractStringColumn(const VgiBatchPtr &batch, const std::string &column_name) {
	std::vector<std::string> values;
	if (!batch || batch->num_rows() == 0) {
		return values;
	}
	auto column = batch->GetColumnByName(column_name);
	if (!column) {
		throw IOException("Column '%s' not found in result batch. Available columns: %s", column_name,
		                  batch->schema()->ToString());
	}
	if (column.is_dictionary() || column.type().type != NANOARROW_TYPE_STRING) {
		throw IOException("Expected string array for column '%s', got %s", column_name, column.type_string());
	}
	for (int64_t i = 0; i < column.length(); i++) {
		if (!column.IsNull(i)) {
			values.push_back(column.GetString(i));
		}
	}
	return values;
}

namespace {

// Replace every dictionary-encoded node with its value type (keeping the node's name,
// flags and metadata). Returns whether anything changed.
bool ResolveDictionaryNode(ArrowSchema *node) {
	bool changed = false;
	if (node->dictionary) {
		ArrowSchema values;
		if (ArrowSchemaDeepCopy(node->dictionary, &values) != NANOARROW_OK) {
			throw InternalException("vgi: schema copy failed");
		}
		std::string name = node->name ? node->name : "";
		int64_t flags = node->flags & ~ARROW_FLAG_DICTIONARY_ORDERED;
		auto metadata = VgiMetadata::FromC(node->metadata).ToC();
		node->release(node);
		*node = values;
		ArrowSchemaSetName(node, name.c_str());
		node->flags = flags;
		ArrowSchemaSetMetadata(node, metadata.empty() ? nullptr : metadata.data());
		ResolveDictionaryNode(node);
		changed = true;
	}
	for (int64_t i = 0; i < node->n_children; i++) {
		changed = ResolveDictionaryNode(node->children[i]) || changed;
	}
	return changed;
}

bool HasDictionary(const ArrowSchema &node) {
	if (node.dictionary) {
		return true;
	}
	for (int64_t i = 0; i < node.n_children; i++) {
		if (HasDictionary(*node.children[i])) {
			return true;
		}
	}
	return false;
}

} // namespace

VgiSchemaPtr ResolveDictionaryTypes(const VgiSchemaPtr &schema) {
	if (!schema || !HasDictionary(schema->c_schema())) {
		return schema;
	}
	ArrowSchema copy;
	schema->ExportTo(&copy);
	ResolveDictionaryNode(&copy);
	return VgiSchema::Adopt(&copy);
}

VgiSchemaPtr DeserializeSchema(const void *data, int64_t size) {
	if (size == 0) {
		return nullptr;
	}
	VgiSchemaPtr schema;
	try {
		schema = ReadSchemaBytes(data, size);
	} catch (const std::exception &e) {
		throw IOException(std::string("Failed to deserialize Arrow schema: ") + e.what());
	}
	return ResolveDictionaryTypes(schema);
}

VgiSchemaPtr DeserializeSchema(const std::vector<uint8_t> &data) {
	return DeserializeSchema(data.data(), static_cast<int64_t>(data.size()));
}

} // namespace vgi
} // namespace duckdb
