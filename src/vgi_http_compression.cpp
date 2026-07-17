// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_http_compression.hpp"

#include <cstring>

#include "duckdb.hpp"
#include "miniz.hpp"
#include "zstd.h"

namespace duckdb {
namespace vgi {

// Cap on the maximum decompressed size we accept, in bytes.  Same value used
// historically for zstd in ``vgi_http_client.cpp``: 1 GiB is generous for any
// realistic VGI RPC response and tight enough to defeat decompression bombs
// (where a tiny compressed body claims an exabyte of output to OOM the host).
static constexpr size_t kMaxDecompressedBytes = 1ULL << 30;  // 1 GiB

static constexpr int kDefaultZstdLevel = 3;
// miniz uses MZ_DEFAULT_LEVEL == 6 for deflate; we mirror it explicitly so
// the constant doesn't drift if the underlying define changes.
static constexpr int kDefaultGzipLevel = 6;

// ---------------------------------------------------------------------------
// zstd
// ---------------------------------------------------------------------------

// Reused per-thread compression / decompression contexts. The one-shot
// ZSTD_compress / ZSTD_decompress entry points allocate and free a full
// CCtx/DCtx internally on EVERY call — measurable on the per-chunk HTTP
// exchange hot path (one compress + one decompress per 2048-row vector).
// A context is created lazily per thread and reused for its lifetime;
// creation failure falls back to the one-shot API (never throws here).
namespace {

struct ZstdCCtxHolder {
	duckdb_zstd::ZSTD_CCtx *ctx = nullptr;
	~ZstdCCtxHolder() {
		if (ctx) {
			duckdb_zstd::ZSTD_freeCCtx(ctx);
		}
	}
};

struct ZstdDCtxHolder {
	duckdb_zstd::ZSTD_DCtx *ctx = nullptr;
	~ZstdDCtxHolder() {
		if (ctx) {
			duckdb_zstd::ZSTD_freeDCtx(ctx);
		}
	}
};

duckdb_zstd::ZSTD_CCtx *GetThreadZstdCCtx() {
	thread_local ZstdCCtxHolder holder;
	if (!holder.ctx) {
		holder.ctx = duckdb_zstd::ZSTD_createCCtx();
	}
	return holder.ctx;
}

duckdb_zstd::ZSTD_DCtx *GetThreadZstdDCtx() {
	thread_local ZstdDCtxHolder holder;
	if (!holder.ctx) {
		holder.ctx = duckdb_zstd::ZSTD_createDCtx();
	}
	return holder.ctx;
}

} // namespace

static std::string ZstdDecompress(const char *data, size_t size) {
	using namespace duckdb_zstd;
	auto frame_size = ZSTD_getFrameContentSize(data, size);
	if (frame_size == ZSTD_CONTENTSIZE_ERROR) {
		throw IOException("VGI zstd decompression failed: not valid zstd data");
	}

	if (frame_size != ZSTD_CONTENTSIZE_UNKNOWN) {
		if (frame_size > kMaxDecompressedBytes) {
			throw IOException(
			    "VGI zstd decompression rejected: declared frame size %llu bytes exceeds %llu byte cap",
			    static_cast<unsigned long long>(frame_size),
			    static_cast<unsigned long long>(kMaxDecompressedBytes));
		}
		std::string decompressed(frame_size, '\0');
		auto *dctx = GetThreadZstdDCtx();
		auto result = dctx ? ZSTD_decompressDCtx(dctx, decompressed.data(), frame_size, data, size)
		                   : ZSTD_decompress(decompressed.data(), frame_size, data, size);
		if (ZSTD_isError(result)) {
			throw IOException("VGI zstd decompression failed: %s", ZSTD_getErrorName(result));
		}
		decompressed.resize(result);
		return decompressed;
	}

	// Unknown size — streaming decompress with the same cap as the
	// frame-header path so a small input claiming infinite output can't
	// OOM us.
	auto *dstream = ZSTD_createDStream();
	if (!dstream) {
		throw IOException("VGI zstd decompression failed: could not create stream");
	}
	ZSTD_initDStream(dstream);

	std::string output;
	ZSTD_inBuffer input_buf = {data, size, 0};
	std::vector<char> tmp(ZSTD_DStreamOutSize());

	while (input_buf.pos < input_buf.size) {
		ZSTD_outBuffer output_buf = {tmp.data(), tmp.size(), 0};
		auto result = ZSTD_decompressStream(dstream, &output_buf, &input_buf);
		if (ZSTD_isError(result)) {
			ZSTD_freeDStream(dstream);
			throw IOException("VGI zstd decompression failed: %s", ZSTD_getErrorName(result));
		}
		if (output.size() + output_buf.pos > kMaxDecompressedBytes) {
			ZSTD_freeDStream(dstream);
			throw IOException(
			    "VGI zstd decompression rejected: streamed output exceeded %llu byte cap",
			    static_cast<unsigned long long>(kMaxDecompressedBytes));
		}
		output.append(tmp.data(), output_buf.pos);
	}

	ZSTD_freeDStream(dstream);
	return output;
}

static std::vector<uint8_t> ZstdCompress(const uint8_t *data, size_t size, int level) {
	using namespace duckdb_zstd;
	auto bound = ZSTD_compressBound(size);
	std::vector<uint8_t> compressed(bound);
	auto *cctx = GetThreadZstdCCtx();
	auto result = cctx ? ZSTD_compressCCtx(cctx, compressed.data(), bound, data, size, level)
	                   : ZSTD_compress(compressed.data(), bound, data, size, level);
	if (ZSTD_isError(result)) {
		throw IOException("VGI zstd compression failed: %s", ZSTD_getErrorName(result));
	}
	compressed.resize(result);
	return compressed;
}

// ---------------------------------------------------------------------------
// gzip (miniz)
// ---------------------------------------------------------------------------
//
// miniz's ``deflateInit2(window_bits=-15)`` emits a raw deflate stream with no
// wrapper; gzip is that stream sandwiched between a 10-byte header and an
// 8-byte footer (CRC32 of uncompressed + ISIZE mod 2^32).  Build both
// directly so we don't depend on miniz's gzip helpers being exported.

static constexpr size_t kGzipHeaderSize = 10;
static constexpr size_t kGzipFooterSize = 8;

static void WriteGzipHeader(unsigned char *out) {
	std::memset(out, 0, kGzipHeaderSize);
	out[0] = 0x1F;  // magic
	out[1] = 0x8B;
	out[2] = 0x08;  // CM = deflate
	out[3] = 0x00;  // FLG (no extras)
	// bytes 4-7: MTIME, leave zero
	// byte 8: XFL (compression-level hint, 0 = unspecified)
	out[9] = 0xFF;  // OS = unknown
}

static void WriteGzipFooter(unsigned char *out, uint32_t crc, uint32_t isize) {
	out[0] = static_cast<unsigned char>(crc & 0xFF);
	out[1] = static_cast<unsigned char>((crc >> 8) & 0xFF);
	out[2] = static_cast<unsigned char>((crc >> 16) & 0xFF);
	out[3] = static_cast<unsigned char>((crc >> 24) & 0xFF);
	out[4] = static_cast<unsigned char>(isize & 0xFF);
	out[5] = static_cast<unsigned char>((isize >> 8) & 0xFF);
	out[6] = static_cast<unsigned char>((isize >> 16) & 0xFF);
	out[7] = static_cast<unsigned char>((isize >> 24) & 0xFF);
}

static std::vector<uint8_t> GzipCompress(const uint8_t *data, size_t size, int level) {
	using namespace duckdb_miniz;
	auto bound = mz_compressBound(static_cast<mz_ulong>(size));
	// Reserve enough room for the deflate body plus the gzip header / footer.
	std::vector<uint8_t> compressed(kGzipHeaderSize + bound + kGzipFooterSize);
	WriteGzipHeader(compressed.data());

	mz_stream stream;
	std::memset(&stream, 0, sizeof(stream));
	stream.next_in = data;
	stream.avail_in = static_cast<mz_uint32>(size);
	stream.next_out = compressed.data() + kGzipHeaderSize;
	stream.avail_out = static_cast<mz_uint32>(bound);

	// window_bits = -15 → raw deflate stream (no zlib wrapper)
	auto init = mz_deflateInit2(&stream, level, MZ_DEFLATED, -15, 9, MZ_DEFAULT_STRATEGY);
	if (init != MZ_OK) {
		throw IOException("VGI gzip compression failed at init: %s", mz_error(init));
	}

	auto status = mz_deflate(&stream, MZ_FINISH);
	if (status != MZ_STREAM_END) {
		mz_deflateEnd(&stream);
		throw IOException("VGI gzip compression failed: %s", mz_error(status));
	}
	auto deflated_bytes = stream.total_out;
	mz_deflateEnd(&stream);

	// CRC32 of the original bytes — required for the gzip footer.  ISIZE is
	// the uncompressed length mod 2^32, which is fine for any payload below
	// the 1 GiB cap.
	auto crc = mz_crc32(MZ_CRC32_INIT, data, size);
	WriteGzipFooter(compressed.data() + kGzipHeaderSize + deflated_bytes,
	                static_cast<uint32_t>(crc),
	                static_cast<uint32_t>(size & 0xFFFFFFFFu));
	compressed.resize(kGzipHeaderSize + deflated_bytes + kGzipFooterSize);
	return compressed;
}

static std::string GzipDecompress(const char *data, size_t size) {
	using namespace duckdb_miniz;
	if (size < kGzipHeaderSize + kGzipFooterSize) {
		throw IOException("VGI gzip decompression failed: input shorter than gzip frame");
	}
	auto *bytes = reinterpret_cast<const unsigned char *>(data);
	if (bytes[0] != 0x1F || bytes[1] != 0x8B || bytes[2] != 0x08) {
		throw IOException("VGI gzip decompression failed: invalid gzip header");
	}
	auto flg = bytes[3];
	// We reject the flags we don't parse rather than silently treating
	// extras as raw deflate input — VGI workers don't emit FEXTRA / FNAME
	// / FCOMMENT / FHCRC / FTEXT, so a stream that has them is malformed
	// from our point of view.
	if (flg & 0xE0) {
		throw IOException("VGI gzip decompression failed: reserved FLG bits set");
	}
	if (flg & (0x04 | 0x08 | 0x10 | 0x02)) {
		throw IOException("VGI gzip decompression failed: unsupported FLG bits 0x%02x", flg);
	}

	const size_t payload_start = kGzipHeaderSize;
	const size_t payload_end = size - kGzipFooterSize;  // CRC32 + ISIZE footer
	if (payload_end < payload_start) {
		throw IOException("VGI gzip decompression failed: malformed frame");
	}

	mz_stream stream;
	std::memset(&stream, 0, sizeof(stream));
	stream.next_in = bytes + payload_start;
	stream.avail_in = static_cast<mz_uint32>(payload_end - payload_start);

	// Raw deflate inflate.
	auto init = mz_inflateInit2(&stream, -15);
	if (init != MZ_OK) {
		throw IOException("VGI gzip decompression failed at init: %s", mz_error(init));
	}

	// 64 KiB output buffer; loop until MZ_STREAM_END or cap breach.  miniz's
	// ``mz_inflate`` bounds output per call to ``avail_out`` so this is the
	// same shape as zstd's bounded streaming branch above.
	std::string output;
	std::vector<unsigned char> tmp(65536);
	while (true) {
		stream.next_out = tmp.data();
		stream.avail_out = static_cast<mz_uint32>(tmp.size());
		auto status = mz_inflate(&stream, MZ_SYNC_FLUSH);
		size_t produced = tmp.size() - stream.avail_out;
		if (produced > 0) {
			if (output.size() + produced > kMaxDecompressedBytes) {
				mz_inflateEnd(&stream);
				throw IOException(
				    "VGI gzip decompression rejected: output exceeded %llu byte cap",
				    static_cast<unsigned long long>(kMaxDecompressedBytes));
			}
			output.append(reinterpret_cast<const char *>(tmp.data()), produced);
		}
		if (status == MZ_STREAM_END) {
			break;
		}
		if (status != MZ_OK) {
			mz_inflateEnd(&stream);
			throw IOException("VGI gzip decompression failed: %s", mz_error(status));
		}
		if (produced == 0 && stream.avail_in == 0) {
			// Defensive: avoid infinite loop on a malformed stream where
			// the decoder neither consumes input nor produces output.
			mz_inflateEnd(&stream);
			throw IOException("VGI gzip decompression stalled");
		}
	}
	mz_inflateEnd(&stream);
	return output;
}

// ---------------------------------------------------------------------------
// Public dispatch
// ---------------------------------------------------------------------------

static bool TokenEqualsCI(const std::string &lhs, const char *rhs) {
	auto rhs_len = std::strlen(rhs);
	if (lhs.size() != rhs_len) {
		return false;
	}
	for (size_t i = 0; i < rhs_len; ++i) {
		char a = lhs[i];
		if (a >= 'A' && a <= 'Z') {
			a = static_cast<char>(a + ('a' - 'A'));
		}
		if (a != rhs[i]) {
			return false;
		}
	}
	return true;
}

HttpEncoding ParseEncoding(const std::string &header_value) {
	// Strip surrounding whitespace and any q= parameters.
	size_t start = 0;
	while (start < header_value.size() && (header_value[start] == ' ' || header_value[start] == '\t')) {
		++start;
	}
	size_t end = header_value.size();
	auto semi = header_value.find(';', start);
	if (semi != std::string::npos) {
		end = semi;
	}
	while (end > start && (header_value[end - 1] == ' ' || header_value[end - 1] == '\t')) {
		--end;
	}
	if (end <= start) {
		return HttpEncoding::NONE;
	}
	auto token = header_value.substr(start, end - start);
	if (TokenEqualsCI(token, "zstd")) {
		return HttpEncoding::ZSTD;
	}
	if (TokenEqualsCI(token, "gzip")) {
		return HttpEncoding::GZIP;
	}
	return HttpEncoding::NONE;
}

std::vector<HttpEncoding> ParseAcceptList(const std::string &header_value) {
	std::vector<HttpEncoding> out;
	size_t pos = 0;
	while (pos < header_value.size()) {
		auto comma = header_value.find(',', pos);
		auto next = comma == std::string::npos ? header_value.size() : comma;
		auto token = header_value.substr(pos, next - pos);
		auto enc = ParseEncoding(token);
		if (enc != HttpEncoding::NONE) {
			bool dup = false;
			for (auto existing : out) {
				if (existing == enc) {
					dup = true;
					break;
				}
			}
			if (!dup) {
				out.push_back(enc);
			}
		}
		if (comma == std::string::npos) {
			break;
		}
		pos = comma + 1;
	}
	return out;
}

const char *EncodingName(HttpEncoding encoding) {
	switch (encoding) {
	case HttpEncoding::ZSTD:
		return "zstd";
	case HttpEncoding::GZIP:
		return "gzip";
	case HttpEncoding::NONE:
	default:
		return "";
	}
}

std::vector<uint8_t> Compress(HttpEncoding encoding, const uint8_t *data, size_t size) {
	switch (encoding) {
	case HttpEncoding::ZSTD:
		return ZstdCompress(data, size, kDefaultZstdLevel);
	case HttpEncoding::GZIP:
		return GzipCompress(data, size, kDefaultGzipLevel);
	case HttpEncoding::NONE:
		return std::vector<uint8_t>(data, data + size);
	default:
		throw IOException("VGI compression: unknown encoding %d", static_cast<int>(encoding));
	}
}

std::string Decompress(HttpEncoding encoding, const char *data, size_t size) {
	switch (encoding) {
	case HttpEncoding::ZSTD:
		return ZstdDecompress(data, size);
	case HttpEncoding::GZIP:
		return GzipDecompress(data, size);
	case HttpEncoding::NONE:
		return std::string(data, size);
	default:
		throw IOException("VGI decompression: unknown encoding %d", static_cast<int>(encoding));
	}
}

} // namespace vgi
} // namespace duckdb
