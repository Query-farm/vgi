// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Layer-1 unit tests for the transport-detection layer in vgi_transport.cpp.
// Validates that LOCATION strings dispatch to the correct TransportType and
// that scheme stripping produces the expected payloads.

#include "catch.hpp"

#include "vgi_transport.hpp"
#include "vgi_httpi.hpp"
#include "vgi_rpc_client.hpp"

#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>

#include <algorithm>
#include <cstring>
#include <stdexcept>

using duckdb::vgi::CanonicalizeBrowserWorkerTarget;
using duckdb::vgi::CanonicalizeHttpiLocation;
using duckdb::vgi::CanonicalizeIrohLocation;
using duckdb::vgi::DetectTransport;
using duckdb::vgi::IsContainerLocation;
using duckdb::vgi::IsContainerSharedLocation;
using duckdb::vgi::IsDatabaseLocation;
using duckdb::vgi::IsGithubAutoLocation;
using duckdb::vgi::IsGithubLocation;
using duckdb::vgi::IsHttpiTransport;
using duckdb::vgi::IsHttpTransport;
using duckdb::vgi::IsIrohTransport;
using duckdb::vgi::IsLaunchLocation;
using duckdb::vgi::IsResolvedWorkerLocation;
using duckdb::vgi::IsTcpTransport;
using duckdb::vgi::IsUnixLocation;
using duckdb::vgi::IsWebWorkerTransport;
using duckdb::vgi::ParseHttpiUrl;
using duckdb::vgi::ParseTcpLocation;
using duckdb::vgi::StripContainerScheme;
using duckdb::vgi::StripGithubAutoScheme;
using duckdb::vgi::StripGithubScheme;
using duckdb::vgi::StripLaunchScheme;
using duckdb::vgi::StripUnixScheme;
using duckdb::vgi::StripWebWorkerScheme;
using duckdb::vgi::TransportType;

namespace {

// Models a transport that delivered a valid batch and then failed before the
// Arrow EOS marker. Physical EOF is deliberately an I/O error, matching an
// Iroh idle timeout/reset rather than a clean half-close.
class ErrorAtEofInputStream final : public arrow::io::InputStream {
public:
	explicit ErrorAtEofInputStream(std::shared_ptr<arrow::Buffer> bytes) : bytes_(std::move(bytes)) {
	}

	arrow::Status Close() override {
		closed_ = true;
		return arrow::Status::OK();
	}
	bool closed() const override {
		return closed_;
	}
	arrow::Result<int64_t> Tell() const override {
		return position_;
	}
	arrow::Result<int64_t> Read(int64_t nbytes, void *out) override {
		if (closed_) {
			return arrow::Status::Invalid("test stream is closed");
		}
		if (position_ >= bytes_->size()) {
			return arrow::Status::IOError("injected transport reset before EOS");
		}
		auto count = std::min(nbytes, bytes_->size() - position_);
		std::memcpy(out, bytes_->data() + position_, static_cast<size_t>(count));
		position_ += count;
		return count;
	}
	arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
		ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes));
		ARROW_ASSIGN_OR_RAISE(auto count, Read(nbytes, buffer->mutable_data()));
		ARROW_RETURN_NOT_OK(buffer->Resize(count, false));
		return std::shared_ptr<arrow::Buffer>(std::move(buffer));
	}

private:
	std::shared_ptr<arrow::Buffer> bytes_;
	int64_t position_ = 0;
	bool closed_ = false;
};

} // namespace

TEST_CASE("DetectTransport routes each scheme correctly", "[transport]") {
	CHECK(DetectTransport("http://localhost:8080") == TransportType::HTTP);
	CHECK(DetectTransport("HTTPS://example.com") == TransportType::HTTP);
	CHECK(DetectTransport("launch:python -m worker") == TransportType::LAUNCH);
	CHECK(DetectTransport("unix:///tmp/foo.sock") == TransportType::UNIX);
	CHECK(DetectTransport("oci://ghcr.io/org/img:tag") == TransportType::CONTAINER);
	CHECK(DetectTransport("docker://library/python:3.13") == TransportType::CONTAINER);
	CHECK(DetectTransport("tcp://localhost:9400") == TransportType::TCP);
	CHECK(DetectTransport("TCP://[fd7a:115c:a1e0::1]:9400") == TransportType::TCP);
	CHECK(DetectTransport("worker:/workers/example.js") == TransportType::WEBWORKER);
	CHECK(DetectTransport("worker:https://cdn.example.com/w.js") == TransportType::WEBWORKER);
	CHECK(DetectTransport("worker:example") == TransportType::WEBWORKER);
	CHECK(DetectTransport("iroh://0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef") ==
	      TransportType::IROH);
	CHECK(DetectTransport("httpi://0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef/vgi") ==
	      TransportType::HTTPI);
	CHECK(DetectTransport("database://memory/main/workers/example?package_version=1") == TransportType::DATABASE);
	CHECK(DetectTransport("/path/to/worker") == TransportType::SUBPROCESS);
	CHECK(DetectTransport("/path/with launch: in middle") == TransportType::SUBPROCESS);
	CHECK(DetectTransport("") == TransportType::SUBPROCESS);
	CHECK(IsDatabaseLocation("DATABASE://memory/main/packages/worker?package_version=1"));
	CHECK_FALSE(IsDatabaseLocation("/database://not-a-scheme"));
	CHECK(IsResolvedWorkerLocation("vgi-artifact:012345"));
}

TEST_CASE("httpi locations preserve a strict canonical base path", "[transport]") {
	const std::string endpoint = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
	CHECK_FALSE(IsHttpiTransport("HTTPI://" + endpoint));
	CHECK_THROWS_AS(CanonicalizeHttpiLocation("HTTPI://" + endpoint + "/vgi/"), std::invalid_argument);
	CHECK(CanonicalizeHttpiLocation("httpi://" + endpoint + "/") == "httpi://" + endpoint);
	CHECK_THROWS_AS(CanonicalizeHttpiLocation("httpi://" + endpoint + "/vgi/"), std::invalid_argument);
	auto parsed = ParseHttpiUrl("httpi://" + endpoint + "/api/v1/catalog_attach");
	CHECK(parsed.endpoint_id == endpoint);
	CHECK(parsed.path == "/api/v1/catalog_attach");
	CHECK(ParseHttpiUrl("httpi://" + endpoint).path.empty());

	CHECK_THROWS_AS(ParseHttpiUrl("httpi://" + endpoint.substr(1)), std::invalid_argument);
	CHECK_THROWS_AS(ParseHttpiUrl("httpi://" + std::string(64, 'A')), std::invalid_argument);
	CHECK_THROWS_AS(ParseHttpiUrl("httpi://" + endpoint + "relative"), std::invalid_argument);
	CHECK_THROWS_AS(ParseHttpiUrl("httpi://" + endpoint + "/a//b"), std::invalid_argument);
	CHECK_THROWS_AS(ParseHttpiUrl("httpi://" + endpoint + "/a/../b"), std::invalid_argument);
	CHECK_THROWS_AS(ParseHttpiUrl("httpi://" + endpoint + "/a/%2e%2e/b"), std::invalid_argument);
	CHECK_THROWS_AS(ParseHttpiUrl("httpi://" + endpoint + "/a%2fb"), std::invalid_argument);
	CHECK_THROWS_AS(ParseHttpiUrl("httpi://" + endpoint + "/a%5cb"), std::invalid_argument);
	CHECK_THROWS_AS(ParseHttpiUrl("httpi://" + endpoint + "/a\x7f" "b"), std::invalid_argument);
	CHECK_THROWS_AS(ParseHttpiUrl("httpi://" + endpoint + "/a?x=1"), std::invalid_argument);
	CHECK_THROWS_AS(ParseHttpiUrl("httpi://" + endpoint + "/a#fragment"), std::invalid_argument);
	CHECK_THROWS_AS(ParseHttpiUrl("httpi://" + endpoint + "/has space"), std::invalid_argument);
}

TEST_CASE("httpi response heads distinguish HTTP from pre-response terminal evidence", "[transport]") {
	using namespace duckdb::vgi::httpi;
	CHECK(ClassifyResponseHead(kRawRepresentation, 200, 2) == ResponseHeadKind::HTTP);
	CHECK(ClassifyResponseHead(kRawRepresentation | kTerminalOnly, 0, 0) == ResponseHeadKind::TERMINAL_ONLY);
	CHECK(ClassifyResponseHead(kRawRepresentation, 0, 0) == ResponseHeadKind::INVALID);
	CHECK(ClassifyResponseHead(kRawRepresentation | kTerminalOnly, 200, 0) == ResponseHeadKind::INVALID);
	CHECK(ClassifyResponseHead(kRawRepresentation | kTerminalOnly, 0, 1) == ResponseHeadKind::INVALID);
	CHECK(ClassifyResponseHead(kRawRepresentation | 0x8000, 200, 0) == ResponseHeadKind::INVALID);
}

TEST_CASE("stream drain rejects a transport failure before Arrow EOS", "[transport]") {
	auto schema = arrow::schema({arrow::field("result", arrow::int32())});
	arrow::Int32Builder builder;
	REQUIRE(builder.Append(42).ok());
	std::shared_ptr<arrow::Array> values;
	REQUIRE(builder.Finish(&values).ok());
	auto batch = arrow::RecordBatch::Make(schema, 1, {values});

	auto sink_result = arrow::io::BufferOutputStream::Create();
	REQUIRE(sink_result.ok());
	auto sink = sink_result.ValueUnsafe();
	auto writer_result = arrow::ipc::MakeStreamWriter(sink, schema);
	REQUIRE(writer_result.ok());
	auto writer = writer_result.ValueUnsafe();
	REQUIRE(writer->WriteRecordBatch(*batch).ok());
	REQUIRE(writer->Close().ok());
	auto bytes_result = sink->Finish();
	REQUIRE(bytes_result.ok());
	auto bytes = bytes_result.ValueUnsafe();
	REQUIRE(bytes->size() > 8);

	// Arrow stream EOS is an 8-byte continuation + zero-length marker. Remove
	// it so the next drain read reaches the transport error instead.
	auto truncated = arrow::SliceBuffer(bytes, 0, bytes->size() - 8);
	auto input = std::make_shared<ErrorAtEofInputStream>(std::move(truncated));
	CHECK_THROWS(duckdb::vgi::ReadUnaryResponse(input, nullptr, "iroh://test"));
}

TEST_CASE("iroh locations are strict canonical browser targets", "[transport]") {
	const std::string endpoint = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
	const std::string canonical = "iroh://" + endpoint;
	CHECK(IsIrohTransport(canonical));
	CHECK_FALSE(IsIrohTransport("IROH://" + endpoint));
	CHECK_THROWS_AS(CanonicalizeIrohLocation("IROH://" + endpoint), std::invalid_argument);
	CHECK(CanonicalizeBrowserWorkerTarget(canonical) == canonical);
	CHECK(CanonicalizeBrowserWorkerTarget("worker:/workers/Example.js") == "/workers/Example.js");

	CHECK_THROWS_AS(CanonicalizeIrohLocation("iroh://"), std::invalid_argument);
	CHECK_THROWS_AS(CanonicalizeIrohLocation("iroh://" + endpoint.substr(1)), std::invalid_argument);
	CHECK_THROWS_AS(CanonicalizeIrohLocation("iroh://" + endpoint + "0"), std::invalid_argument);
	auto uppercase_id = endpoint;
	uppercase_id[10] = 'A';
	CHECK_THROWS_AS(CanonicalizeIrohLocation("iroh://" + uppercase_id), std::invalid_argument);
	auto non_hex_id = endpoint;
	non_hex_id[10] = 'g';
	CHECK_THROWS_AS(CanonicalizeIrohLocation("iroh://" + non_hex_id), std::invalid_argument);
	CHECK_THROWS_AS(CanonicalizeIrohLocation(canonical + "/path"), std::invalid_argument);
	CHECK_THROWS_AS(CanonicalizeIrohLocation(canonical + "?query=1"), std::invalid_argument);
	CHECK_THROWS_AS(CanonicalizeBrowserWorkerTarget("http://example.test"), std::invalid_argument);
}

TEST_CASE("TCP locations parse hostnames, IPv4, and bracketed IPv6", "[transport]") {
	std::string host;
	int port = 0;

	ParseTcpLocation("tcp://worker.example:9400", host, port);
	CHECK(host == "worker.example");
	CHECK(port == 9400);

	ParseTcpLocation("tcp://100.101.102.103:1", host, port);
	CHECK(host == "100.101.102.103");
	CHECK(port == 1);

	ParseTcpLocation("TCP://[fd7a:115c:a1e0::1234:5678]:65535", host, port);
	CHECK(host == "fd7a:115c:a1e0::1234:5678");
	CHECK(port == 65535);
}

TEST_CASE("TCP locations reject ambiguous or malformed authorities", "[transport]") {
	std::string host;
	int port = 0;
	CHECK_FALSE(IsTcpTransport("udp://host:9400"));
	CHECK_THROWS_AS(ParseTcpLocation("http://host:9400", host, port), std::invalid_argument);
	CHECK_THROWS_AS(ParseTcpLocation("tcp://host", host, port), std::invalid_argument);
	CHECK_THROWS_AS(ParseTcpLocation("tcp://host:", host, port), std::invalid_argument);
	CHECK_THROWS_AS(ParseTcpLocation("tcp://host:0", host, port), std::invalid_argument);
	CHECK_THROWS_AS(ParseTcpLocation("tcp://host:65536", host, port), std::invalid_argument);
	CHECK_THROWS_AS(ParseTcpLocation("tcp://host:9400/path", host, port), std::invalid_argument);
	CHECK_THROWS_AS(ParseTcpLocation("tcp://::1:9400", host, port), std::invalid_argument);
	CHECK_THROWS_AS(ParseTcpLocation("tcp://[]:9400", host, port), std::invalid_argument);
	CHECK_THROWS_AS(ParseTcpLocation("tcp://[::1]9400", host, port), std::invalid_argument);
	CHECK_THROWS_AS(ParseTcpLocation("tcp://[::1]:abc", host, port), std::invalid_argument);
}

TEST_CASE("worker: scheme predicate + strip", "[transport]") {
	CHECK(IsWebWorkerTransport("worker:/workers/example.js"));
	CHECK(IsWebWorkerTransport("worker:https://cdn.example.com/w.js"));
	CHECK(IsWebWorkerTransport("WORKER:example")); // scheme is case-insensitive
	CHECK_FALSE(IsWebWorkerTransport("/path/to/worker"));
	CHECK_FALSE(IsWebWorkerTransport("http://x"));

	// Strip preserves the case-sensitive remainder (URLs/paths).
	CHECK(StripWebWorkerScheme("worker:/workers/Example.js") == "/workers/Example.js");
	CHECK(StripWebWorkerScheme("worker:https://Cdn/W.js") == "https://Cdn/W.js");
	CHECK(StripWebWorkerScheme("worker://host/x.js") == "//host/x.js");
	CHECK(StripWebWorkerScheme("worker:name") == "name");
	CHECK_THROWS_AS(StripWebWorkerScheme("http://x"), std::invalid_argument);
}

TEST_CASE("Container scheme detection and stripping", "[transport]") {
	CHECK(IsContainerLocation("oci://ghcr.io/org/img:tag"));
	CHECK(IsContainerLocation("docker://img"));
	CHECK(IsContainerLocation("OCI://Img:Tag"));  // scheme is case-insensitive
	CHECK_FALSE(IsContainerLocation("http://x"));
	CHECK_FALSE(IsContainerLocation("/bare/path"));
	// Strips the scheme and preserves original-case image refs.
	CHECK(StripContainerScheme("oci://ghcr.io/org/Img:Tag") == "ghcr.io/org/Img:Tag");
	CHECK(StripContainerScheme("docker://img:1.2") == "img:1.2");
	// Drops the pool-disambiguation "#<hash>" suffix.
	CHECK(StripContainerScheme("oci://ghcr.io/org/img:tag#deadbeef") == "ghcr.io/org/img:tag");
	CHECK_THROWS_AS(StripContainerScheme("http://x"), std::invalid_argument);
}

TEST_CASE("Shared-container internal scheme detection", "[transport]") {
	CHECK(IsContainerSharedLocation("container-shared:oci://ghcr.io/org/img:tag#deadbeef"));
	CHECK_FALSE(IsContainerSharedLocation("oci://img"));
	CHECK_FALSE(IsContainerSharedLocation("http://x"));
	CHECK_FALSE(IsContainerSharedLocation(""));
}

TEST_CASE("Scheme predicates are mutually exclusive", "[transport]") {
	struct Case {
		const char *loc;
		bool http;
		bool launch;
		bool unix_;
	} cases[] = {
	    {"http://x", true, false, false},  {"https://x", true, false, false},   {"launch:foo", false, true, false},
	    {"unix:///x", false, false, true}, {"/some/path", false, false, false}, {"", false, false, false},
	};
	for (const auto &c : cases) {
		INFO("location: " << c.loc);
		CHECK(IsHttpTransport(c.loc) == c.http);
		CHECK(IsLaunchLocation(c.loc) == c.launch);
		CHECK(IsUnixLocation(c.loc) == c.unix_);
	}
}

TEST_CASE("StripUnixScheme returns the path after the prefix", "[transport]") {
	CHECK(StripUnixScheme("unix:///tmp/foo.sock") == "/tmp/foo.sock");
	CHECK(StripUnixScheme("unix://") == "");
	CHECK_THROWS_AS(StripUnixScheme("http://x"), std::invalid_argument);
}

TEST_CASE("StripLaunchScheme returns the argv payload", "[transport]") {
	CHECK(StripLaunchScheme("launch:python -m foo") == "python -m foo");
	CHECK(StripLaunchScheme("launch:") == "");
	CHECK_THROWS_AS(StripLaunchScheme("unix:///x"), std::invalid_argument);
}

TEST_CASE("github:// and github-auto:// detection is disjoint", "[transport]") {
	// github-auto:// must NOT match the github:// predicate (and vice versa).
	CHECK(IsGithubLocation("github://owner/repo@v1/asset.tar.gz"));
	CHECK_FALSE(IsGithubLocation("github-auto://owner/repo@v1"));
	CHECK(IsGithubAutoLocation("github-auto://owner/repo@v1"));
	CHECK_FALSE(IsGithubAutoLocation("github://owner/repo@v1/asset.tar.gz"));
	// Case-insensitive scheme; never confused with oci://ghcr.io or http://.
	CHECK(IsGithubLocation("GITHUB://o/r@v1/a"));
	CHECK_FALSE(IsGithubLocation("oci://ghcr.io/o/r:tag"));
	CHECK_FALSE(IsGithubLocation("https://github.com/o/r"));
	CHECK_FALSE(IsGithubAutoLocation(""));
}

TEST_CASE("StripGithubScheme keeps the remainder incl. fragment", "[transport]") {
	CHECK(StripGithubScheme("github://o/r@v1/a.tar.gz#sha256=ab") == "o/r@v1/a.tar.gz#sha256=ab");
	CHECK(StripGithubAutoScheme("github-auto://o/r@v1") == "o/r@v1");
	CHECK_THROWS_AS(StripGithubScheme("github-auto://o/r@v1"), std::invalid_argument);
	CHECK_THROWS_AS(StripGithubAutoScheme("github://o/r@v1/a"), std::invalid_argument);
}
