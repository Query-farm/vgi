// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Unit tests for the worker-batch validation helpers (vgi_batch_validation).
// A conforming worker can never put a mismatched schema on the wire (the
// vgi-rpc producer writer is bound to output_schema), so the type-confusion
// guard is exercised here directly rather than through an integration fixture.
#include "catch.hpp"

#include "vgi_batch_validation.hpp"
#include "vgi_ipc.hpp"

#include "duckdb/common/exception.hpp"

#include "nanoarrow/nanoarrow.h"

#include <cstring>

using namespace duckdb;
using duckdb::vgi::ParseWorkerBatchValidation;
using duckdb::vgi::ValidateWireBatchTypes;
using duckdb::vgi::ValidateWorkerBatch;
using duckdb::vgi::VgiBatchBuilder;
using duckdb::vgi::VgiBatchPtr;
using duckdb::vgi::VgiField;
using duckdb::vgi::VgiSchemaPtr;
using duckdb::vgi::WorkerBatchValidation;
namespace vtype = duckdb::vgi::type;
namespace vappend = duckdb::vgi::append;

static VgiBatchPtr Int64Column(std::initializer_list<int64_t> values) {
	VgiBatchBuilder b(duckdb::vgi::MakeSchema({vtype::Int64().Named("c")}));
	for (auto v : values) {
		vappend::Int(b.column(0), v);
		b.FinishRow();
	}
	return b.Finish();
}

static VgiBatchPtr Utf8Column(std::initializer_list<const char *> values) {
	VgiBatchBuilder b(duckdb::vgi::MakeSchema({vtype::Utf8().Named("c")}));
	for (auto v : values) {
		vappend::String(b.column(0), v);
		b.FinishRow();
	}
	return b.Finish();
}

// A one-column schema node of `field`, for the expected-type list.
static VgiSchemaPtr TypeOf(const VgiField &field) {
	return duckdb::vgi::MakeSchema({field.Named("c")});
}

TEST_CASE("ParseWorkerBatchValidation accepts the three levels case-insensitively", "[batch-validation]") {
	CHECK(ParseWorkerBatchValidation("full") == WorkerBatchValidation::FULL);
	CHECK(ParseWorkerBatchValidation("FULL") == WorkerBatchValidation::FULL);
	CHECK(ParseWorkerBatchValidation(" structural ") == WorkerBatchValidation::STRUCTURAL);
	CHECK(ParseWorkerBatchValidation("none") == WorkerBatchValidation::NONE);
	CHECK_THROWS_AS(ParseWorkerBatchValidation("fulll"), InvalidInputException);
	CHECK_THROWS_AS(ParseWorkerBatchValidation(""), InvalidInputException);
}

TEST_CASE("ValidateWireBatchTypes accepts a matching batch", "[batch-validation]") {
	auto batch = Int64Column({1, 2, 3});
	auto int64 = TypeOf(vtype::Int64());

	CHECK_NOTHROW(ValidateWireBatchTypes(*batch, {&int64->field(0)}, "w", "f"));
	// Nullability / field metadata are ignored: only the type structure matters.
	auto int64_not_null = duckdb::vgi::MakeSchema(
	    {vtype::Int64().Named("other", false).WithMetadata(duckdb::vgi::VgiMetadata({{"k", "v"}}))});
	CHECK_NOTHROW(ValidateWireBatchTypes(*batch, {&int64_not_null->field(0)}, "w", "f"));
}

TEST_CASE("ValidateWireBatchTypes rejects a type mismatch", "[batch-validation]") {
	// A utf8 array declared as int64 at bind: ArrowToDuckDB would read the utf8
	// offset/data buffers as an int64 buffer -> misread. Must throw.
	auto batch = Utf8Column({"a", "bb", "ccc"});
	auto int64 = TypeOf(vtype::Int64());

	CHECK_THROWS_AS(ValidateWireBatchTypes(*batch, {&int64->field(0)}, "w", "geo"), IOException);
}

TEST_CASE("ValidateWireBatchTypes rejects a column-count mismatch", "[batch-validation]") {
	auto batch = Int64Column({1});
	auto int64 = TypeOf(vtype::Int64());

	CHECK_THROWS_AS(ValidateWireBatchTypes(*batch, {&int64->field(0), &int64->field(0)}, "w", "f"), IOException);
	CHECK_THROWS_AS(ValidateWireBatchTypes(*batch, {}, "w", "f"), IOException);
}

TEST_CASE("ValidateWireBatchTypes skips a null expected slot", "[batch-validation]") {
	// A null expected entry (an out-of-range projection index) is not checked.
	auto batch = Int64Column({1});

	CHECK_NOTHROW(ValidateWireBatchTypes(*batch, {nullptr}, "w", "f"));
}

TEST_CASE("ValidateWorkerBatch levels catch buffer-content corruption", "[batch-validation]") {
	// A valid int64 array passes every level.
	auto good = Int64Column({1, 2, 3});
	CHECK_NOTHROW(ValidateWorkerBatch(good.get(), WorkerBatchValidation::FULL, "w"));
	CHECK_NOTHROW(ValidateWorkerBatch(good.get(), WorkerBatchValidation::STRUCTURAL, "w"));

	// A utf8 array whose offsets run past its data buffer, as a worker would send it:
	// serialize ["ab"], then point the last offset far past the body. Caught at decode
	// time (the C data interface carries no buffer sizes; the IPC body does) and
	// reported by ValidateWorkerBatch at the session's level.
	auto schema = duckdb::vgi::MakeSchema({vtype::Utf8().Named("c")});
	duckdb::vgi::VgiBatchBuilder builder(schema);
	duckdb::vgi::append::String(builder.column(0), "ab");
	builder.FinishRow();
	auto bytes = duckdb::vgi::SerializeBatch(*builder.Finish())->ToString();
	const char needle[8] = {0, 0, 0, 0, 2, 0, 0, 0}; // int32 offsets {0, 2}
	auto at = bytes.rfind(std::string(needle, 8)); // the body (after the header)
	REQUIRE(at != std::string::npos);
	const char corrupt[4] = {0, 0, 0x10, 0}; // 0x100000
	bytes.replace(at + 4, 4, std::string(corrupt, 4));
	auto bad_batch = duckdb::vgi::DeserializeBatch(duckdb::vgi::VgiBuffer::FromString(bytes));
	REQUIRE(bad_batch);
	CHECK(!bad_batch->structural_error().empty());
	CHECK_THROWS_AS(ValidateWorkerBatch(bad_batch.get(), WorkerBatchValidation::STRUCTURAL, "w"), IOException);
	CHECK_THROWS_AS(ValidateWorkerBatch(bad_batch.get(), WorkerBatchValidation::FULL, "w"), IOException);
	// NONE does no work.
	CHECK_NOTHROW(ValidateWorkerBatch(bad_batch.get(), WorkerBatchValidation::NONE, "w"));
	CHECK_NOTHROW(ValidateWorkerBatch(nullptr, WorkerBatchValidation::FULL, "w"));
}
