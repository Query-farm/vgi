// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Unit tests for the worker-batch validation helpers (vgi_batch_validation).
// A conforming worker can never put a mismatched schema on the wire (the
// vgi-rpc producer writer is bound to output_schema), so the type-confusion
// guard is exercised here directly rather than through an integration fixture.
#include "catch.hpp"

#include "vgi_batch_validation.hpp"

#include "duckdb/common/exception.hpp"

#include <arrow/api.h>

using namespace duckdb;
using duckdb::vgi::ParseWorkerBatchValidation;
using duckdb::vgi::ValidateWireBatchTypes;
using duckdb::vgi::ValidateWorkerBatch;
using duckdb::vgi::WorkerBatchValidation;

static std::shared_ptr<arrow::RecordBatch> OneColumn(const std::shared_ptr<arrow::DataType> &type,
                                                     const std::shared_ptr<arrow::Array> &array) {
	auto schema = arrow::schema({arrow::field("c", type)});
	return arrow::RecordBatch::Make(schema, array->length(), {array});
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
	arrow::Int64Builder b;
	REQUIRE(b.AppendValues({1, 2, 3}).ok());
	std::shared_ptr<arrow::Array> arr;
	REQUIRE(b.Finish(&arr).ok());
	auto batch = OneColumn(arrow::int64(), arr);

	CHECK_NOTHROW(ValidateWireBatchTypes(*batch, {arrow::int64()}, "w", "f"));
	// Nullability / field metadata are ignored: only the type structure matters.
	CHECK_NOTHROW(ValidateWireBatchTypes(*batch, {arrow::int64()}, "w", "f"));
}

TEST_CASE("ValidateWireBatchTypes rejects a type mismatch", "[batch-validation]") {
	// A utf8 array declared as int64 at bind: ArrowToDuckDB would read the utf8
	// offset/data buffers as an int64 buffer -> misread. Must throw.
	arrow::StringBuilder b;
	REQUIRE(b.AppendValues({"a", "bb", "ccc"}).ok());
	std::shared_ptr<arrow::Array> arr;
	REQUIRE(b.Finish(&arr).ok());
	auto batch = OneColumn(arrow::utf8(), arr);

	CHECK_THROWS_AS(ValidateWireBatchTypes(*batch, {arrow::int64()}, "w", "geo"), IOException);
}

TEST_CASE("ValidateWireBatchTypes rejects a column-count mismatch", "[batch-validation]") {
	arrow::Int64Builder b;
	REQUIRE(b.AppendValues({1}).ok());
	std::shared_ptr<arrow::Array> arr;
	REQUIRE(b.Finish(&arr).ok());
	auto batch = OneColumn(arrow::int64(), arr);

	CHECK_THROWS_AS(ValidateWireBatchTypes(*batch, {arrow::int64(), arrow::int64()}, "w", "f"), IOException);
	CHECK_THROWS_AS(ValidateWireBatchTypes(*batch, {}, "w", "f"), IOException);
}

TEST_CASE("ValidateWireBatchTypes skips a null expected slot", "[batch-validation]") {
	// A null expected entry (an out-of-range projection index) is not checked.
	arrow::Int64Builder b;
	REQUIRE(b.AppendValues({1}).ok());
	std::shared_ptr<arrow::Array> arr;
	REQUIRE(b.Finish(&arr).ok());
	auto batch = OneColumn(arrow::int64(), arr);

	CHECK_NOTHROW(ValidateWireBatchTypes(*batch, {nullptr}, "w", "f"));
}

TEST_CASE("ValidateWorkerBatch levels catch buffer-content corruption", "[batch-validation]") {
	// A valid int64 array passes every level.
	arrow::Int64Builder b;
	REQUIRE(b.AppendValues({1, 2, 3}).ok());
	std::shared_ptr<arrow::Array> arr;
	REQUIRE(b.Finish(&arr).ok());
	auto good = OneColumn(arrow::int64(), arr);
	CHECK_NOTHROW(ValidateWorkerBatch(good.get(), WorkerBatchValidation::FULL, "w"));
	CHECK_NOTHROW(ValidateWorkerBatch(good.get(), WorkerBatchValidation::STRUCTURAL, "w"));

	// A utf8 array whose offsets run past its data buffer: caught structurally.
	auto offsets = arrow::Buffer::FromString(std::string("\x00\x00\x00\x00\x00\x00\x10\x00", 8)); // [0, 0x100000]
    auto data = arrow::Buffer::FromString("ab");
	auto bad = arrow::MakeArray(arrow::ArrayData::Make(arrow::utf8(), 1, {nullptr, offsets, data}));
	auto bad_batch = OneColumn(arrow::utf8(), bad);
	CHECK_THROWS_AS(ValidateWorkerBatch(bad_batch.get(), WorkerBatchValidation::STRUCTURAL, "w"), IOException);
	CHECK_THROWS_AS(ValidateWorkerBatch(bad_batch.get(), WorkerBatchValidation::FULL, "w"), IOException);
	// NONE does no work.
	CHECK_NOTHROW(ValidateWorkerBatch(bad_batch.get(), WorkerBatchValidation::NONE, "w"));
	CHECK_NOTHROW(ValidateWorkerBatch(nullptr, WorkerBatchValidation::FULL, "w"));
}
