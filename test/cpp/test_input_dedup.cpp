// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catch.hpp"
#include "vgi_input_dedup.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include <string>

using namespace duckdb;
using namespace duckdb::vgi;

namespace {
void CheckIdentity(const InputDedup &d, idx_t n) {
	REQUIRE(d.trivial);
	REQUIRE(d.n == n);
	REQUIRE(d.k == n);
	for (idx_t i = 0; i < n; i++) {
		REQUIRE(d.distinct.get_index(i) == i);
		REQUIRE(d.orig_to_distinct[i] == i);
		REQUIRE(d.Group(i).size() == 1);
		REQUIRE(d.Group(i)[0] == i);
	}
}
void CheckMapping(const InputDedup &d, const std::vector<idx_t> &mapping) {
	REQUIRE(d.orig_to_distinct == mapping);
	idx_t covered = 0;
	for (idx_t group = 0; group < d.k; group++) {
		REQUIRE_FALSE(d.Group(group).empty());
		REQUIRE(d.distinct.get_index(group) == d.Group(group)[0]);
		for (auto row : d.Group(group)) {
			REQUIRE(mapping[row] == group);
			covered++;
		}
	}
	REQUIRE(covered == mapping.size());
}
} // namespace

TEST_CASE("Dedup exact strings, nulls and non-key outer columns", "[dedup]") {
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), {LogicalType::VARCHAR, LogicalType::BIGINT});
	std::string a(65536, 'x'), b = a;
	b.back() = 'y';
	vector<Value> values {
	    Value(a), Value(b), Value(a), Value(LogicalType::VARCHAR), Value(""), Value(LogicalType::VARCHAR),
	    Value(b), Value("")};
	for (idx_t i = 0; i < values.size(); i++) {
		chunk.SetValue(0, i, values[i]);
		chunk.SetValue(1, i, Value::BIGINT(i));
	}
	chunk.SetCardinality(values.size());
	auto d = BuildInputDedup(chunk, {0});
	REQUIRE(d.k == 4);
	CheckMapping(d, {0, 1, 0, 2, 3, 2, 1, 3});
	CheckIdentity(BuildInputDedup(chunk, {0, 1}), chunk.size());
}

TEST_CASE("Dedup handles blob bytes, dictionary selections and constants", "[dedup]") {
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), {LogicalType::BLOB});
	const std::string a("a\0x", 3), b("a\0y", 3);
	chunk.SetValue(0, 0, Value::BLOB_RAW(a));
	chunk.SetValue(0, 1, Value::BLOB_RAW(b));
	chunk.SetValue(0, 2, Value::BLOB_RAW(a));
	chunk.SetCardinality(3);
	SelectionVector selection(5);
	for (idx_t i = 0; i < 5; i++)
		selection.set_index(i, i % 3);
	DataChunk sliced;
	sliced.InitializeEmpty({LogicalType::BLOB});
	sliced.Reference(chunk);
	sliced.Slice(selection, 5);
	CheckMapping(BuildInputDedup(sliced, {0}), {0, 1, 0, 0, 1});
	chunk.data[0].Reference(Value::BLOB_RAW(a));
	CheckMapping(BuildInputDedup(chunk, {0}), {0, 0, 0});
}

TEST_CASE("Dedup keeps canonical nested and numeric equality", "[dedup]") {
	DataChunk chunk;
	auto list_type = LogicalType::LIST(LogicalType::INTEGER);
	chunk.Initialize(Allocator::DefaultAllocator(), {LogicalType::VARCHAR, list_type, LogicalType::DOUBLE});
	for (idx_t i = 0; i < 6; i++) {
		chunk.SetValue(0, i, Value("same"));
		chunk.SetValue(1, i,
		               i < 4 ? Value::LIST(LogicalType::INTEGER, {Value::INTEGER(i % 2), Value(LogicalType::INTEGER)})
		                     : Value(list_type));
		chunk.SetValue(2, i, Value::DOUBLE(1.25));
	}
	chunk.SetCardinality(6);
	CheckMapping(BuildInputDedup(chunk, {0, 1, 2}), {0, 1, 0, 1, 2, 2});
}

TEST_CASE("Dedup compares fixed-width values bitwise, with nulls, selections and mixed keys", "[dedup]") {
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(),
	                 {LogicalType::BIGINT, LogicalType::HUGEINT, LogicalType::DATE, LogicalType::DECIMAL(18, 3),
	                  LogicalType::VARCHAR, LogicalType::DOUBLE});
	// Rows 0/2/5 share every key; row 3 differs only in the HUGEINT high half, row 4 only
	// in being NULL in the BIGINT column, row 1 only in the DOUBLE (canonical) column.
	const hugeint_t big(1, 7);
	for (idx_t i = 0; i < 6; i++) {
		chunk.SetValue(0, i, i == 4 ? Value(LogicalType::BIGINT) : Value::BIGINT(-5));
		chunk.SetValue(1, i, Value::HUGEINT(i == 3 ? hugeint_t(2, 7) : big));
		chunk.SetValue(2, i, Value::DATE(date_t(19000)));
		chunk.SetValue(3, i, Value::DECIMAL(int64_t(12345), 18, 3));
		chunk.SetValue(4, i, Value("k"));
		chunk.SetValue(5, i, Value::DOUBLE(i == 1 ? 2.5 : 1.5));
	}
	chunk.SetCardinality(6);
	CheckMapping(BuildInputDedup(chunk, {0, 1, 2, 3, 4, 5}), {0, 1, 0, 2, 3, 0});
	CheckMapping(BuildInputDedup(chunk, {0}), {0, 0, 0, 0, 1, 0});

	// Dictionary selection and constant vectors over an integer column.
	DataChunk ints;
	ints.Initialize(Allocator::DefaultAllocator(), {LogicalType::INTEGER});
	for (idx_t i = 0; i < 3; i++) {
		ints.SetValue(0, i, Value::INTEGER(int32_t(i * 10)));
	}
	ints.SetCardinality(3);
	SelectionVector selection(6);
	for (idx_t i = 0; i < 6; i++) {
		selection.set_index(i, (i * 2) % 3);
	}
	DataChunk sliced;
	sliced.InitializeEmpty({LogicalType::INTEGER});
	sliced.Reference(ints);
	sliced.Slice(selection, 6);
	CheckMapping(BuildInputDedup(sliced, {0}), {0, 1, 2, 0, 1, 2});
	ints.data[0].Reference(Value::INTEGER(42));
	CheckMapping(BuildInputDedup(ints, {0}), {0, 0, 0});
}

TEST_CASE("Dedup over a full vector of all-distinct and low-cardinality integers", "[dedup]") {
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), {LogicalType::BIGINT});
	auto data = FlatVector::GetData<int64_t>(chunk.data[0]);
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		data[i] = int64_t(i) * 1000003;
	}
	chunk.SetCardinality(STANDARD_VECTOR_SIZE);
	auto unique = BuildInputDedup(chunk, {0});
	REQUIRE(unique.trivial);
	REQUIRE(unique.k == STANDARD_VECTOR_SIZE);
	std::vector<idx_t> mapping(STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		data[i] = int64_t(i % 16);
		mapping[i] = i % 16;
	}
	auto repeated = BuildInputDedup(chunk, {0});
	REQUIRE(repeated.k == 16);
	CheckMapping(repeated, mapping);
}

TEST_CASE("Dedup budgets return complete identity after partial work", "[dedup]") {
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), {LogicalType::VARCHAR});
	for (idx_t i = 0; i < 100; i++)
		chunk.SetValue(0, i, Value(std::string(64, 'x')));
	chunk.SetCardinality(100);
	InputDedupOptions options;
	options.max_work_bytes = 200;
	CheckIdentity(BuildInputDedup(chunk, {0}, options), 100);
	options.max_work_bytes = 1024 * 1024;
	options.max_comparisons = 1;
	CheckIdentity(BuildInputDedup(chunk, {0}, options), 100);
	options.large_input_bytes = 1;
	options.sample_work_bytes = 1;
	CheckIdentity(BuildInputDedup(chunk, {0}, options), 100);
	CheckIdentity(BuildInputDedup(chunk, {}), 100);
	chunk.SetCardinality(0);
	CheckIdentity(BuildInputDedup(chunk, {0}), 0);
}

#ifndef VGI_DEDUP_TEST_HASH_COLLISIONS
TEST_CASE("Large unique batches bypass full key work while periodic repeats reduce", "[dedup]") {
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), {LogicalType::VARCHAR});
	for (idx_t i = 0; i < 2048; i++) {
		auto value = std::string(65512, 'x') + std::to_string(1000000000000000000ULL + i % 64);
		chunk.SetValue(0, i, Value(value));
	}
	chunk.SetCardinality(2048);
	auto repeated = BuildInputDedup(chunk, {0});
	REQUIRE(repeated.k == 64);
	std::vector<idx_t> mapping(2048);
	for (idx_t i = 0; i < 2048; i++)
		mapping[i] = i % 64;
	CheckMapping(repeated, mapping);
	for (idx_t i = 0; i < 128; i++) {
		auto value = std::string(65512, 'x') + std::to_string(1000000000000000000ULL + (i == 127 ? 0 : i));
		chunk.SetValue(0, i, Value(value));
	}
	chunk.SetCardinality(128);
	// One real duplicate is intentionally not collapsed: the sampled saving is
	// too small. The identity fallback must still preserve both original rows.
	CheckIdentity(BuildInputDedup(chunk, {0}), 128);
}
#endif
