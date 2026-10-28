// © Copyright 2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// The nanoarrow-backed data types (vgi_arrow.hpp) and IPC streams (vgi_ipc.hpp).

#include "catch.hpp"

#include "vgi_arrow.hpp"
#include "vgi_ipc.hpp"

#include "duckdb/common/exception.hpp"

#include "nanoarrow/nanoarrow.h"

#include <algorithm>
#include <string>

using namespace duckdb::vgi;

namespace {

VgiSchemaPtr SampleSchema() {
	return MakeSchema({type::Int64().Named("id", false), type::Utf8().Named("name"),
	                   type::List(type::Utf8()).Named("tags", false),
	                   type::Dictionary(type::Int16(), type::Utf8()).Named("kind", false),
	                   type::Struct({type::Float64().Named("x"), type::Bool().Named("ok")}).Named("point"),
	                   type::Map(type::Utf8(), type::Int64()).Named("counts", false),
	                   type::Timestamp('u', "UTC").Named("at")},
	                  VgiMetadata({{"origin", "test"}}));
}

// Rows i = 0..n-1: id=i, name="n<i>" (null when i%3==2), tags=["t"]*(i%3), kind=dict index i%2,
// point={x=i/2, ok=i odd} (null when i%4==3), counts={"c": i}, at=i*1000000.
VgiBatchPtr SampleBatch(int64_t n) {
	VgiBatchBuilder b(SampleSchema());
	ArrowArray *kind_dict = b.column(3)->dictionary;
	append::String(kind_dict, "even");
	append::String(kind_dict, "odd");
	for (int64_t i = 0; i < n; i++) {
		append::Int(b.column(0), i);
		if (i % 3 == 2) {
			append::Null(b.column(1));
		} else {
			append::String(b.column(1), "n" + std::to_string(i));
		}
		for (int64_t k = 0; k < i % 3; k++) {
			append::String(b.column(2)->children[0], "t");
		}
		append::FinishElement(b.column(2));
		append::Int(b.column(3), i % 2);
		auto *point = b.column(4);
		if (i % 4 == 3) {
			append::Null(point);
		} else {
			append::Double(point->children[0], static_cast<double>(i) / 2);
			append::Bool(point->children[1], i % 2 == 1);
			append::FinishElement(point);
		}
		auto *counts = b.column(5);
		append::String(counts->children[0]->children[0], "c");
		append::Int(counts->children[0]->children[1], i);
		append::FinishElement(counts->children[0]);
		append::FinishElement(counts);
		append::Int(b.column(6), i * 1000000);
		b.FinishRow();
	}
	return b.Finish(VgiMetadata::Make({{"vgi.protocol_state", "output"}}));
}

void CheckRow(const VgiBatch &batch, int64_t row, int64_t i) {
	CHECK(batch.column(0).GetInt(row) == i);
	if (i % 3 == 2) {
		CHECK(batch.column(1).IsNull(row));
	} else {
		REQUIRE(!batch.column(1).IsNull(row));
		CHECK(batch.column(1).GetString(row) == "n" + std::to_string(i));
	}
	int64_t begin = 0;
	int64_t end = 0;
	batch.column(2).ListRange(row, begin, end);
	CHECK(end - begin == i % 3);
	auto kind = batch.column(3);
	REQUIRE(kind.is_dictionary());
	CHECK(kind.dictionary().GetString(kind.GetInt(row)) == (i % 2 ? "odd" : "even"));
	auto point = batch.column(4);
	if (i % 4 == 3) {
		CHECK(point.IsNull(row));
	} else {
		CHECK(point.child(0).GetDouble(row) == static_cast<double>(i) / 2);
		CHECK(point.child(1).GetBool(row) == (i % 2 == 1));
	}
	auto counts = batch.column(5);
	counts.ListRange(row, begin, end);
	REQUIRE(end - begin == 1);
	auto entries = counts.child(0);
	CHECK(entries.child(0).GetString(begin) == "c");
	CHECK(entries.child(1).GetInt(begin) == i);
	CHECK(batch.column(6).GetInt(row) == i * 1000000);
}

} // namespace

TEST_CASE("VgiSchema describes and compares types", "[vgi-arrow]") {
	auto schema = SampleSchema();
	REQUIRE(schema->num_fields() == 7);
	CHECK(schema->GetFieldIndex("kind") == 3);
	CHECK(schema->GetFieldIndex("missing") == -1);
	CHECK(!schema->field_nullable(0));
	CHECK(schema->field_nullable(1));
	CHECK(schema->metadata().Get("origin") == std::string("test"));
	CHECK(TypeToString(schema->field(2)) == "list<item: string>");
	CHECK(TypeToString(schema->field(3)) == "dictionary<values=string, indices=int16>");
	CHECK(TypeToString(schema->field(6)) == "timestamp[us, tz=UTC]");
	CHECK(TypeToString(schema->field(5)) == "map<string, int64>");
	auto info = DescribeType(schema->field(3));
	CHECK(info.is_dictionary);
	CHECK(info.storage == NANOARROW_TYPE_INT16);
	CHECK(schema->Equals(*SampleSchema(), /*check_metadata=*/true));
	CHECK(!schema->Equals(*schema->RemoveField(1)));
	CHECK(schema->Select({3, 0})->field_name(0) == "kind");
	CHECK(schema->WithMetadata(VgiMetadata()).get()->metadata().empty());
}

TEST_CASE("Schema edits preserve IPC identity without relaxing field contracts", "[vgi-arrow]") {
	auto original = MakeSchema({type::Int32().Named("n").WithMetadata(VgiMetadata({{"raw", "source"}}))},
	                           VgiMetadata({{"table", "metadata"}}));
	auto result = MakeSchema({type::Int64().Named("wide")});
	auto expanded = original->AddField(1, result->field(0));
	auto bytes = SerializeSchema(*expanded);
	auto decoded = ReadSchemaBytes(bytes->data(), bytes->size());
	CHECK(expanded->Equals(*decoded, true));

	ArrowSchema root;
	expanded->ExportTo(&root);
	root.flags |= ARROW_FLAG_NULLABLE;
	REQUIRE(ArrowSchemaSetName(&root, "C interface wrapper") == NANOARROW_OK);
	auto wrapper = VgiSchema::Adopt(&root);
	CHECK(expanded->Equals(*wrapper, true));
	auto sink = std::make_shared<VgiBufferOutputStream>();
	auto writer = VgiIpcWriter::Open(sink, expanded);
	CHECK_NOTHROW(writer->Write(*VgiBatch::MakeEmpty(wrapper)));

	auto nonnull = MakeSchema({type::Int64().Named("wide", false)});
	auto renamed = MakeSchema({type::Int64().Named("renamed")});
	auto plain = MakeSchema({type::Int32().Named("n")});
	CHECK_FALSE(expanded->Equals(*expanded->SetField(1, nonnull->field(0)), true));
	CHECK_FALSE(expanded->Equals(*expanded->SetField(1, renamed->field(0)), true));
	CHECK_FALSE(expanded->Equals(*expanded->SetField(0, plain->field(0)), true));
	CHECK_FALSE(expanded->Equals(*expanded->WithMetadata(VgiMetadata()), true));
	CHECK(expanded->Equals(*expanded->WithMetadata(VgiMetadata()), false));
	CHECK_THROWS(writer->Write(*VgiBatch::MakeEmpty(expanded->SetField(1, nonnull->field(0)))));
	writer->Close();
	auto encoded = sink->Finish();
	auto reader = VgiIpcReader::Open(std::make_shared<VgiBufferReader>(encoded));
	auto empty = reader->ReadNext();
	REQUIRE(empty);
	CHECK(empty->num_rows() == 0);
	CHECK_FALSE(reader->ReadNext());
}

TEST_CASE("VgiBatch builds, reads, slices and exports", "[vgi-arrow]") {
	auto batch = SampleBatch(10);
	REQUIRE(batch->num_rows() == 10);
	REQUIRE(batch->num_columns() == 7);
	CHECK(batch->metadata()->Get("vgi.protocol_state") == std::string("output"));
	for (int64_t i = 0; i < 10; i++) {
		CheckRow(*batch, i, i);
	}
	ValidateBatch(*batch, VgiValidation::FULL);

	auto slice = batch->Slice(3, 5);
	REQUIRE(slice->num_rows() == 5);
	for (int64_t r = 0; r < 5; r++) {
		CheckRow(*slice, r, r + 3);
	}
	auto slice2 = slice->Slice(1, 2);
	CheckRow(*slice2, 0, 4);
	CheckRow(*slice2, 1, 5);

	auto selected = batch->SelectColumns({6, 0});
	CHECK(selected->column_name(0) == "at");
	CHECK(selected->column(1).GetInt(9) == 9);

	// The exported array outlives every VgiBatch handle.
	ArrowArray exported;
	slice->ExportTo(&exported);
	batch.reset();
	slice.reset();
	slice2.reset();
	selected.reset();
	REQUIRE(exported.length == 5);
	REQUIRE(exported.n_children == 7);
	for (int64_t i = 0; i < exported.n_children; i++) {
		CHECK(exported.children[i]->release != nullptr);
		CHECK(exported.children[i]->length == 5);
	}
	CHECK(exported.children[0]->offset == 3);
	CHECK(static_cast<const int64_t *>(exported.children[0]->buffers[1])[3] == 3);
	exported.release(&exported);
	CHECK(exported.release == nullptr);
}

TEST_CASE("IPC streams round-trip VgiBatch", "[vgi-arrow]") {
	auto batch = SampleBatch(17);
	SECTION("stream with several batches and metadata") {
		auto sink = std::make_shared<VgiBufferOutputStream>();
		auto writer = VgiIpcWriter::Open(sink, batch->schema());
		writer->Write(*batch);
		VgiMetadata other({{"k", "v"}});
		writer->Write(*batch->Slice(5, 7), &other); // a slice: compacted on write
		writer->Write(*VgiBatch::MakeEmpty(batch->schema()));
		writer->Close();
		auto bytes = sink->Finish();

		auto reader = VgiIpcReader::Open(std::make_shared<VgiBufferReader>(bytes));
		CHECK(reader->schema()->Equals(*batch->schema(), /*check_metadata=*/true));
		auto first = reader->ReadNext();
		REQUIRE(first);
		CHECK(first->metadata()->Get("vgi.protocol_state") == std::string("output"));
		for (int64_t i = 0; i < 17; i++) {
			CheckRow(*first, i, i);
		}
		ValidateBatch(*first, VgiValidation::FULL);
		auto second = reader->ReadNext();
		REQUIRE(second);
		REQUIRE(second->num_rows() == 7);
		CHECK(second->metadata()->Get("k") == std::string("v"));
		for (int64_t r = 0; r < 7; r++) {
			CheckRow(*second, r, r + 5);
		}
		auto empty = reader->ReadNext();
		REQUIRE(empty);
		CHECK(empty->num_rows() == 0);
		// Empty variable-size columns still have a readable offsets[0].
		CHECK(empty->c_array().children[1]->buffers[1] != nullptr);
		CHECK(empty->c_array().children[2]->buffers[1] != nullptr);
		CHECK(!reader->ReadNext());
	}
	SECTION("one-shot helpers and compression") {
		for (const char *codec : {"none", "zstd", "lz4"}) {
			VgiIpcWriteOptions options;
			options.codec = codec;
			auto bytes = SerializeBatch(*batch, nullptr, options);
			auto back = DeserializeBatch(bytes);
			REQUIRE(back);
			for (int64_t i = 0; i < 17; i++) {
				CheckRow(*back, i, i);
			}
		}
		auto schema_bytes = SerializeSchema(*batch->schema());
		auto schema = ReadSchemaBytes(schema_bytes->data(), schema_bytes->size());
		CHECK(schema->Equals(*batch->schema(), /*check_metadata=*/true));
	}
	SECTION("a truncated stream is an error, not a crash") {
		auto bytes = SerializeBatch(*batch);
		auto truncated = bytes->Slice(0, bytes->size() / 2);
		CHECK_THROWS(DeserializeBatch(truncated));
	}
}

TEST_CASE("TakeRows and ConcatBatches", "[vgi-arrow]") {
	auto batch = SampleBatch(12);
	auto taken = TakeRows(batch, std::vector<int64_t> {11, 0, 5, 5, 3});
	REQUIRE(taken->num_rows() == 5);
	int64_t expected[] = {11, 0, 5, 5, 3};
	for (int64_t r = 0; r < 5; r++) {
		CheckRow(*taken, r, expected[r]);
	}
	ValidateBatch(*taken, VgiValidation::FULL);

	// Taking from a slice uses the slice's row numbers.
	auto from_slice = TakeRows(batch->Slice(4, 6), std::vector<int64_t> {0, 5});
	CheckRow(*from_slice, 0, 4);
	CheckRow(*from_slice, 1, 9);

	auto joined = ConcatBatches({batch->Slice(0, 3), SampleBatch(5), batch->Slice(10, 2)});
	REQUIRE(joined->num_rows() == 10);
	int64_t want[] = {0, 1, 2, 0, 1, 2, 3, 4, 10, 11};
	for (int64_t r = 0; r < 10; r++) {
		CheckRow(*joined, r, want[r]);
	}
	ValidateBatch(*joined, VgiValidation::FULL);
	// Dictionaries were merged: each batch contributed its own values.
	CHECK(joined->column(3).dictionary().length() == 6);
}

TEST_CASE("Row selection keeps nested dictionaries", "[vgi-arrow]") {
	auto schema = MakeSchema({type::List(type::Dictionary(type::Int16(), type::Utf8())).Named("tags")});
	auto make = [&](std::vector<std::vector<std::string>> rows, std::vector<std::string> dict) {
		VgiBatchBuilder b(schema);
		ArrowArray *list = b.column(0);
		for (auto &d : dict) {
			append::String(list->children[0]->dictionary, d);
		}
		for (auto &row : rows) {
			for (auto &v : row) {
				int64_t idx = std::find(dict.begin(), dict.end(), v) - dict.begin();
				append::Int(list->children[0], idx);
			}
			append::FinishElement(list);
			b.FinishRow();
		}
		return b.Finish();
	};
	auto read = [](const VgiBatch &batch, int64_t row) {
		std::vector<std::string> out;
		int64_t begin = 0, end = 0;
		auto list = batch.column(0);
		list.ListRange(row, begin, end);
		auto items = list.child(0);
		for (int64_t i = begin; i < end; i++) {
			out.push_back(items.dictionary().GetString(items.GetInt(i)));
		}
		return out;
	};
	auto a = make({{"x", "y"}, {"y"}}, {"x", "y"});
	auto b = make({{"z"}, {}}, {"z"});
	auto joined = ConcatBatches({a, b});
	REQUIRE(joined->num_rows() == 4);
	CHECK(read(*joined, 0) == std::vector<std::string> {"x", "y"});
	CHECK(read(*joined, 1) == std::vector<std::string> {"y"});
	CHECK(read(*joined, 2) == std::vector<std::string> {"z"});
	CHECK(read(*joined, 3).empty());
	ValidateBatch(*joined, VgiValidation::FULL);
	auto taken = TakeRows(joined, std::vector<int64_t> {2, 0});
	CHECK(read(*taken, 0) == std::vector<std::string> {"z"});
	CHECK(read(*taken, 1) == std::vector<std::string> {"x", "y"});
}
