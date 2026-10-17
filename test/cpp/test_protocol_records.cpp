// © Copyright 2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// The generated protocol records (src/generated/vgi_protocol_records.{hpp,cpp}) and their
// runtime (src/vgi_wire_codec.cpp).
//
// - Every record encodes with EXACTLY the schema vgi_protocol_schemas.hpp declares for it
//   (names, order, types, nullability, metadata): peers such as vgi-rpc-go compare request
//   schemas with arrow.Schema.Equal, so "close enough" fails every call.
// - Records round-trip: ToBatch -> ValidateFull -> FromBatch gives back an equal record, for
//   every shape the protocol uses (maps, nested lists, list<struct>, enums, timestamps,
//   large_binary, optional members).
// - Decoding keeps the client's compatibility contract: a missing nullable column is absent,
//   a missing or null required column is a protocol error, and so is a column of the wrong
//   type or an enum value the client does not know.

#include "catch.hpp"

#include "../../src/generated/vgi_protocol_records.hpp"
#include "vgi_wire_codec.hpp"

#include "duckdb/common/exception.hpp"

#include <arrow/api.h>

#include <functional>
#include <string>

using namespace duckdb::vgi;
using namespace duckdb::vgi::generated;

namespace {

std::shared_ptr<arrow::Buffer> Buf(const std::string &s) {
	return arrow::Buffer::FromString(s);
}

template <class T>
T RoundTrip(const T &value) {
	auto batch = ToBatch(value);
	REQUIRE(batch);
	REQUIRE(batch->num_rows() == 1);
	auto status = batch->ValidateFull();
	INFO(status.ToString());
	REQUIRE(status.ok());
	return Decode<T>(batch, 0, "test-worker");
}

std::string ErrorOf(const std::function<void()> &fn) {
	try {
		fn();
	} catch (const std::exception &e) {
		return e.what();
	}
	return "";
}

} // namespace

TEST_CASE("Every record encodes with its protocol schema", "[protocol-records]") {
	auto pairs = RecordSchemaPairsForTesting();
	REQUIRE(pairs.size() > 50);
	for (auto &p : pairs) {
		INFO(p.name << "\n record:  " << p.record->ToString(true) << "\n factory: " << p.factory->ToString(true));
		CHECK(p.record->Equals(*p.factory, /*check_metadata=*/true));
	}
}

TEST_CASE("Default-constructed records encode valid one-row batches", "[protocol-records]") {
	for (auto &entry : DefaultRecordBatchesForTesting()) {
		INFO(entry.first);
		REQUIRE(entry.second);
		CHECK(entry.second->num_rows() == 1);
		auto status = entry.second->ValidateFull();
		INFO(status.ToString());
		CHECK(status.ok());
	}
}

TEST_CASE("Records round-trip through ToBatch/FromBatch", "[protocol-records]") {
	SECTION("maps, optional map, lists, binary") {
		SchemaInfo info;
		info.comment = "a schema";
		info.tags = {{"owner", "ops"}, {"tier", "gold"}};
		info.attach_opaque_data = {0, 1, 2, 255};
		info.path = {"catalog", "main"};
		info.estimated_object_count = std::map<std::string, int64_t> {{"TABLE", 3}, {"VIEW", 0}};
		CHECK(RoundTrip(info) == info);

		SchemaInfo bare;
		auto decoded = RoundTrip(bare);
		CHECK(decoded == bare);
		CHECK(!decoded.comment.has_value());
		CHECK(!decoded.estimated_object_count.has_value());
	}

	SECTION("list<struct> with timestamps and nested optionals") {
		CatalogInfo info;
		info.name = "cat";
		info.data_version_spec = ">=1";
		info.attach_option_specs = {{1, 2}, {}, {3}};
		CatalogRelease r1;
		r1.version = "1.0";
		r1.released_at = 1700000000000000LL;
		r1.summary = "first";
		CatalogRelease r2;
		r2.version = "2.0";
		r2.summary = "second";
		r2.notes_url = "https://example.com";
		info.releases = {r1, r2};
		CHECK(RoundTrip(info) == info);
	}

	SECTION("enums, large_binary, list<large_binary>, doubles, nulls") {
		InitRequest req;
		req.bind_call = {9, 9};
		req.output_schema = {1};
		req.projection_ids = std::vector<int64_t> {0, 2, 5};
		req.pushdown_filters = Buf("filters");
		req.join_keys = std::vector<LargeBytes> {Buf("k1"), Buf(""), Buf("k3")};
		req.phase = VgiInitPhase::TableBufferingFinalize;
		req.order_by_column_name = "x";
		req.order_by_direction = VgiOrderByDirection::Desc;
		req.order_by_null_order = VgiNullOrder::NullsFirst;
		req.order_by_limit = 10;
		req.tablesample_percentage = 12.5;
		CHECK(RoundTrip(req) == req);

		InitRequest empty;
		CHECK(RoundTrip(empty) == empty);
	}

	SECTION("enum dictionaries carry every enumerator in protocol order") {
		InitRequest req;
		req.phase = VgiInitPhase::Finalize;
		auto batch = ToBatch(req);
		auto column = batch->GetColumnByName("phase");
		REQUIRE(column->type_id() == arrow::Type::DICTIONARY);
		const auto &dict = static_cast<const arrow::DictionaryArray &>(*column);
		const auto &values = static_cast<const arrow::StringArray &>(*dict.dictionary());
		REQUIRE(values.length() == 4);
		CHECK(values.GetString(0) == "INPUT");
		CHECK(values.GetString(3) == "TABLE_BUFFERING_FINALIZE");
		CHECK(dict.GetValueIndex(0) == 1);
		CHECK(std::string(ToWire(VgiInitPhase::Finalize)) == "FINALIZE");
		CHECK(VgiInitPhaseFromWire("INPUT") == VgiInitPhase::Input);
		CHECK(!VgiInitPhaseFromWire("NOPE").has_value());
	}
}

TEST_CASE("Decoding keeps the compatibility contract", "[protocol-records]") {
	SchemaInfo info;
	info.path = {"main"};
	auto full = ToBatch(info);

	SECTION("a missing nullable column is absent") {
		auto idx = full->schema()->GetFieldIndex("estimated_object_count");
		auto batch = full->RemoveColumn(idx).ValueOrDie();
		auto decoded = Decode<SchemaInfo>(batch, 0, "w");
		CHECK(!decoded.estimated_object_count.has_value());
		CHECK(decoded.path == info.path);
	}

	SECTION("a missing required column names the column, record and worker") {
		auto idx = full->schema()->GetFieldIndex("path");
		auto batch = full->RemoveColumn(idx).ValueOrDie();
		auto msg = ErrorOf([&] { Decode<SchemaInfo>(batch, 0, "the-worker"); });
		CHECK(msg.find("Required column 'path' not found in SchemaInfo response") != std::string::npos);
		CHECK(msg.find("the-worker") != std::string::npos);
	}

	SECTION("a null in a required column is an error") {
		auto idx = full->schema()->GetFieldIndex("attach_opaque_data");
		auto nulls = arrow::MakeArrayOfNull(arrow::binary(), 1).ValueOrDie();
		auto field = arrow::field("attach_opaque_data", arrow::binary(), /*nullable=*/true);
		auto batch = full->SetColumn(idx, field, nulls).ValueOrDie();
		auto msg = ErrorOf([&] { Decode<SchemaInfo>(batch, 0, "w"); });
		CHECK(msg.find("unexpected null value") != std::string::npos);
	}

	SECTION("a column of the wrong type is an error, not a silent default") {
		auto idx = full->schema()->GetFieldIndex("comment");
		arrow::Int64Builder b;
		REQUIRE(b.Append(7).ok());
		auto batch = full->SetColumn(idx, arrow::field("comment", arrow::int64()), b.Finish().ValueOrDie()).ValueOrDie();
		auto msg = ErrorOf([&] { Decode<SchemaInfo>(batch, 0, "w"); });
		CHECK(msg.find("Column 'comment' in SchemaInfo") != std::string::npos);
		CHECK(msg.find("expected utf8") != std::string::npos);
	}

	SECTION("extra columns from a newer peer are ignored") {
		arrow::StringBuilder b;
		REQUIRE(b.Append("future").ok());
		auto batch =
		    full->AddColumn(full->num_columns(), arrow::field("added_later", arrow::utf8()), b.Finish().ValueOrDie())
		        .ValueOrDie();
		CHECK(Decode<SchemaInfo>(batch, 0, "w") == info);
	}

	SECTION("an unknown enum value is an error naming the enum") {
		InitRequest req;
		req.phase = VgiInitPhase::Input;
		auto batch = ToBatch(req);
		auto idx = batch->schema()->GetFieldIndex("phase");
		arrow::StringBuilder dict_builder;
		REQUIRE(dict_builder.Append("SOMETHING_NEW").ok());
		arrow::Int16Builder index_builder;
		REQUIRE(index_builder.Append(0).ok());
		auto column = arrow::DictionaryArray::FromArrays(batch->schema()->field(idx)->type(),
		                                                 index_builder.Finish().ValueOrDie(),
		                                                 dict_builder.Finish().ValueOrDie())
		                  .ValueOrDie();
		auto changed = batch->SetColumn(idx, batch->schema()->field(idx), column).ValueOrDie();
		auto msg = ErrorOf([&] { Decode<InitRequest>(changed, 0, "w"); });
		CHECK(msg.find("unknown phase 'SOMETHING_NEW' in InitRequest (not a VgiInitPhase value)") != std::string::npos);
	}

	SECTION("a row outside the batch is an error") {
		auto msg = ErrorOf([&] { Decode<SchemaInfo>(full, 3, "w"); });
		CHECK(msg.find("has 1 rows; row 3 was requested") != std::string::npos);
	}
}
