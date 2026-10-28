// © Copyright 2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// The generated protocol records (src/generated/vgi_protocol_records.{hpp,cpp}) and their
// runtime (src/vgi_wire_codec.cpp).
//
// - Every record encodes with EXACTLY the schema vgi_protocol_schemas.hpp declares for it
//   (names, order, types, nullability, metadata): peers such as vgi-rpc-go compare request
//   schemas with arrow.Schema.Equal, so "close enough" fails every call.
// - Records round-trip: ToBatch -> ValidateBatch(FULL) -> FromBatch gives back an equal record, for
//   every shape the protocol uses (maps, nested lists, list<struct>, enums, timestamps,
//   large_binary, optional members).
// - Decoding keeps the client's compatibility contract: a missing nullable column is absent,
//   a missing or null required column is a protocol error, and so is a column of the wrong
//   type or an enum value the client does not know.

#include "catch.hpp"

#include "../../src/generated/vgi_protocol_records.hpp"
#include "vgi_wire_codec.hpp"
#include "vgi_record_access.hpp"
#include "vgi_rpc_types.hpp"
#include "../../src/generated/vgi_request_builders.hpp"

#include "duckdb/common/exception.hpp"

#include "vgi_arrow.hpp"

#include "nanoarrow/nanoarrow.h"

#include <functional>
#include <string>

using namespace duckdb::vgi;
using namespace duckdb::vgi::generated;

namespace {

LargeBytes Buf(const std::string &s) {
	return VgiBuffer::FromString(s);
}

template <class T>
T RoundTrip(const T &value) {
	auto batch = ToBatch(value);
	REQUIRE(batch);
	REQUIRE(batch->num_rows() == 1);
	REQUIRE_NOTHROW(ValidateBatch(*batch, VgiValidation::FULL));
	return Decode<T>(batch, 0, "test-worker");
}

// A one-row batch of `schema` whose column j is row 0 of `batch`'s column
// `sources[j]`, or -- when sources[j] < 0 -- whatever `fill(j, array)` appends.
VgiBatchPtr Rebuild(const VgiBatchPtr &batch, const VgiSchemaPtr &schema, const std::vector<int> &sources,
                    const std::function<void(int, ArrowArray *)> &fill = nullptr) {
	REQUIRE(batch->num_rows() == 1);
	REQUIRE(static_cast<int>(sources.size()) == schema->num_fields());
	VgiBatchBuilder builder(schema);
	for (int j = 0; j < schema->num_fields(); j++) {
		if (sources[j] >= 0) {
			AppendElement(builder.column(j), batch->column(sources[j]), 0);
		} else {
			fill(j, builder.column(j));
		}
	}
	builder.FinishRow();
	return builder.Finish(batch->metadata());
}

std::vector<int> Identity(int n) {
	std::vector<int> out;
	for (int i = 0; i < n; i++) {
		out.push_back(i);
	}
	return out;
}

// `batch` without column `idx`.
VgiBatchPtr RemoveColumn(const VgiBatchPtr &batch, int idx) {
	REQUIRE(idx >= 0);
	auto sources = Identity(batch->num_columns());
	sources.erase(sources.begin() + idx);
	return Rebuild(batch, batch->schema()->RemoveField(idx), sources);
}

// `batch` with column `idx` replaced by `field`, its value appended by `fill`.
VgiBatchPtr SetColumn(const VgiBatchPtr &batch, int idx, const ArrowSchema &field,
                      const std::function<void(ArrowArray *)> &fill) {
	REQUIRE(idx >= 0);
	auto sources = Identity(batch->num_columns());
	sources[idx] = -1;
	return Rebuild(batch, batch->schema()->SetField(idx, field), sources, [&](int, ArrowArray *a) { fill(a); });
}

// `batch` with `field` appended as its last column, its value appended by `fill`.
VgiBatchPtr AddColumn(const VgiBatchPtr &batch, const ArrowSchema &field,
                      const std::function<void(ArrowArray *)> &fill) {
	auto sources = Identity(batch->num_columns());
	sources.push_back(-1);
	return Rebuild(batch, batch->schema()->AddField(batch->num_columns(), field), sources,
	               [&](int, ArrowArray *a) { fill(a); });
}

// The schema with its schema- and field-level metadata, for failure diagnostics.
std::string Describe(const VgiSchema &schema) {
	auto pairs = [](const VgiMetadata &md) {
		std::string out;
		for (int64_t i = 0; i < md.size(); i++) {
			out += (i ? ", " : "") + md.key(i) + "=" + md.value(i);
		}
		return out;
	};
	std::string out;
	for (int i = 0; i < schema.num_fields(); i++) {
		out += "\n  " + schema.field_name(i) + ": " + TypeToString(schema.field(i)) +
		       (schema.field_nullable(i) ? "" : " not null");
		auto md = schema.field_metadata(i);
		if (!md.empty()) {
			out += " {" + pairs(md) + "}";
		}
	}
	if (!schema.metadata().empty()) {
		out += "\n  -- schema metadata: " + pairs(schema.metadata());
	}
	return out;
}

// A standalone field description, as a schema node.
VgiSchemaPtr FieldOf(const VgiField &field) {
	return MakeSchema({field});
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

TEST_CASE("Semantic record access uses the strict wire decoder", "[protocol-records]") {
	SchemaInfo info;
	info.path = {"literal.schema"};
	info.comment = "";
	auto batch = ToBatch(info);
	RecordBatchSingleRow row(batch, 0, "SchemaInfo", "semantic-worker");
	CHECK(row["comment"].as<std::string>() == std::optional<std::string>(""));
	CHECK(row["path"].value_not_null<std::vector<std::string>>() == info.path);
	CHECK(row["estimated_object_count"].is_null());
	CHECK_THROWS(row["missing"].is_null());
	CHECK_THROWS(RecordBatchSingleRow(batch, 1, "SchemaInfo", "semantic-worker"));

	SECTION("a malformed optional value does not become a default") {
		auto field = FieldOf(type::Int64().Named("comment"));
		auto changed = SetColumn(batch, batch->schema()->GetFieldIndex("comment"), field->field(0),
		                         [](ArrowArray *a) { append::Int(a, 7); });
		RecordBatchSingleRow bad(changed, 0, "SchemaInfo", "semantic-worker");
		CHECK_THROWS(bad["comment"].value_or("fallback"));
	}
	SECTION("duplicate map keys cannot be silently overwritten") {
		auto idx = batch->schema()->GetFieldIndex("tags");
		auto changed = SetColumn(batch, idx, batch->schema()->field(idx), [](ArrowArray *a) {
			auto *entries = a->children[0];
			for (const auto &value : {"first", "second"}) {
				append::String(entries->children[0], "owner");
				append::String(entries->children[1], value);
				append::FinishElement(entries);
			}
			append::FinishElement(a);
		});
		RecordBatchSingleRow bad(changed, 0, "SchemaInfo", "semantic-worker");
		CHECK_THROWS((bad["tags"].value_not_null<std::map<std::string, std::string>>()));
	}
}

TEST_CASE("Every record encodes with its protocol schema", "[protocol-records]") {
	auto pairs = RecordSchemaPairsForTesting();
	REQUIRE(pairs.size() > 50);
	for (auto &p : pairs) {
		INFO(p.name << "\n record:  " << Describe(*p.record) << "\n factory: " << Describe(*p.factory));
		CHECK(p.record->Equals(*p.factory, /*check_metadata=*/true));
	}
}

TEST_CASE("Default-constructed records encode valid one-row batches", "[protocol-records]") {
	for (auto &entry : DefaultRecordBatchesForTesting()) {
		INFO(entry.first);
		REQUIRE(entry.second);
		CHECK(entry.second->num_rows() == 1);
		CHECK_NOTHROW(ValidateBatch(*entry.second, VgiValidation::FULL));
	}
}

TEST_CASE("Records round-trip through ToBatch/FromBatch", "[protocol-records]") {
	SECTION("maps, optional map, lists, binary") {
		SchemaInfo info;
		info.comment = "a schema";
		info.tags = {{"owner", "ops"}, {"tier", "gold"}};
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
		REQUIRE(column);
		REQUIRE(column.is_dictionary());
		auto values = column.dictionary();
		REQUIRE(values.length() == 4);
		CHECK(values.GetString(0) == "INPUT");
		CHECK(values.GetString(3) == "TABLE_BUFFERING_FINALIZE");
		CHECK(column.GetInt(0) == 1);
		CHECK(std::string(ToWire(VgiInitPhase::Finalize)) == "FINALIZE");
		CHECK(VgiInitPhaseFromWire("INPUT") == VgiInitPhase::Input);
		CHECK(!VgiInitPhaseFromWire("NOPE").has_value());
	}
}

TEST_CASE("Enums accept only their canonical member names", "[protocol-records]") {
	CHECK(FunctionTypeFromWire("TABLE") == FunctionType::Table);
	CHECK(FunctionTypeFromWire("TABLE_BUFFERING") == FunctionType::TableBuffering);
	CHECK(!FunctionTypeFromWire("table").has_value());
	CHECK(!FunctionTypeFromWire("table_in_out").has_value());
	CHECK(!MacroTypeFromWire("scalar").has_value());
	CHECK(std::string(ToWire(FunctionType::Table)) == "TABLE");
	CHECK(!VgiInitPhaseFromWire("input").has_value());
}

TEST_CASE("Function metadata binary dictionaries preserve the byte API", "[protocol-records]") {
	FunctionInfo info;
	info.arguments = {0xff, 0, 0x80};
	info.output_schema = {};
	CHECK(RoundTrip(info) == info);
	auto batch = ToBatch(info);
	for (const auto &name : {"arguments", "output_schema", "parameter_default_values"}) {
		auto column = batch->GetColumnByName(name);
		REQUIRE(column.is_dictionary());
		CHECK(column.storage_type() == NANOARROW_TYPE_INT32);
		CHECK(column.dictionary().storage_type() == NANOARROW_TYPE_BINARY);
	}
	CHECK(batch->GetColumnByName("parameter_default_values").IsNull(0));
	info.parameter_default_values = Bytes {};
	CHECK(RoundTrip(info).parameter_default_values == info.parameter_default_values);
	CHECK(!ToBatch(info)->GetColumnByName("parameter_default_values").IsNull(0));
	info.parameter_default_values = Bytes {0xfe, 0, 0xff};
	CHECK(RoundTrip(info) == info);
}

TEST_CASE("Binary dictionary null references respect field nullability", "[protocol-records]") {
	FunctionInfo info;
	auto batch = ToBatch(info);
	for (const auto &name : {"arguments", "output_schema", "parameter_default_values"}) {
		INFO(name);
		auto idx = batch->schema()->GetFieldIndex(name);
		auto changed = SetColumn(batch, idx, batch->schema()->field(idx), [](ArrowArray *a) {
			append::Null(a->dictionary);
			append::Int(a, 0);
		});
		REQUIRE_NOTHROW(ValidateBatch(*changed, VgiValidation::FULL));
		if (std::string(name) == "parameter_default_values") {
			CHECK(!Decode<FunctionInfo>(changed, 0, "w").parameter_default_values.has_value());
		} else {
			CHECK(ErrorOf([&] { Decode<FunctionInfo>(changed, 0, "w"); }).find("unexpected null value") !=
			      std::string::npos);
		}
	}
}

TEST_CASE("Decoding keeps the compatibility contract", "[protocol-records]") {
	SchemaInfo info;
	info.path = {"main"};
	auto full = ToBatch(info);

	SECTION("a missing nullable column is absent") {
		auto idx = full->schema()->GetFieldIndex("estimated_object_count");
		auto batch = RemoveColumn(full, idx);
		auto decoded = Decode<SchemaInfo>(batch, 0, "w");
		CHECK(!decoded.estimated_object_count.has_value());
		CHECK(decoded.path == info.path);
	}

	SECTION("a missing required column names the column, record and worker") {
		auto idx = full->schema()->GetFieldIndex("path");
		auto batch = RemoveColumn(full, idx);
		auto msg = ErrorOf([&] { Decode<SchemaInfo>(batch, 0, "the-worker"); });
		CHECK(msg.find("Required column 'path' not found in SchemaInfo response") != std::string::npos);
		CHECK(msg.find("the-worker") != std::string::npos);
	}

	SECTION("a null in a required column is an error") {
		auto idx = full->schema()->GetFieldIndex("path");
		auto field = FieldOf(type::List(type::Utf8()).Named("path", /*is_nullable=*/true));
		auto batch = SetColumn(full, idx, field->field(0), [](ArrowArray *a) { append::Null(a); });
		auto msg = ErrorOf([&] { Decode<SchemaInfo>(batch, 0, "w"); });
		CHECK(msg.find("unexpected null value") != std::string::npos);
	}

	SECTION("a null non-nullable list, map or list item is an error") {
		auto path_idx = full->schema()->GetFieldIndex("path");
		auto list_field = FieldOf(type::List(type::Utf8()).Named("path", true));
		auto batch = SetColumn(full, path_idx, list_field->field(0), [](ArrowArray *a) { append::Null(a); });
		CHECK(ErrorOf([&] { Decode<SchemaInfo>(batch, 0, "w"); }).find("unexpected null value") != std::string::npos);

		auto tags_idx = full->schema()->GetFieldIndex("tags");
		auto map_field = FieldOf(type::Map(type::Utf8(), type::Utf8()).Named("tags", true));
		batch = SetColumn(full, tags_idx, map_field->field(0), [](ArrowArray *a) { append::Null(a); });
		CHECK(ErrorOf([&] { Decode<SchemaInfo>(batch, 0, "w"); }).find("unexpected null value") != std::string::npos);

		auto missing = RemoveColumn(full, path_idx);
		CHECK(ErrorOf([&] { Decode<SchemaInfo>(missing, 0, "w"); }).find("Required column 'path'") !=
		      std::string::npos);

		// The declared `path` field (non-nullable items), carrying ["a", null].
		auto with_null_item = SetColumn(full, path_idx, full->schema()->field(path_idx), [](ArrowArray *a) {
			append::String(a->children[0], "a");
			append::Null(a->children[0]);
			append::FinishElement(a);
		});
		auto msg = ErrorOf([&] { Decode<SchemaInfo>(with_null_item, 0, "w"); });
		CHECK(msg.find("unexpected null value") != std::string::npos);
		CHECK(msg.find("path[]") != std::string::npos);
	}

	SECTION("a column of the wrong type is an error, not a silent default") {
		auto idx = full->schema()->GetFieldIndex("comment");
		auto field = FieldOf(type::Int64().Named("comment"));
		auto batch = SetColumn(full, idx, field->field(0), [](ArrowArray *a) { append::Int(a, 7); });
		auto msg = ErrorOf([&] { Decode<SchemaInfo>(batch, 0, "w"); });
		CHECK(msg.find("Column 'comment' in SchemaInfo") != std::string::npos);
		CHECK(msg.find("expected utf8") != std::string::npos);
	}

	SECTION("extra columns from a newer peer are ignored") {
		auto field = FieldOf(type::Utf8().Named("added_later"));
		auto batch = AddColumn(full, field->field(0), [](ArrowArray *a) { append::String(a, "future"); });
		REQUIRE(batch->num_columns() == full->num_columns() + 1);
		CHECK(Decode<SchemaInfo>(batch, 0, "w") == info);
	}

	SECTION("an unknown enum value is an error naming the enum") {
		InitRequest req;
		req.phase = VgiInitPhase::Input;
		auto batch = ToBatch(req);
		auto idx = batch->schema()->GetFieldIndex("phase");
		// Same dictionary<int16, utf8> field, but a dictionary holding only a value
		// this client does not know.
		auto changed = SetColumn(batch, idx, batch->schema()->field(idx), [](ArrowArray *a) {
			append::String(a->dictionary, "SOMETHING_NEW");
			append::Int(a, 0);
		});
		auto msg = ErrorOf([&] { Decode<InitRequest>(changed, 0, "w"); });
		CHECK(msg.find("unknown phase 'SOMETHING_NEW' in InitRequest (not a VgiInitPhase value)") != std::string::npos);
	}

	SECTION("a row outside the batch is an error") {
		auto msg = ErrorOf([&] { Decode<SchemaInfo>(full, 3, "w"); });
		CHECK(msg.find("has 1 rows; row 3 was requested") != std::string::npos);
	}
}

TEST_CASE("V3 capability structs keep nested shape and optional presence", "[protocol-records]") {
	ClientQueryCapabilities query;
	query.encodings = {"sql.v1"};
	query.semantic_profiles = {"duckdb.v1"};
	query.engine_version = "2.0.0-dev";
	auto owner = ToBatch(query);
	VgiColumn value(&owner->c_view(), &owner->schema()->c_schema());
	auto batch = BuildClientCapabilities("duckdb", {}, {}, true, {}, std::nullopt, value);
	owner.reset();
	CHECK(Decode<ClientCapabilities>(batch, 0, "w").query_pushdown == std::make_optional(query));
	auto absent = BuildClientCapabilities("duckdb", {}, {}, true, {}, std::nullopt, VgiColumn());
	CHECK(!Decode<ClientCapabilities>(absent, 0, "w").query_pushdown.has_value());

	auto wrong = ToBatch(SchemaInfo {});
	VgiColumn wrong_value(&wrong->c_view(), &wrong->schema()->c_schema());
	CHECK_THROWS(BuildClientCapabilities("duckdb", {}, {}, true, {}, std::nullopt, wrong_value));

	auto renamed = SetColumn(ToBatch(query), 0, FieldOf(type::List(type::Utf8(), false).Named("wrong", false))->field(0),
	                         [](ArrowArray *a) { append::FinishElement(a); });
	VgiColumn renamed_value(&renamed->c_view(), &renamed->schema()->c_schema());
	CHECK_THROWS(BuildClientCapabilities("duckdb", {}, {}, true, {}, std::nullopt, renamed_value));
}
