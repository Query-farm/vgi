// © Copyright 2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Every request record this client hand-builds is checked against the schema
// the protocol declares for it.
//
// Each SDK grew a schema-parity test during the 1.4.0 work, and each one found
// real drift. The client — the peer every worker has to agree with — had none,
// because until now the generated header did not carry the *request* schemas at
// all: a method's params schema is the outer envelope (`request: binary`), and
// the record itself rides inside as an opaque blob. So `vgi_rpc_types.cpp` built
// BindRequest, InitRequest and CatalogAttachRequest by hand against nothing.
//
// Two defects reached HEAD through that gap, both found only when a worker
// started validating its declared parameter contract:
//
//   * catalog_attach's `options` was built non-nullable against a protocol that
//     declares it nullable — missed by the nullability migration, which audited
//     the generated headers and the hand-built InitRequest but never the
//     hand-built CatalogAttachRequest;
//   * bind emitted 12, 13 or 14 columns depending on whether the call was a
//     COPY, and put schema_name before the copy fields rather than last.
//
// Neither is visible from this side alone: the client writes the bytes and
// nothing here reads them back.
//
// ---------------------------------------------------------------------------
// The rule: EXACT equality, not "close enough"
// ---------------------------------------------------------------------------
//
// Every emitted record must match the declared schema field-for-field — same
// names, same order, same types, same nullability. A field the client has no
// value for ships as an explicit null; it does not get left out.
//
// That is stricter than it first looks like it needs to be, and the strictness
// is load-bearing. Every SDK reads these records BY NAME, so it is tempting to
// conclude a subset is wire-safe and that padding a record with a dozen
// always-null columns is noise. This test was written that way, and it was
// wrong: vgi-rpc-go validates a request against its declared parameter
// contract with `arrow.Schema.Equal`, which is order-, name-, type- AND
// nullability-sensitive. Against a Go worker, an InitRequest carrying 18 of the
// declared 19 columns — the missing one nullable, and one DuckDB genuinely has
// no value for — failed EVERY call with "parameter schema mismatch", printing
// two nineteen-line schemas that differ in a single row.
//
// So the rule is equality, and the comment on each builder says which of its
// null columns are null-by-design rather than forgotten.

#include "catch.hpp"

#include "vgi_rpc_types.hpp"
#include "vgi_table_buffering_builders.hpp"
#include "../../src/generated/vgi_protocol_schemas.hpp"

#include <arrow/api.h>

#include <string>
#include <vector>

using namespace duckdb::vgi;

namespace {

// Every way the built record departs from the declaration, as sentences naming
// the record and field. Reported as a list rather than a bare
// `arrow::Schema::Equals` because the failure this guards against is a peer
// rejecting the whole batch with no detail — the diagnostics ARE the point.
// Go's own message prints both schemas in full and leaves the reader to diff
// nineteen lines by eye.
std::vector<std::string> DiffSchema(const std::string &record, const arrow::Schema &declared,
                                    const arrow::Schema &built) {
	std::vector<std::string> problems;

	std::vector<std::string> want, got;
	for (const auto &f : declared.fields()) {
		want.push_back(f->name());
	}
	for (const auto &f : built.fields()) {
		got.push_back(f->name());
	}
	if (want != got) {
		// Report the column lists whole. A missing or reordered column shifts
		// every field after it, and a per-field diff would then read as a dozen
		// unrelated type errors rather than one dropped or misplaced column.
		auto join = [](const std::vector<std::string> &v) {
			std::string out;
			for (const auto &n : v) {
				out += (out.empty() ? "" : ", ") + n;
			}
			return out;
		};
		std::string detail;
		for (const auto &n : want) {
			if (built.GetFieldIndex(n) < 0) {
				detail += "\n  " + record + "." + n +
				          ": declared but the builder does not emit it (ship an explicit null — a "
				          "peer comparing with Schema::Equals rejects the record for a missing "
				          "nullable column)";
			}
		}
		for (const auto &n : got) {
			if (declared.GetFieldIndex(n) < 0) {
				detail += "\n  " + record + "." + n + ": emitted but the protocol declares no such field";
			}
		}
		problems.push_back(record + ": column names/order: protocol declares [" + join(want) +
		                   "] but the builder emits [" + join(got) + "]" + detail);
		return problems;
	}

	for (int i = 0; i < declared.num_fields(); i++) {
		const auto &d = *declared.field(i);
		const auto &b = *built.field(i);
		const std::string path = record + "." + d.name();
		if (!d.type()->Equals(*b.type())) {
			problems.push_back(path + ": protocol declares " + d.type()->ToString() + " but the builder emits " +
			                   b.type()->ToString());
		}
		if (d.nullable() != b.nullable()) {
			problems.push_back(path + ": protocol declares nullable=" + (d.nullable() ? "true" : "false") +
			                   " but the builder emits nullable=" + (b.nullable() ? "true" : "false"));
		}
	}
	return problems;
}

void RequireMatches(const std::string &record, const std::shared_ptr<arrow::Schema> &declared,
                    const std::shared_ptr<arrow::RecordBatch> &built) {
	REQUIRE(built != nullptr);
	auto problems = DiffSchema(record, *declared, *built->schema());
	if (!problems.empty()) {
		std::string joined;
		for (const auto &p : problems) {
			joined += "\n  " + p;
		}
		FAIL(record + " does not match the protocol:" + joined);
	}
}

// Some builders return the outer RPC params (`request: binary`) rather than the
// inner record — the buffered-table ones serialize and wrap in one step. Pull
// the record back out so the check runs against the bytes that actually go on
// the wire, which is the stronger place to run it.
std::shared_ptr<arrow::RecordBatch> UnwrapRequest(const std::shared_ptr<arrow::RecordBatch> &params) {
	REQUIRE(params != nullptr);
	int idx = params->schema()->GetFieldIndex("request");
	REQUIRE(idx >= 0);
	auto col = std::static_pointer_cast<arrow::BinaryArray>(params->column(idx));
	REQUIRE(col->length() == 1);
	REQUIRE(!col->IsNull(0));
	auto view = col->GetView(0);
	return DeserializeFromIpcBytes(reinterpret_cast<const uint8_t *>(view.data()), view.size());
}

} // namespace

// ---------------------------------------------------------------------------
// BindRequest — all three call shapes.
//
// The COPY variants are here because the defect was shape-DEPENDENT: the plain
// bind emitted 12 columns, a COPY FROM bind 13, a COPY TO bind 13, and the
// declaration is 14 in every case. A single-shape test would have passed on
// whichever shape it happened to pick.
// ---------------------------------------------------------------------------

TEST_CASE("BuildBindRequest matches the declared schema", "[schema-parity]") {
	RequireMatches("BindRequest", generated::BindRequestSchema(), BuildBindRequest("f", {}, "TABLE"));
}

TEST_CASE("BuildBindRequest matches with a COPY FROM context", "[schema-parity]") {
	CopyFromBindContext copy_from;
	copy_from.format = "acme.lines";
	copy_from.file_path = "/tmp/x";
	RequireMatches("BindRequest(copy_from)", generated::BindRequestSchema(),
	               BuildBindRequest("f", {}, "TABLE", {}, {}, {}, {}, {}, false, {}, {}, &copy_from));
}

TEST_CASE("BuildBindRequest matches with a COPY TO context", "[schema-parity]") {
	CopyToBindContext copy_to;
	copy_to.format = "acme.lines_out";
	copy_to.file_path = "/tmp/y";
	RequireMatches("BindRequest(copy_to)", generated::BindRequestSchema(),
	               BuildBindRequest("f", {}, "TABLE", {}, {}, {}, {}, {}, false, {}, {}, nullptr, &copy_to));
}

TEST_CASE("BuildBindRequest matches with time travel and a schema name", "[schema-parity]") {
	RequireMatches("BindRequest(at + schema_name)", generated::BindRequestSchema(),
	               BuildBindRequest("f", {}, "TABLE", {}, {}, {}, {}, {}, false, "version", "3", nullptr, nullptr,
	                                "data"));
}

// ---------------------------------------------------------------------------
// InitRequest — one schema regardless of phase, so one case covers it.
// ---------------------------------------------------------------------------

TEST_CASE("BuildInitRequest matches the declared schema", "[schema-parity]") {
	RequireMatches("InitRequest", generated::InitRequestSchema(), BuildInitRequest({}, {}));
}

TEST_CASE("BuildInitRequest matches when redeeming split tokens", "[schema-parity]") {
	RequireMatches("InitRequest(splits)", generated::InitRequestSchema(),
	               BuildInitRequest({}, {}, {}, {}, nullptr, {}, "", {}, {}, {}, {}, {}, -1, -1.0, -1, std::nullopt, {},
	                                {std::string("token-bytes")}));
}

// ---------------------------------------------------------------------------
// The scan-planning pair.
// ---------------------------------------------------------------------------

TEST_CASE("BuildTableFunctionPlanRequest matches the declared schema", "[schema-parity]") {
	RequireMatches("TableFunctionPlanRequest", generated::TableFunctionPlanRequestSchema(),
	               BuildTableFunctionPlanRequest({}, {}, {}, {}, /*min_splits=*/8, /*target_split_bytes=*/0, {}));
}

TEST_CASE("BuildTableFunctionCardinalityRequest matches the declared schema", "[schema-parity]") {
	RequireMatches("TableFunctionCardinalityRequest", generated::TableFunctionCardinalityRequestSchema(),
	               BuildTableFunctionCardinalityRequest({}));
}

// ---------------------------------------------------------------------------
// catalog_attach — the record whose `options` nullability was wrong at HEAD.
// ---------------------------------------------------------------------------

TEST_CASE("BuildCatalogAttachRequest matches the declared schema", "[schema-parity]") {
	RequireMatches("CatalogAttachRequest", generated::CatalogAttachRequestSchema(),
	               BuildCatalogAttachRequest("cat"));
}

// ---------------------------------------------------------------------------
// The buffered-table trio. These wrap into the outer params, so they exercise
// the unwrap path — the record is checked as it appears on the wire.
// ---------------------------------------------------------------------------

TEST_CASE("BuildTableBufferingProcessInner matches the declared schema", "[schema-parity]") {
	RequireMatches("TableBufferingProcessRequest", generated::TableBufferingProcessRequestSchema(),
	               UnwrapRequest(BuildTableBufferingProcessInner("f", "main", {1, 2}, {3, 4}, {})));
}

TEST_CASE("BuildTableBufferingProcessInner matches with a batch index", "[schema-parity]") {
	RequireMatches("TableBufferingProcessRequest(batch_index)", generated::TableBufferingProcessRequestSchema(),
	               UnwrapRequest(BuildTableBufferingProcessInner("f", "main", {1, 2}, {3, 4}, {}, 7)));
}

TEST_CASE("BuildTableBufferingCombineInner matches the declared schema", "[schema-parity]") {
	std::vector<std::vector<uint8_t>> state_ids = {{1}, {2}};
	RequireMatches("TableBufferingCombineRequest", generated::TableBufferingCombineRequestSchema(),
	               UnwrapRequest(BuildTableBufferingCombineInner("f", "main", {1, 2}, state_ids, {})));
}

TEST_CASE("BuildTableBufferingDestructorInner matches the declared schema", "[schema-parity]") {
	RequireMatches("TableBufferingDestructorRequest", generated::TableBufferingDestructorRequestSchema(),
	               UnwrapRequest(BuildTableBufferingDestructorInner("f", "main", {1, 2}, {})));
}

// ---------------------------------------------------------------------------
// The checker itself. A parity test that cannot fail is worse than none, and
// each of these three rules exists because a different real defect slipped past
// the absence of it.
// ---------------------------------------------------------------------------

TEST_CASE("DiffSchema catches the drift classes it exists for", "[schema-parity]") {
	auto declared = arrow::schema({
	    arrow::field("a", arrow::utf8(), /*nullable=*/false),
	    arrow::field("b", arrow::binary(), /*nullable=*/true),
	    arrow::field("c", arrow::int64(), /*nullable=*/true),
	});

	SECTION("an exact match is clean") {
		REQUIRE(DiffSchema("R", *declared, *declared).empty());
	}

	SECTION("omitting a NULLABLE field is still a failure — the Go finding") {
		auto built = arrow::schema({arrow::field("a", arrow::utf8(), false),
		                            arrow::field("c", arrow::int64(), true)});
		auto problems = DiffSchema("R", *declared, *built);
		REQUIRE(problems.size() == 1);
		REQUIRE(problems[0].find("R.b: declared but the builder does not emit it") != std::string::npos);
	}

	SECTION("a wrong type is caught") {
		auto built = arrow::schema({arrow::field("a", arrow::utf8(), false),
		                            arrow::field("b", arrow::binary(), true),
		                            arrow::field("c", arrow::int32(), true)});
		auto problems = DiffSchema("R", *declared, *built);
		REQUIRE(problems.size() == 1);
		REQUIRE(problems[0].find("int32") != std::string::npos);
	}

	SECTION("a wrong nullability is caught — the catalog_attach defect") {
		auto built = arrow::schema({arrow::field("a", arrow::utf8(), false),
		                            arrow::field("b", arrow::binary(), false),
		                            arrow::field("c", arrow::int64(), true)});
		auto problems = DiffSchema("R", *declared, *built);
		REQUIRE(problems.size() == 1);
		REQUIRE(problems[0].find("nullable=") != std::string::npos);
	}

	SECTION("a reordering is caught — the bind defect") {
		auto built = arrow::schema({arrow::field("a", arrow::utf8(), false),
		                            arrow::field("c", arrow::int64(), true),
		                            arrow::field("b", arrow::binary(), true)});
		auto problems = DiffSchema("R", *declared, *built);
		REQUIRE(problems.size() == 1);
		REQUIRE(problems[0].find("column names/order") != std::string::npos);
	}

	SECTION("a phantom column is caught") {
		auto built = arrow::schema({arrow::field("a", arrow::utf8(), false),
		                            arrow::field("b", arrow::binary(), true),
		                            arrow::field("c", arrow::int64(), true),
		                            arrow::field("zzz", arrow::utf8(), true)});
		auto problems = DiffSchema("R", *declared, *built);
		REQUIRE(problems.size() == 1);
		REQUIRE(problems[0].find("R.zzz: emitted but the protocol declares no such field") != std::string::npos);
	}
}
