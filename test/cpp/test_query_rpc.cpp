// © Copyright 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
#include "catch.hpp"
#include "../support/vgi_contract_fixtures.hpp"
#include "vgi_ipc.hpp"

#include "vgi_query_rpc.hpp"
#include "vgi_arrow_ipc.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_record_access.hpp"
#include "vgi_attach_parameters.hpp"
#include "vgi_rpc_types.hpp"
#include "../../src/generated/vgi_protocol_schemas.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/database.hpp"

using namespace duckdb;
using namespace duckdb::vgi;
using namespace duckdb::vgi_test;

namespace {
constexpr int64_t NOW = 1800000000000;

VgiQueryCapabilities Capabilities() {
	VgiQueryCapabilities result;
	result.backend_engine = "duckdb";
	result.backend_version = "1.5.0";
	result.capability_revision = "reference-v1";
	return result;
}

VgiColumn View(const VgiBatchPtr &batch) {
	return VgiColumn(&batch->c_view(), &batch->schema()->c_schema());
}

VgiBatchPtr CapabilityBatch(const VgiQueryCapabilities &caps = Capabilities()) {
	auto context = Struct(generated::QueryEvaluationContextSchema(),
	                      {BuildStringScalar(caps.default_evaluation_context.timezone), BuildStringScalar("binary"),
	                       BuildStringScalar("asc"), BuildStringScalar("nulls_last"), BuildBoolScalar(true)});
	return VgiBatch::FromColumns(generated::QueryCapabilitiesSchema(), 1,
	              {BuildStringScalar(caps.encoding), BuildStringListScalar(caps.semantic_profiles),
	               BuildStringListScalar(caps.execution_modes), BuildStringScalar(caps.backend_engine),
	               BuildStringScalar(caps.backend_version), BuildStringScalar(caps.capability_revision),
	               context, BuildInt64Scalar(caps.max_query_bytes)});
}

VgiSchemaPtr Parameters(const std::string &mode = "named", const std::string &name = "limit") {
	return MakeSchema({type::Int64().Named(name)}, Metadata({"vgi.query.parameters.v1"}, {mode}));
}

VgiBatchPtr Accepted() {
	auto output = MakeSchema({type::Int64().Named("answer")});
	return VgiBatch::FromColumns(generated::CatalogQueryPrepareResultSchema(), 1,
	    {BuildStringScalar("accepted"), BuildOptionalStringScalar(std::nullopt), BuildOptionalStringScalar(std::nullopt),
	     BuildBinaryScalarRequired({1, 2, 3}), BuildBinaryScalarRequired(SerializeSchemaToIpcBytes(output)),
	     BuildStringScalar("query_order"), BuildOptionalInt64Scalar(std::nullopt), BuildOptionalStringScalar(std::nullopt),
	     BuildBinaryScalarRequired(QueryEvaluationContextDigest(Capabilities().default_evaluation_context)),
	     BuildInt64Scalar(NOW + 60000), BuildOptionalInt64Scalar(42), BuildOptionalInt64Scalar(std::nullopt)});
}

VgiBatchPtr Replace(const VgiBatchPtr &batch, const std::string &name, ArrowArray column) {
	return ReplaceColumn(batch, batch->schema()->GetFieldIndex(name), column);
}

VgiQueryPrepareResult Parse(const VgiBatchPtr &batch) {
	return ParseVgiQueryPrepareResult(batch, Capabilities(), "query_order", NOW, "query-test");
}
} // namespace

TEST_CASE("Query context digest matches the shared Python golden vector", "[query-rpc]") {
	auto digest = QueryEvaluationContextDigest({});
	std::string hex;
	for (auto byte : digest) {
		hex += "0123456789abcdef"[byte >> 4];
		hex += "0123456789abcdef"[byte & 15];
	}
	CHECK(hex == "0d33dfb516d67c8096fb174f0ae08a04f028ad5669e4416cde2973a2dd352d34");
	VgiQueryEvaluationContext changed;
	changed.preserve_insertion_order = false;
	CHECK(QueryEvaluationContextDigest(changed) != digest);
}

TEST_CASE("Query offer is absent by default and explicitly limited when enabled", "[query-rpc]") {
	for (bool enabled : {false, true}) {
		auto attach = BuildCatalogAttachRequest("catalog", {}, "", "", enabled);
		RecordBatchSingleRow row(attach, 0, "attach", "query-test");
		auto capabilities = DeserializeFromIpcBytes(row["client_capabilities"].value_not_null<std::vector<uint8_t>>());
		auto query = capabilities->GetColumnByName("query_pushdown");
		REQUIRE(bool(query));
		CHECK(query.IsNull(0) == !enabled);
		if (!enabled) continue;
		auto offer = StructRow(query, generated::ClientQueryCapabilitiesSchema());
		RecordBatchSingleRow values(offer, 0, "query offer", "query-test");
		CHECK(values["engine_version"].value_not_null<std::string>() == DuckDB::LibraryVersion());
		CHECK(values["encodings"].value_not_null<std::vector<std::string>>() == std::vector<std::string>{VGI_QUERY_ENCODING});
		CHECK(values["semantic_profiles"].value_not_null<std::vector<std::string>>() ==
		      std::vector<std::string>{VGI_QUERY_EXPLICIT_PROFILE});
	}
}

TEST_CASE("Query capability agreement rejects unsolicited or unoffered selections", "[query-rpc]") {
	CHECK(ParseVgiQueryCapabilities(View(CapabilityBatch()), true, "query-test").has_value());
	CHECK_THROWS_AS(ParseVgiQueryCapabilities(View(CapabilityBatch()), false, "query-test"), InvalidInputException);
	auto caps = Capabilities();
	caps.execution_modes.push_back("automatic");
	CHECK_THROWS_AS(ParseVgiQueryCapabilities(View(CapabilityBatch(caps)), true, "query-test"), InvalidInputException);
	caps = Capabilities();
	caps.max_query_bytes = VGI_QUERY_MAX_SQL_BYTES + 1;
	CHECK_THROWS_AS(ParseVgiQueryCapabilities(View(CapabilityBatch(caps)), true, "query-test"), InvalidInputException);
	auto capability = CapabilityBatch();
	VgiBatchBuilder malformed(capability->schema());
	for (int i = 0; i < capability->num_columns(); ++i) {
		if (i == 1) {
			append::String(malformed.column(i)->children[0], VGI_QUERY_EXPLICIT_PROFILE);
			append::Null(malformed.column(i)->children[0]);
			append::FinishElement(malformed.column(i));
		} else AppendElement(malformed.column(i), capability->column(i), 0);
	}
	malformed.FinishRow();
	CHECK_THROWS_AS(ParseVgiQueryCapabilities(View(malformed.Finish()), true, "query-test"), InvalidInputException);
	auto field = CopyField(generated::QueryCapabilitiesSchema()->c_schema()).Named("query", true);
	auto null_batch = VgiBatch::FromColumns(MakeSchema({field}), 1, {NullArray(field)});
	CHECK_FALSE(ParseVgiQueryCapabilities(null_batch->column(0), false, "query-test").has_value());
}

TEST_CASE("Explicit query preparation preserves parameter mapping and transaction identity", "[query-rpc]") {
	CatalogRpcContext ctx {nullptr, {1, 2}, {3, 4}};
	auto request = BuildVgiQueryPrepareRequest(ctx, Capabilities(), "select $limit", Parameters());
	CHECK(request->schema()->Equals(*generated::CatalogQueryPrepareRequestSchema(), true));
	RecordBatchSingleRow row(request, 0, "prepare", "query-test");
	CHECK(row["transaction_opaque_data"].value_not_null<std::vector<uint8_t>>() == ctx.transaction_opaque_data);
	CHECK(row["mode"].value_not_null<std::string>() == "explicit");
	CHECK(row["required_ordering"].value_not_null<std::string>() == "query_order");
	CHECK(row["parameter_schema"].value_not_null<std::vector<uint8_t>>() == SerializeSchemaToIpcBytes(Parameters()));
	auto references = request->GetColumnByName("referenced_objects");
	int64_t begin, end;
	references.ListRange(0, begin, end);
	CHECK(end == begin);
	CHECK_THROWS_AS(BuildVgiQueryPrepareRequest(ctx, Capabilities(), "select 1", Parameters("named", "$limit")), InvalidInputException);
	CHECK_THROWS_AS(BuildVgiQueryPrepareRequest(ctx, Capabilities(), "select ?", Parameters("positional", "0")), InvalidInputException);
	CHECK_THROWS_AS(BuildVgiQueryPrepareRequest(ctx, Capabilities(), "select 1", MakeSchema({})), InvalidInputException);
	auto small = Capabilities();
	small.max_query_bytes = 3;
	CHECK_THROWS_AS(BuildVgiQueryPrepareRequest(ctx, small, "select 1", Parameters()), InvalidInputException);
}

TEST_CASE("Accepted query preparations enforce the context ordering expiry and result union", "[query-rpc]") {
	auto accepted = Accepted();
	auto result = Parse(accepted);
	CHECK(result.accepted);
	CHECK(result.estimated_rows == 42);
	CHECK(result.output_schema->field_name(0) == "answer");
	CHECK_THROWS_AS(Parse(Replace(accepted, "context_digest", BuildBinaryScalarRequired({0}))), InvalidInputException);
	CHECK_THROWS_AS(Parse(Replace(accepted, "result_ordering", BuildStringScalar("unspecified"))), InvalidInputException);
	CHECK_THROWS_AS(Parse(Replace(accepted, "expires_at_epoch_ms", BuildInt64Scalar(NOW))), InvalidInputException);
	CHECK_THROWS_AS(Parse(Replace(accepted, "expires_at_epoch_ms", BuildInt64Scalar(NOW + 3600001))), InvalidInputException);
	CHECK_THROWS_AS(Parse(Replace(accepted, "reason_code", BuildStringScalar("unsupported"))), InvalidInputException);
	CHECK_THROWS_AS(Parse(Replace(accepted, "estimated_rows", BuildInt64Scalar(-1))), InvalidInputException);
	auto schema_bytes = SerializeSchemaToIpcBytes(MakeSchema({type::Int64().Named("x")}));
	schema_bytes.push_back(0);
	CHECK_THROWS_AS(Parse(Replace(accepted, "output_schema", BuildBinaryScalarRequired(schema_bytes))), InvalidInputException);
}

TEST_CASE("Declined query preparations cannot also allocate a token or accepted fields", "[query-rpc]") {
	auto schema = generated::CatalogQueryPrepareResultSchema();
	std::vector<ArrowArray> columns;
	columns.push_back(BuildStringScalar("declined"));
	columns.push_back(BuildStringScalar("unsupported_statement"));
	for (int i = 2; i < schema->num_fields(); ++i) columns.push_back(NullArray(CopyField(schema->field(i))));
	auto declined = VgiBatch::FromColumns(schema, 1, columns);
	CHECK_FALSE(Parse(declined).accepted);
	CHECK_THROWS_AS(Parse(Replace(declined, "prepared_query_token", BuildBinaryScalarRequired({1}))), InvalidInputException);
	CHECK_THROWS_AS(Parse(Replace(declined, "reason_code", BuildStringScalar("Bad reason"))), InvalidInputException);
	CHECK_THROWS_AS(Parse(Replace(declined, "reason_code", BuildOptionalStringScalar(std::nullopt))), std::exception);
}

TEST_CASE("Query record IPC accepts exactly one batch and rejects trailing data", "[query-rpc]") {
	auto batch = Accepted();
	auto bytes = SerializeToIpcBytes(batch);
	CHECK(Parse(DeserializeVgiQueryRecord(bytes)).accepted);
	bytes.push_back(0);
	CHECK_THROWS_AS(DeserializeVgiQueryRecord(bytes), InvalidInputException);
	auto sink = std::make_shared<VgiBufferOutputStream>();
	auto writer = VgiIpcWriter::Open(sink, batch->schema());
	writer->Write(*batch);
	writer->Write(*batch);
	writer->Close();
	auto two_batches = sink->Finish()->ToVector();
	CHECK_THROWS_AS(DeserializeVgiQueryRecord(two_batches), InvalidInputException);
}
