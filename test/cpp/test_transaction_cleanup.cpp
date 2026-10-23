// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catch.hpp"
#include "duckdb.hpp"
#include "core_functions_extension.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "storage/vgi_schema_set.hpp"
#include "storage/vgi_transaction.hpp"
#include "vgi_extension.hpp"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>

using namespace duckdb;

namespace {

// Observe ownership using the existing entry containers, without exposing
// transaction-manager internals or adding a production diagnostic API.
class LifetimeEntry : public CatalogEntry {
public:
	LifetimeEntry(Catalog &catalog, string name, std::shared_ptr<unsigned> destroyed)
	    : CatalogEntry(CatalogType::SCHEMA_ENTRY, catalog, std::move(name)), destroyed(std::move(destroyed)) {
	}
	~LifetimeEntry() override {
		++*destroyed;
	}

private:
	std::shared_ptr<unsigned> destroyed;
};

struct LifetimeObservation {
	std::shared_ptr<unsigned> transaction_entry = std::make_shared<unsigned>(0);
	std::shared_ptr<unsigned> schema_entry = std::make_shared<unsigned>(0);
	CatalogEntry *retained_entry = nullptr;
	string token;
};

string TokenHex(const std::vector<uint8_t> &token) {
	string result;
	for (auto byte : token) {
		result += "0123456789abcdef"[byte >> 4];
		result += "0123456789abcdef"[byte & 15];
	}
	return result;
}

LifetimeObservation Observe(Connection &connection) {
	auto &catalog = Catalog::GetCatalog(*connection.context, "w");
	auto &transaction = VgiTransaction::Get(*connection.context, catalog);
	REQUIRE(transaction.SchemaCache());
	LifetimeObservation observation;
	observation.token = TokenHex(transaction.GetTransactionOpaqueData());
	REQUIRE_FALSE(observation.token.empty());
	transaction.point_in_time_entries.push_back(
	    make_uniq<LifetimeEntry>(catalog, "transaction_lifetime", observation.transaction_entry));
	auto schema = transaction.SchemaCache()->GetEntry(*connection.context, "main");
	REQUIRE(schema);
	REQUIRE_FALSE(schema->HasChild());
	// The child follows the real schema root through cache retirement and
	// destruction, so this also checks retention of actual loaded metadata.
	schema->SetChild(make_uniq<LifetimeEntry>(catalog, "retained_lifetime", observation.schema_entry));
	observation.retained_entry = schema.get();
	return observation;
}

void Execute(Connection &connection, const string &sql) {
	auto result = connection.Query(sql);
	INFO(sql);
	INFO((result->HasError() ? result->GetError() : "ok"));
	REQUIRE_FALSE(result->HasError());
}

string ReadLabel(PreparedStatement &statement) {
	vector<Value> parameters;
	auto result = statement.Execute(parameters, false);
	INFO((result->HasError() ? result->GetError() : "ok"));
	REQUIRE_FALSE(result->HasError());
	auto &materialized = result->Cast<MaterializedQueryResult>();
	REQUIRE(materialized.RowCount() == 1);
	REQUIRE(materialized.GetValue(0, 0).GetValue<int64_t>() == 1);
	return materialized.GetValue(1, 0).ToString();
}

void CheckRetired(const LifetimeObservation &observation) {
	CHECK(*observation.transaction_entry == 1);
	CHECK(*observation.schema_entry == 0);
	if (*observation.schema_entry == 0) {
		CHECK(observation.retained_entry->name == "main");
	}
}

string ShellQuote(const string &value) {
	return "'" + StringUtil::Replace(value, "'", "'\"'\"'") + "'";
}

void RunCompletionCase(const string &operation, const string &mode) {
	const auto worker = std::getenv("VGI_TRANSACTION_CLEANUP_TEST_WORKER");
	const auto root = std::getenv("VGI_TRANSACTION_CLEANUP_TEST_DIR");
	REQUIRE(worker != nullptr);
	REQUIRE(root != nullptr);
	const auto directory = std::filesystem::path(root) / (operation + "_" + mode);
	REQUIRE(std::filesystem::create_directories(directory));
	const auto location = "env VGI_TRANSACTION_CLEANUP_PROBE_DIR=" + ShellQuote(directory.string()) + " " + worker;
	std::vector<LifetimeObservation> observations;
	{
		DuckDB db(nullptr);
		db.LoadStaticExtension<CoreFunctionsExtension>();
		db.LoadStaticExtension<VgiExtension>();
		Connection connection(db);
		Connection other(db);
		Execute(connection, "ATTACH 'transaction_cleanup_probe' AS w (TYPE vgi, LOCATION '" +
		                        StringUtil::Replace(location, "'", "''") + "')");

		Execute(other, "BEGIN");
		auto other_statement = other.Prepare("SELECT id, label FROM w.main.items");
		REQUIRE_FALSE(other_statement->HasError());
		const auto other_label = ReadLabel(*other_statement);
		observations.push_back(Observe(other));
		REQUIRE(other_label == "tx:" + observations.front().token);

		for (unsigned iteration = 0; iteration < 3; ++iteration) {
			INFO(operation << " " << mode << " iteration " << iteration);
			Execute(connection, "BEGIN");
			auto statement = connection.Prepare("SELECT id, label FROM w.main.items");
			INFO((statement->HasError() ? statement->GetError() : "prepared"));
			REQUIRE_FALSE(statement->HasError());
			const auto label = ReadLabel(*statement);
			observations.push_back(Observe(connection));
			const auto completed = observations.back();
			REQUIRE(label == "tx:" + completed.token);
			{
				std::ofstream control(directory / "control.json");
				control << "{\"" << operation << "\":\"" << mode << "\"}\n";
				REQUIRE(control.good());
				std::ofstream expected(directory / "expected.jsonl", std::ios::app);
				expected << "{\"token\":\"" << completed.token << "\",\"operation\":\"" << operation
				         << "\",\"mode\":\"" << mode << "\"}\n";
				REQUIRE(expected.good());
			}
			auto result = connection.Query(operation);
			INFO((result->HasError() ? result->GetError() : "ok"));
			CHECK(result->HasError() == (mode != "success"));
			if (mode != "success" && result->HasError()) {
				const auto suffix = mode == "reject" ? " rejected before completion" : " response error after completion";
				CHECK(result->GetError().find("transaction_cleanup_probe: " + operation + suffix) != string::npos);
			}
			CheckRetired(completed);

			// The failed transaction must not retain ownership, poison the next
			// transaction, or invalidate another connection's active snapshot.
			CHECK(*observations.front().transaction_entry == 0);
			CHECK(*observations.front().schema_entry == 0);
			CHECK(ReadLabel(*other_statement) == other_label);
			Execute(connection, "BEGIN");
			const auto next_label = ReadLabel(*statement);
			observations.push_back(Observe(connection));
			CHECK(next_label == "tx:" + observations.back().token);
			CHECK(next_label != label);
			Execute(connection, "ROLLBACK");
			CheckRetired(observations.back());
		}
		Execute(other, "ROLLBACK");
		for (const auto &observation : observations) {
			CheckRetired(observation);
		}
	}
	// Retained metadata has the catalog's lifetime; the existing bounded
	// retention policy is unchanged by this transaction ownership regression.
	for (const auto &observation : observations) {
		CHECK(*observation.transaction_entry == 1);
		CHECK(*observation.schema_entry == 1);
	}
}

} // namespace

TEST_CASE("Transaction completion releases local ownership on success and provider errors", "[transaction-cleanup][.]") {
	for (const auto &operation : {"commit", "rollback"}) {
		for (const auto &mode : {"success", "reject", "after_completion_error"}) {
			DYNAMIC_SECTION(operation << " " << mode) {
				RunCompletionCase(operation, mode);
			}
		}
	}
}
