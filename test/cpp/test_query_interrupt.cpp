// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catch.hpp"
#include "duckdb.hpp"
#include "core_functions_extension.hpp"
#include "vgi_extension.hpp"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <thread>

using namespace duckdb;

namespace {

void ExecuteQuerySetup(Connection &connection, const string &sql) {
	auto result = connection.Query(sql);
	INFO(sql);
	INFO((result->HasError() ? result->GetError() : "ok"));
	REQUIRE_FALSE(result->HasError());
}

} // namespace

TEST_CASE("Explicit query interruption leaves the connection usable for detach", "[query-interrupt][.]") {
	const auto worker = std::getenv("VGI_QUERY_TEST_WORKER");
	const auto marker = std::getenv("VGI_QUERY_READ_MARKER");
	REQUIRE(worker != nullptr);
	REQUIRE(marker != nullptr);
	REQUIRE(std::string(worker).size() > 0);
	REQUIRE(std::string(marker).size() > 0);
	std::filesystem::remove(marker);

	DuckDB db(nullptr);
	db.LoadStaticExtension<CoreFunctionsExtension>();
	db.LoadStaticExtension<VgiExtension>();
	Connection connection(db);
	ExecuteQuerySetup(connection, "SET vgi_query_enabled=true");
	ExecuteQuerySetup(connection, "ATTACH 'queries' AS w (TYPE vgi, LOCATION '" +
	                             StringUtil::Replace(worker, "'", "''") + "')");

	unique_ptr<MaterializedQueryResult> result;
	std::exception_ptr failure;
	std::atomic<bool> finished {false};
	std::thread query_thread([&] {
		try {
			result = connection.Query(
			    "SELECT * FROM vgi_query('w', 'SELECT i AS result FROM numbers ORDER BY i')");
		} catch (...) {
			failure = std::current_exception();
		}
		finished.store(true);
	});

	// The worker records the execution and enters its bounded slow read before
	// creating the marker, so this cannot interrupt only parsing or preparation.
	const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
	while (!std::filesystem::exists(marker) && !finished.load() && std::chrono::steady_clock::now() < deadline) {
		std::this_thread::sleep_for(std::chrono::milliseconds(25));
	}
	const bool reached_read = std::filesystem::exists(marker);
	const auto interrupt_time = std::chrono::steady_clock::now();
	connection.Interrupt();
	query_thread.join();
	const auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - interrupt_time).count();

	// Keep the database alive and run the real detach RPC. The Python runner
	// subsequently checks one execution and no preparations/results/attachments.
	ExecuteQuerySetup(connection, "DETACH w");
	INFO("query interruption completed after " << elapsed << " seconds");
	REQUIRE(reached_read);
	REQUIRE(failure == nullptr);
	REQUIRE(result != nullptr);
	INFO((result->HasError() ? result->GetError() : "unexpected successful query"));
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Lower(result->GetError()).find("interrupt") != string::npos);
	REQUIRE(elapsed < 15.0);
}
