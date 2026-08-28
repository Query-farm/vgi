// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace duckdb {

class ClientContext;

namespace vgi {

struct DatabaseWorkerCoordinates {
	std::string catalog;
	std::string schema;
	std::string table;
	std::string worker_name;
	std::string package_version;
	std::string expected_sha256;
};

DatabaseWorkerCoordinates ParseDatabaseWorkerLocation(const std::string &location);

struct DatabaseWorkerResolution {
	std::string token;
	std::string digest;
	std::string entrypoint;
	std::shared_ptr<void> lifetime_anchor;
};

// Resolve exactly once at ATTACH. The returned lifetime anchor pins the
// immutable artifact while the catalog exists; spawned/pooled workers acquire
// their own reference to the same cross-process lease.
DatabaseWorkerResolution ResolveDatabaseWorker(const std::string &location, ClientContext &context);

struct ResolvedWorkerLaunch {
	std::string entrypoint;
	std::shared_ptr<void> lifetime_anchor;
};

ResolvedWorkerLaunch AcquireResolvedWorkerLaunch(const std::string &token);

struct WorkerCacheEntry {
	std::string artifact_id;
	std::string source;
	std::string digest;
	std::string package_format;
	std::string entrypoint;
	std::string directory;
	uint64_t size_bytes = 0;
	int64_t age_seconds = 0;
	bool in_use = false;
};

struct WorkerCachePruneResult {
	int64_t removed = 0;
	uint64_t bytes_removed = 0;
	int64_t skipped_in_use = 0;
};

std::vector<WorkerCacheEntry> ListWorkerCache(ClientContext &context);
WorkerCachePruneResult PruneWorkerCache(ClientContext &context, bool flush_all = false);

} // namespace vgi
} // namespace duckdb
