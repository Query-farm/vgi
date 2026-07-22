// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// SQLite-backed PerValueDiskBackend: same-host multi-process + cross-restart warm
// reuse for the per-value memo arena. WAL mode gives concurrent readers + one writer
// across processes on one host. Disk is off the hot path (touched only on a cold-arena
// hydrate and on a store), so one connection guarded by a mutex is ample.
//
// Selected over LMDB by the Step-8 bake-off: reference-grade embedded multi-process DB,
// single-file amalgamation to vendor, TTL reaping as a DELETE, no hand-rolled reaper or
// flock, and it sidesteps the DuckDB-reentrancy trap by being a separate engine. LMDB
// remains a drop-in alternative behind this same interface if write throughput ever
// proves insufficient (it did not — see the commit).

#include "vgi_memo_arena.hpp"

#include <mutex>

namespace duckdb {
namespace vgi {

#ifdef VGI_ENABLE_SQLITE

} // namespace vgi
} // namespace duckdb

#include <sqlite3.h>

namespace duckdb {
namespace vgi {

namespace {

class SqliteDiskBackend : public PerValueDiskBackend {
public:
	explicit SqliteDiskBackend(sqlite3 *db) : db_(db) {}
	~SqliteDiskBackend() override {
		if (db_) {
			sqlite3_close(db_);
		}
	}

	void Persist(const std::string &static_fp, const std::string & /*schema_ipc*/,
	             const std::vector<PersistedSlot> &slots) override {
		std::lock_guard<std::mutex> lg(mu_);
		if (!db_ || slots.empty()) {
			return;
		}
		sqlite3_exec(db_, "BEGIN", nullptr, nullptr, nullptr);
		const char *sql = "INSERT OR REPLACE INTO slots"
		                  "(static_fp,input_blob,ipc,rows,expires_unix,scope,etag,last_modified,revalidatable)"
		                  " VALUES(?,?,?,?,?,?,?,?,?)";
		sqlite3_stmt *st = nullptr;
		if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) {
			sqlite3_exec(db_, "ROLLBACK", nullptr, nullptr, nullptr);
			return;
		}
		for (const auto &s : slots) {
			sqlite3_bind_text(st, 1, static_fp.data(), static_cast<int>(static_fp.size()), SQLITE_STATIC);
			sqlite3_bind_blob(st, 2, s.input_blob.data(), static_cast<int>(s.input_blob.size()), SQLITE_STATIC);
			sqlite3_bind_blob(st, 3, s.ipc.data(), static_cast<int>(s.ipc.size()), SQLITE_STATIC);
			sqlite3_bind_int64(st, 4, s.rows);
			sqlite3_bind_int64(st, 5, s.expires_unix);
			sqlite3_bind_text(st, 6, s.scope.data(), static_cast<int>(s.scope.size()), SQLITE_STATIC);
			sqlite3_bind_text(st, 7, s.etag.data(), static_cast<int>(s.etag.size()), SQLITE_STATIC);
			sqlite3_bind_text(st, 8, s.last_modified.data(), static_cast<int>(s.last_modified.size()), SQLITE_STATIC);
			sqlite3_bind_int(st, 9, s.revalidatable ? 1 : 0);
			sqlite3_step(st);
			sqlite3_reset(st);
		}
		sqlite3_finalize(st);
		sqlite3_exec(db_, "COMMIT", nullptr, nullptr, nullptr);
	}

	bool Hydrate(const std::string &static_fp, int64_t now_unix, std::string & /*schema_ipc_out*/,
	             std::vector<PersistedSlot> &slots_out) override {
		std::lock_guard<std::mutex> lg(mu_);
		if (!db_) {
			return false;
		}
		const char *sql = "SELECT input_blob,ipc,rows,expires_unix,scope,etag,last_modified,revalidatable"
		                  " FROM slots WHERE static_fp=? AND (expires_unix<0 OR expires_unix>?)";
		sqlite3_stmt *st = nullptr;
		if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) {
			return false;
		}
		sqlite3_bind_text(st, 1, static_fp.data(), static_cast<int>(static_fp.size()), SQLITE_STATIC);
		sqlite3_bind_int64(st, 2, now_unix);
		bool any = false;
		while (sqlite3_step(st) == SQLITE_ROW) {
			any = true;
			PersistedSlot p;
			auto blob = [&](int col, std::string &dst) {
				const void *d = sqlite3_column_blob(st, col);
				int n = sqlite3_column_bytes(st, col);
				if (d && n > 0) {
					dst.assign(static_cast<const char *>(d), static_cast<size_t>(n));
				}
			};
			blob(0, p.input_blob);
			blob(1, p.ipc);
			p.rows = sqlite3_column_int64(st, 2);
			p.expires_unix = sqlite3_column_int64(st, 3);
			blob(4, p.scope);
			blob(5, p.etag);
			blob(6, p.last_modified);
			p.revalidatable = sqlite3_column_int(st, 7) != 0;
			slots_out.push_back(std::move(p));
		}
		sqlite3_finalize(st);
		return any;
	}

	void Flush() override {
		std::lock_guard<std::mutex> lg(mu_);
		if (db_) {
			sqlite3_exec(db_, "DELETE FROM slots", nullptr, nullptr, nullptr);
			// Reclaim the WAL immediately (TRUNCATE checkpoints then shrinks the -wal file
			// to zero), so a flush leaves no on-disk residue.
			sqlite3_exec(db_, "PRAGMA wal_checkpoint(TRUNCATE)", nullptr, nullptr, nullptr);
		}
	}

private:
	std::mutex mu_;
	sqlite3 *db_ = nullptr;
};

} // namespace

std::shared_ptr<PerValueDiskBackend> MakeSqliteDiskBackend(const std::string &dir) {
	const std::string path = dir + "/vgi_per_value.sqlite";
	sqlite3 *db = nullptr;
	if (sqlite3_open(path.c_str(), &db) != SQLITE_OK) {
		if (db) {
			sqlite3_close(db);
		}
		return nullptr;
	}
	// WAL: concurrent readers + one writer across same-host processes; NORMAL sync is
	// crash-safe under WAL. busy_timeout so a concurrent writer waits rather than errors.
	sqlite3_exec(db, "PRAGMA journal_mode=WAL", nullptr, nullptr, nullptr);
	sqlite3_exec(db, "PRAGMA synchronous=NORMAL", nullptr, nullptr, nullptr);
	sqlite3_exec(db, "PRAGMA busy_timeout=5000", nullptr, nullptr, nullptr);
	// Auto-checkpoint the WAL every ~1000 pages (~4 MB) — the default, set explicitly so
	// the -wal file stays bounded even though the connection is a leaked process-lifetime
	// singleton (never cleanly closed). Verified: 1M stores through one connection leaves
	// the WAL at ~0.3 MB while the main DB holds the data. NORMAL sync keeps this crash-safe.
	sqlite3_exec(db, "PRAGMA wal_autocheckpoint=1000", nullptr, nullptr, nullptr);
	const char *ddl = "CREATE TABLE IF NOT EXISTS slots("
	                  "static_fp TEXT NOT NULL, input_blob BLOB NOT NULL, ipc BLOB NOT NULL,"
	                  "rows INTEGER NOT NULL, expires_unix INTEGER NOT NULL,"
	                  "scope TEXT, etag TEXT, last_modified TEXT, revalidatable INTEGER,"
	                  "PRIMARY KEY(static_fp,input_blob))";
	if (sqlite3_exec(db, ddl, nullptr, nullptr, nullptr) != SQLITE_OK) {
		sqlite3_close(db);
		return nullptr;
	}
	return std::make_shared<SqliteDiskBackend>(db);
}

#else // !VGI_ENABLE_SQLITE

std::shared_ptr<PerValueDiskBackend> MakeSqliteDiskBackend(const std::string & /*dir*/) {
	return nullptr; // SQLite not compiled in (e.g. WASM) — memory-only
}

#endif

} // namespace vgi
} // namespace duckdb
