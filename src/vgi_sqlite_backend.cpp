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

#include <chrono>
#include <cstdio>
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

// Error logging for the disk tier. Failures are rare and operationally important (a
// silent disk cache is worse than no cache), so these go to stderr UNCONDITIONALLY —
// not gated behind VGI_STDERR_LOG. A fatal error disables the backend (see disabled_)
// so it logs once and degrades to memory-only rather than spamming.
void LogDiskErr(const char *what, sqlite3 *db) {
	std::fprintf(stderr, "[vgi] per-value disk cache: %s%s%s\n", what, db ? ": " : "",
	             db ? sqlite3_errmsg(db) : "");
	std::fflush(stderr);
}

bool IsFatalSqlite(int rc) {
	// Errors that will keep failing → stop trying (disable). Transient ones (BUSY after
	// the busy_timeout, FULL when the disk clears) are logged but not disabling.
	return rc == SQLITE_CORRUPT || rc == SQLITE_NOTADB || rc == SQLITE_CANTOPEN || rc == SQLITE_READONLY;
}

class SqliteDiskBackend : public PerValueDiskBackend {
public:
	explicit SqliteDiskBackend(sqlite3 *db) : db_(db) {}
	~SqliteDiskBackend() override {
		if (db_) {
			sqlite3_close(db_);
		}
	}

	void SetMaxBytes(int64_t max_bytes) override {
		std::lock_guard<std::mutex> lg(mu_);
		max_bytes_ = max_bytes;
	}

	void Persist(const std::string &static_fp, const std::string & /*schema_ipc*/,
	             const std::vector<PersistedSlot> &slots) override {
		std::lock_guard<std::mutex> lg(mu_);
		if (!db_ || disabled_ || slots.empty()) {
			return;
		}
		const int64_t now_unix = NowUnix();
		sqlite3_exec(db_, "BEGIN", nullptr, nullptr, nullptr);
		const char *sql = "INSERT OR REPLACE INTO slots"
		                  "(static_fp,input_blob,ipc,rows,expires_unix,last_used,"
		                  "scope,etag,last_modified,revalidatable)"
		                  " VALUES(?,?,?,?,?,?,?,?,?,?)";
		sqlite3_stmt *st = nullptr;
		if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) {
			HandleErrLocked("persist prepare failed", sqlite3_errcode(db_));
			sqlite3_exec(db_, "ROLLBACK", nullptr, nullptr, nullptr);
			return;
		}
		int first_err = SQLITE_OK;
		for (const auto &s : slots) {
			sqlite3_bind_text(st, 1, static_fp.data(), static_cast<int>(static_fp.size()), SQLITE_STATIC);
			sqlite3_bind_blob(st, 2, s.input_blob.data(), static_cast<int>(s.input_blob.size()), SQLITE_STATIC);
			sqlite3_bind_blob(st, 3, s.ipc.data(), static_cast<int>(s.ipc.size()), SQLITE_STATIC);
			sqlite3_bind_int64(st, 4, s.rows);
			sqlite3_bind_int64(st, 5, s.expires_unix);
			sqlite3_bind_int64(st, 6, now_unix); // last_used (LRU)
			sqlite3_bind_text(st, 7, s.scope.data(), static_cast<int>(s.scope.size()), SQLITE_STATIC);
			sqlite3_bind_text(st, 8, s.etag.data(), static_cast<int>(s.etag.size()), SQLITE_STATIC);
			sqlite3_bind_text(st, 9, s.last_modified.data(), static_cast<int>(s.last_modified.size()), SQLITE_STATIC);
			sqlite3_bind_int(st, 10, s.revalidatable ? 1 : 0);
			const int rc = sqlite3_step(st);
			if (rc != SQLITE_DONE && first_err == SQLITE_OK) {
				first_err = rc;
			}
			sqlite3_reset(st);
		}
		sqlite3_finalize(st);
		if (sqlite3_exec(db_, "COMMIT", nullptr, nullptr, nullptr) != SQLITE_OK && first_err == SQLITE_OK) {
			first_err = sqlite3_errcode(db_);
		}
		if (first_err != SQLITE_OK) {
			HandleErrLocked("persist write failed", first_err);
			return; // skip reap/evict on a failed write
		}
		// Reap expired rows and evict LRU to stay under the byte cap (bounded, off the hot
		// path — this runs only on a store, i.e. a cache miss).
		ReapAndEvictLocked(now_unix);
	}

	bool Hydrate(const std::string &static_fp, int64_t now_unix, std::string & /*schema_ipc_out*/,
	             std::vector<PersistedSlot> &slots_out) override {
		std::lock_guard<std::mutex> lg(mu_);
		if (!db_ || disabled_) {
			return false;
		}
		const char *sql = "SELECT input_blob,ipc,rows,expires_unix,scope,etag,last_modified,revalidatable"
		                  " FROM slots WHERE static_fp=? AND (expires_unix<0 OR expires_unix>?)";
		sqlite3_stmt *st = nullptr;
		if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) {
			HandleErrLocked("hydrate prepare failed", sqlite3_errcode(db_));
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
		if (any) {
			// LRU touch: mark this key's live rows as recently used so eviction favours
			// genuinely cold static keys.
			sqlite3_stmt *up = nullptr;
			if (sqlite3_prepare_v2(
			        db_, "UPDATE slots SET last_used=? WHERE static_fp=? AND (expires_unix<0 OR expires_unix>?)",
			        -1, &up, nullptr) == SQLITE_OK) {
				sqlite3_bind_int64(up, 1, now_unix);
				sqlite3_bind_text(up, 2, static_fp.data(), static_cast<int>(static_fp.size()), SQLITE_STATIC);
				sqlite3_bind_int64(up, 3, now_unix);
				sqlite3_step(up);
				sqlite3_finalize(up);
			}
		}
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
	// Log a disk error (throttled so a full/busy disk can't spam) and disable the backend
	// on a fatal error so it degrades to memory-only instead of failing on every store.
	void HandleErrLocked(const char *what, int rc) {
		err_count_++;
		if (err_count_ <= 3 || (err_count_ % 1000) == 0) {
			LogDiskErr(what, db_);
		}
		if (IsFatalSqlite(rc)) {
			if (!disabled_) {
				LogDiskErr("disabling per-value disk tier (degrading to memory-only)", db_);
			}
			disabled_ = true;
		}
	}
	static int64_t NowUnix() {
		return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch())
		    .count();
	}
	int64_t PragmaIntLocked(const char *pragma) {
		sqlite3_stmt *st = nullptr;
		int64_t v = 0;
		if (sqlite3_prepare_v2(db_, pragma, -1, &st, nullptr) == SQLITE_OK) {
			if (sqlite3_step(st) == SQLITE_ROW) {
				v = sqlite3_column_int64(st, 0);
			}
			sqlite3_finalize(st);
		}
		return v;
	}
	// Live data size = (total pages - freelist pages) * page size. O(1) pragmas, and it
	// SHRINKS as rows are deleted (freed pages join the freelist, reused by later inserts),
	// so evict-to-fit terminates without a VACUUM.
	int64_t UsedBytesLocked() {
		const int64_t page = PragmaIntLocked("PRAGMA page_size");
		return (PragmaIntLocked("PRAGMA page_count") - PragmaIntLocked("PRAGMA freelist_count")) * page;
	}
	void ReapAndEvictLocked(int64_t now_unix) {
		// Reap expired rows first.
		sqlite3_stmt *rp = nullptr;
		if (sqlite3_prepare_v2(db_, "DELETE FROM slots WHERE expires_unix>=0 AND expires_unix<=?", -1, &rp,
		                       nullptr) == SQLITE_OK) {
			sqlite3_bind_int64(rp, 1, now_unix);
			sqlite3_step(rp);
			sqlite3_finalize(rp);
		}
		if (max_bytes_ <= 0) {
			return;
		}
		// Evict least-recently-used rows in batches until under the cap. Bounded loop.
		for (int guard = 0; guard < 100000 && UsedBytesLocked() > max_bytes_; guard++) {
			const int before = sqlite3_total_changes(db_);
			sqlite3_exec(db_, "DELETE FROM slots WHERE rowid IN (SELECT rowid FROM slots ORDER BY last_used ASC LIMIT 512)",
			             nullptr, nullptr, nullptr);
			if (sqlite3_total_changes(db_) == before) {
				break; // nothing left to delete
			}
		}
	}

	std::mutex mu_;
	sqlite3 *db_ = nullptr;
	int64_t max_bytes_ = 0; // 0 = unlimited
	bool disabled_ = false; // set on a fatal error → memory-only
	uint64_t err_count_ = 0;
};

} // namespace

std::shared_ptr<PerValueDiskBackend> MakeSqliteDiskBackend(const std::string &dir) {
	const std::string path = dir + "/vgi_per_value.sqlite";
	sqlite3 *db = nullptr;
	if (sqlite3_open(path.c_str(), &db) != SQLITE_OK) {
		LogDiskErr(("open failed (" + path + ") — per-value disk tier off").c_str(), db);
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
	// Schema version 2 adds the last_used column (LRU eviction). On a pre-v2 file, drop and
	// recreate — the per-value tier is a cache, so invalidating it on upgrade is fine.
	{
		sqlite3_stmt *st = nullptr;
		int64_t ver = 0;
		if (sqlite3_prepare_v2(db, "PRAGMA user_version", -1, &st, nullptr) == SQLITE_OK) {
			if (sqlite3_step(st) == SQLITE_ROW) {
				ver = sqlite3_column_int64(st, 0);
			}
			sqlite3_finalize(st);
		}
		if (ver != 0 && ver < 2) {
			sqlite3_exec(db, "DROP TABLE IF EXISTS slots", nullptr, nullptr, nullptr);
		}
		sqlite3_exec(db, "PRAGMA user_version=2", nullptr, nullptr, nullptr);
	}
	const char *ddl = "CREATE TABLE IF NOT EXISTS slots("
	                  "static_fp TEXT NOT NULL, input_blob BLOB NOT NULL, ipc BLOB NOT NULL,"
	                  "rows INTEGER NOT NULL, expires_unix INTEGER NOT NULL, last_used INTEGER NOT NULL DEFAULT 0,"
	                  "scope TEXT, etag TEXT, last_modified TEXT, revalidatable INTEGER,"
	                  "PRIMARY KEY(static_fp,input_blob));"
	                  "CREATE INDEX IF NOT EXISTS slots_lru ON slots(last_used);";
	if (sqlite3_exec(db, ddl, nullptr, nullptr, nullptr) != SQLITE_OK) {
		LogDiskErr("schema init failed — per-value disk tier off", db);
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
