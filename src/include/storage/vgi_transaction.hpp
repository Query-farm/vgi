#pragma once

#include "duckdb/common/enums/access_mode.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

#include <mutex>

namespace duckdb {

class VgiCatalog;

enum class VgiTransactionState { TRANSACTION_NOT_YET_STARTED, TRANSACTION_STARTED, TRANSACTION_FINISHED };

class VgiTransaction : public Transaction {
public:
	VgiTransaction(VgiCatalog &vgi_catalog, TransactionManager &manager, ClientContext &context);
	~VgiTransaction() override;

	void Start(ClientContext &context);
	void Commit(ClientContext &context);
	void Rollback();  // Uses stored context_ from Start

	static VgiTransaction &Get(ClientContext &context, Catalog &catalog);

	AccessMode GetAccessMode() const {
		return access_mode_;
	}

	//! Get the transaction ID assigned by the worker (empty if none).
	const std::vector<uint8_t> &GetTransactionId() const {
		return transaction_id_;
	}

	//! Temporary storage for point-in-time catalog entries (time travel).
	//! These are created during GetEntry with an AT clause and must remain
	//! alive until the query completes. Cleaned up when the transaction ends.
	vector<unique_ptr<CatalogEntry>> point_in_time_entries;

private:
	VgiTransactionState transaction_state = VgiTransactionState::TRANSACTION_NOT_YET_STARTED;
	AccessMode access_mode_;
	VgiCatalog &vgi_catalog_;
	// (Rollback uses the base class's `Transaction::context` weak_ptr to
	// safely reach a ClientContext if one is still alive at rollback time.)
	std::vector<uint8_t> transaction_id_;   // Assigned by worker's catalog_transaction_begin
};

class VgiTransactionManager : public TransactionManager {
public:
	VgiTransactionManager(AttachedDatabase &db_p, VgiCatalog &vgi_catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	VgiCatalog &vgi_catalog_;
	std::mutex transaction_lock_;
	reference_map_t<Transaction, unique_ptr<VgiTransaction>> transactions_;
};

} // namespace duckdb
