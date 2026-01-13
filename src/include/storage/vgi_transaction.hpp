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

	void Start();
	void Commit();
	void Rollback();

	static VgiTransaction &Get(ClientContext &context, Catalog &catalog);

	AccessMode GetAccessMode() const {
		return access_mode_;
	}

private:
	VgiTransactionState transaction_state = VgiTransactionState::TRANSACTION_NOT_YET_STARTED;
	AccessMode access_mode_;
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
