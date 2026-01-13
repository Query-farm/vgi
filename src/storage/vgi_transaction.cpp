#include "storage/vgi_transaction.hpp"

#include "storage/vgi_catalog.hpp"

#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

namespace duckdb {

// VgiTransaction implementation

VgiTransaction::VgiTransaction(VgiCatalog &vgi_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), access_mode_(vgi_catalog.GetAccessMode()) {
}

VgiTransaction::~VgiTransaction() = default;

void VgiTransaction::Start() {
	transaction_state = VgiTransactionState::TRANSACTION_STARTED;
}

void VgiTransaction::Commit() {
	transaction_state = VgiTransactionState::TRANSACTION_FINISHED;
}

void VgiTransaction::Rollback() {
	transaction_state = VgiTransactionState::TRANSACTION_FINISHED;
}

VgiTransaction &VgiTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<VgiTransaction>();
}

// VgiTransactionManager implementation

VgiTransactionManager::VgiTransactionManager(AttachedDatabase &db_p, VgiCatalog &vgi_catalog)
    : TransactionManager(db_p), vgi_catalog_(vgi_catalog) {
}

Transaction &VgiTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<VgiTransaction>(vgi_catalog_, *this, context);
	transaction->Start();
	auto &result = *transaction;
	std::lock_guard<std::mutex> l(transaction_lock_);
	transactions_[result] = std::move(transaction);
	return result;
}

ErrorData VgiTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &vgi_transaction = transaction.Cast<VgiTransaction>();
	vgi_transaction.Commit();
	std::lock_guard<std::mutex> l(transaction_lock_);
	transactions_.erase(transaction);
	return ErrorData();
}

void VgiTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &vgi_transaction = transaction.Cast<VgiTransaction>();
	vgi_transaction.Rollback();
	std::lock_guard<std::mutex> l(transaction_lock_);
	transactions_.erase(transaction);
}

void VgiTransactionManager::Checkpoint(ClientContext &context, bool force) {
	// No-op for VGI catalogs - they are remote
}

} // namespace duckdb
