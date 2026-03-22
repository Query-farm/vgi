#include "storage/vgi_transaction.hpp"

#include "storage/vgi_catalog.hpp"
#include "vgi_catalog_api.hpp"

#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

namespace duckdb {

// VgiTransaction implementation

VgiTransaction::VgiTransaction(VgiCatalog &vgi_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), access_mode_(vgi_catalog.GetAccessMode()), vgi_catalog_(vgi_catalog),
      context_(context) {
}

VgiTransaction::~VgiTransaction() = default;

void VgiTransaction::Start(ClientContext &context) {
	transaction_state = VgiTransactionState::TRANSACTION_STARTED;

	// Call the worker's catalog_transaction_begin RPC if transactions are supported
	auto &attach_result = vgi_catalog_.attach_result();
	if (attach_result && attach_result->supports_transactions) {
		auto &params = vgi_catalog_.attach_parameters();
		transaction_id_ = vgi::InvokeCatalogTransactionBegin(
		    params->worker_path(), attach_result->attach_id, context,
		    params->worker_debug(), params->use_pool());
	}
}

void VgiTransaction::Commit(ClientContext &context) {
	// Call the worker's catalog_transaction_commit RPC
	if (!transaction_id_.empty()) {
		auto &params = vgi_catalog_.attach_parameters();
		auto &attach_result = vgi_catalog_.attach_result();
		vgi::InvokeCatalogTransactionCommit(
		    params->worker_path(), attach_result->attach_id, transaction_id_,
		    context, params->worker_debug(), params->use_pool());
	}
	transaction_state = VgiTransactionState::TRANSACTION_FINISHED;
}

void VgiTransaction::Rollback() {
	if (!transaction_id_.empty()) {
		auto &params = vgi_catalog_.attach_parameters();
		auto &attach_result = vgi_catalog_.attach_result();
		vgi::InvokeCatalogTransactionRollback(
		    params->worker_path(), attach_result->attach_id, transaction_id_,
		    context_, params->worker_debug(), params->use_pool());
	}
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
	transaction->Start(context);
	auto &result = *transaction;
	std::lock_guard<std::mutex> l(transaction_lock_);
	transactions_[result] = std::move(transaction);
	return result;
}

ErrorData VgiTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &vgi_transaction = transaction.Cast<VgiTransaction>();
	vgi_transaction.Commit(context);
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
