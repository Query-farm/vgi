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

	// Check catalog version — clear cache if metadata changed since last query.
	// This catches DDL changes from other sessions between queries.
	vgi_catalog_.CheckAndInvalidateCache(context, /*transaction_id=*/{});

	// Call the worker's catalog_transaction_begin RPC if transactions are supported
	auto &attach_result = vgi_catalog_.attach_result();
	if (attach_result && attach_result->supports_transactions) {
		auto &params = vgi_catalog_.attach_parameters();
		vgi::CatalogRpcContext rpc_ctx{params, attach_result->attach_id, {}};
		transaction_id_ = vgi::InvokeCatalogTransactionBegin(rpc_ctx, context);
	}
}

void VgiTransaction::Commit(ClientContext &context) {
	// Call the worker's catalog_transaction_commit RPC
	if (!transaction_id_.empty()) {
		auto &params = vgi_catalog_.attach_parameters();
		auto &attach_result = vgi_catalog_.attach_result();
		vgi::CatalogRpcContext rpc_ctx{params, attach_result->attach_id, transaction_id_};
		vgi::InvokeCatalogTransactionCommit(rpc_ctx, context);
	}
	transaction_state = VgiTransactionState::TRANSACTION_FINISHED;

	// Check catalog version — clear cache only if metadata actually changed.
	// For DDL transactions (CREATE TABLE, DROP, etc.), the worker's version
	// will have incremented. For read-only queries, version stays the same
	// and the cache is preserved.
	vgi_catalog_.CheckAndInvalidateCache(context, /*transaction_id=*/{});
}

void VgiTransaction::Rollback() {
	if (!transaction_id_.empty()) {
		auto &params = vgi_catalog_.attach_parameters();
		auto &attach_result = vgi_catalog_.attach_result();
		vgi::CatalogRpcContext rpc_ctx{params, attach_result->attach_id, transaction_id_};
		vgi::InvokeCatalogTransactionRollback(rpc_ctx, context_);
	}
	transaction_state = VgiTransactionState::TRANSACTION_FINISHED;

	// Check catalog version — clear cache only if metadata actually changed.
	vgi_catalog_.CheckAndInvalidateCache(context_, /*transaction_id=*/{});
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
