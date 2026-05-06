#include "storage/vgi_transaction.hpp"

#include "storage/vgi_catalog.hpp"
#include "vgi_catalog_api.hpp"

#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

namespace duckdb {

// VgiTransaction implementation

VgiTransaction::VgiTransaction(VgiCatalog &vgi_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), access_mode_(vgi_catalog.GetAccessMode()), vgi_catalog_(vgi_catalog) {
	// The base class stores `context` as a weak_ptr<ClientContext>; we
	// reach it via that weak_ptr in Rollback() so we never deref a freed
	// ClientContext during cross-session/shutdown rollback paths.
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

	// We deliberately do NOT call CheckAndInvalidateCache here. Rationale:
	//
	//   - DDL paths (CreateSchema, DropSchema, CreateTable, …) call
	//     ClearCache eagerly inside their RPC handlers, before Commit runs.
	//     By the time we reach this point in those flows, the cache is
	//     already invalidated.
	//
	//   - Other-session changes that bumped the worker's catalog_version
	//     during this transaction are caught at the next Transaction::Start
	//     (one transaction late, but no in-flight query observes stale
	//     metadata because the next query polls before it executes).
	//
	//   - For read-only sessions (the common case), no work in this
	//     transaction could have moved the worker's version, so polling
	//     here can only ever return noop. Removing the call drops two
	//     catalog_version RPCs per query (Commit + Rollback paths).
	//
	// If a future write path forgets to ClearCache after a worker
	// mutation, the next Start probe still catches it — staleness is
	// bounded to one transaction.
}

void VgiTransaction::Rollback() {
	transaction_state = VgiTransactionState::TRANSACTION_FINISHED;

	// Lock the base class's weak_ptr<ClientContext>. If the original session
	// has gone away (cross-session rollback during shutdown reconciliation,
	// extension teardown, etc.), we have no context to dispatch on — skip
	// the worker RPC and the cache-version probe. The cache will be
	// rechecked on the next Start anyway, and the worker's transaction will
	// be reaped by its idle timeout / pool eviction.
	auto ctx = context.lock();
	if (!ctx) {
		return;
	}

	if (!transaction_id_.empty()) {
		auto &params = vgi_catalog_.attach_parameters();
		auto &attach_result = vgi_catalog_.attach_result();
		if (attach_result) {
			vgi::CatalogRpcContext rpc_ctx{params, attach_result->attach_id, transaction_id_};
			vgi::InvokeCatalogTransactionRollback(rpc_ctx, *ctx);
		}
	}

	// Same rationale as Commit: skip CheckAndInvalidateCache. A rollback
	// undoes any pending writes, so the worker's version cannot have moved
	// because of this session's actions. Other-session changes are caught
	// at the next Transaction::Start.
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
