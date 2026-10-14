// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "storage/vgi_transaction.hpp"

#include "storage/vgi_catalog.hpp"
#include "storage/vgi_schema_set.hpp"
#include "vgi_catalog_rpc.hpp"

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
	vgi_catalog_.CheckAndInvalidateCache(context, /*transaction_opaque_data=*/{});

	// Call the worker's catalog_transaction_begin RPC if transactions are supported
	auto &attach_result = vgi_catalog_.attach_result();
	if (attach_result && attach_result->supports_transactions) {
		auto &params = vgi_catalog_.attach_parameters();
		vgi::CatalogRpcContext rpc_ctx{params, attach_result->attach_opaque_data, {}};
		transaction_opaque_data_ = vgi::InvokeCatalogTransactionBegin(rpc_ctx, context);
		// A frozen catalog promises invariant schema/object information even
		// when transactions scope function state. It can retain shared caches.
		if (!transaction_opaque_data_.empty() && !attach_result->catalog_version_frozen) {
			transaction_schemas_ = make_uniq<VgiSchemaSet>(vgi_catalog_);
		}
	}
}

void VgiTransaction::Commit(ClientContext &context) {
	// Call the worker's catalog_transaction_commit RPC
	if (!transaction_opaque_data_.empty()) {
		auto &params = vgi_catalog_.attach_parameters();
		auto &attach_result = vgi_catalog_.attach_result();
		vgi::CatalogRpcContext rpc_ctx{params, attach_result->attach_opaque_data, transaction_opaque_data_};
		vgi::InvokeCatalogTransactionCommit(rpc_ctx, context);
	}
	transaction_state = VgiTransactionState::TRANSACTION_FINISHED;

	// Retain entries referenced by bound statements, but do not publish this
	// transaction's metadata cache as the catalog's committed cache.
	ClearSchemaCache();
	// The next Start still checks committed catalog/result-cache versions.
}

void VgiTransaction::Rollback() {
	transaction_state = VgiTransactionState::TRANSACTION_FINISHED;

	// Entries loaded after DDL belong only to this transaction. Rollback must
	// discard them without clearing another connection's snapshot.
	ClearSchemaCache();

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

	if (!transaction_opaque_data_.empty()) {
		auto &params = vgi_catalog_.attach_parameters();
		auto &attach_result = vgi_catalog_.attach_result();
		if (attach_result) {
			vgi::CatalogRpcContext rpc_ctx{params, attach_result->attach_opaque_data, transaction_opaque_data_};
			vgi::InvokeCatalogTransactionRollback(rpc_ctx, *ctx);
		}
	}

	// No extra version RPC is needed. Transaction-local cache entries were
	// discarded above; other-session changes are caught at the next Start.
}

void VgiTransaction::ClearSchemaCache() {
	if (transaction_schemas_) {
		vgi_catalog_.AbsorbDroppedEntries(transaction_schemas_->HarvestEntries());
	}
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
