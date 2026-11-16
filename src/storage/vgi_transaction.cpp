// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "storage/vgi_transaction.hpp"

#include "storage/vgi_catalog.hpp"
#include "storage/vgi_schema_set.hpp"
#include "vgi_catalog_rpc.hpp"
#include "vgi_subprocess.hpp" // CleanupRpcScope
#include "vgi_http_params.hpp"
#include "vgi_logging.hpp"

#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

namespace duckdb {

VgiTransaction::VgiTransaction(VgiCatalog &vgi_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), access_mode_(vgi_catalog.GetAccessMode()), vgi_catalog_(vgi_catalog) {
}

VgiTransaction::~VgiTransaction() = default;

void VgiTransaction::Start(ClientContext &context) {
	transaction_state = VgiTransactionState::TRANSACTION_STARTED;

	// Catches DDL committed by other sessions since the last query.
	vgi_catalog_.CheckAndInvalidateCache(context, /*transaction_opaque_data=*/{});

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
	transaction_state = VgiTransactionState::TRANSACTION_FINISHED;

	// Runs before the commit RPC so bound statements keep their entries even if
	// the provider throws (DuckDB ends the transaction either way). Transaction-
	// local metadata is never published as the catalog's committed cache.
	ClearSchemaCache();

	if (!transaction_opaque_data_.empty()) {
		auto &params = vgi_catalog_.attach_parameters();
		auto &attach_result = vgi_catalog_.attach_result();
		vgi::CatalogRpcContext rpc_ctx{params, attach_result->attach_opaque_data, transaction_opaque_data_};
		vgi::InvokeCatalogTransactionCommit(rpc_ctx, context);
	}
	// The next Start still checks committed catalog/result-cache versions.
}

void VgiTransaction::Rollback() {
	transaction_state = VgiTransactionState::TRANSACTION_FINISHED;

	// Entries loaded after DDL belong only to this transaction. Rollback must
	// discard them without clearing another connection's snapshot.
	ClearSchemaCache();

	// The session may be gone (shutdown/teardown rollback): then skip the worker
	// RPCs. Remote transaction cleanup is then the provider's responsibility;
	// disconnect or pool eviction does not necessarily reclaim it.
	auto ctx = context.lock();
	if (!ctx) {
		return;
	}

	// A failed statement leaves the context interrupted (Executor::PushError), but
	// these cleanup RPCs must still reach the worker, so they are not
	// Ctrl-C-interruptible on non-HTTP transports (see CleanupRpcScope).
	vgi::CleanupRpcScope cleanup_scope;

	DropPendingCtasTables(*ctx);

	if (!transaction_opaque_data_.empty()) {
		auto &params = vgi_catalog_.attach_parameters();
		auto &attach_result = vgi_catalog_.attach_result();
		if (attach_result) {
			vgi::CatalogRpcContext rpc_ctx{params, attach_result->attach_opaque_data, transaction_opaque_data_};
			vgi::InvokeCatalogTransactionRollback(rpc_ctx, *ctx);
		}
	}

}

void VgiTransaction::AddPendingCtasTable(const std::string &schema_name, const std::string &table_name) {
	std::lock_guard<std::mutex> guard(pending_ctas_lock_);
	pending_ctas_tables_.emplace_back(schema_name, table_name);
}

void VgiTransaction::RemovePendingCtasTable(const std::string &schema_name, const std::string &table_name) {
	std::lock_guard<std::mutex> guard(pending_ctas_lock_);
	for (auto it = pending_ctas_tables_.begin(); it != pending_ctas_tables_.end(); ++it) {
		if (it->first == schema_name && it->second == table_name) {
			pending_ctas_tables_.erase(it);
			return;
		}
	}
}

// A CTAS that created its table on a non-transactional attach (the worker has
// no transaction to roll the CREATE back) and then failed: drop the table, so
// the failed statement leaves nothing behind. Best effort: a drop failure is
// logged, never raised, since the rollback must complete. CREATE OR REPLACE
// cannot restore the table it replaced.
void VgiTransaction::DropPendingCtasTables(ClientContext &ctx) {
	std::vector<std::pair<std::string, std::string>> pending;
	{
		std::lock_guard<std::mutex> guard(pending_ctas_lock_);
		pending.swap(pending_ctas_tables_);
	}
	if (pending.empty()) {
		return;
	}
	auto &params = vgi_catalog_.attach_parameters();
	auto &attach_result = vgi_catalog_.attach_result();
	if (!attach_result) {
		return;
	}
	for (auto &table : pending) {
		std::string outcome = "dropped";
		try {
			vgi::CatalogRpcContext rpc_ctx {params, attach_result->attach_opaque_data, {}};
			vgi::InvokeCatalogTableDrop(rpc_ctx, table.first, table.second, /*ignore_not_found=*/true,
			                            /*cascade=*/false, ctx);
		} catch (std::exception &ex) {
			outcome = std::string("failed: ") + ex.what();
		}
		try {
			VGI_LOG(ctx, "vgi.ctas.drop_after_failure",
			        {{"schema", table.first}, {"table", table.second}, {"outcome", outcome}});
		} catch (...) {
		}
	}
	// The dropped table's cached entry must not outlive the rollback.
	vgi_catalog_.ClearCache(/*force=*/false);
}

void VgiTransaction::ClearSchemaCache() {
	if (transaction_schemas_) {
		vgi_catalog_.AbsorbDroppedEntries(transaction_schemas_->HarvestEntries());
	}
}

VgiTransaction &VgiTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<VgiTransaction>();
}

VgiTransactionManager::VgiTransactionManager(AttachedDatabase &db_p, VgiCatalog &vgi_catalog)
    : TransactionManager(db_p), vgi_catalog_(vgi_catalog) {
}

Transaction &VgiTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<VgiTransaction>(vgi_catalog_, *this, context);
	{
		// DuckDB calls this from MetaTransaction::GetTransaction with the
		// MetaTransaction mutex held; RPCs below must not look up secrets.
		vgi::TransactionStartScope start_scope;
		transaction->Start(context);
	}
	auto &result = *transaction;
	std::lock_guard<std::mutex> l(transaction_lock_);
	transactions_[result] = std::move(transaction);
	return result;
}

unique_ptr<VgiTransaction> VgiTransactionManager::TakeTransaction(Transaction &transaction) {
	std::lock_guard<std::mutex> l(transaction_lock_);
	auto entry = transactions_.find(transaction);
	if (entry == transactions_.end()) {
		throw InternalException("VGI transaction is not owned by this manager");
	}
	auto owned_transaction = std::move(entry->second);
	transactions_.erase(entry);
	return owned_transaction;
}

ErrorData VgiTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	// Release local ownership on both success and exception, without holding
	// the manager mutex across cache retirement or provider RPCs.
	auto owned_transaction = TakeTransaction(transaction);
	owned_transaction->Commit(context);
	return ErrorData();
}

void VgiTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto owned_transaction = TakeTransaction(transaction);
	owned_transaction->Rollback();
}

void VgiTransactionManager::Checkpoint(ClientContext &context, bool force) {
	// No-op for VGI catalogs - they are remote
}

} // namespace duckdb
