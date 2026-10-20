#pragma once

#include <memory>
#include <string>

namespace duckdb {
class ClientContext;
struct HTTPParams;

namespace vgi {

// HTTPParams construction, and the DuckDB secret-manager re-entrancy it hides.
//
// HTTPFSUtil::InitializeParameters(context, url) looks up an "http" secret for
// the URL through KeyValueSecretReader, which opens a system catalog
// transaction: MetaTransaction::GetTransaction, which takes the
// MetaTransaction mutex. VgiTransactionManager::StartTransaction runs *under*
// that mutex (DuckDB calls it from GetTransaction), so any secret lookup
// reached from VgiTransaction::Start self-deadlocks the thread.
// See https://github.com/duckdb/duckdb/issues/22258.

// Params for a pre-signed URL (externalized batch download, upload URL PUT).
// No secret is looked up, so this is safe under the MetaTransaction mutex, and
// no secret's bearer_token / extra_http_headers can reach a third-party
// storage host (a pre-signed URL carries its auth in the query string; an
// extra Authorization header breaks the signature). Session HTTP settings
// (retries, keep-alive, TLS verification, CA file, ...) and the configured
// http_proxy still apply. The caller sets the timeout and should send
// SecretFreeUserAgent() as User-Agent, which httpfs only fills in from a
// database it is not given here.
std::unique_ptr<HTTPParams> InitSecretFreeHttpParams(ClientContext &context);
std::string SecretFreeUserAgent(ClientContext &context);

// HTTPUtil::InitializeParameters(context, url), secrets included. Throws, not
// deadlocks, when called from inside VgiTransaction::Start; callers on that
// path must use cached params (VgiAttachParameters::GetOrInitHttpParams) or
// InitSecretFreeHttpParams.
std::unique_ptr<HTTPParams> InitHttpParamsWithSecrets(ClientContext &context, const std::string &url);

// Marks this thread as inside VgiTransactionManager::StartTransaction (under
// the MetaTransaction mutex) for InitHttpParamsWithSecrets' check.
class TransactionStartScope {
public:
	TransactionStartScope();
	~TransactionStartScope();
	TransactionStartScope(const TransactionStartScope &) = delete;
	TransactionStartScope &operator=(const TransactionStartScope &) = delete;

	static bool Active();
};

} // namespace vgi
} // namespace duckdb
