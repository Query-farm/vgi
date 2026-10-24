#include "vgi_http_params.hpp"

#include "duckdb.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_file_opener.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {
namespace vgi {

namespace {

// Session settings without a database or client context. httpfs's
// InitializeParameters reads every HTTP setting through the opener, then
// builds a KeyValueSecretReader from it; with no database the reader returns
// before touching the secret manager (KeyValueSecretReader::Initialize), so
// no catalog transaction is opened.
class SecretFreeFileOpener : public FileOpener {
public:
	explicit SecretFreeFileOpener(ClientContext &context_p) : context(context_p) {
	}

	SettingLookupResult TryGetCurrentSetting(const string &key, Value &result) override {
		return context.TryGetCurrentSetting(key, result);
	}
	SettingLookupResult TryGetCurrentSetting(const string &key, Value &result, FileOpenerInfo &) override {
		return context.TryGetCurrentSetting(key, result);
	}
	optional_ptr<ClientContext> TryGetClientContext() override {
		return nullptr;
	}
	optional_ptr<DatabaseInstance> TryGetDatabase() override {
		return nullptr;
	}
	HTTPUtil &GetHTTPUtil() override {
		return HTTPUtil::Get(*context.db);
	}
	Logger &GetLogger() const override {
		return Logger::Get(context);
	}

private:
	ClientContext &context;
};

thread_local int transaction_start_depth = 0;

} // namespace

std::unique_ptr<HTTPParams> InitSecretFreeHttpParams(ClientContext &context) {
	auto &http_util = HTTPUtil::Get(*context.db);
	SecretFreeFileOpener opener(context);
	// No file_path: nothing to match a secret scope against, and it keeps
	// httpfs off its S3 secret-type branch.
	FileOpenerInfo info;
	auto owned = http_util.InitializeParameters(&opener, &info);
	// httpfs ran HTTPParams::Initialize with neither database nor context, so
	// it skipped the configured http_proxy and HTTP logging. Initialize reads
	// only database settings and the client config, never secrets.
	ClientContextFileOpener context_opener(context);
	owned->Initialize(&context_opener);
	return std::unique_ptr<HTTPParams>(owned.release());
}

std::string SecretFreeUserAgent(ClientContext &context) {
	// What httpfs sets when it has a database (HTTPFSUtil::InitializeParameters).
	return StringUtil::Format("%s %s", context.db->config.UserAgent(), DuckDB::SourceID());
}

std::unique_ptr<HTTPParams> InitHttpParamsWithSecrets(ClientContext &context, const std::string &url) {
	if (TransactionStartScope::Active()) {
		throw IOException("VGI internal error: HTTP secret lookup for '%s' inside VGI transaction start would "
		                  "deadlock (https://github.com/duckdb/duckdb/issues/22258); use cached or secret-free "
		                  "HTTP parameters on this path",
		                  url);
	}
	auto owned = HTTPUtil::Get(*context.db).InitializeParameters(context, url);
	return std::unique_ptr<HTTPParams>(owned.release());
}

TransactionStartScope::TransactionStartScope() {
	transaction_start_depth++;
}

TransactionStartScope::~TransactionStartScope() {
	transaction_start_depth--;
}

bool TransactionStartScope::Active() {
	return transaction_start_depth > 0;
}

} // namespace vgi
} // namespace duckdb
