// © Copyright 2026 Query Farm LLC - https://query.farm
#include "vgi_attach_secret.hpp"

#include "vgi_attach_credentials.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "vgi_function_docs.hpp"

namespace duckdb {
namespace vgi {

namespace {

constexpr const char *KEY_BEARER_TOKEN = "bearer_token";
constexpr const char *KEY_OAUTH_REFRESH_TOKEN = "oauth_refresh_token";
constexpr const char *KEY_OPTIONS = "options";

// The built-in storages a user creates secrets in. Anything else (notably the
// Orchard remote storage a worker can register after attach) is worker-backed
// and must neither supply a credential to an ATTACH nor see a LOCATION.
bool IsLocalStorage(const std::string &storage_mode) {
	return storage_mode == SecretManager::TEMPORARY_STORAGE_NAME ||
	       storage_mode == SecretManager::LOCAL_FILE_STORAGE_NAME;
}

LogicalType OptionsMapType() {
	return LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
}

// CREATE SECRET (TYPE vgi_attach, ...). Every key is redacted: the type exists
// only to hold credentials, so duckdb_secrets() shows none of them.
unique_ptr<BaseSecret> CreateAttachSecret(ClientContext &, CreateSecretInput &input) {
	auto secret = make_uniq<KeyValueSecret>(input.scope, input.type, input.provider, input.name);
	for (const auto &entry : input.options) {
		auto key = StringUtil::Lower(entry.first);
		if (key == KEY_OPTIONS && !entry.second.IsNull()) {
			for (const auto &kv : MapValue::GetChildren(entry.second)) {
				const auto &pair = StructValue::GetChildren(kv);
				const auto opt_name = StringUtil::Lower(pair[0].ToString());
				if (opt_name.empty()) {
					throw BinderException("vgi_attach secret OPTIONS must not contain an empty option name");
				}
				if (opt_name == KEY_BEARER_TOKEN || opt_name == KEY_OAUTH_REFRESH_TOKEN) {
					throw BinderException("vgi_attach secret OPTIONS must not contain '%s'; set it as the "
					                      "secret's own %s parameter",
					                      opt_name, StringUtil::Upper(opt_name));
				}
				if (pair[1].IsNull()) {
					throw BinderException("vgi_attach secret OPTIONS value for '%s' must not be NULL", opt_name);
				}
			}
		}
		secret->secret_map[key] = entry.second;
		secret->redact_keys.insert(key);
	}
	auto bearer = secret->secret_map.find(KEY_BEARER_TOKEN);
	auto refresh = secret->secret_map.find(KEY_OAUTH_REFRESH_TOKEN);
	if (bearer != secret->secret_map.end() && refresh != secret->secret_map.end()) {
		throw BinderException("A vgi_attach secret cannot hold both BEARER_TOKEN and OAUTH_REFRESH_TOKEN");
	}
	// Keys a later DuckDB might add on its own are redacted too.
	secret->redact_keys.insert(KEY_BEARER_TOKEN);
	secret->redact_keys.insert(KEY_OAUTH_REFRESH_TOKEN);
	secret->redact_keys.insert(KEY_OPTIONS);
	return std::move(secret);
}

std::vector<SecretEntry> LocalSecretsOfType(ClientContext &context, const std::string &type) {
	auto &manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	std::vector<SecretEntry> out;
	vector<SecretEntry> all;
	try {
		all = manager.AllSecrets(transaction);
	} catch (const PermissionException &) {
		// enable_external_access = false: the persistent secret directory can't be
		// read, and DuckDB enumerates every storage at once. Implicit lookup then
		// finds nothing rather than failing an ATTACH that never asked for a
		// secret (attach_secret '<name>' still reaches a temporary secret).
		return out;
	}
	for (auto &entry : all) {
		if (!entry.secret || !IsLocalStorage(entry.storage_mode)) {
			continue;
		}
		if (!StringUtil::CIEquals(entry.secret->GetType(), type)) {
			continue;
		}
		out.push_back(entry);
	}
	return out;
}

ResolvedAttachSecret FromKeyValue(const KeyValueSecret &kv, const std::string &source) {
	ResolvedAttachSecret out;
	out.found = true;
	out.name = kv.GetName();
	out.source = source;
	for (const auto &entry : kv.secret_map) {
		auto key = StringUtil::Lower(entry.first);
		if (entry.second.IsNull()) {
			continue;
		}
		if (key == KEY_BEARER_TOKEN) {
			out.bearer_token = entry.second.ToString();
		} else if (key == KEY_OAUTH_REFRESH_TOKEN) {
			out.oauth_refresh_token = entry.second.ToString();
		} else if (key == KEY_OPTIONS) {
			for (const auto &item : MapValue::GetChildren(entry.second)) {
				const auto &pair = StructValue::GetChildren(item);
				if (pair[0].IsNull() || pair[1].IsNull()) {
					continue;
				}
				out.options[StringUtil::Lower(pair[0].ToString())] = pair[1];
			}
		}
	}
	return out;
}

// vgi_which_attach_secret(location): which vgi_attach secret an ATTACH of
// `location` would use by scope lookup (DuckDB's which_secret, with the
// boundary rule). Names only, never values.
struct WhichAttachSecretData : public TableFunctionData {
	bool found = false;
	bool finished = false;
	std::string name;
	bool persistent = false;
	std::string storage;
};

unique_ptr<FunctionData> WhichAttachSecretBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	names = {"name", "persistent", "storage"};
	return_types = {LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::VARCHAR};
	auto data = make_uniq<WhichAttachSecretData>();
	if (input.inputs.empty() || input.inputs[0].IsNull()) {
		return std::move(data);
	}
	auto entry = LookupScopedKeyValueSecret(context, input.inputs[0].ToString(), VGI_ATTACH_SECRET_TYPE);
	if (entry) {
		data->found = true;
		data->name = entry->secret->GetName();
		data->persistent = entry->persist_type == SecretPersistType::PERSISTENT;
		data->storage = entry->storage_mode;
	}
	return std::move(data);
}

void WhichAttachSecretScan(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<WhichAttachSecretData>();
	if (data.finished || !data.found) {
		return;
	}
	output.SetValue(0, 0, Value(data.name));
	output.SetValue(1, 0, Value::BOOLEAN(data.persistent));
	output.SetValue(2, 0, Value(data.storage));
	output.SetCardinality(1);
	data.finished = true;
}

} // namespace

void RegisterAttachSecretType(ExtensionLoader &loader) {
	SecretType type;
	type.name = VGI_ATTACH_SECRET_TYPE;
	type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	type.default_provider = "config";
	type.extension = "vgi";
	loader.RegisterSecretType(std::move(type));

	CreateSecretFunction fn;
	fn.secret_type = VGI_ATTACH_SECRET_TYPE;
	fn.provider = "config";
	fn.function = CreateAttachSecret;
	fn.named_parameters[KEY_BEARER_TOKEN] = LogicalType::VARCHAR;
	fn.named_parameters[KEY_OAUTH_REFRESH_TOKEN] = LogicalType::VARCHAR;
	fn.named_parameters[KEY_OPTIONS] = OptionsMapType();
	loader.RegisterFunction(std::move(fn));

	TableFunction which("vgi_which_attach_secret", {LogicalType::VARCHAR}, WhichAttachSecretScan,
	                    WhichAttachSecretBind);
	CreateTableFunctionInfo which_info(which);
	which_info.descriptions.push_back(MakeFunctionDescription(
	    "Name the vgi_attach secret an ATTACH of the given LOCATION would use when found by scope (no "
	    "attach_secret option). Applies the URL-boundary rule and ignores unscoped secrets. Returns no row "
	    "when none matches. Never returns secret values.",
	    {"location"}, {LogicalType::VARCHAR}, {"SELECT * FROM vgi_which_attach_secret('https://sales.example.com');"}));
	loader.RegisterFunction(std::move(which_info));
}

std::unique_ptr<SecretEntry> LookupScopedKeyValueSecret(ClientContext &context, const std::string &location,
                                                        const std::string &type) {
	const SecretEntry *best = nullptr;
	int64_t best_score = -1;
	bool best_temporary = false;
	auto candidates = LocalSecretsOfType(context, type);
	for (const auto &entry : candidates) {
		const auto *kv = dynamic_cast<const KeyValueSecret *>(entry.secret.get());
		if (!kv) {
			continue;
		}
		const auto &scope = kv->GetScope();
		std::vector<std::string> scopes(scope.begin(), scope.end());
		const int64_t score = BoundaryScopeScore(scopes, location);
		if (score < 0) {
			continue;
		}
		const bool temporary = entry.storage_mode == SecretManager::TEMPORARY_STORAGE_NAME;
		bool better = false;
		if (!best || score > best_score) {
			better = true;
		} else if (score == best_score) {
			if (temporary != best_temporary) {
				better = temporary;
			} else {
				better = kv->GetName() < best->secret->GetName();
			}
		}
		if (better) {
			best = &entry;
			best_score = score;
			best_temporary = temporary;
		}
	}
	if (!best) {
		return nullptr;
	}
	return make_uniq<SecretEntry>(*best);
}

ResolvedAttachSecret ResolveAttachSecret(ClientContext &context, const std::string &location, bool named,
                                         const std::string &secret_name) {
	if (named) {
		if (secret_name.empty()) {
			return {}; // attach_secret '' turns resolution off for this ATTACH.
		}
		auto &manager = SecretManager::Get(context);
		auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
		const BaseSecret *found = nullptr;
		vector<SecretEntry> all;
		try {
			all = manager.AllSecrets(transaction);
		} catch (const PermissionException &) {
			// Persistent secrets unreadable (enable_external_access = false): only
			// the temporary storage can hold it.
			auto temp = manager.GetSecretByName(transaction, secret_name, SecretManager::TEMPORARY_STORAGE_NAME);
			if (temp) {
				all.push_back(*temp);
			}
		}
		for (auto &entry : all) {
			if (!entry.secret || !IsLocalStorage(entry.storage_mode)) {
				continue;
			}
			if (StringUtil::CIEquals(entry.secret->GetName(), secret_name)) {
				// Prefer a temporary secret over a persistent one of the same name,
				// as DuckDB's own lookup does.
				if (!found || entry.storage_mode == SecretManager::TEMPORARY_STORAGE_NAME) {
					found = entry.secret.get();
				}
			}
		}
		if (!found) {
			throw BinderException("attach_secret: no secret named '%s' exists", secret_name);
		}
		if (!StringUtil::CIEquals(found->GetType(), VGI_ATTACH_SECRET_TYPE)) {
			throw BinderException("attach_secret: secret '%s' is of type '%s', not '%s'", secret_name,
			                      found->GetType(), VGI_ATTACH_SECRET_TYPE);
		}
		const auto *kv = dynamic_cast<const KeyValueSecret *>(found);
		if (!kv) {
			throw BinderException("attach_secret: secret '%s' is not a key-value secret", secret_name);
		}
		return FromKeyValue(*kv, "attach_secret");
	}
	auto entry = LookupScopedKeyValueSecret(context, location, VGI_ATTACH_SECRET_TYPE);
	if (!entry) {
		return {};
	}
	return FromKeyValue(dynamic_cast<const KeyValueSecret &>(*entry->secret), "scope");
}

} // namespace vgi
} // namespace duckdb
