// © Copyright 2025, 2026 Query Farm LLC - https://query.farm

#include "vgi_iroh_config.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "vgi_transport.hpp"

#include <algorithm>
#include <limits>
#include <utility>

namespace duckdb {
namespace vgi {

IrohSecretKey::IrohSecretKey(std::string encoded) : bytes_(encoded.begin(), encoded.end()) {
	volatile char *source = encoded.empty() ? nullptr : encoded.data();
	for (size_t i = 0; i < encoded.size(); ++i) {
		source[i] = 0;
	}
}

IrohSecretKey::~IrohSecretKey() {
	Wipe();
}

IrohSecretKey::IrohSecretKey(IrohSecretKey &&other) noexcept : bytes_(std::move(other.bytes_)) {
	other.Wipe();
}

IrohSecretKey &IrohSecretKey::operator=(IrohSecretKey &&other) noexcept {
	if (this != &other) {
		Wipe();
		bytes_ = std::move(other.bytes_);
		other.Wipe();
	}
	return *this;
}

void IrohSecretKey::Wipe() noexcept {
	// Volatile stores prevent the compiler from optimizing away the clear.
	volatile char *ptr = bytes_.empty() ? nullptr : bytes_.data();
	for (size_t i = 0; i < bytes_.size(); ++i) {
		ptr[i] = 0;
	}
	bytes_.clear();
}

std::string IrohSecretKey::CopyEncoded() const {
	return std::string(bytes_.begin(), bytes_.end());
}

namespace {

std::string SecretKeyFromScope(ClientContext &context, const std::string &scope) {
	auto &manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	auto match = manager.LookupSecret(transaction, scope, "iroh");
	if (!match.HasMatch()) {
		return {};
	}
	const auto *kv = dynamic_cast<const KeyValueSecret *>(&match.GetSecret());
	if (!kv) {
		throw BinderException("VGI Iroh identity secret for '%s' is not a key-value secret", scope);
	}
	auto entry = kv->secret_map.find("secret_key");
	if (entry == kv->secret_map.end()) {
		throw BinderException("VGI Iroh identity secret for '%s' is missing SECRET_KEY", scope);
	}
	auto encoded = entry->second.ToString();
	if (encoded.empty()) {
		throw BinderException("VGI Iroh identity secret for '%s' has an empty SECRET_KEY", scope);
	}
	return encoded;
}

} // namespace

std::shared_ptr<IrohClientConfig>
ResolveIrohClientConfig(ClientContext &context, const std::string &location,
	                    std::string explicit_secret_key,
	                    std::vector<std::string> relay_urls, bool no_relay,
	                    std::string remote_relay_url,
	                    std::vector<std::string> direct_addresses,
	                    uint64_t connect_timeout_seconds, uint64_t io_timeout_seconds) {
	if (!IsIrohTransport(location) && !IsHttpiTransport(location)) {
		throw BinderException("VGI Iroh configuration requires an iroh:// or httpi:// LOCATION");
	}
	if (no_relay && !relay_urls.empty()) {
		throw BinderException("iroh_no_relay and iroh_relay_urls are mutually exclusive");
	}
	if (no_relay && !remote_relay_url.empty()) {
		throw BinderException("iroh_no_relay and iroh_remote_relay_url are mutually exclusive");
	}
	if (connect_timeout_seconds == 0 || io_timeout_seconds == 0) {
		throw BinderException("VGI Iroh timeout settings must be greater than zero");
	}
	if (connect_timeout_seconds > std::numeric_limits<uint64_t>::max() / 1000 ||
	    io_timeout_seconds > std::numeric_limits<uint64_t>::max() / 1000) {
		throw BinderException("VGI Iroh timeout settings are too large");
	}
	for (const auto &relay : relay_urls) {
		if (relay.empty()) {
			throw BinderException("iroh_relay_urls must not contain an empty URL");
		}
	}
	for (const auto &address : direct_addresses) {
		if (address.empty()) {
			throw BinderException("iroh_direct_addresses must not contain an empty address");
		}
	}

	auto result = std::make_shared<IrohClientConfig>();
	if (IsIrohTransport(location)) {
		auto canonical = CanonicalizeIrohLocation(location);
		result->protocol = IrohProtocol::ARROW_MUX;
		result->endpoint_id = canonical.substr(std::string("iroh://").size());
	} else {
		auto parsed = ParseHttpiUrl(location);
		result->protocol = IrohProtocol::HTTP;
		result->endpoint_id = std::move(parsed.endpoint_id);
		result->base_path = std::move(parsed.path);
	}
	result->canonical_scope = "iroh://" + result->endpoint_id;
	result->relay_urls = std::move(relay_urls);
	result->no_relay = no_relay;
	result->remote_relay_url = std::move(remote_relay_url);
	result->direct_addresses = std::move(direct_addresses);
	result->connect_timeout_seconds = connect_timeout_seconds;
	result->io_timeout_seconds = io_timeout_seconds;

	std::string encoded_key;
	if (!explicit_secret_key.empty()) {
		encoded_key = std::move(explicit_secret_key);
		result->identity_source = IrohIdentitySource::ATTACH;
	} else {
		encoded_key = SecretKeyFromScope(context, result->canonical_scope);
		if (!encoded_key.empty()) {
			result->identity_source = IrohIdentitySource::SCOPED_SECRET;
		}
	}
	if (!encoded_key.empty()) {
		result->secret_key = std::make_shared<IrohSecretKey>(std::move(encoded_key));
	}
	return result;
}

namespace {

std::vector<std::string> IrohListOption(const Value &value) {
	std::vector<std::string> out;
	if (value.IsNull()) {
		return out;
	}
	if (value.type().id() == LogicalTypeId::LIST || value.type().id() == LogicalTypeId::ARRAY) {
		for (const auto &child : ListValue::GetChildren(value)) {
			out.push_back(child.DefaultCastAs(LogicalType::VARCHAR).ToString());
		}
		return out;
	}
	// Connection-string values are VARCHAR: accept a comma-separated spelling.
	auto encoded = value.ToString();
	size_t start = 0;
	while (start <= encoded.size()) {
		auto comma = encoded.find(',', start);
		auto item = encoded.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
		if (!item.empty()) {
			out.push_back(std::move(item));
		}
		if (comma == std::string::npos) {
			break;
		}
		start = comma + 1;
	}
	return out;
}

std::string NonEmptyIrohString(const std::string &name, const Value &value) {
	auto s = value.IsNull() ? std::string() : value.ToString();
	if (s.empty()) {
		throw BinderException("%s must not be empty", name);
	}
	return s;
}

} // namespace

bool ApplyIrohOption(const std::string &lower_name, const Value &value, IrohOptions &out) {
	if (lower_name == "iroh_secret_key") {
		out.secret_key = NonEmptyIrohString(lower_name, value);
	} else if (lower_name == "iroh_no_relay") {
		out.no_relay = !value.IsNull() && value.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>();
	} else if (lower_name == "iroh_relay_urls") {
		out.relay_urls = IrohListOption(value);
	} else if (lower_name == "iroh_remote_relay_url") {
		out.remote_relay_url = NonEmptyIrohString(lower_name, value);
	} else if (lower_name == "iroh_direct_addresses") {
		out.direct_addresses = IrohListOption(value);
	} else {
		return false;
	}
	return true;
}

std::shared_ptr<IrohClientConfig> BuildIrohClientConfigForLocation(ClientContext &context,
                                                                   const std::string &location, IrohOptions options,
                                                                   const char *entry_name) {
	const bool is_iroh_location = IsIrohTransport(location) || IsHttpiTransport(location);
	if (!is_iroh_location) {
		if (options.Any()) {
			throw BinderException("Iroh %s options require an iroh:// or httpi:// LOCATION", entry_name);
		}
		return nullptr;
	}
#if defined(__EMSCRIPTEN__)
	if (options.Any()) {
		throw BinderException("DuckDB-WASM Iroh identity and address resolution are owned by the application adapter");
	}
	return nullptr;
#else
	auto positive_setting = [&](const char *name, int64_t fallback) -> uint64_t {
		Value value;
		auto configured = context.TryGetCurrentSetting(name, value) ? value.GetValue<int64_t>() : fallback;
		if (configured <= 0) {
			throw BinderException("%s must be greater than zero", name);
		}
		return static_cast<uint64_t>(configured);
	};
	return ResolveIrohClientConfig(context, location, std::move(options.secret_key), std::move(options.relay_urls),
	                               options.no_relay, std::move(options.remote_relay_url),
	                               std::move(options.direct_addresses),
	                               positive_setting("vgi_iroh_connect_timeout_seconds", 30),
	                               positive_setting("vgi_iroh_io_timeout_seconds", 300));
#endif
}

const char *IrohIdentitySourceName(IrohIdentitySource source) {
	switch (source) {
	case IrohIdentitySource::EPHEMERAL:
		return "ephemeral";
	case IrohIdentitySource::SCOPED_SECRET:
		return "secret";
	case IrohIdentitySource::ATTACH:
		return "attach";
	}
	return "unknown";
}

} // namespace vgi
} // namespace duckdb
