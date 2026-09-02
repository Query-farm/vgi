// © Copyright 2025, 2026 Query Farm LLC - https://query.farm

#include "vgi_iroh_config.hpp"

#include "duckdb/common/exception.hpp"
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
