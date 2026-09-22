// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace duckdb {

class ClientContext;
class Value;

namespace vgi {

enum class IrohProtocol : uint8_t {
	ARROW_MUX = 1,
	HTTP = 2,
};

enum class IrohIdentitySource : uint8_t {
	EPHEMERAL = 0,
	SCOPED_SECRET = 1,
	ATTACH = 2,
};

// Move-only storage for the encoded Iroh secret key. Its bytes are wiped when
// the last per-catalog configuration reference is released. The key is never
// exposed by diagnostics, cache keys, or telemetry.
class IrohSecretKey {
public:
	IrohSecretKey() = default;
	explicit IrohSecretKey(std::string encoded);
	~IrohSecretKey();

	IrohSecretKey(const IrohSecretKey &) = delete;
	IrohSecretKey &operator=(const IrohSecretKey &) = delete;
	IrohSecretKey(IrohSecretKey &&other) noexcept;
	IrohSecretKey &operator=(IrohSecretKey &&other) noexcept;

	bool empty() const {
		return bytes_.empty();
	}
	std::string CopyEncoded() const;

private:
	void Wipe() noexcept;
	std::vector<char> bytes_;
};

struct IrohClientConfig {
	IrohProtocol protocol = IrohProtocol::ARROW_MUX;
	IrohIdentitySource identity_source = IrohIdentitySource::EPHEMERAL;
	std::string endpoint_id;
	std::string base_path;
	std::string canonical_scope;
	std::shared_ptr<IrohSecretKey> secret_key;
	// Client endpoint relay configuration is distinct from the remote address
	// hints below. Remote hints make private/direct-only endpoints dialable.
	std::vector<std::string> relay_urls;
	bool no_relay = false;
	std::string remote_relay_url;
	std::vector<std::string> direct_addresses;
	uint64_t connect_timeout_seconds = 30;
	uint64_t io_timeout_seconds = 300;
};

// Build and validate the immutable Iroh configuration captured by ATTACH.
// Identity precedence is explicit key > longest matching TYPE iroh secret >
// process-lifetime ephemeral identity. An invalid configured key is left for
// the native Iroh layer to reject; it must never fall back to ephemeral.
std::shared_ptr<IrohClientConfig>
ResolveIrohClientConfig(ClientContext &context, const std::string &location,
	                    std::string explicit_secret_key,
	                    std::vector<std::string> relay_urls, bool no_relay,
	                    std::string remote_relay_url,
	                    std::vector<std::string> direct_addresses,
	                    uint64_t connect_timeout_seconds, uint64_t io_timeout_seconds);

const char *IrohIdentitySourceName(IrohIdentitySource source);

// The user-facing `iroh_*` options, shared by ATTACH and vgi_catalogs() so both
// entry points accept the same names, types and validation.
struct IrohOptions {
	std::string secret_key;
	std::vector<std::string> relay_urls;
	bool no_relay = false;
	std::string remote_relay_url;
	std::vector<std::string> direct_addresses;

	bool Any() const {
		return !secret_key.empty() || !relay_urls.empty() || no_relay || !remote_relay_url.empty() ||
		       !direct_addresses.empty();
	}
};

// If `lower_name` is an `iroh_*` option, apply `value` to `out` and return true.
// List options take a VARCHAR[] or (for connection-string values) a
// comma-separated VARCHAR. Throws BinderException on an invalid value.
bool ApplyIrohOption(const std::string &lower_name, const Value &value, IrohOptions &out);

// Build the Iroh configuration for `location` at an entry point (`entry_name` is
// used in errors, e.g. "ATTACH"). Returns nullptr for a location that is not
// iroh:// / httpi://, and on DuckDB-WASM (where the page adapter owns identity
// and addressing); throws BinderException if options were given for such a
// location. With no options this is the default configuration: a scoped
// `TYPE iroh` secret if one matches, else an ephemeral identity, default relays,
// and the vgi_iroh_*_timeout_seconds settings.
std::shared_ptr<IrohClientConfig> BuildIrohClientConfigForLocation(ClientContext &context,
                                                                   const std::string &location, IrohOptions options,
                                                                   const char *entry_name);

} // namespace vgi
} // namespace duckdb
