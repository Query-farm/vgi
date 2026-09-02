// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace duckdb {

class ClientContext;

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

} // namespace vgi
} // namespace duckdb
