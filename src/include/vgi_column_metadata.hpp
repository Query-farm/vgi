// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once

#include <map>
#include "vgi_arrow.hpp"
#include <optional>
#include <string>

namespace duckdb {
namespace vgi {
struct NativeColumnMetadata {
	std::optional<std::string> comment;
	std::optional<std::map<std::string, std::string>> tags;
};

// Presence is part of the existing Arrow carrier: missing, empty comment and
// explicit empty tags are distinct. Neither helper changes the schema.
NativeColumnMetadata ReadNativeColumnMetadata(const ArrowSchema &field);
void ValidateNativeColumnMetadata(const VgiSchemaPtr &schema);
} // namespace vgi
} // namespace duckdb
