// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once

#include <map>
#include <memory>
#include <optional>
#include <string>

namespace arrow {
class Field;
class Schema;
} // namespace arrow

namespace duckdb {
namespace vgi {
struct NativeColumnMetadata {
	std::optional<std::string> comment;
	std::optional<std::map<std::string, std::string>> tags;
};

// Presence is part of the existing Arrow carrier: missing, empty comment and
// explicit empty tags are distinct. Neither helper changes the schema.
NativeColumnMetadata ReadNativeColumnMetadata(const std::shared_ptr<arrow::Field> &field);
void ValidateNativeColumnMetadata(const std::shared_ptr<arrow::Schema> &schema);
} // namespace vgi
} // namespace duckdb
