// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once
#include "vgi_catalog_metadata.hpp"

namespace duckdb {
struct CreateTableInfo;
namespace vgi {
constexpr const char *VGI_CONSTRAINT_READ = "vgi.constraint.records.read.v1";
constexpr const char *VGI_CONSTRAINT_CREATE = "vgi.constraint.create.v1";
constexpr const char *VGI_CONSTRAINT_MUTATE = "vgi.constraint.mutate.v1";
constexpr const char *VGI_CONSTRAINT_ENFORCE = "vgi.constraint.enforce.v1";
std::vector<uint8_t> BuildNativeConstraintCapabilities();
void ParseNativeConstraintCapabilities(CatalogAttachResult &result, const std::vector<uint8_t> &bytes);
void RequireNativeConstraintCapabilities(const CatalogAttachResult &result, const char *operation);
void DecodeNativeConstraints(VgiTableInfo &info, bool check_nullability = true);
void ValidateNativeConstraintList(const std::shared_ptr<arrow::Array> &array, int64_t row);
std::vector<std::vector<uint8_t>> EncodeNativeConstraints(const CreateTableInfo &info,
                                                          std::shared_ptr<arrow::Schema> &schema);
std::vector<uint8_t> BuildNativeNotNullChange(const VgiTableInfo &info, const std::string &column, bool add);
std::vector<uint8_t> BuildNativeConstraintsApply(const std::vector<uint8_t> &attachment,
                                                 const std::vector<uint8_t> &transaction, const VgiTableInfo &info,
                                                 const std::vector<uint8_t> &change);
void ValidateNativeConstraintSnapshot(const std::vector<uint8_t> &bytes, const VgiTableInfo &before);
} // namespace vgi
} // namespace duckdb
