// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once
#include "vgi_catalog_metadata.hpp"

namespace duckdb {
struct CreateTableInfo;
class ClientContext;
namespace vgi {
constexpr const char *VGI_CONSTRAINT_READ = "vgi.constraint.records.read.v1";
constexpr const char *VGI_CONSTRAINT_CREATE = "vgi.constraint.create.v1";
constexpr const char *VGI_CONSTRAINT_MUTATE = "vgi.constraint.mutate.v1";
constexpr const char *VGI_CONSTRAINT_ENFORCE = "vgi.constraint.enforce.v1";
constexpr const char *VGI_CONSTRAINT_SCALAR_READ = "vgi.expression.duckdb.scalar.v1";
std::vector<uint8_t> BuildNativeConstraintCapabilities();
void ParseNativeConstraintCapabilities(CatalogAttachResult &result, const std::vector<uint8_t> &bytes);
void RequireNativeConstraintCapabilities(const CatalogAttachResult &result, const char *operation);
void RequireNativeConstraintRead(const CatalogAttachResult &result, const VgiTableInfo &info);
void RequireNativeConstraintWrite(const CatalogAttachResult &result, const VgiTableInfo &info);
void DecodeNativeConstraints(VgiTableInfo &info, bool check_nullability = true);
void AppendNativeCheckConstraints(ClientContext &context, const VgiTableInfo &info, CreateTableInfo &create);
void ValidateNativeConstraintList(const VgiColumn &array, int64_t row);
std::vector<std::vector<uint8_t>> EncodeNativeConstraints(const CreateTableInfo &info, VgiSchemaPtr &schema);
std::vector<uint8_t> BuildNativeNotNullChange(const VgiTableInfo &info, const std::string &column, bool add);
std::vector<uint8_t> BuildNativeConstraintsApply(const std::vector<uint8_t> &attachment,
                                                 const std::vector<uint8_t> &transaction, const VgiTableInfo &info,
                                                 const std::vector<uint8_t> &change);
void ValidateNativeConstraintSnapshot(const std::vector<uint8_t> &bytes, const VgiTableInfo &before);
} // namespace vgi
} // namespace duckdb
