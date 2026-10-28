// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once
#include "vgi_catalog_metadata.hpp"
namespace duckdb {
struct CreateIndexInfo;
class TableCatalogEntry;
namespace vgi {
constexpr const char *VGI_INDEX_READ = "vgi.index.definition.read.v1";
constexpr const char *VGI_INDEX_CREATE = "vgi.index.definition.create.v1";
constexpr const char *VGI_INDEX_DROP = "vgi.index.drop.v1";
void RequireNativeIndexCapabilities(const CatalogAttachResult &result, const char *operation, bool unique = false,
                                    bool transaction = false);
VgiIndexInfo ParseNativeIndexInfo(const VgiBatchPtr &batch);
std::vector<uint8_t> BuildNativeIndexTableRef(const std::string &schema, const std::string &table);
std::vector<uint8_t> EncodeNativeIndexDefinition(const CreateIndexInfo &info, const TableCatalogEntry &table);
std::vector<uint8_t> BuildNativeIndexCreate(const std::vector<uint8_t> &attachment,
                                            const std::vector<uint8_t> &transaction, const std::string &schema,
                                            const std::string &name, const std::vector<uint8_t> &definition,
                                            const std::string &on_conflict);
void ValidateNativeIndexOptionDefinitions(const VgiBatchPtr &batch);
} // namespace vgi
} // namespace duckdb
