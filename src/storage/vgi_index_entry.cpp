// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "storage/vgi_index_entry.hpp"

namespace duckdb {

VgiIndexEntry::VgiIndexEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateIndexInfo &info,
                             const vgi::VgiIndexInfo &index_info)
    : IndexCatalogEntry(catalog, schema, info), index_info_(index_info) {
	for (const auto &tag : index_info.tags) tags[tag.first] = tag.second;
}

VgiIndexEntry::~VgiIndexEntry() = default;

string VgiIndexEntry::GetSchemaName() const {
	return index_info_.schema_name;
}

string VgiIndexEntry::GetTableName() const {
	return index_info_.table_name;
}

} // namespace duckdb
