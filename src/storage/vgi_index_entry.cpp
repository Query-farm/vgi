#include "storage/vgi_index_entry.hpp"

namespace duckdb {

VgiIndexEntry::VgiIndexEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateIndexInfo &info,
                             const vgi::VgiIndexInfo &index_info)
    : IndexCatalogEntry(catalog, schema, info), index_info_(index_info) {
}

VgiIndexEntry::~VgiIndexEntry() = default;

string VgiIndexEntry::GetSchemaName() const {
	return index_info_.schema_name;
}

string VgiIndexEntry::GetTableName() const {
	return index_info_.table_name;
}

} // namespace duckdb
