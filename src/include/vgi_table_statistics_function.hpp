#pragma once

namespace duckdb {

class ExtensionLoader;

namespace vgi {

void RegisterVgiTableStatisticsFunction(ExtensionLoader &loader);

} // namespace vgi
} // namespace duckdb
