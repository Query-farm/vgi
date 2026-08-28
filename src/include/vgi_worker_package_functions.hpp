// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

namespace duckdb {
class ExtensionLoader;
namespace vgi {

void RegisterVgiWorkerPackageMacro(ExtensionLoader &loader);
void RegisterVgiWorkerCacheFunctions(ExtensionLoader &loader);

} // namespace vgi
} // namespace duckdb
