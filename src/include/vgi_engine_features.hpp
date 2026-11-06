// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

// Register vgi_engine_features(): one row per engine-dependent capability
// (see vgi_engine_compat.hpp) with whether this build has it.
void RegisterVgiEngineFeaturesFunction(ExtensionLoader &loader);

} // namespace duckdb
