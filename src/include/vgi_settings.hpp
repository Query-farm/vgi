// © Copyright 2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
#pragma once

namespace duckdb {
class ClientContext;
class DBConfig;
class Value;
enum class SetScope : unsigned char;
namespace vgi {

// Register/validate built-in flat settings, including values supplied at database open.
// The LOCATION policy is registered separately so it can seed its monotonic policy state.
void RegisterVgiSettings(DBConfig &config);
void RegisterVgiSetting(DBConfig &config, const char *name);
void ApplyAllowedTransportsSetting(ClientContext &context, SetScope scope, Value &parameter);

} // namespace vgi
} // namespace duckdb
