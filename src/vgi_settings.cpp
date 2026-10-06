// © Copyright 2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
#include "vgi_settings.hpp"
#include <algorithm>
#include "vgi_settings_defaults.hpp"
#include "vgi_location_policy.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {
namespace vgi {
namespace {

enum class Kind { BOOLEAN, BIGINT, UBIGINT, VARCHAR, THRESHOLDS };
enum class Id {
#define VGI_SETTING(id, ...) id,
#include "vgi_settings_registry.inc"
#undef VGI_SETTING
};

struct Definition {
	const char *name;
	Kind kind;
	uint64_t minimum;
	uint64_t maximum;
	const char *choices;
	bool global;
	const char *description;
};

const Definition definitions[] = {
#define VGI_SETTING(id, name, kind, min, max, choices, global, description)                                            \
	{name, Kind::kind, min, max, choices, global, description},
#include "vgi_settings_registry.inc"
#undef VGI_SETTING
};

LogicalType Type(Kind kind) {
	switch (kind) {
	case Kind::BOOLEAN:
		return LogicalType::BOOLEAN;
	case Kind::BIGINT:
		return LogicalType::BIGINT;
	case Kind::UBIGINT:
		return LogicalType::UBIGINT;
	case Kind::VARCHAR:
		return LogicalType::VARCHAR;
	case Kind::THRESHOLDS:
		return LogicalType::MAP(LogicalType::VARCHAR, LogicalType::BIGINT);
	}
	throw InternalException("Unknown VGI setting type");
}

void Validate(const Definition &d, Value &value) {
	if (value.IsNull()) {
		throw InvalidInputException("%s must not be NULL", d.name);
	}
	if (d.kind == Kind::BIGINT || d.kind == Kind::UBIGINT) {
		bool negative = d.kind == Kind::BIGINT && value.GetValue<int64_t>() < 0;
		auto number = negative ? uint64_t(0) : value.GetValue<uint64_t>();
		if (negative || number < d.minimum || number > d.maximum) {
			throw InvalidInputException("%s must be between %llu and %llu", d.name,
			                            static_cast<unsigned long long>(d.minimum),
			                            static_cast<unsigned long long>(d.maximum));
		}
	} else if (d.kind == Kind::VARCHAR && *d.choices) {
		auto input = StringUtil::Lower(value.GetValue<string>());
		auto choices = StringUtil::Split(d.choices, ',');
		if (std::find(choices.begin(), choices.end(), input) == choices.end()) {
			throw InvalidInputException("%s must be one of %s (got '%s')", d.name, d.choices, input);
		}
		value = Value(input);
	} else if (d.kind == Kind::THRESHOLDS) {
		auto choices = StringUtil::Split(d.choices, ',');
		for (auto &entry : MapValue::GetChildren(value)) {
			auto &pair = StructValue::GetChildren(entry);
			if (pair[0].IsNull() || pair[1].IsNull()) {
				throw InvalidInputException("%s keys and thresholds must not be NULL", d.name);
			}
			auto key = pair[0].GetValue<string>();
			if (std::find(choices.begin(), choices.end(), key) == choices.end()) {
				throw InvalidInputException("Unknown %s key '%s'; expected one of %s", d.name, key, d.choices);
			}
			if (pair[1].GetValue<int64_t>() < 0) {
				throw InvalidInputException("%s threshold for '%s' must be nonnegative", d.name, key);
			}
		}
	} else if (string(d.name) == "vgi_allowed_transports") {
		value = Value(FormatAllowedTransports(ParseAllowedTransports(value.GetValue<string>())));
	}
}

template <Id ID>
void Set(ClientContext &context, SetScope scope, Value &parameter) {
	const auto &d = definitions[static_cast<size_t>(ID)];
	if (d.global && scope != SetScope::GLOBAL && scope != SetScope::AUTOMATIC) {
		throw InvalidInputException("%s can only be set globally (shared by connections to this database)", d.name);
	}
	Validate(d, parameter);
	if (ID == Id::ALLOWED_TRANSPORTS) {
		ApplyAllowedTransportsSetting(context, scope, parameter);
	}
}

Value Default(Kind kind, const char *value) {
	return Value(value);
}
Value Default(Kind kind, bool value) {
	return Value::BOOLEAN(value);
}
Value Default(Kind kind, uint64_t value) {
	return Value::UBIGINT(value);
}
Value Default(Kind kind, int64_t value) {
	if (kind != Kind::THRESHOLDS) {
		return Value::BIGINT(value);
	}
	const auto &d = definitions[static_cast<size_t>(Id::EAGER_LOAD_THRESHOLD)];
	vector<Value> keys, values;
	for (auto &key : StringUtil::Split(d.choices, ',')) {
		keys.emplace_back(key);
		values.push_back(Value::BIGINT(value));
	}
	return Value::MAP(LogicalType::VARCHAR, LogicalType::BIGINT, std::move(keys), std::move(values));
}

template <Id ID, class T>
void Register(DBConfig &config, T default_value) {
	const auto &d = definitions[static_cast<size_t>(ID)];
	auto type = Type(d.kind);
	config.AddExtensionOption(d.name, d.description, type, Default(d.kind, default_value), Set<ID>,
	                          d.global ? SetScope::GLOBAL : SetScope::SESSION);
	// DuckDB copies startup extension options without calling the SET callback.
	Value current;
	if (config.TryGetCurrentSetting(d.name, current)) {
		current = current.DefaultCastAs(type);
		Validate(d, current);
		config.SetOptionByName(d.name, current);
	}
}

} // namespace

void RegisterVgiSetting(DBConfig &config, const char *name) {
#define VGI_SETTING(id, setting_name, ...)                                                                             \
	if (string(name) == setting_name) {                                                                                \
		Register<Id::id>(config, defaults::id);                                                                        \
		return;                                                                                                        \
	}
#include "vgi_settings_registry.inc"
#undef VGI_SETTING
	throw InternalException("Unknown VGI setting '%s'", name);
}

void RegisterVgiSettings(DBConfig &config) {
	for (const auto &d : definitions) {
		if (string(d.name) != "vgi_allowed_transports") {
			RegisterVgiSetting(config, d.name);
		}
	}
}

} // namespace vgi
} // namespace duckdb
