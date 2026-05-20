// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "duckdb.hpp"

namespace duckdb {

class VgiExtension : public Extension {
public:
	void Load(ExtensionLoader &db) override;
	std::string Name() override;
	std::string Version() const override;
};

} // namespace duckdb
