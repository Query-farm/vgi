// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "storage/vgi_index_set.hpp"
#include "storage/vgi_index_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/common/enums/index_constraint_type.hpp"

#include "storage/vgi_catalog.hpp"
#include "storage/vgi_transaction.hpp"
#include "storage/vgi_schema_entry.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_logging.hpp"

namespace duckdb {

static unique_ptr<VgiIndexEntry> CreateIndexEntryFromInfo(Catalog &catalog, SchemaCatalogEntry &schema,
                                                          const vgi::VgiIndexInfo &index_info) {
	CreateIndexInfo info;
	info.index_name = index_info.name;
	info.table = index_info.table_name;
	info.index_type = index_info.index_type;

	// Map constraint_type string to IndexConstraintType enum
	if (index_info.constraint_type == "UNIQUE") {
		info.constraint_type = IndexConstraintType::UNIQUE;
	} else if (index_info.constraint_type == "PRIMARY") {
		info.constraint_type = IndexConstraintType::PRIMARY;
	} else {
		info.constraint_type = IndexConstraintType::NONE;
	}

	// Parse expression strings into ParsedExpression objects
	for (auto &expr_str : index_info.expressions) {
		try {
			auto expr_list = Parser::ParseExpressionList(expr_str);
			if (!expr_list.empty()) {
				info.parsed_expressions.push_back(std::move(expr_list[0]));
			}
		} catch (const std::exception &e) {
			VGI_STDERR_DEBUG("[VGI] index.parse_warning name=%s expr=%s error=%s\n", index_info.name.c_str(),
			                 expr_str.c_str(), e.what());
		} catch (...) {
			VGI_STDERR_DEBUG("[VGI] index.parse_warning name=%s expr=%s error=unknown\n", index_info.name.c_str(),
			                 expr_str.c_str());
		}
	}

	// Copy options
	for (auto &[key, val] : index_info.options) {
		info.options[key] = Value(val);
	}

	// Set comment and tags
	if (!index_info.comment.empty()) {
		info.comment = Value(index_info.comment);
	}
	for (auto &[key, val] : index_info.tags) {
		info.tags[key] = val;
	}

	return make_uniq<VgiIndexEntry>(catalog, schema, info, index_info);
}

VgiIndexSet::VgiIndexSet(Catalog &catalog, VgiSchemaEntry &schema) : VgiCatalogSet(catalog), schema_(schema) {
}

optional_ptr<CatalogEntry> VgiIndexSet::GetEntry(ClientContext &context, const std::string &name) {
	std::lock_guard<std::mutex> lock(entry_lock_);

	// Check if we have the entry cached
	auto it = GetEntries().find(name);
	if (it != GetEntries().end()) {
		return it->second.get();
	}

	// Load this specific index from the worker
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();

	if (!attach_params || !attach_result) {
		return nullptr;
	}

	auto &vgi_tx = VgiTransaction::Get(context, catalog_);
	vgi::CatalogRpcContext rpc_ctx{attach_params, attach_result->attach_opaque_data, vgi_tx.GetTransactionOpaqueData()};

	auto index_info_opt = vgi::InvokeCatalogIndexGet(rpc_ctx, schema_.name, name, context);

	if (!index_info_opt) {
		return nullptr;
	}

	auto &index_info = *index_info_opt;
	auto index_entry = CreateIndexEntryFromInfo(catalog_, schema_, index_info);
	auto result = index_entry.get();
	GetEntries()[name] = std::move(index_entry);

	return result;
}

void VgiIndexSet::LoadEntries(ClientContext &context, const std::lock_guard<std::mutex> &/*_load_lock*/) {
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();

	if (!attach_params || !attach_result) {
		return;
	}

	// Call catalog_schema_contents_indexes via RPC
	auto &vgi_tx_load = VgiTransaction::Get(context, catalog_);
	vgi::CatalogRpcContext rpc_ctx{attach_params, attach_result->attach_opaque_data, vgi_tx_load.GetTransactionOpaqueData()};
	auto indexes = vgi::InvokeCatalogSchemaContentsIndexes(rpc_ctx, schema_.name, context);

	for (auto &index_info : indexes) {
		auto index_entry = CreateIndexEntryFromInfo(catalog_, schema_, index_info);
		{ std::lock_guard<std::mutex> __entry_lk(entry_lock_); CreateEntryLocked(std::move(index_entry)); }
	}
}

} // namespace duckdb
