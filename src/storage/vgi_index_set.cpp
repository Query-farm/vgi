// © Copyright 2026 Query Farm LLC - https://query.farm
#include "storage/vgi_index_set.hpp"
#include "storage/vgi_index_entry.hpp"
#include "storage/vgi_catalog.hpp"
#include "storage/vgi_schema_entry.hpp"
#include "storage/vgi_transaction.hpp"
#include "vgi_catalog_rpc.hpp"
#include "vgi_index_contract.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"

namespace duckdb {
VgiIndexSet::VgiIndexSet(Catalog &catalog, VgiSchemaEntry &schema) : VgiCatalogSet(catalog, &schema), schema_(schema) {
}

void VgiIndexSet::LoadEntries(ClientContext &context, const std::lock_guard<std::mutex> &) {
	auto &owner = catalog_.Cast<VgiCatalog>();
	auto &attachment = owner.attach_result();
	if (!attachment || !attachment->constraint_profiles.count(vgi::VGI_INDEX_READ))
		return;
	auto &transaction = VgiTransaction::Get(context, catalog_);
	vgi::CatalogRpcContext rpc {owner.attach_parameters(), attachment->attach_opaque_data,
	                            transaction.GetTransactionOpaqueData()};
	const auto generation = Generation();
	vector<unique_ptr<VgiIndexEntry>> pending;
	case_insensitive_set_t names;
	for (const auto &index : schema_.TakeIndexes(rpc, context)) {
		vgi::RequireNativeIndexCapabilities(*attachment, vgi::VGI_INDEX_READ, index.unique);
		if (!names.insert(index.name).second)
			throw IOException("Duplicate case-insensitive index name");
		if (index.schema_name != schema_.name.GetIdentifierName())
			throw IOException("VGI index belongs to another schema");
		CreateIndexInfo info;
		info.SetQualifiedName(schema_.GetQualifiedName(Identifier(index.name)));
		info.table = Identifier(index.table_name);
		info.index_type = index.algorithm;
		info.constraint_type = index.unique ? IndexConstraintType::UNIQUE : IndexConstraintType::NONE;
		for (const auto &column : index.columns)
			info.parsed_expressions.push_back(make_uniq<ColumnRefExpression>(Identifier(column)));
		if (index.comment)
			info.comment = Value(*index.comment);
		for (const auto &tag : index.tags)
			info.tags[tag.first] = tag.second;
		pending.push_back(make_uniq<VgiIndexEntry>(catalog_, schema_, info, index));
	}
	std::lock_guard<std::mutex> lock(entry_lock_);
	if (generation != Generation())
		throw IOException("Index catalog changed during discovery; retry the statement");
	for (auto &entry : pending) {
		if (GetEntries().find(entry->name.GetIdentifierName()) == GetEntries().end())
			CreateEntryLocked(std::move(entry));
	}
}
} // namespace duckdb
