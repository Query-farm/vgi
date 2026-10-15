// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_constraint_contract.hpp"
#include "vgi_index_contract.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/main/settings.hpp"
#include "storage/vgi_schema_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/comment_on_column_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/constraints/check_constraint.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include "storage/vgi_catalog.hpp"
#include "storage/vgi_table_entry.hpp"
#include "storage/vgi_transaction.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_rpc.hpp"
#include "vgi_exception.hpp"
#include "vgi_logging.hpp"
#include "vgi_rpc_types.hpp"

namespace duckdb {

VgiSchemaEntry::VgiSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, const vgi::VgiSchemaInfo &schema_info)
    : SchemaCatalogEntry(catalog, info), schema_info_(schema_info),
      // Default missing keys to 1: workers that don't populate the field bias toward
      // eager bulk-load, matching the semantics described on VgiSchemaInfo.
      estimated_counts_(ObjectCountsFromMap(schema_info.estimated_object_count, /*default_value=*/1)),
      indexes_(catalog, *this), tables_(catalog, *this), views_(catalog, *this), scalar_functions_(catalog, *this),
      aggregate_functions_(catalog, *this), table_functions_(catalog, *this),
      scalar_macros_(catalog, *this, CatalogType::MACRO_ENTRY),
      table_macros_(catalog, *this, CatalogType::TABLE_MACRO_ENTRY) {
}

VgiSchemaEntry::VgiSchemaEntry(Catalog &catalog, CreateSchemaInfo &info,
                               std::shared_ptr<const vgi::VgiSchemaContents> seed)
    : SchemaCatalogEntry(catalog, info), info_source_(seed), schema_info_ready_(false), indexes_(catalog, *this), tables_(catalog, *this),
      views_(catalog, *this), scalar_functions_(catalog, *this), aggregate_functions_(catalog, *this),
      table_functions_(catalog, *this), scalar_macros_(catalog, *this, CatalogType::MACRO_ENTRY),
      table_macros_(catalog, *this, CatalogType::TABLE_MACRO_ENTRY), seed_(std::move(seed)) {
}

VgiSchemaEntry::~VgiSchemaEntry() = default;

void VgiSchemaEntry::EnsureSchemaInfo() {
	if (schema_info_ready_.load(std::memory_order_acquire)) {
		return;
	}
	std::lock_guard<std::mutex> lock(schema_info_mutex_);
	if (schema_info_ready_.load(std::memory_order_relaxed)) {
		return;
	}
	schema_info_ = vgi::DecodeContentsSchemaInfo(*info_source_);
	estimated_counts_ = ObjectCountsFromMap(schema_info_.estimated_object_count, /*default_value=*/1);
	if (schema_info_.comment.has_value()) {
		comment = Value(*schema_info_.comment);
	}
	for (auto &tag : schema_info_.tags) {
		tags[tag.first] = tag.second;
	}
	schema_info_ready_.store(true, std::memory_order_release);
}

std::shared_ptr<const vgi::VgiSchemaContents> VgiSchemaEntry::ClaimSeed(const std::string &kind) {
	std::lock_guard<std::mutex> lock(seed_mutex_);
	if (!seed_ || !seed_taken_.insert(kind).second) {
		return nullptr;
	}
	return seed_;
}

void VgiSchemaEntry::DiscardSeed(const std::string &kind) {
	std::lock_guard<std::mutex> lock(seed_mutex_);
	if (!seed_) {
		return;
	}
	// Seeds are claimed under their RPC spelling; the macro sets share one
	// CacheKindName ("macro") for both macro kinds. Function kinds match their
	// RPC spelling case-insensitively (scalar_function == SCALAR_FUNCTION).
	if (StringUtil::CIEquals(kind, "macro")) {
		seed_taken_.insert("SCALAR_MACRO");
		seed_taken_.insert("TABLE_MACRO");
	} else {
		seed_taken_.insert(kind);
	}
}

bool VgiSchemaEntry::HasSeed(const std::string &kind) {
	std::lock_guard<std::mutex> lock(seed_mutex_);
	return seed_ && seed_taken_.find(kind) == seed_taken_.end();
}

namespace {
// One event per seeded kind decoded — each kind of each schema is decoded at
// most once, on first use. Lets tests (and profiling) see which parts of a
// catalog_contents snapshot a workload actually touched.
template <class T>
std::vector<T> LogSeedDecode(ClientContext &context, const std::string &schema, const char *kind,
                             std::vector<T> items) {
	VGI_LOG(context, "catalog.seed_decode",
	        {{"schema", schema}, {"kind", kind}, {"items", std::to_string(items.size())}});
	return items;
}
} // namespace

std::vector<vgi::VgiTableInfo> VgiSchemaEntry::TakeTables(const vgi::CatalogRpcContext &rpc_ctx,
                                                          ClientContext &context) {
	if (auto seed = ClaimSeed("table")) {
		return LogSeedDecode(context, name, "table", vgi::DecodeContentsTables(*seed, context));
	}
	return vgi::InvokeCatalogSchemaContentsTables(rpc_ctx, name, context);
}

std::vector<vgi::VgiIndexInfo> VgiSchemaEntry::TakeIndexes(const vgi::CatalogRpcContext &rpc_ctx,
                                                         ClientContext &context) {
	if (auto seed = ClaimSeed("index"))
		return LogSeedDecode(context, name, "index", vgi::DecodeContentsIndexes(*seed));
	return vgi::InvokeCatalogSchemaContentsIndexes(rpc_ctx, name, context);
}

std::vector<vgi::VgiViewInfo> VgiSchemaEntry::TakeViews(const vgi::CatalogRpcContext &rpc_ctx,
                                                        ClientContext &context) {
	if (auto seed = ClaimSeed("view")) {
		return LogSeedDecode(context, name, "view", vgi::DecodeContentsViews(*seed));
	}
	return vgi::InvokeCatalogSchemaContentsViews(rpc_ctx, name, context);
}

std::vector<vgi::VgiFunctionInfo> VgiSchemaEntry::FetchFunctions(const vgi::CatalogRpcContext &rpc_ctx,
                                                                 const char *rpc_type, ClientContext &context) {
	if (auto seed = ClaimSeed(rpc_type)) {
		return LogSeedDecode(context, name, rpc_type, vgi::DecodeContentsFunctions(*seed, rpc_type));
	}
	return vgi::InvokeCatalogSchemaContentsFunctions(rpc_ctx, name, rpc_type, context);
}

std::vector<vgi::VgiMacroInfo> VgiSchemaEntry::FetchMacros(const vgi::CatalogRpcContext &rpc_ctx,
                                                           const char *rpc_type, ClientContext &context) {
	if (auto seed = ClaimSeed(rpc_type)) {
		return LogSeedDecode(context, name, rpc_type, vgi::DecodeContentsMacros(*seed, rpc_type));
	}
	return vgi::InvokeCatalogSchemaContentsMacros(rpc_ctx, name, rpc_type, context);
}

std::vector<vgi::VgiMacroInfo> VgiSchemaEntry::GetMacroInventory(const vgi::CatalogRpcContext &rpc_ctx,
                                                                 CatalogType macro_type, ClientContext &context) {
	std::lock_guard<std::mutex> lock(macro_discovery_mutex_);
	auto &inventory = macro_type == CatalogType::MACRO_ENTRY ? scalar_macro_inventory_ : table_macro_inventory_;
	if (!inventory) {
		const char *rpc_type = macro_type == CatalogType::MACRO_ENTRY ? "SCALAR_MACRO" : "TABLE_MACRO";
		inventory = FetchMacros(rpc_ctx, rpc_type, context);
	}
	return *inventory;
}

std::vector<vgi::VgiFunctionInfo> VgiSchemaEntry::GetFunctionInventory(const vgi::CatalogRpcContext &rpc_ctx,
                                                                       CatalogType function_type,
                                                                       ClientContext &context) {
	std::lock_guard<std::mutex> lock(macro_discovery_mutex_);
	std::optional<std::vector<vgi::VgiFunctionInfo>> *inventory;
	const char *rpc_type;
	switch (function_type) {
	case CatalogType::SCALAR_FUNCTION_ENTRY:
		inventory = &scalar_function_inventory_;
		rpc_type = "SCALAR_FUNCTION";
		break;
	case CatalogType::AGGREGATE_FUNCTION_ENTRY:
		inventory = &aggregate_function_inventory_;
		rpc_type = "AGGREGATE_FUNCTION";
		break;
	case CatalogType::TABLE_FUNCTION_ENTRY:
		inventory = &table_function_inventory_;
		rpc_type = "TABLE_FUNCTION";
		break;
	default:
		throw InternalException("Unsupported VGI function inventory catalog type");
	}
	if (!*inventory) {
		*inventory = FetchFunctions(rpc_ctx, rpc_type, context);
	}
	return **inventory;
}

std::shared_ptr<const case_insensitive_set_t>
VgiSchemaEntry::GetMacroCallableNames(const vgi::CatalogRpcContext &rpc_ctx, bool trust_empty_kinds,
                                      ClientContext &context) {
	std::lock_guard<std::mutex> lock(macro_discovery_mutex_);
	if (macro_callable_names_ && (macro_callable_names_->empty() || macro_callable_names_complete_)) {
		return macro_callable_names_;
	}
	(void)trust_empty_kinds;

	if (!scalar_macro_inventory_) {
		scalar_macro_inventory_ = FetchMacros(rpc_ctx, "SCALAR_MACRO", context);
	}
	if (!table_macro_inventory_) {
		table_macro_inventory_ = FetchMacros(rpc_ctx, "TABLE_MACRO", context);
	}

	auto callable_names = std::make_shared<case_insensitive_set_t>();
	for (const auto &macro : *scalar_macro_inventory_) {
		callable_names->insert(macro.name);
	}
	for (const auto &macro : *table_macro_inventory_) {
		callable_names->insert(macro.name);
	}

	// The caller only asks for callable names after observing a non-empty macro
	// inventory. Keep this defensive empty check so future callers cannot turn an
	// empty macro lookup into three unrelated function-inventory RPCs.
	if (!callable_names->empty()) {
		const auto add_functions = [&](std::optional<std::vector<vgi::VgiFunctionInfo>> &inventory,
		                               const char *function_type) {
			if (!inventory) {
				inventory = FetchFunctions(rpc_ctx, function_type, context);
			}
			for (const auto &function : *inventory) {
				callable_names->insert(function.name);
			}
		};
		// Estimated counts are a discovery optimization, not an authority for
		// names referenced by a macro body. Some workers register executable
		// functions outside their declarative catalog model and report zero here
		// even though the function inventory is non-empty. Once a macro exists,
		// fetch all callable inventories exactly once so owner-relative names are
		// never left on DuckDB's ambient search path.
		add_functions(scalar_function_inventory_, "SCALAR_FUNCTION");
		add_functions(aggregate_function_inventory_, "AGGREGATE_FUNCTION");
		add_functions(table_function_inventory_, "TABLE_FUNCTION");
	}

	macro_callable_names_ = callable_names;
	macro_callable_names_complete_ = scalar_function_inventory_.has_value() &&
	                                 aggregate_function_inventory_.has_value() &&
	                                 table_function_inventory_.has_value();
	return macro_callable_names_;
}

namespace {
// Push the harvested entries from a child catalog set into the parent
// VgiCatalog's deferred-drop graveyard so any raw CatalogEntry* held by a
// bound query/prepared statement stays valid until the catalog itself is
// destroyed (or vgi_clear_cache() is called explicitly). Without this,
// schema-level DDL (DROP TABLE, ALTER, etc.) yanks pointers that DuckDB's
// optimizer/binder may still be holding from a previous LookupEntry.
void DeferDrop(Catalog &catalog, std::vector<unique_ptr<CatalogEntry>> entries) {
	if (entries.empty()) {
		return;
	}
	catalog.Cast<VgiCatalog>().AbsorbDroppedEntries(std::move(entries));
}
void DeferDrop(Catalog &catalog, unique_ptr<CatalogEntry> entry) {
	if (!entry) {
		return;
	}
	catalog.Cast<VgiCatalog>().AbsorbDroppedEntry(std::move(entry));
}
} // namespace

optional_ptr<CatalogEntry> VgiSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	auto &vgi_catalog = catalog.Cast<VgiCatalog>();
	if (vgi_catalog.GetAccessMode() == AccessMode::READ_ONLY) {
		throw BinderException(vgi::ErrorInfo(vgi::error_subtype::kReadOnly).Set(vgi::error_key::kCatalog, catalog.GetName()).Set(vgi::error_key::kOperation, "CREATE TABLE"),
		                      StringUtil::Format("Cannot CREATE TABLE in read-only VGI catalog '%s'", catalog.GetName()));
	}

	auto &create_info = info.base->Cast<CreateTableInfo>();
	auto &context = transaction.GetContext();

	// The shared encoder preserves defaults for both CREATE and ADD COLUMN.
	auto columns_schema = vgi::DuckDBColumnsToArrowSchema(context, create_info.columns);

	// The binder accepts case-insensitive SQL spellings. Resolve referenced FK
	// coordinates to actual catalog names before the raw-path wire boundary.
	for (auto &constraint : create_info.constraints) {
		if (constraint->type != ConstraintType::FOREIGN_KEY) continue;
		auto &fk = constraint->Cast<ForeignKeyConstraint>();
		if (fk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
			fk.info.table = create_info.table;
			for (auto &column : fk.pk_columns) column = create_info.columns.GetColumn(column).Name();
		} else {
			auto target = tables_.GetEntry(context, fk.info.table);
			if (!target) throw BinderException("Referenced table %s does not exist", fk.info.table);
			auto &entry = target->Cast<VgiTableEntry>();
			fk.info.table = entry.name;
			for (auto &column : fk.pk_columns) column = entry.GetColumns().GetColumn(column).Name();
		}
	}

	// Canonical definitions use raw column names, never engine/rowid indices.
	auto constraint_definitions = vgi::EncodeNativeConstraints(create_info, columns_schema);
	if (!constraint_definitions.empty()) {
		vgi::RequireNativeConstraintCapabilities(*vgi_catalog.attach_result(), vgi::VGI_CONSTRAINT_CREATE);
	}

	auto on_conflict = vgi::MapOnConflict(create_info.on_conflict);

	// Get transaction ID
	auto &vgi_tx = VgiTransaction::Get(context, catalog);

	// Send DDL RPC
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();
	vgi::CatalogRpcContext rpc_ctx{attach_params, attach_result->attach_opaque_data, vgi_tx.GetTransactionOpaqueData()};
	if (!constraint_definitions.empty() && !rpc_ctx.transaction_opaque_data.empty() &&
	    !attach_result->constraint_profiles.count("vgi.transaction.ddl.v1")) {
		throw NotImplementedException("Constraint CREATE in a transaction requires vgi.transaction.ddl.v1");
	}
	// CREATE (including an unconstrained REPLACE) may take effect before a lost
	// response. Drop cached definitions and the empty-table shortcut before
	// dispatch so failure cannot preserve stale constraints or hide a created table.
	DeferDrop(catalog, tables_.HarvestEntries());
	tables_.MarkPopulatedLocked();
	if (create_info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		DeferDrop(catalog, indexes_.HarvestEntries());
		DeferDrop(catalog, views_.HarvestEntries());
	}
	vgi::InvokeCatalogTableCreate(rpc_ctx, name, create_info.table, columns_schema, on_conflict,
	                             constraint_definitions, context);
	return tables_.GetEntry(context, create_info.table);
}

optional_ptr<CatalogEntry> VgiSchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
	throw BinderException("VGI catalogs are read-only");
}

optional_ptr<CatalogEntry> VgiSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                       TableCatalogEntry &table) {
	auto &owner = catalog.Cast<VgiCatalog>();
	if (owner.GetAccessMode() == AccessMode::READ_ONLY) throw BinderException("Cannot CREATE INDEX in read-only VGI catalog");
	auto &context = transaction.GetContext();
	const auto &collation = Settings::Get<DefaultCollationSetting>(context);
	if (!collation.empty() && !StringUtil::CIEquals(collation, "binary"))
		throw BinderException("VGI raw index keys require binary default collation");
	auto &tx = VgiTransaction::Get(context, catalog);
	auto &attached = *owner.attach_result();
	vgi::CatalogRpcContext rpc {owner.attach_parameters(), attached.attach_opaque_data, tx.GetTransactionOpaqueData()};
	vgi::RequireNativeIndexCapabilities(attached, vgi::VGI_INDEX_CREATE,
	    info.constraint_type == IndexConstraintType::UNIQUE, !rpc.transaction_opaque_data.empty());
	auto definition = vgi::EncodeNativeIndexDefinition(info, table);
	vgi::RequireCatalogIndexAlgorithm(rpc, name, table.name, info.index_type, context);
	// Lost responses cannot leave stale index listings, including a cached empty kind.
	DeferDrop(catalog, indexes_.HarvestEntries());
	indexes_.MarkPopulatedLocked();
	vgi::InvokeCatalogIndexCreate(rpc, name, info.index_name, definition, vgi::MapOnConflict(info.on_conflict), context);
	return nullptr;
}

optional_ptr<CatalogEntry> VgiSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	auto &vgi_catalog = catalog.Cast<VgiCatalog>();
	if (vgi_catalog.GetAccessMode() == AccessMode::READ_ONLY) {
		throw BinderException(vgi::ErrorInfo(vgi::error_subtype::kReadOnly).Set(vgi::error_key::kCatalog, catalog.GetName()).Set(vgi::error_key::kOperation, "CREATE VIEW"),
		                      StringUtil::Format("Cannot CREATE VIEW in read-only VGI catalog '%s'", catalog.GetName()));
	}

	auto &context = transaction.GetContext();

	// Extract the SQL definition from the SelectStatement
	std::string definition;
	if (info.query) {
		definition = info.query->ToString();
	}

	std::optional<std::string> comment;
	if (!info.comment.IsNull()) {
		comment = info.comment.ToString();
	}
	std::map<std::string, std::string> tags;
	for (const auto &tag : info.tags) {
		tags.emplace(tag.first, tag.second);
	}
	std::map<std::string, std::string> column_comments;
	for (const auto &entry : info.column_comments_map) {
		if (!entry.second.IsNull()) {
			column_comments.emplace(entry.first, entry.second.ToString());
		}
	}
	auto on_conflict = vgi::MapOnConflict(info.on_conflict);

	// Get transaction ID
	auto &vgi_tx = VgiTransaction::Get(context, catalog);

	// Send DDL RPC
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();
	vgi::CatalogRpcContext rpc_ctx{attach_params, attach_result->attach_opaque_data, vgi_tx.GetTransactionOpaqueData()};
	vgi::InvokeCatalogViewCreate(rpc_ctx, name, info.view_name, definition, std::vector<std::string>(info.aliases.begin(), info.aliases.end()),
	                             on_conflict, comment, tags, column_comments, context);

	// Invalidate view cache
	DeferDrop(catalog, views_.HarvestEntries());
	// Same DDL safety net as CreateTable: clear any cached zero-count
	// emptiness so the next view lookup doesn't get short-circuited.
	views_.MarkPopulatedLocked();
	return nullptr;
}

optional_ptr<CatalogEntry> VgiSchemaEntry::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
	throw BinderException("VGI catalogs are read-only");
}

optional_ptr<CatalogEntry> VgiSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                               CreateTableFunctionInfo &info) {
	throw BinderException("VGI catalogs are read-only");
}

optional_ptr<CatalogEntry> VgiSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                              CreateCopyFunctionInfo &info) {
	throw BinderException("VGI catalogs are read-only");
}

optional_ptr<CatalogEntry> VgiSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                CreatePragmaFunctionInfo &info) {
	throw BinderException("VGI catalogs are read-only");
}

optional_ptr<CatalogEntry> VgiSchemaEntry::CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) {
	throw BinderException("VGI catalogs are read-only");
}

optional_ptr<CatalogEntry> VgiSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw BinderException("VGI catalogs are read-only");
}

void VgiSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	auto &vgi_catalog = catalog.Cast<VgiCatalog>();
	if (vgi_catalog.GetAccessMode() == AccessMode::READ_ONLY) {
		throw BinderException(vgi::ErrorInfo(vgi::error_subtype::kReadOnly).Set(vgi::error_key::kCatalog, catalog.GetName()).Set(vgi::error_key::kOperation, "ALTER"),
		                      StringUtil::Format("Cannot alter objects in read-only VGI catalog '%s'", catalog.GetName()));
	}

	auto &context = transaction.GetContext();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();
	auto &vgi_tx = VgiTransaction::Get(context, catalog);
	vgi::CatalogRpcContext rpc_ctx{attach_params, attach_result->attach_opaque_data, vgi_tx.GetTransactionOpaqueData()};

	// Handle COMMENT ON table or view
	if (info.type == AlterType::SET_COMMENT) {
		auto &comment_info = info.Cast<SetCommentInfo>();
		if (comment_info.entry_catalog_type == CatalogType::TABLE_ENTRY) {
			bool is_null = comment_info.comment_value.IsNull();
			auto comment_str = is_null ? "" : comment_info.comment_value.ToString();
			vgi::InvokeCatalogTableCommentSet(rpc_ctx, name, comment_info.name, comment_str, is_null,
			                                   false, context);
			DeferDrop(catalog, tables_.HarvestEntry(comment_info.name));
			return;
		}
		if (comment_info.entry_catalog_type == CatalogType::VIEW_ENTRY) {
			bool is_null = comment_info.comment_value.IsNull();
			auto comment_str = is_null ? "" : comment_info.comment_value.ToString();
			vgi::InvokeCatalogViewCommentSet(rpc_ctx, name, comment_info.name, comment_str, is_null,
			                                  false, context);
			DeferDrop(catalog, views_.HarvestEntry(comment_info.name));
			return;
		}
		throw BinderException("COMMENT ON %s is not supported for VGI catalogs",
		                       EnumUtil::ToString(comment_info.entry_catalog_type));
	}

	// Handle COMMENT ON COLUMN
	if (info.type == AlterType::SET_COLUMN_COMMENT) {
		auto &col_comment = info.Cast<SetColumnCommentInfo>();
		bool is_null = col_comment.comment_value.IsNull();
		auto comment_str = is_null ? "" : col_comment.comment_value.ToString();
		if (col_comment.catalog_entry_type == CatalogType::VIEW_ENTRY) {
			auto entry = views_.GetEntry(context, col_comment.name);
			if (!entry) {
				throw BinderException("View \"%s\" does not exist", col_comment.name);
			}
			auto &view = entry->Cast<ViewCatalogEntry>();
			view.BindView(context);
			auto columns = view.GetColumnInfo();
			bool found = false;
			if (columns) {
				for (const auto &column : columns->names) {
					if (column == col_comment.column_name) {
						found = true;
						break;
					}
				}
			}
			if (!found) {
				throw BinderException("View \"%s\" does not have a column with name \"%s\"",
				                      col_comment.name, col_comment.column_name);
			}
			vgi::InvokeCatalogViewColumnCommentSet(rpc_ctx, name, col_comment.name, col_comment.column_name,
			                                    comment_str, is_null, false, context);
			DeferDrop(catalog, views_.HarvestEntry(col_comment.name));
		} else if (col_comment.catalog_entry_type == CatalogType::TABLE_ENTRY) {
			vgi::InvokeCatalogTableColumnCommentSet(rpc_ctx, name, col_comment.name, col_comment.column_name,
			                                     comment_str, is_null, false, context);
			DeferDrop(catalog, tables_.HarvestEntry(col_comment.name));
		} else {
			throw BinderException("COMMENT ON COLUMN is not supported for this VGI object kind");
		}
		return;
	}

	// Handle ALTER VIEW (rename)
	if (info.type == AlterType::ALTER_VIEW) {
		auto &alter_view = info.Cast<AlterViewInfo>();
		if (alter_view.alter_view_type == AlterViewType::RENAME_VIEW) {
			auto &rename = alter_view.Cast<RenameViewInfo>();
			vgi::InvokeCatalogViewRename(rpc_ctx, name, alter_view.name, rename.new_view_name,
			                              false, context);
		} else {
			throw BinderException("ALTER VIEW %s is not supported for VGI catalogs",
			                       EnumUtil::ToString(alter_view.alter_view_type));
		}
		// Dependent views may reference the old name, so clear the entire views cache
		DeferDrop(catalog, views_.HarvestEntries());
		return;
	}

	if (info.type != AlterType::ALTER_TABLE) {
		throw BinderException("ALTER %s is not supported for VGI catalogs", EnumUtil::ToString(info.type));
	}

	// Structural changes can rename or remove index targets. Invalidate before
	// dispatch, including ambiguous failures; the provider owns dependencies.
	DeferDrop(catalog, indexes_.HarvestEntries());
	auto &alter_table = info.Cast<AlterTableInfo>();
	auto &schema_name = name;  // VgiSchemaEntry::name
	auto &table_name = alter_table.name;

	switch (alter_table.alter_table_type) {
	case AlterTableType::ADD_COLUMN: {
		auto &add_col = alter_table.Cast<AddColumnInfo>();
		// Convert single column to Arrow schema
		ColumnList single_col;
		single_col.AddColumn(add_col.new_column.Copy());
		auto col_schema = vgi::DuckDBColumnsToArrowSchema(context, single_col);
		vgi::InvokeCatalogTableColumnAdd(rpc_ctx, schema_name, table_name, col_schema,
		                                  add_col.if_column_not_exists, context);
		break;
	}
	case AlterTableType::REMOVE_COLUMN: {
		auto &rem_col = alter_table.Cast<RemoveColumnInfo>();
		vgi::InvokeCatalogTableColumnDrop(rpc_ctx, schema_name, table_name, rem_col.removed_column,
		                                   rem_col.if_column_exists, rem_col.cascade, context);
		break;
	}
	case AlterTableType::RENAME_COLUMN: {
		auto &ren_col = alter_table.Cast<RenameColumnInfo>();
		vgi::InvokeCatalogTableColumnRename(rpc_ctx, schema_name, table_name,
		                                     ren_col.old_name, ren_col.new_name, context);
		break;
	}
	case AlterTableType::RENAME_TABLE: {
		auto &ren_tbl = alter_table.Cast<RenameTableInfo>();
		vgi::InvokeCatalogTableRename(rpc_ctx, schema_name, table_name, ren_tbl.new_table_name,
		                               false, context);
		break;
	}
	case AlterTableType::ALTER_COLUMN_TYPE: {
		auto &change_type = alter_table.Cast<ChangeColumnTypeInfo>();
		// Create a single-column Arrow schema from the column name and new type
		ColumnList single_col;
		single_col.AddColumn(ColumnDefinition(change_type.column_name, change_type.target_type));
		auto col_schema = vgi::DuckDBColumnsToArrowSchema(context, single_col);
		auto expression_str = change_type.expression ? change_type.expression->ToString() : "";
		vgi::InvokeCatalogTableColumnTypeChange(rpc_ctx, schema_name, table_name, col_schema,
		                                         expression_str, context);
		break;
	}
	case AlterTableType::SET_DEFAULT: {
		auto &set_def = alter_table.Cast<SetDefaultInfo>();
		if (set_def.expression) {
			// SET DEFAULT
			vgi::InvokeCatalogTableColumnDefaultSet(rpc_ctx, schema_name, table_name, set_def.column_name,
			                                         set_def.expression->ToString(), context);
		} else {
			// DROP DEFAULT (null expression)
			vgi::InvokeCatalogTableColumnDefaultDrop(rpc_ctx, schema_name, table_name, set_def.column_name,
			                                          context);
		}
		break;
	}
	case AlterTableType::SET_NOT_NULL:
	case AlterTableType::DROP_NOT_NULL: {
		vgi::RequireNativeConstraintCapabilities(*attach_result, vgi::VGI_CONSTRAINT_MUTATE);
		if (!rpc_ctx.transaction_opaque_data.empty() && !attach_result->constraint_profiles.count("vgi.transaction.ddl.v1")) {
			throw NotImplementedException("Constraint ALTER in a transaction requires vgi.transaction.ddl.v1");
		}
		const bool add = alter_table.alter_table_type == AlterTableType::SET_NOT_NULL;
		auto column = add ? alter_table.Cast<SetNotNullInfo>().column_name : alter_table.Cast<DropNotNullInfo>().column_name;
		// Fresh read ties stable constraint identity to its current definition revision.
		auto current = vgi::InvokeCatalogTableGet(rpc_ctx, schema_name, table_name, context);
		if (!current) throw BinderException("Table %s does not exist", table_name);
		auto change = vgi::BuildNativeNotNullChange(*current, column, add);
		if (!change.empty()) {
			// A remote mutation may succeed even if its response is lost or malformed.
			DeferDrop(catalog, tables_.HarvestEntry(table_name));
			vgi::InvokeCatalogTableConstraintsApply(rpc_ctx, *current, change, context);
		}
		break;
	}
	default:
		throw BinderException("ALTER TABLE %s is not supported for VGI catalogs",
		                       EnumUtil::ToString(alter_table.alter_table_type));
	}

	// Invalidate the specific table entry to force re-fetch
	DeferDrop(catalog, tables_.HarvestEntry(table_name));

	// Structural changes (rename, drop/rename column, type change) can invalidate
	// dependent views — clear view cache so stale definitions are re-fetched.
	switch (alter_table.alter_table_type) {
	case AlterTableType::RENAME_TABLE:
	case AlterTableType::RENAME_COLUMN:
	case AlterTableType::REMOVE_COLUMN:
	case AlterTableType::ALTER_COLUMN_TYPE:
		DeferDrop(catalog, views_.HarvestEntries());
		break;
	default:
		break;
	}
}

void VgiSchemaEntry::Scan(ClientContext &context, CatalogType type,
                          const std::function<void(CatalogEntry &)> &callback) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
		// DuckDB stores tables and views in a single CatalogSet, so a TABLE_ENTRY
		// scan naturally surfaces both — duckdb_tables()/duckdb_constraints() then
		// skip non-table entries by type, while duckdb_columns() dispatches views
		// to its ViewColumnHelper (which binds the view to expose its columns and
		// column comments). VGI keeps views in a separate set, so we must include
		// them here too; otherwise VGI views never appear in duckdb_columns().
		tables_.Scan(context, callback);
		views_.Scan(context, callback);
		break;
	case CatalogType::VIEW_ENTRY:
		views_.Scan(context, callback);
		break;
	case CatalogType::SCALAR_FUNCTION_ENTRY:
		// DuckDB's duckdb_functions() only scans SCALAR_FUNCTION_ENTRY, not separate
		// AGGREGATE or MACRO entries. Include all function types here so they appear
		// in duckdb_functions().
		scalar_functions_.Scan(context, callback);
		aggregate_functions_.Scan(context, callback);
		scalar_macros_.Scan(context, callback);
		break;
	case CatalogType::AGGREGATE_FUNCTION_ENTRY:
		aggregate_functions_.Scan(context, callback);
		break;
	case CatalogType::TABLE_FUNCTION_ENTRY:
		// Same pattern: table macros must be included in TABLE_FUNCTION_ENTRY scan.
		table_functions_.Scan(context, callback);
		table_macros_.Scan(context, callback);
		break;
	case CatalogType::MACRO_ENTRY:
		scalar_macros_.Scan(context, callback);
		break;
	case CatalogType::TABLE_MACRO_ENTRY:
		table_macros_.Scan(context, callback);
		break;
	case CatalogType::INDEX_ENTRY:
		indexes_.Scan(context, callback);
		break;
	default:
		// Other types not yet implemented
		break;
	}
}

void VgiSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw InternalException("VgiSchemaEntry::Scan without context not supported");
}

void VgiSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	if (info.type != CatalogType::TABLE_ENTRY && info.type != CatalogType::VIEW_ENTRY && info.type != CatalogType::INDEX_ENTRY) {
		throw BinderException("DROP %s is not supported for VGI catalogs", EnumUtil::ToString(info.type));
	}

	auto &vgi_catalog = catalog.Cast<VgiCatalog>();
	if (vgi_catalog.GetAccessMode() == AccessMode::READ_ONLY) {
		throw BinderException(vgi::ErrorInfo(vgi::error_subtype::kReadOnly).Set(vgi::error_key::kCatalog, catalog.GetName()).Set(vgi::error_key::kOperation, "DROP " + EnumUtil::ToString(info.type)),
		                      StringUtil::Format("Cannot DROP %s in read-only VGI catalog '%s'",
		                                         EnumUtil::ToString(info.type), catalog.GetName()));
	}

	bool ignore_not_found = (info.if_not_found == OnEntryNotFound::RETURN_NULL);

	auto &vgi_tx = VgiTransaction::Get(context, catalog);

	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();
	vgi::CatalogRpcContext rpc_ctx{attach_params, attach_result->attach_opaque_data, vgi_tx.GetTransactionOpaqueData()};

	if (info.type == CatalogType::INDEX_ENTRY) {
		vgi::RequireNativeIndexCapabilities(*attach_result, vgi::VGI_INDEX_DROP, false,
		                                  !rpc_ctx.transaction_opaque_data.empty());
		if (info.cascade) throw BinderException("Native VGI DROP INDEX CASCADE is unsupported");
		// Lookup validates unsupported associated/PRIMARY definitions before effects.
		auto bound = indexes_.GetEntry(context, info.name);
		const auto raw_name = bound ? bound->name : info.name;
		auto current = vgi::InvokeCatalogIndexGet(rpc_ctx, name, raw_name, context);
		if (!current && !ignore_not_found) throw BinderException("Index %s does not exist", info.name);
		DeferDrop(catalog, indexes_.HarvestEntries());
		vgi::InvokeCatalogIndexDrop(rpc_ctx, name, current ? current->name : info.name, ignore_not_found, false, context);
		return;
	}

	if (info.type == CatalogType::VIEW_ENTRY) {
		vgi::InvokeCatalogViewDrop(rpc_ctx, name, info.name, ignore_not_found, info.cascade, context);
		if (info.cascade) {
			// Dependents may live in other schemas; invalidate this transaction's
			// whole catalog view, retaining entries held by bound statements.
			vgi_tx.ClearSchemaCache();
			vgi_catalog.ClearCache();
		} else {
			DeferDrop(catalog, views_.HarvestEntry(info.name));
		}
		return;
	}

	DeferDrop(catalog, indexes_.HarvestEntries());
	vgi::InvokeCatalogTableDrop(rpc_ctx, name, info.name, ignore_not_found, info.cascade, context);
	if (info.cascade) {
		vgi_tx.ClearSchemaCache();
		vgi_catalog.ClearCache();
		return;
	}

	DeferDrop(catalog, tables_.HarvestEntry(info.name));
	// Views referencing the dropped table are now stale; clear to force re-fetch.
	// With cascade, the worker may have also dropped dependent views.
	DeferDrop(catalog, views_.HarvestEntries());
}

optional_ptr<CatalogEntry> VgiSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                       const EntryLookupInfo &lookup_info) {
	auto type = lookup_info.GetCatalogType();
	auto &entry_name = lookup_info.GetEntryName();
	auto &context = transaction.GetContext();

	switch (type) {
	case CatalogType::TABLE_ENTRY: {
		// DuckDB resolves FROM clauses as TABLE_ENTRY; fall back to views
		auto result = tables_.GetEntry(context, lookup_info);
		if (!result) {
			result = views_.GetEntry(context, entry_name);
		}
		return result;
	}
	case CatalogType::VIEW_ENTRY:
		return views_.GetEntry(context, entry_name);
	case CatalogType::INDEX_ENTRY:
		return indexes_.GetEntry(context, entry_name);
	case CatalogType::SCALAR_FUNCTION_ENTRY: {
		// DuckDB resolves all function calls via SCALAR_FUNCTION_ENTRY first.
		// Fall back to aggregate functions and macros (they share a set in the
		// default catalog).
		auto result = scalar_functions_.GetEntry(context, entry_name);
		if (!result) {
			result = aggregate_functions_.GetEntry(context, entry_name);
		}
		if (!result) {
			result = scalar_macros_.GetEntry(context, entry_name);
		}
		return result;
	}
	case CatalogType::AGGREGATE_FUNCTION_ENTRY:
		return aggregate_functions_.GetEntry(context, entry_name);
	case CatalogType::TABLE_FUNCTION_ENTRY: {
		// Same pattern: table macros are looked up via TABLE_FUNCTION_ENTRY.
		auto result = table_functions_.GetEntry(context, entry_name);
		if (!result) {
			result = table_macros_.GetEntry(context, entry_name);
		}
		return result;
	}
	case CatalogType::MACRO_ENTRY:
		return scalar_macros_.GetEntry(context, entry_name);
	case CatalogType::TABLE_MACRO_ENTRY:
		return table_macros_.GetEntry(context, entry_name);
	default:
		// Other types not yet implemented
		return nullptr;
	}
}

VgiCatalogSet &VgiSchemaEntry::GetCatalogSet(CatalogType type) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
		return tables_;
	case CatalogType::VIEW_ENTRY:
		return views_;
	case CatalogType::INDEX_ENTRY:
		return indexes_;
	case CatalogType::SCALAR_FUNCTION_ENTRY:
		return scalar_functions_;
	case CatalogType::AGGREGATE_FUNCTION_ENTRY:
		return aggregate_functions_;
	case CatalogType::TABLE_FUNCTION_ENTRY:
		return table_functions_;
	case CatalogType::MACRO_ENTRY:
		return scalar_macros_;
	case CatalogType::TABLE_MACRO_ENTRY:
		return table_macros_;
	default:
		throw InternalException("Unsupported catalog type for VgiSchemaEntry");
	}
}

} // namespace duckdb
