#include "storage/vgi_schema_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
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
#include "vgi_catalog_api.hpp"
#include "vgi_rpc_types.hpp"

namespace duckdb {

VgiSchemaEntry::VgiSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, const vgi::VgiSchemaInfo &schema_info)
    : SchemaCatalogEntry(catalog, info), schema_info_(schema_info), tables_(catalog, *this), views_(catalog, *this),
      scalar_functions_(catalog, *this), aggregate_functions_(catalog, *this),
      table_functions_(catalog, *this),
      scalar_macros_(catalog, *this, CatalogType::MACRO_ENTRY),
      table_macros_(catalog, *this, CatalogType::TABLE_MACRO_ENTRY) {
}

VgiSchemaEntry::~VgiSchemaEntry() = default;

optional_ptr<CatalogEntry> VgiSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	auto &vgi_catalog = catalog.Cast<VgiCatalog>();
	if (vgi_catalog.GetAccessMode() == AccessMode::READ_ONLY) {
		throw BinderException("Cannot CREATE TABLE in read-only VGI catalog '%s'", catalog.GetName());
	}

	auto &create_info = info.base->Cast<CreateTableInfo>();
	auto &context = transaction.GetContext();

	// Convert DuckDB columns to Arrow schema, then enrich with default expressions
	auto columns_schema = vgi::DuckDBColumnsToArrowSchema(context, create_info.columns);
	{
		arrow::FieldVector enriched_fields;
		idx_t col_idx = 0;
		for (auto &col : create_info.columns.Logical()) {
			auto field = columns_schema->field(static_cast<int>(col_idx));
			if (col.HasDefaultValue()) {
				auto default_str = col.DefaultValue().ToString();
				auto metadata = field->metadata() ? field->metadata()->Copy() : std::make_shared<arrow::KeyValueMetadata>();
				metadata->Append("default", default_str);
				field = field->WithMetadata(metadata);
			}
			enriched_fields.push_back(field);
			col_idx++;
		}
		columns_schema = arrow::schema(enriched_fields);
	}

	// Extract constraints
	std::vector<int> not_null_constraints;
	std::vector<std::vector<int>> unique_constraints;
	std::vector<std::string> check_constraints;
	std::vector<std::vector<int>> primary_key_constraints;
	std::vector<std::vector<uint8_t>> foreign_key_constraints;

	for (auto &constraint : create_info.constraints) {
		switch (constraint->type) {
		case ConstraintType::NOT_NULL: {
			auto &nn = constraint->Cast<NotNullConstraint>();
			not_null_constraints.push_back(static_cast<int>(nn.index.index));
			break;
		}
		case ConstraintType::UNIQUE: {
			auto &uq = constraint->Cast<UniqueConstraint>();
			std::vector<int> col_indices;
			if (uq.HasIndex()) {
				// Single-column unique: index is set directly
				col_indices.push_back(static_cast<int>(uq.GetIndex().index));
			} else {
				// Multi-column unique: resolve names to indices
				for (auto &col_name : uq.GetColumnNames()) {
					string col_name_copy = col_name;
					auto col_idx = create_info.columns.GetColumnIndex(col_name_copy);
					col_indices.push_back(static_cast<int>(col_idx.index));
				}
			}
			if (uq.IsPrimaryKey()) {
				primary_key_constraints.push_back(std::move(col_indices));
			} else {
				unique_constraints.push_back(std::move(col_indices));
			}
			break;
		}
		case ConstraintType::CHECK: {
			auto &chk = constraint->Cast<CheckConstraint>();
			// Use expression->ToString(), not chk.ToString() which wraps in "CHECK(...)"
			check_constraints.push_back(chk.expression->ToString());
			break;
		}
		case ConstraintType::FOREIGN_KEY: {
			auto &fk = constraint->Cast<ForeignKeyConstraint>();
			// Serialize FK as IPC bytes matching the TableInfo FK format
			auto fk_bytes = vgi::SerializeForeignKeyToIpcBytes(
			    fk.fk_columns, fk.pk_columns, fk.info.table, fk.info.schema);
			foreign_key_constraints.push_back(std::move(fk_bytes));
			break;
		}
		default:
			break;
		}
	}

	auto on_conflict = vgi::MapOnConflict(create_info.on_conflict);

	// Get transaction ID
	auto &vgi_tx = VgiTransaction::Get(context, catalog);

	// Send DDL RPC
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();
	vgi::CatalogRpcContext rpc_ctx{attach_params, attach_result->attach_id, vgi_tx.GetTransactionId()};
	vgi::InvokeCatalogTableCreate(rpc_ctx, name, create_info.table, columns_schema, on_conflict,
	                               not_null_constraints, unique_constraints, check_constraints,
	                               primary_key_constraints, foreign_key_constraints, context);

	// Invalidate table cache and re-fetch the newly created table
	tables_.ClearEntries();
	// REPLACE may have changed the table schema, invalidating dependent views
	if (create_info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		views_.ClearEntries();
	}
	return tables_.GetEntry(context, create_info.table);
}

optional_ptr<CatalogEntry> VgiSchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
	throw BinderException("VGI catalogs are read-only");
}

optional_ptr<CatalogEntry> VgiSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                       TableCatalogEntry &table) {
	throw BinderException("VGI catalogs are read-only");
}

optional_ptr<CatalogEntry> VgiSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	auto &vgi_catalog = catalog.Cast<VgiCatalog>();
	if (vgi_catalog.GetAccessMode() == AccessMode::READ_ONLY) {
		throw BinderException("Cannot CREATE VIEW in read-only VGI catalog '%s'", catalog.GetName());
	}

	auto &context = transaction.GetContext();

	// Extract the SQL definition from the SelectStatement
	std::string definition;
	if (info.query) {
		definition = info.query->ToString();
	}

	auto on_conflict = vgi::MapOnConflict(info.on_conflict);

	// Get transaction ID
	auto &vgi_tx = VgiTransaction::Get(context, catalog);

	// Send DDL RPC
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();
	vgi::CatalogRpcContext rpc_ctx{attach_params, attach_result->attach_id, vgi_tx.GetTransactionId()};
	vgi::InvokeCatalogViewCreate(rpc_ctx, name, info.view_name, definition, on_conflict, context);

	// Invalidate view cache
	views_.ClearEntries();
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
		throw BinderException("Cannot alter objects in read-only VGI catalog '%s'", catalog.GetName());
	}

	auto &context = transaction.GetContext();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();
	auto &vgi_tx = VgiTransaction::Get(context, catalog);
	vgi::CatalogRpcContext rpc_ctx{attach_params, attach_result->attach_id, vgi_tx.GetTransactionId()};

	// Handle COMMENT ON table or view
	if (info.type == AlterType::SET_COMMENT) {
		auto &comment_info = info.Cast<SetCommentInfo>();
		if (comment_info.entry_catalog_type == CatalogType::TABLE_ENTRY) {
			bool is_null = comment_info.comment_value.IsNull();
			auto comment_str = is_null ? "" : comment_info.comment_value.ToString();
			vgi::InvokeCatalogTableCommentSet(rpc_ctx, name, comment_info.name, comment_str, is_null,
			                                   false, context);
			tables_.DropEntry(comment_info.name);
			return;
		}
		if (comment_info.entry_catalog_type == CatalogType::VIEW_ENTRY) {
			bool is_null = comment_info.comment_value.IsNull();
			auto comment_str = is_null ? "" : comment_info.comment_value.ToString();
			vgi::InvokeCatalogViewCommentSet(rpc_ctx, name, comment_info.name, comment_str, is_null,
			                                  false, context);
			views_.DropEntry(comment_info.name);
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
		vgi::InvokeCatalogTableColumnCommentSet(rpc_ctx, name, col_comment.name, col_comment.column_name,
		                                         comment_str, is_null, false, context);
		tables_.DropEntry(col_comment.name);
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
		views_.ClearEntries();
		return;
	}

	if (info.type != AlterType::ALTER_TABLE) {
		throw BinderException("ALTER %s is not supported for VGI catalogs", EnumUtil::ToString(info.type));
	}

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
	case AlterTableType::SET_NOT_NULL: {
		auto &set_nn = alter_table.Cast<SetNotNullInfo>();
		vgi::InvokeCatalogTableNotNullSet(rpc_ctx, schema_name, table_name, set_nn.column_name, context);
		break;
	}
	case AlterTableType::DROP_NOT_NULL: {
		auto &drop_nn = alter_table.Cast<DropNotNullInfo>();
		vgi::InvokeCatalogTableNotNullDrop(rpc_ctx, schema_name, table_name, drop_nn.column_name, context);
		break;
	}
	default:
		throw BinderException("ALTER TABLE %s is not supported for VGI catalogs",
		                       EnumUtil::ToString(alter_table.alter_table_type));
	}

	// Invalidate the specific table entry to force re-fetch
	tables_.DropEntry(table_name);

	// Structural changes (rename, drop/rename column, type change) can invalidate
	// dependent views — clear view cache so stale definitions are re-fetched.
	switch (alter_table.alter_table_type) {
	case AlterTableType::RENAME_TABLE:
	case AlterTableType::RENAME_COLUMN:
	case AlterTableType::REMOVE_COLUMN:
	case AlterTableType::ALTER_COLUMN_TYPE:
		views_.ClearEntries();
		break;
	default:
		break;
	}
}

void VgiSchemaEntry::Scan(ClientContext &context, CatalogType type,
                          const std::function<void(CatalogEntry &)> &callback) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
		tables_.Scan(context, callback);
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
	default:
		// Other types not yet implemented
		break;
	}
}

void VgiSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw InternalException("VgiSchemaEntry::Scan without context not supported");
}

void VgiSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	if (info.type != CatalogType::TABLE_ENTRY && info.type != CatalogType::VIEW_ENTRY) {
		throw BinderException("DROP %s is not supported for VGI catalogs", EnumUtil::ToString(info.type));
	}

	auto &vgi_catalog = catalog.Cast<VgiCatalog>();
	if (vgi_catalog.GetAccessMode() == AccessMode::READ_ONLY) {
		throw BinderException("Cannot DROP %s in read-only VGI catalog '%s'",
		                       EnumUtil::ToString(info.type), catalog.GetName());
	}

	bool ignore_not_found = (info.if_not_found == OnEntryNotFound::RETURN_NULL);

	auto &vgi_tx = VgiTransaction::Get(context, catalog);

	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();
	vgi::CatalogRpcContext rpc_ctx{attach_params, attach_result->attach_id, vgi_tx.GetTransactionId()};

	if (info.type == CatalogType::VIEW_ENTRY) {
		vgi::InvokeCatalogViewDrop(rpc_ctx, name, info.name, ignore_not_found, info.cascade, context);
		if (info.cascade) {
			// Cascade may have dropped views that depend on this view
			views_.ClearEntries();
		} else {
			views_.DropEntry(info.name);
		}
		return;
	}

	vgi::InvokeCatalogTableDrop(rpc_ctx, name, info.name, ignore_not_found, info.cascade, context);

	tables_.DropEntry(info.name);
	// Views referencing the dropped table are now stale; clear to force re-fetch.
	// With cascade, the worker may have also dropped dependent views.
	views_.ClearEntries();
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
