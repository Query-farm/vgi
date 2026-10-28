// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_arrow.hpp"
#include "nanoarrow/nanoarrow.hpp"

namespace duckdb {
namespace vgi_test {

inline vgi::VgiMetadata Metadata(const std::vector<std::string> &keys, const std::vector<std::string> &values) {
	vgi::VgiMetadata result;
	for (size_t i = 0; i < keys.size(); ++i)
		result.Append(keys[i], values.at(i));
	return result;
}
inline vgi::VgiField Field(const std::string &name, vgi::VgiField type, bool nullable = true,
                           const vgi::VgiMetadata &metadata = {}) {
	auto result = type.Named(name, nullable);
	if (!metadata.empty())
		result.metadata = metadata;
	return result;
}
inline vgi::VgiField CopyField(const ArrowSchema &field) {
	vgi::VgiField result;
	result.name = field.name ? field.name : "";
	result.format = field.format;
	result.nullable = (field.flags & ARROW_FLAG_NULLABLE) != 0;
	result.metadata = vgi::VgiMetadata::FromC(field.metadata);
	result.map_keys_sorted = (field.flags & ARROW_FLAG_MAP_KEYS_SORTED) != 0;
	for (int64_t i = 0; i < field.n_children; ++i)
		result.children.push_back(CopyField(*field.children[i]));
	if (field.dictionary)
		result.dictionary = std::make_shared<vgi::VgiField>(CopyField(*field.dictionary));
	return result;
}
inline ArrowArray Column(const vgi::VgiBatchPtr &batch, int index) {
	nanoarrow::UniqueArray exported;
	batch->ExportTo(exported.get());
	ArrowArray result;
	ArrowArrayMove(exported->children[index], &result);
	return result;
}
inline vgi::VgiBatchPtr ReplaceColumn(const vgi::VgiBatchPtr &batch, int index, ArrowArray replacement) {
	std::vector<ArrowArray> columns;
	for (int i = 0; i < batch->num_columns(); ++i)
		columns.push_back(i == index ? replacement : Column(batch, i));
	return vgi::VgiBatch::FromColumns(batch->schema(), batch->num_rows(), std::move(columns));
}
inline ArrowArray Struct(const vgi::VgiSchemaPtr &schema, std::vector<ArrowArray> columns) {
	ArrowArray result;
	vgi::VgiBatch::FromColumns(schema, 1, std::move(columns))->ExportTo(&result);
	return result;
}
inline ArrowArray NullArray(vgi::VgiField type, int64_t count = 1) {
	vgi::VgiBatchBuilder builder(vgi::MakeSchema({std::move(type)}));
	vgi::append::Null(builder.column(0), count);
	builder.SetRows(count);
	return Column(builder.Finish(), 0);
}
inline ArrowArray ListRows(const vgi::VgiBatchPtr &rows) {
	auto field = CopyField(rows->schema()->c_schema());
	vgi::VgiBatchBuilder builder(vgi::MakeSchema({vgi::type::List(field)}));
	vgi::VgiColumn values(&rows->c_view(), &rows->schema()->c_schema());
	for (int64_t i = 0; i < rows->num_rows(); ++i)
		vgi::AppendElement(builder.column(0)->children[0], values, i);
	vgi::append::FinishElement(builder.column(0));
	builder.FinishRow();
	return Column(builder.Finish(), 0);
}
inline ArrowArray ListColumn(const vgi::VgiBatchPtr &rows, int index) {
	vgi::VgiBatchBuilder builder(vgi::MakeSchema({vgi::type::List(CopyField(rows->schema()->field(index)))}));
	for (int64_t i = 0; i < rows->num_rows(); ++i)
		vgi::AppendElement(builder.column(0)->children[0], rows->column(index), i);
	vgi::append::FinishElement(builder.column(0));
	builder.FinishRow();
	return Column(builder.Finish(), 0);
}
inline vgi::VgiBatchPtr StructRow(const vgi::VgiColumn &column, const vgi::VgiSchemaPtr &schema) {
	vgi::VgiBatchBuilder builder(schema);
	for (int i = 0; i < builder.num_columns(); ++i)
		vgi::AppendElement(builder.column(i), column.child(i), 0);
	builder.FinishRow();
	return builder.Finish();
}
inline ArrowArray PathList(const std::vector<std::vector<std::optional<std::string>>> &paths) {
	vgi::VgiBatchBuilder builder(vgi::MakeSchema({vgi::type::List(vgi::type::List(vgi::type::Utf8()))}));
	auto list = builder.column(0);
	for (const auto &path : paths) {
		for (const auto &component : path) {
			if (component)
				vgi::append::String(list->children[0]->children[0], *component);
			else
				vgi::append::Null(list->children[0]->children[0]);
		}
		vgi::append::FinishElement(list->children[0]);
	}
	vgi::append::FinishElement(list);
	builder.FinishRow();
	return Column(builder.Finish(), 0);
}
} // namespace vgi_test
} // namespace duckdb
