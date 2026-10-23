// © Copyright 2026 Query Farm LLC - https://query.farm
#include "vgi_column_metadata.hpp"

#include "duckdb/common/exception.hpp"
#include "yyjson.hpp"
#include <arrow/api.h>
#include <arrow/extension_type.h>
#include <arrow/util/utf8.h>
#include <set>

namespace duckdb {
namespace vgi {
namespace {
using namespace duckdb_yyjson; // NOLINT

void Utf8(const std::string &text, const char *label) {
	// Arrow's UTF-8 DFA tables must be initialized even when this validation
	// is the process's first Arrow text operation. Local static initialization
	// serializes concurrent callers before they can read the tables.
	static const bool initialized = [] {
		arrow::util::InitializeUTF8();
		return true;
	}();
	(void)initialized;
	if (!arrow::util::ValidateUTF8(reinterpret_cast<const uint8_t *>(text.data()), text.size())) {
		throw InvalidInputException("VGI column metadata: %s must be valid UTF-8", label);
	}
}

std::map<std::string, std::string> Tags(const std::string &text) {
	Utf8(text, "tags");
	std::unique_ptr<yyjson_doc, decltype(&yyjson_doc_free)> doc(yyjson_read(text.data(), text.size(), 0),
	                                                            yyjson_doc_free);
	if (!doc || !yyjson_is_obj(yyjson_doc_get_root(doc.get()))) {
		throw InvalidInputException("VGI column metadata: tags must be a JSON object of strings");
	}
	std::map<std::string, std::string> result;
	yyjson_obj_iter iterator;
	yyjson_obj_iter_init(yyjson_doc_get_root(doc.get()), &iterator);
	while (auto key = yyjson_obj_iter_next(&iterator)) {
		auto value = yyjson_obj_iter_get_val(key);
		if (!yyjson_is_str(value)) {
			throw InvalidInputException("VGI column metadata: tag values must be strings");
		}
		// JSON strings can contain escaped NUL. Never use C-string lengths or
		// case folding: raw keys such as a.b and a/B remain distinct.
		std::string name(yyjson_get_str(key), yyjson_get_len(key));
		std::string content(yyjson_get_str(value), yyjson_get_len(value));
		if (!result.emplace(std::move(name), std::move(content)).second) {
			throw InvalidInputException("VGI column metadata: duplicate JSON tag key");
		}
	}
	return result;
}
} // namespace

NativeColumnMetadata ReadNativeColumnMetadata(const std::shared_ptr<arrow::Field> &field) {
	NativeColumnMetadata result;
	if (auto metadata = field->metadata()) {
		std::set<std::string> keys;
		for (int64_t index = 0; index < metadata->size(); ++index) {
			const auto &key = metadata->key(index);
			if (!keys.insert(key).second)
				throw InvalidInputException("VGI column metadata: duplicate metadata key");
			if (key == "comment") {
				Utf8(metadata->value(index), "comment");
				result.comment = metadata->value(index);
			} else if (key == "tags") {
				result.tags = Tags(metadata->value(index));
			}
		}
	}
	return result;
}

void ValidateNativeColumnMetadata(const std::shared_ptr<arrow::Schema> &schema) {
	std::vector<std::shared_ptr<arrow::Field>> pending = schema->fields();
	while (!pending.empty()) {
		auto field = pending.back();
		pending.pop_back();
		ReadNativeColumnMetadata(field);
		auto type = field->type();
		while (type->id() == arrow::Type::DICTIONARY || type->id() == arrow::Type::EXTENSION) {
			type = type->id() == arrow::Type::DICTIONARY
			           ? std::static_pointer_cast<arrow::DictionaryType>(type)->value_type()
			           : std::static_pointer_cast<arrow::ExtensionType>(type)->storage_type();
		}
		for (const auto &child : type->fields())
			pending.push_back(child);
	}
}
} // namespace vgi
} // namespace duckdb
