// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_wire_codec.hpp"

namespace duckdb {
namespace vgi {

// Field access for the semantic validators which inspect raw Arrow shapes before
// decoding. Use the same strict runtime as generated records: malformed values
// never become defaults, null collection members are rejected, and map keys are
// unique. Ordinary protocol records should use generated::Decode instead.
class RecordBatchSingleRow {
public:
	RecordBatchSingleRow(const std::shared_ptr<arrow::RecordBatch> &batch, int64_t row, const char *description,
	                     const std::string &worker_path)
	    : batch_(batch), row_(row), description_(description), worker_path_(worker_path),
	      reader_(batch_, row_, description_.c_str(), worker_path_) {
	}
	RecordBatchSingleRow(const RecordBatchSingleRow &) = delete;
	RecordBatchSingleRow &operator=(const RecordBatchSingleRow &) = delete;

	class ColumnValue {
	public:
		ColumnValue(const RecordBatchSingleRow &owner, std::string name) : owner_(owner), name_(std::move(name)) {
		}
		template <class T>
		T value_not_null() const {
			T value {};
			owner_.reader_.Required(name_.c_str(), value);
			return value;
		}
		template <class T>
		std::optional<T> as() const {
			std::optional<T> value;
			owner_.reader_.Optional(name_.c_str(), value);
			return value;
		}
		template <class T>
		T value_or(T fallback) const {
			return as<T>().value_or(std::move(fallback));
		}
		std::string value_or(const char *fallback) const {
			return value_or(std::string(fallback));
		}
		bool is_null() const {
			auto column = owner_.batch_->GetColumnByName(name_);
			wire::Ctx ctx {owner_.description_, &owner_.worker_path_, name_, owner_.row_};
			if (!column)
				wire::ThrowMissing(ctx);
			return wire::IsNullValue(*column, owner_.row_, ctx);
		}

	private:
		const RecordBatchSingleRow &owner_;
		std::string name_;
	};

	ColumnValue operator[](const std::string &name) const {
		return ColumnValue(*this, name);
	}

private:
	std::shared_ptr<arrow::RecordBatch> batch_;
	int64_t row_;
	std::string description_;
	std::string worker_path_;
	wire::RowReader reader_;
};

} // namespace vgi
} // namespace duckdb
