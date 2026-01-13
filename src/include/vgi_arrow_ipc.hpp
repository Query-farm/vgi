#pragma once

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <memory>
#include <string>
#include <vector>

namespace duckdb {
namespace vgi {

// Serialize an Arrow RecordBatch to IPC stream format bytes
std::shared_ptr<arrow::Buffer> SerializeRecordBatch(const std::shared_ptr<arrow::RecordBatch> &batch);

// Input stream wrapper for reading from a file descriptor
class FdInputStream : public arrow::io::InputStream {
public:
	explicit FdInputStream(int fd);
	~FdInputStream() override = default;

	arrow::Status Close() override;
	bool closed() const override;
	arrow::Result<int64_t> Tell() const override;
	arrow::Result<int64_t> Read(int64_t nbytes, void *out) override;
	arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;

private:
	int fd_;
	int64_t position_;
	bool is_open_;
};

// Read a single RecordBatch from a file descriptor (Arrow IPC stream format)
std::shared_ptr<arrow::RecordBatch> ReadRecordBatch(int fd);

// Extract string values from a result batch column
std::vector<std::string> ExtractStringColumn(const std::shared_ptr<arrow::RecordBatch> &batch,
                                             const std::string &column_name = "value");

} // namespace vgi
} // namespace duckdb
