// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// Arrow IPC stream reading/writing backed by nanoarrow's IPC codec.
//
// Drop-in replacements for the Arrow C++ entry points the extension uses on the
// wire: arrow::ipc::RecordBatchStreamReader::Open, arrow::ipc::MakeStreamWriter,
// arrow::ipc::SerializeSchema and arrow::ipc::ReadSchema. Every byte is encoded and
// decoded by nanoarrow; decoded arrays reach Arrow C++ through the C Data Interface
// (arrow::ImportRecordBatch), which is zero-copy, so the rest of the extension is
// unchanged.
//
// VGI_IPC_BACKEND (read once per process) routes entry points back to Arrow C++ for
// A/B comparison on one binary: "arrow" (both directions), "arrow-read" (decoding
// only) or "arrow-write" (encoding only).

#include <arrow/result.h>
#include <arrow/status.h>

#include <memory>

namespace arrow {
class Buffer;
class Schema;
namespace io {
class InputStream;
class OutputStream;
} // namespace io
namespace ipc {
class DictionaryMemo;
class RecordBatchStreamReader;
class RecordBatchWriter;
struct IpcWriteOptions;
} // namespace ipc
} // namespace arrow

namespace duckdb {
namespace vgi {
namespace nanoipc {

//! True unless VGI_IPC_BACKEND=arrow.
bool Enabled();

arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchStreamReader>>
OpenStreamReader(std::shared_ptr<arrow::io::InputStream> input);

arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchWriter>>
MakeStreamWriter(std::shared_ptr<arrow::io::OutputStream> sink, const std::shared_ptr<arrow::Schema> &schema);

arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchWriter>>
MakeStreamWriter(std::shared_ptr<arrow::io::OutputStream> sink, const std::shared_ptr<arrow::Schema> &schema,
                 const arrow::ipc::IpcWriteOptions &options);

//! An encapsulated Schema message (no end-of-stream marker), as arrow::ipc::SerializeSchema.
arrow::Result<std::shared_ptr<arrow::Buffer>> SerializeSchema(const arrow::Schema &schema);

//! Read one Schema message from the stream, as arrow::ipc::ReadSchema. dictionary_memo is
//! accepted for call-site compatibility and left untouched.
arrow::Result<std::shared_ptr<arrow::Schema>> ReadSchema(arrow::io::InputStream *stream,
                                                         arrow::ipc::DictionaryMemo *dictionary_memo);

} // namespace nanoipc
} // namespace vgi
} // namespace duckdb
