#pragma once

#include <arrow/io/file.h>
#include <arrow/util/compression.h>
#include <parquet/api/writer.h>
#include <parquet/stream_writer.h>

#include "abstract_chunk_writer.hpp"
#include "storage/table/value_segment.hpp"
#include "utils/literal.hpp"

namespace skyrise {
namespace detail {

class ParquetOutputProxy : public arrow::io::OutputStream {
 public:
  using WriteCallback = std::function<void(const char* data, size_t length)>;

  static std::shared_ptr<ParquetOutputProxy> Make(WriteCallback callback) {
    auto proxy = std::make_shared<ParquetOutputProxy>();
    proxy->callback_ = std::move(callback);
    return proxy;
  }

  arrow::Status Close() override { Fail("Close is not implemented for ParquetOutputProxy"); }

  arrow::Result<int64_t> Tell() const override { return {bytes_written_}; }

  bool closed() const override { return false; }

  arrow::Status Write(const void* data, int64_t nbytes) override {
    callback_(static_cast<const char*>(data), nbytes);
    bytes_written_ += nbytes;
    return arrow::Status::OK();
  }

  ParquetOutputProxy() = default;

 private:
  int64_t bytes_written_ = 0;
  WriteCallback callback_;
};

}  // namespace detail

struct ParquetFormatWriterOptions {
  parquet::Compression::type compression = parquet::Compression::ZSTD;
  int compression_level = arrow::util::Codec::UseDefaultCompressionLevel();
  const bool consolidate_row_groups = false;

  /**
   * Target size for a row group. Considers the uncompressed estimated row sizes.
   * Chunks passed to the writted will be merged as long as their total estimated size reacher the
   * kRowGroupSizeThresholdBytes at least. Once the threshold is surpassed, the chunks will be written out as a chunk.
   * Applied only if consolidate_row_groups_ set to true, else each chunk will be mapped to exactly one row group.
   */
  const size_t kRowGroupSizeThresholdBytes = 128LL * 1024LL * 1024LL;

  explicit ParquetFormatWriterOptions(const bool consolidate_row_groups)
      : consolidate_row_groups(consolidate_row_groups) {}

  ParquetFormatWriterOptions() : consolidate_row_groups(false) {}
};

class ParquetFormatWriter : public AbstractFormatWriter {
 public:
  using Configuration = ParquetFormatWriterOptions;

  explicit ParquetFormatWriter(Configuration config);

  void Initialize(const TableColumnDefinitions& skyrise_schema) override;
  void ProcessChunk(std::shared_ptr<const Chunk> chunk) override;
  void Finalize() override;

 private:
  static std::shared_ptr<parquet::schema::Node> SkyriseTypeToParquetType(const std::string& name, const DataType type,
                                                                         const bool nullable);

  void CopySegmentToParquetColumn(const std::shared_ptr<AbstractSegment>& segment,
                                  parquet::ColumnWriter* column_writer);

  template <typename SegmentType, typename VectorBatchType>
  void GenericCopySegmentToParquetColumn(SegmentType* segment, VectorBatchType* column_writer);

  void FlushBuffer();

  // Helper to estimate the size of a chunk in bytes
  static size_t EstimateChunkSize(const std::shared_ptr<const Chunk>& chunk);

  std::vector<std::shared_ptr<const Chunk>> chunk_buffer_;
  size_t estimated_buffered_bytes_ = 0;

  Configuration config_;

  std::shared_ptr<detail::ParquetOutputProxy> output_proxy_;
  std::unique_ptr<parquet::ParquetFileWriter> writer_;
};
}  // namespace skyrise
