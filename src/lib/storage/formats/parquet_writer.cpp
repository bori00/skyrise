#include "parquet_writer.hpp"

#include <algorithm>
#include <iterator>

namespace skyrise {

ParquetFormatWriter::ParquetFormatWriter(Configuration config) : config_(std::move(config)) {
  output_proxy_ =
      detail::ParquetOutputProxy::Make([this](const char* data, size_t length) { WriteToOutput(data, length); });
}

void ParquetFormatWriter::Initialize(const TableColumnDefinitions& skyrise_schema) {
  parquet::WriterProperties::Builder builder;
  builder.compression(config_.compression);
  builder.compression_level(config_.compression_level);
  parquet::schema::NodeVector fields;

  for (const auto& column : skyrise_schema) {
    fields.push_back(SkyriseTypeToParquetType(column.name, column.data_type, column.nullable));
  }

  const auto parquet_schema = std::static_pointer_cast<parquet::schema::GroupNode>(
      parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));

  writer_ = parquet::ParquetFileWriter::Open(output_proxy_, parquet_schema, builder.build());
};

void ParquetFormatWriter::ProcessChunk(std::shared_ptr<const Chunk> chunk) {
  if (config_.consolidate_row_groups) {
    // Add chunk to the buffer. When buffer size surpasses thresholds, write all buffered chunks into one row group.
    const size_t chunk_bytes = EstimateChunkSize(chunk);
    chunk_buffer_.push_back(std::move(chunk));
    estimated_buffered_bytes_ += chunk_bytes;

    if (estimated_buffered_bytes_ >= config_.kRowGroupSizeThresholdBytes) {
      FlushBuffer();
    }
  } else {
    .  // Process chunk immediately, write it into a row group.
        parquet::RowGroupWriter* row_group_writer = writer_->AppendRowGroup();
    for (size_t i = 0; i < chunk->GetColumnCount(); ++i) {
      const auto segment = chunk->GetSegment(i);
      parquet::ColumnWriter* column_writer = row_group_writer->NextColumn();
      CopySegmentToParquetColumn(segment, column_writer);
    }
  }
};

void ParquetFormatWriter::Finalize() {
  if (!chunk_buffer_.empty()) {
    FlushBuffer();
  }
  writer_->Close();
}

template <>
void ParquetFormatWriter::GenericCopySegmentToParquetColumn(ValueSegment<std::string>* segment,
                                                            parquet::ByteArrayWriter* column_writer) {
  const auto& segment_values = segment->Values();
  const bool is_nullable = segment->IsNullable();
  for (size_t i = 0; i < segment_values.size(); ++i) {
    const int16_t definition_level = !(is_nullable && segment->NullValues()[i]);
    const parquet::ByteArray byte_value(segment_values[i]);
    column_writer->WriteBatch(1, &definition_level, nullptr, &byte_value);
  }
}

template <typename SegmentType, typename VectorBatchType>
void ParquetFormatWriter::GenericCopySegmentToParquetColumn(SegmentType* segment, VectorBatchType* column_writer) {
  if (segment->IsNullable()) {
    std::vector<int16_t> definition_levels{};
    definition_levels.reserve(segment->NullValues().size());
    std::transform(segment->NullValues().cbegin(), segment->NullValues().cend(), std::back_inserter(definition_levels),
                   [](const bool is_null) { return !is_null; });
    column_writer->WriteBatch(segment->Size(), definition_levels.data(), nullptr, segment->Values().data());
  } else {
    column_writer->WriteBatch(segment->Size(), nullptr, nullptr, segment->Values().data());
  }
}

void ParquetFormatWriter::CopySegmentToParquetColumn(const std::shared_ptr<AbstractSegment>& segment,
                                                     parquet::ColumnWriter* column_writer) {
  switch (segment->GetDataType()) {
    case DataType::kLong:
      GenericCopySegmentToParquetColumn(dynamic_cast<ValueSegment<int64_t>*>(segment.get()),
                                        dynamic_cast<parquet::Int64Writer*>(column_writer));
      return;
    case DataType::kInt:
      GenericCopySegmentToParquetColumn(dynamic_cast<ValueSegment<int32_t>*>(segment.get()),
                                        dynamic_cast<parquet::Int32Writer*>(column_writer));
      return;
    case DataType::kFloat:
      GenericCopySegmentToParquetColumn(dynamic_cast<ValueSegment<float>*>(segment.get()),
                                        dynamic_cast<parquet::FloatWriter*>(column_writer));
      return;
    case DataType::kDouble:
      GenericCopySegmentToParquetColumn(dynamic_cast<ValueSegment<double>*>(segment.get()),
                                        dynamic_cast<parquet::DoubleWriter*>(column_writer));
      return;
    case DataType::kString:
      GenericCopySegmentToParquetColumn(dynamic_cast<ValueSegment<std::string>*>(segment.get()),
                                        dynamic_cast<parquet::ByteArrayWriter*>(column_writer));
      return;
    default:
      Fail("Invalid type found.");
  }
}

std::shared_ptr<parquet::schema::Node> ParquetFormatWriter::SkyriseTypeToParquetType(const std::string& name,
                                                                                     const DataType type,
                                                                                     const bool nullable) {
  const auto repetition = nullable ? parquet::Repetition::OPTIONAL : parquet::Repetition::REQUIRED;
  switch (type) {
    case DataType::kFloat:
      return parquet::schema::PrimitiveNode::Make(name, repetition, parquet::Type::FLOAT, parquet::ConvertedType::NONE);
    case DataType::kDouble:
      return parquet::schema::PrimitiveNode::Make(name, repetition, parquet::Type::DOUBLE,
                                                  parquet::ConvertedType::NONE);
    case DataType::kInt:
      return parquet::schema::PrimitiveNode::Make(name, repetition, parquet::Type::INT32, parquet::ConvertedType::NONE);
    case DataType::kLong:
      return parquet::schema::PrimitiveNode::Make(name, repetition, parquet::Type::INT64, parquet::ConvertedType::NONE);
    case DataType::kString:
      return parquet::schema::PrimitiveNode::Make(name, repetition, parquet::Type::BYTE_ARRAY,
                                                  parquet::ConvertedType::UTF8);
    case DataType::kNull:
      Fail("NULL is not a supported column type.");
    default:
      Fail("Unknown column type encountered.");
  }
}

void ParquetFormatWriter::FlushBuffer() {
  if (chunk_buffer_.empty()) return;

  // Append exactly ONE row group for all buffered chunks
  parquet::RowGroupWriter* row_group_writer = writer_->AppendRowGroup();
  const size_t column_count = chunk_buffer_[0]->GetColumnCount();

  for (size_t col_idx = 0; col_idx < column_count; ++col_idx) {
    parquet::ColumnWriter* column_writer = row_group_writer->NextColumn();

    // Write all segments for this column sequentially into the same ColumnWriter
    for (const auto& chunk : chunk_buffer_) {
      const auto segment = chunk->GetSegment(col_idx);
      CopySegmentToParquetColumn(segment, column_writer);
    }
  }

  chunk_buffer_.clear();
  estimated_buffered_bytes_ = 0;
}

size_t ParquetFormatWriter::EstimateChunkSize(const std::shared_ptr<const Chunk>& chunk) {
  size_t total_bytes = 0;

  for (size_t col_idx = 0; col_idx < chunk->GetColumnCount(); ++col_idx) {
    const auto segment = chunk->GetSegment(col_idx);
    total_bytes += segment->MemoryUsage();
  }
  return total_bytes;
}

}  // namespace skyrise
