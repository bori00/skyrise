#include "limit_operator.hpp"

#include "expression/evaluation/expression_evaluator.hpp"
#include "storage/table/value_segment.hpp"

using namespace std::string_literals;  // NOLINT

namespace {

const std::string kName = "Limit";

}  // namespace

namespace skyrise {

LimitOperator::LimitOperator(std::shared_ptr<const AbstractOperator> input_operator,
                             std::shared_ptr<AbstractExpression> row_count)
    : AbstractOperator(OperatorType::kLimit, std::move(input_operator)), row_count_(row_count) {}

const std::shared_ptr<AbstractExpression>& LimitOperator::RowCount() const { return row_count_; }

const std::string& LimitOperator::Name() const { return kName; }

std::shared_ptr<const Table> LimitOperator::OnExecute(const std::shared_ptr<OperatorExecutionContext>& /*context*/) {
  const std::shared_ptr<const Table>& input_table = LeftInputTable();

  auto row_count_evaluator = std::make_shared<ExpressionEvaluator>();
  // TODO: use int64_t
  auto row_count_result = row_count_evaluator->EvaluateExpressionToResult<int32_t>(*row_count_);

  Assert(!row_count_result->IsNull(0), "The row count cannot be null");

  unsigned int row_count_int = 1;  // row_count_result.get()->GetValues()[0];

  // AWS_LOGSTREAM_INFO(kWorkerTag.c_str(), "Limiting number of rows to " << row_count_int);

  std::vector<std::shared_ptr<Chunk>> output_chunks;
  output_chunks.reserve(std::min(input_table->ChunkCount(), row_count_int));

  for (ChunkId chunk_id = 0; chunk_id < input_table->ChunkCount(); ++chunk_id) {
    const auto input_chunk = input_table->GetChunk(chunk_id);
    const unsigned int chunk_size = input_chunk->Size();

    if (row_count_int >= chunk_size) {
      // CASE 1: Keep the entire chunk
      // We can share the pointer because we aren't modifying it.
      // const_pointer_cast is needed because the output table expects mutable chunks
      output_chunks.push_back(std::const_pointer_cast<Chunk>(input_chunk));
      row_count_int -= chunk_size;
      // AWS_LOGSTREAM_INFO(kWorkerTag.c_str(), "Chunk with rows " << chunk_size);
    } else {
      // CASE 2: Slice the final chunk (keep first 'rows_to_keep' rows)
      Segments output_segments;
      // AWS_LOGSTREAM_INFO(kWorkerTag.c_str(), "Final chunk " << chunk_size);

      for (ColumnId column_id{0}; column_id < input_table->GetColumnCount(); ++column_id) {
        // Resolve the column type dynamically
        ResolveDataType(input_table->ColumnDataType(column_id), [&](const auto resolved_data_type) {
          using DataType = std::decay_t<decltype(resolved_data_type)>;

          std::vector<DataType> segment_values;
          segment_values.reserve(row_count_int);

          const auto input_segment = input_chunk->GetSegment(column_id);

          for (ChunkOffset i = 0; i < row_count_int; ++i) {
            // Access the value from the segment (returns AllTypeVariant)
            // We use std::get<DataType> to extract the typed value.
            // Note: This assumes the segment is not NULL at this position.
            auto value = (*input_segment)[i];
            segment_values.push_back(std::get<DataType>(value));
          }

          // Create a new ValueSegment with the copied data
          output_segments.push_back(std::make_shared<ValueSegment<DataType>>(std::move(segment_values)));
        });
      }

      output_chunks.push_back(std::make_shared<Chunk>(output_segments));
      row_count_int = 0;
      break;
    }
  }

  return std::make_shared<Table>(input_table->ColumnDefinitions(), std::move(output_chunks));
}

}  // namespace skyrise
