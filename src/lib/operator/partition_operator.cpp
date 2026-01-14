#include "partition_operator.hpp"

#include <boost/container_hash/hash.hpp>

#include "all_type_variant.hpp"
#include "sort_operator.hpp"
#include "storage/table/chunk.hpp"
#include "storage/table/value_segment.hpp"
#include "utils/assert.hpp"

namespace {

const std::string kName = "Partition";

}  // namespace

namespace skyrise {

PartitionOperator::PartitionOperator(std::shared_ptr<AbstractOperator> input,
                                     std::shared_ptr<AbstractPartitioningFunction> partitioning_function,
                                     bool sort_within_partition)
    : AbstractOperator(OperatorType::kPartition, std::move(input), nullptr),
      partitioning_function_(std::move(partitioning_function)),
      sort_within_partition_(sort_within_partition) {}

const std::string& PartitionOperator::Name() const { return kName; }

std::shared_ptr<const Table> PartitionOperator::OnExecute(
    const std::shared_ptr<OperatorExecutionContext>& /*operator_execution_context*/) {
  Assert(LeftInput(), "Input operator must not be nullptr.");
  Assert(LeftInputTable(), "Input table must not be nullptr.");

  const auto input_table = LeftInputTable();
  const ChunkOffset chunk_count = input_table->ChunkCount();
  const ColumnCount column_count = input_table->GetColumnCount();

  const PartitionedPositionLists position_lists = partitioning_function_->Partition(input_table);

  // Materialize partitions in a new table with one chunk per partition
  std::vector<std::shared_ptr<Chunk>> output_chunks;
  output_chunks.reserve(position_lists.size());

  for (const auto& position_list : position_lists) {
    Segments segments(column_count);

    // Materialize column by column from original table
    for (ColumnCount column_id = 0; column_id < column_count; ++column_id) {
      // NOLINTNEXTLINE(performance-unnecessary-value-param)
      ResolveDataType(input_table->ColumnDataType(column_id), [&](auto data_type) {
        using ColumnDataType = decltype(data_type);

        std::vector<std::vector<ColumnDataType>*> input_segments;
        input_segments.reserve(chunk_count);

        for (size_t i = 0; i < chunk_count; ++i) {
          const auto current_chunk = input_table->GetChunk(i);
          const auto abstract_segment = current_chunk->GetSegment(column_id);
          const auto typed_segment = std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(abstract_segment);
          input_segments.emplace_back(&typed_segment->Values());
        }

        std::vector<ColumnDataType> output_segment_values;
        output_segment_values.reserve(position_list.size());

        for (const auto& [chunk_index, relative_row_index] : position_list) {
          const auto& current_segment_values = *input_segments[chunk_index];
          output_segment_values.emplace_back(current_segment_values[relative_row_index]);
        }

        segments[column_id] = std::make_shared<ValueSegment<ColumnDataType>>(std::move(output_segment_values));
      });
    }

    auto partition_chunk = std::make_shared<Chunk>(segments);

    if (sort_within_partition_) {
      std::shared_ptr<Table> partition_table = std::make_shared<Table>(
          input_table->ColumnDefinitions(), std::vector<std::shared_ptr<Chunk>>{partition_chunk});

      // sort the chunk by each partitioning column
      RowIdPositionList previously_sorted_position_list;
      for (const auto& partition_column_id : partitioning_function_->PartitionColumnIds()) {
        Assert(partition_column_id < partition_table->GetColumnCount(), "Column to partition is out of range.");
        Assert(!partition_table->ColumnDefinitions()[partition_column_id].nullable,
               "Nullable columns are not supported.");
        // NOLINTNEXTLINE(performance-unnecessary-value-param)
        ResolveDataType(partition_table->ColumnDataType(partition_column_id), [&](auto data_type) {
          using ColumnDataType = decltype(data_type);

          AWS_LOGSTREAM_INFO(kWorkerTag.c_str(),
                             "PartitionOperator - sort partition by partition column " << partition_column_id);
          SortOperator::SortImplementation<ColumnDataType> sort_by_column(partition_table, partition_column_id);
          previously_sorted_position_list = sort_by_column.Sort(previously_sorted_position_list);
        });
      }

      auto fragment_scheduler = std::make_shared<FragmentScheduler>();
      auto sorted_partition_table = SortOperator::MaterializeOutputTable(
          partition_table, previously_sorted_position_list, fragment_scheduler, true);
      Assert(sorted_partition_table->ChunkCount() == 1,
             "The sorted partition table must still contain just one chunk.");
      auto sorted_partition_chunk = sorted_partition_table->GetChunk(0);
      output_chunks.emplace_back(sorted_partition_chunk);
    } else {
      AWS_LOGSTREAM_INFO(kWorkerTag.c_str(), "PartitionOperator - no sort");
      output_chunks.emplace_back(partition_chunk);
    }
  }

  return std::make_shared<Table>(LeftInputTable()->ColumnDefinitions(), std::move(output_chunks));
}

}  // namespace skyrise
