/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"

namespace skyrise {

/**
 * Sorts a table by one or multiple columns.
 */
class SortOperator : public AbstractOperator {
 public:
  SortOperator(std::shared_ptr<const AbstractOperator> input_operator,
               const std::vector<SortColumnDefinition>& sort_definitions);

  const std::vector<SortColumnDefinition>& SortDefinitions() const;

  const std::string& Name() const override;

  std::shared_ptr<const Table> OnExecute(
      const std::shared_ptr<OperatorExecutionContext>& operator_execution_context = nullptr) override;

  /**
   * Given an unsorted_table and a position_list defining the output order, this materializes all columns in the table.
   */
  static std::shared_ptr<Table> MaterializeOutputTable(const std::shared_ptr<const Table>& unsorted_table,
                                                       const RowIdPositionList& position_list,
                                                       const std::shared_ptr<FragmentScheduler>& fragment_scheduler,
                                                       bool single_chunk = false);

  template <typename SortColumnDataType>
  class SortImplementation {
   public:
    using RowIdValuePair = std::pair<RowId, SortColumnDataType>;

    SortImplementation(std::shared_ptr<const Table> input_table, const ColumnId column_id,
                       const SortMode sort_mode = SortMode::kAscending)
        : input_table_(std::move(input_table)), column_id_(column_id), sort_mode_(sort_mode) {
      const size_t row_count = input_table_->RowCount();
      row_id_value_pairs_.reserve(row_count);
      null_value_rows_.reserve(row_count);
    }

    // Sorts input_table_, potentially taking the pre-existing order of previously_sorted_position_list into account.
    // Returns a RowIdPositionList, which can be used for either an input to the next call of sort or for materializing
    // the output table.
    RowIdPositionList Sort(const RowIdPositionList& previously_sorted_position_list) {
      // (1) Prepare Sort: Creating RowId-Value-Structure.
      MaterializeSortColumn(previously_sorted_position_list);

      // (2) After we got our ValueRowId Map we sort the map by the value of the pair.
      const auto sort_with_comparator = [&](auto comparator) {
        std::stable_sort(row_id_value_pairs_.begin(), row_id_value_pairs_.end(),
                         // NOLINTNEXTLINE(performance-unnecessary-value-param)
                         [&comparator](RowIdValuePair a, RowIdValuePair b) { return comparator(a.second, b.second); });
      };
      if (sort_mode_ == SortMode::kAscending) {
        sort_with_comparator(std::less<>());
      } else {
        sort_with_comparator(std::greater<>());
      }

      // (3) Insert null rows in front of all non-NULL rows.
      if (!null_value_rows_.empty()) {
        row_id_value_pairs_.insert(row_id_value_pairs_.begin(), null_value_rows_.begin(), null_value_rows_.end());
      }

      RowIdPositionList position_list;
      position_list.reserve(row_id_value_pairs_.size());
      for (const auto& [row_id, _] : row_id_value_pairs_) {
        position_list.push_back(row_id);
      }

      return position_list;
    }

   protected:
    /**
     * Completely materializes the sort column to create a vector of RowId-Value pairs.
     */
    void MaterializeSortColumn(const RowIdPositionList& previously_sorted_position_list) {
      // If there was no RowIdPositionList passed, this is the first sorting run, and we simply fill our values and
      // nulls data structures from our input table. Otherwise, we will materialize according to the PosList which is
      // the result of the last run.
      if (!previously_sorted_position_list.empty()) {
        MaterializeColumnFromPositionList(previously_sorted_position_list);
      } else {
        for (ChunkId chunk_id = 0; chunk_id < input_table_->ChunkCount(); ++chunk_id) {
          const std::shared_ptr<AbstractSegment> abstract_segment =
              input_table_->GetChunk(chunk_id)->GetSegment(column_id_);

          for (ChunkOffset chunk_offset = 0; chunk_offset < abstract_segment->Size(); ++chunk_offset) {
            const AllTypeVariant value = (*abstract_segment)[chunk_offset];

            if (VariantIsNull(value)) {
              null_value_rows_.emplace_back(RowId{.chunk_id = chunk_id, .chunk_offset = chunk_offset},
                                            SortColumnDataType());
            } else {
              row_id_value_pairs_.emplace_back(RowId{.chunk_id = chunk_id, .chunk_offset = chunk_offset},
                                               std::get<SortColumnDataType>(value));
            }
          }
        }
      }
    }

    void MaterializeColumnFromPositionList(const RowIdPositionList& position_list) {
      for (const auto& row_id : position_list) {
        const auto [chunk_id, chunk_offset] = row_id;

        const std::shared_ptr<AbstractSegment> segment = input_table_->GetChunk(chunk_id)->GetSegment(column_id_);
        const AllTypeVariant value = (*segment)[chunk_offset];

        if (VariantIsNull(value)) {
          null_value_rows_.emplace_back(row_id, SortColumnDataType());
        } else {
          row_id_value_pairs_.emplace_back(row_id, std::get<SortColumnDataType>(value));
        }
      }
    }

    const std::shared_ptr<const Table> input_table_;

    // Column to sort by.
    const ColumnId column_id_;
    const SortMode sort_mode_;

    std::vector<RowIdValuePair> row_id_value_pairs_;

    // Stored as RowIdValuePair for better type compatibility with row_id_value_pairs_ even if the null value is unused.
    std::vector<RowIdValuePair> null_value_rows_;
  };

 protected:
  const std::vector<SortColumnDefinition> sort_definitions_;
};

}  // namespace skyrise
