#include "sorted_merge_join_operator.hpp"

#include <unordered_map>
#include <utility>

#include "all_type_variant.hpp"
#include "storage/table/table_column_definition.hpp"
#include "storage/table/value_segment.hpp"

namespace {

const std::string kNameInner = "InnerSortedMergeJoin";
const std::string kNameLeftOuter = "LeftOuterSortedMergeJoin";
const std::string kNameRightOuter = "RightOuterSortedMergeJoin";
const std::string kNameFullOuter = "FullOuterSortedMergeJoin";

}  // namespace

namespace skyrise {

/**
 * RUNNING EXAMPLE: The operator is explained by using the following table schemas and instances as example
 *
 * Let R, S be tables with the following instance:
 *        ----- R -----               ----- S -----
 *        | A | B | C |               | C | D | E |
 *        ...CHUNK 0...               ...CHUNK 0...
 *        | a | 2 | 1 |               | 2 | x | p |
 *        | b | 2 | 2 |   JOIN on C   | 2 | y | q |
 *        ...CHUNK 1...               | 3 | z | f |
 *        | c | 3 | 2 |               ...CHUNK 1...
 *        | d | 0 | 4 |               | 1 | z | l |
 *        | d | 0 | 3 |               | 3 | b | c |
 *        -------------               -------------
 *
 * The table below is the result of a LEFT-OUTER-JOIN-Operation on S and R (with C as JOIN-Attribute):
 *
 * All matches are contained in one chunk. If JoinMode is LeftOuter, RightOuter or FullOuter up to two chunks are
 * appended. One chunk for all dangling tuples of the left input table and one chunk for the dangling tuples of the
 * right input table.
 *
 *  RowId = (ChunkIndex, ChunkOffset)
 *  _ = NULL
 *
 *     RowId    -- R LEFT OUTER JOIN S --    RowId
 *              | A | B |S.C|R.C| D | E |
 *              .........CHUNK 1.........
 *     (0,0)    | a | 2 | 1 | 1 | z | l |    (1,0)
 *  #  (0,1)    | b | 2 | 2 | 2 | x | p |    (0,0)
 *     (0,1)    | b | 2 | 2 | 2 | y | q |    (0,1)
 *     (1,0)    | c | 3 | 2 | 2 | x | p |    (0,0)
 *     (1,0)    | c | 3 | 2 | 2 | y | p |    (0,1)
 *     (1,2)    | d | 0 | 3 | 3 | z | f |    (0,2)
 *     (1,2)    | d | 0 | 3 | 3 | b | c |    (1,1)
 *              .........CHUNK 2.........
 *     (1,1)    | d | 0 | 4 | _ | _ | _ |
 *              -------------------------
 *
 */
SortedMergeJoinOperator::SortedMergeJoinOperator(std::shared_ptr<const AbstractOperator> left_input,
                                                 std::shared_ptr<const AbstractOperator> right_input,
                                                 std::shared_ptr<JoinOperatorPredicate> predicate,
                                                 const JoinMode join_mode)
    : AbstractJoinOperator(OperatorType::kHashJoin, left_input, right_input, predicate, join_mode) {}

const std::string& SortedMergeJoinOperator::Name() const {
  switch (join_mode_) {
    case JoinMode::kInner:
      return kNameInner;
    case JoinMode::kLeftOuter:
      return kNameLeftOuter;
    case JoinMode::kRightOuter:
      return kNameRightOuter;
    case JoinMode::kFullOuter:
      return kNameFullOuter;
    default:
      Fail("Unsupported join mode. Implemented join modes are Inner, LeftOuter, RightOuter and FullOuter.");
  }
}

void SortedMergeJoinOperator::FillPositionLists() {
  // NOLINTNEXTLINE(performance-unnecessary-value-param)
  AWS_LOGSTREAM_INFO(kWorkerTag.c_str(), "SortedMergeJoinOperator::FillPositionLists");
  ResolveDataType(LeftInputTable()->ColumnDataType(predicate_->column_id_left), [&](auto data_type) {
    using ColumnDataType = decltype(data_type);

    std::vector<std::vector<bool>> dangling_tuples_right;
    dangling_tuples_right_count_ = 0;
    dangling_tuples_right.reserve(RightInputTable()->ChunkCount());
    for (ChunkId right_chunk_id = 0; right_chunk_id < RightInputTable()->ChunkCount(); ++right_chunk_id) {
      dangling_tuples_right.emplace_back(RightInputTable()->GetChunk(right_chunk_id)->Size(), true);
      dangling_tuples_right_count_ += dangling_tuples_right.back().size();
    }

    for (ChunkId left_chunk_id = 0; left_chunk_id < LeftInputTable()->ChunkCount(); ++left_chunk_id) {
      const auto left_input_chunk = LeftInputTable()->GetChunk(left_chunk_id);
      const auto left_abstract_segment = left_input_chunk->GetSegment(predicate_->column_id_left);
      const auto left_typed_segment = std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(left_abstract_segment);
      const auto left_segment_values = left_typed_segment->Values();

      dangling_tuples_left_.emplace_back(left_input_chunk->Size(), true);

      for (ChunkId right_chunk_id = 0; right_chunk_id < RightInputTable()->ChunkCount(); ++right_chunk_id) {
        const auto right_input_chunk = RightInputTable()->GetChunk(right_chunk_id);
        const auto right_abstract_segment = right_input_chunk->GetSegment(predicate_->column_id_right);
        const auto right_typed_segment =
            std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(right_abstract_segment);
        const auto right_segment_values = right_typed_segment->Values();

        size_t left_segment_values_first_index_with_same_value = 0;
        size_t left_segment_values_index = 0;
        size_t right_segment_values_index = 0;

        while (right_segment_values_index < right_segment_values.size()) {
          // Note: all data types supported by ResolveDataType are directly comparable via <
          while (left_segment_values_index + 1 < left_segment_values.size() &&
                 left_segment_values.at(left_segment_values_index) < right_segment_values[right_segment_values_index]) {
            if (left_segment_values.at(left_segment_values_index) !=
                left_segment_values.at(left_segment_values_index + 1)) {
              left_segment_values_first_index_with_same_value = left_segment_values_index + 1;
            }
            left_segment_values_index++;
          }
          if (left_segment_values_index == left_segment_values.size() - 1 &&
              left_segment_values.at(left_segment_values_index) < right_segment_values[right_segment_values_index]) {
            break;  // no further matches
          }

          while (left_segment_values_index + 1 < left_segment_values.size() &&
                 left_segment_values.at(left_segment_values_index + 1) ==
                     left_segment_values.at(left_segment_values_index)) {
            left_segment_values_index++;
          }

          if (left_segment_values.at(left_segment_values_index) == right_segment_values[right_segment_values_index]) {
            for (size_t left_match_index = left_segment_values_first_index_with_same_value;
                 left_match_index <= left_segment_values_index; ++left_match_index) {
              position_lists_[right_chunk_id].emplace_back(RowId(left_chunk_id, left_match_index),
                                                           right_segment_values_index);
              if (dangling_tuples_left_[left_chunk_id][left_match_index]) {
                dangling_tuples_left_[left_chunk_id][left_match_index] = false;
                dangling_tuples_left_count_--;
              }
            }

            if (dangling_tuples_right[right_chunk_id][right_segment_values_index]) {
              dangling_tuples_right[right_chunk_id][right_segment_values_index] = false;
              dangling_tuples_right_count_--;
            }
          }

          right_segment_values_index++;
        }
      }
    }

    for (ChunkId right_chunk_id = 0; right_chunk_id < RightInputTable()->ChunkCount(); ++right_chunk_id) {
      dangling_tuples_right_offsets_.emplace_back();
      for (size_t right_chunk_offset = 0; right_chunk_offset < dangling_tuples_right.back().size();
           right_chunk_offset++) {
        if (dangling_tuples_right[right_chunk_id][right_chunk_offset]) {
          dangling_tuples_right_offsets_.back().emplace_back(right_chunk_offset);
        }
      }
    }
  });
}

}  // namespace skyrise
