#include "hash_join_operator.hpp"

#include <unordered_map>
#include <utility>

#include "all_type_variant.hpp"
#include "storage/table/table_column_definition.hpp"
#include "storage/table/value_segment.hpp"

namespace {

const std::string kNameInner = "InnerHashJoin";
const std::string kNameLeftOuter = "LeftOuterHashJoin";
const std::string kNameRightOuter = "RightOuterHashJoin";
const std::string kNameFullOuter = "FullOuterHashJoin";

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
HashJoinOperator::HashJoinOperator(std::shared_ptr<const AbstractOperator> left_input,
                                   std::shared_ptr<const AbstractOperator> right_input,
                                   std::shared_ptr<JoinOperatorPredicate> predicate, const JoinMode join_mode)
    : AbstractJoinOperator(OperatorType::kHashJoin, left_input, right_input, predicate, join_mode) {}

const std::string& HashJoinOperator::Name() const {
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

void HashJoinOperator::FillPositionLists() {
  // NOLINTNEXTLINE(performance-unnecessary-value-param)
  ResolveDataType(LeftInputTable()->ColumnDataType(predicate_->column_id_left), [&](auto data_type) {
    using ColumnDataType = decltype(data_type);

    /*
     * The Build-Table is central for determining join-matches for a given join-column-value.
     * Currently, the HashJoin only supports simple predicates with equality in one column per table.
     *
     * This data-structure associates every value of the Column that forms the Join-Predicate for the
     * left table with the RowIds of the Rows, where this value is present.
     * Hint: RowId = (ChunkIndex, ChunkOffset)
     *
     * Resulting Build Table for the aforementioned example (see top of method):
     *    1 => [(0,0)]
     *    2 => [(0,1); (1,0)]
     *    3 => [(1,2)]
     *    4 => [(1,1)]
     */
    std::unordered_multimap<ColumnDataType, RowId> build_table;

    for (ChunkId i = 0; i < LeftInputTable()->ChunkCount(); ++i) {
      const auto input_chunk = LeftInputTable()->GetChunk(i);
      const auto abstract_segment = input_chunk->GetSegment(predicate_->column_id_left);
      const auto typed_segment = std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(abstract_segment);
      const auto segment_values = typed_segment->Values();

      dangling_tuples_left_.emplace_back(input_chunk->Size(), true);

      for (ChunkOffset j = 0; j < segment_values.size(); ++j) {
        build_table.emplace(segment_values[j], RowId{.chunk_id = i, .chunk_offset = j});
      }
    }

    /*
     * In the probe phase, the join matches are identified and thus the position lists are created.
     * This is achieved by iterating over the rows of the right table. For each row r, the following
     * algorithm looks up in the build table which RowIds of the left table are associated with the
     * value of the join column in r.
     *
     * In addition, all rows of the left table are marked as true if they have at least one join
     * match.
     */
    for (ChunkId i = 0; i < RightInputTable()->ChunkCount(); ++i) {
      const auto input_chunk = RightInputTable()->GetChunk(i);
      const auto abstract_segment = input_chunk->GetSegment(predicate_->column_id_right);
      const auto typed_segment = std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(abstract_segment);
      const auto segment_values = typed_segment->Values();

      std::vector<ChunkOffset> dangling_offsets_in_segment;

      for (ChunkOffset j = 0; j < segment_values.size(); ++j) {
        auto matches = build_table.equal_range(segment_values[j]);

        if (matches.first == matches.second) {
          // No matches for tuple with RowId (i,j) of the right table were found.
          dangling_offsets_in_segment.emplace_back(j);
          dangling_tuples_right_count_++;
          continue;
        }

        for (auto it = matches.first; it != matches.second; ++it) {
          if (dangling_tuples_left_[it->second.chunk_id][it->second.chunk_offset]) {
            dangling_tuples_left_count_--;
            // Mark corresponding tuple of left table as matched.
            dangling_tuples_left_[it->second.chunk_id][it->second.chunk_offset] = false;
          }
          position_lists_[i].emplace_back(RowId{it->second.chunk_id, it->second.chunk_offset}, j);
        }
      }

      // Add ChunkOffsets to ChunkIndex=i to indicate which tuples had no match.
      dangling_tuples_right_offsets_.emplace_back(dangling_offsets_in_segment);
    }
  });
}

}  // namespace skyrise
