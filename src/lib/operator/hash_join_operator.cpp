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
     * table_A with the RowIds of the Rows, where this value is present.
     * Hint: RowId = (ChunkIndex, ChunkOffset)
     *
     * Table_A is selected as the smaller table based on row count out of the left and right table.
     *
     * Resulting Build Table for the aforementioned example (see top of method):
     *    1 => [(0,0)]
     *    2 => [(0,1); (1,0)]
     *    3 => [(1,2)]
     *    4 => [(1,1)]
     */
    std::unordered_multimap<ColumnDataType, RowId> build_table;
    table_A_matches_left = true;

    AWS_LOGSTREAM_INFO("HashJoinOperator", "Left table: " << LeftInputTable()->RowCount()
                                                          << " ; Right Table: " << RightInputTable()->RowCount());

    if (LeftInputTable()->RowCount() > RightInputTable()->RowCount()) {
      // if the left table turns out to be larger than the right one, then use the right table as the build side and the
      // left table as the probe side, to save memory

      table_A_matches_left = false;
      AWS_LOGSTREAM_INFO("HashJoinOperator", "Switch the left and right side of the tables");
    } else {
      table_A_matches_left = true;
      AWS_LOGSTREAM_INFO("HashJoinOperator", "Don't switch the left and right side of the tables");
    }

    auto build_side_join_column_id = table_A_matches_left ? predicate_->column_id_left : predicate_->column_id_right;
    auto probe_side_join_column_id = table_A_matches_left ? predicate_->column_id_right : predicate_->column_id_left;
    auto build_side_table = table_A = table_A_matches_left ? LeftInputTable() : RightInputTable();
    auto probe_side_table = table_B = table_A_matches_left ? RightInputTable() : LeftInputTable();

    dangling_tuples_table_A_count_ = build_side_table->RowCount();
    dangling_tuples_table_A_.reserve(build_side_table->ChunkCount());
    dangling_tuples_table_B_count_ = 0;
    dangling_tuples_table_B_offsets_.reserve(RightInputTable()->ChunkCount());

    for (ChunkId i = 0; i < build_side_table->ChunkCount(); ++i) {
      const auto input_chunk = build_side_table->GetChunk(i);
      const auto abstract_segment = input_chunk->GetSegment(build_side_join_column_id);
      const auto typed_segment = std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(abstract_segment);
      const auto segment_values = typed_segment->Values();

      dangling_tuples_table_A_.emplace_back(input_chunk->Size(), true);

      for (ChunkOffset j = 0; j < segment_values.size(); ++j) {
        build_table.emplace(segment_values[j], RowId{.chunk_id = i, .chunk_offset = j});
      }
    }

    /*
     * In the probe phase, the join matches are identified and thus the position lists are created.
     * This is achieved by iterating over the rows of the table_B. For each row r, the following
     * algorithm looks up in the build table which RowIds of the table_A are associated with the
     * value of the join column in r.
     *
     * In addition, all rows of the table A are marked as true if they have at least one join
     * match.
     */
    for (ChunkId i = 0; i < probe_side_table->ChunkCount(); ++i) {
      const auto input_chunk = RightInputTable()->GetChunk(i);
      const auto abstract_segment = input_chunk->GetSegment(probe_side_join_column_id);
      const auto typed_segment = std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(abstract_segment);
      const auto segment_values = typed_segment->Values();

      std::vector<ChunkOffset> dangling_offsets_in_segment;

      for (ChunkOffset j = 0; j < segment_values.size(); ++j) {
        auto matches = build_table.equal_range(segment_values[j]);

        if (matches.first == matches.second) {
          // No matches for tuple with RowId (i,j) of the right table were found.
          dangling_offsets_in_segment.emplace_back(j);
          dangling_tuples_table_B_count_++;
          continue;
        }

        for (auto it = matches.first; it != matches.second; ++it) {
          if (dangling_tuples_table_A_[it->second.chunk_id][it->second.chunk_offset]) {
            dangling_tuples_table_A_count_--;
            // Mark corresponding tuple of table_A as matched.
            dangling_tuples_table_A_[it->second.chunk_id][it->second.chunk_offset] = false;
          }
          position_lists_[i].emplace_back(RowId{it->second.chunk_id, it->second.chunk_offset}, j);
        }
      }

      // Add ChunkOffsets to ChunkIndex=i to indicate which tuples had no match.
      dangling_tuples_table_B_offsets_.emplace_back(dangling_offsets_in_segment);
    }
  });
}

}  // namespace skyrise
