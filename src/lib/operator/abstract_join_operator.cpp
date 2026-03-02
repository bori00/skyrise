#include "abstract_join_operator.hpp"

#include <unordered_map>
#include <utility>

#include "all_type_variant.hpp"
#include "storage/table/table_column_definition.hpp"
#include "storage/table/value_segment.hpp"

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
AbstractJoinOperator::AbstractJoinOperator(OperatorType operator_type,
                                           std::shared_ptr<const AbstractOperator> left_input,
                                           std::shared_ptr<const AbstractOperator> right_input,
                                           std::shared_ptr<JoinOperatorPredicate> predicate, const JoinMode join_mode)
    : AbstractOperator(operator_type, std::move(left_input), std::move(right_input)),
      predicate_(std::move(predicate)),
      join_mode_(join_mode) {
  Assert(predicate_->predicate_condition == PredicateCondition::kEquals, "JoinOperator only supports Equi-Joins.");
  Assert(join_mode_ == JoinMode::kInner || join_mode_ == JoinMode::kLeftOuter || join_mode_ == JoinMode::kRightOuter ||
             join_mode_ == JoinMode::kFullOuter,
         "JoinOperator only supports Inner, LeftOuter, RightOuter, and FullOuter Joins.");
}

std::shared_ptr<const Table> AbstractJoinOperator::OnExecute(
    const std::shared_ptr<OperatorExecutionContext>& /*operator_execution_context*/) {
  Assert(!LeftInputTable()->ColumnIsNullable(predicate_->column_id_left) &&
             !RightInputTable()->ColumnIsNullable(predicate_->column_id_right),
         "JoinOperator does not support nullable columns.");
  Assert(LeftInputTable()->ColumnDataType(predicate_->column_id_left) ==
             RightInputTable()->ColumnDataType(predicate_->column_id_right),
         "Left and right join column must have the same type.");

  // Data structures for result table.
  std::vector<std::shared_ptr<Chunk>> output_chunks;
  output_chunks.reserve(DetermineOutputChunkCount());

  TableColumnDefinitions definitions = Concatenated(BuildLeftSchema(), BuildRightSchema());

  // Build and probe.
  FillPositionLists();

  const size_t result_column_count = ComputeResultColumnCount();
  const size_t result_row_count = ComputeResultRowCount();

  MaterializeMatches(result_column_count, result_row_count, output_chunks);

  // Materialize all dangling tuples of left table for left/full outer joins.
  if (join_mode_ == JoinMode::kLeftOuter || join_mode_ == JoinMode::kFullOuter) {
    MaterializeDanglingTuplesOfLeftTable(result_column_count, output_chunks);
  }

  // Materialize all dangling tuples of right table for right/full outer joins.
  if (join_mode_ == JoinMode::kRightOuter || join_mode_ == JoinMode::kFullOuter) {
    MaterializeDanglingTuplesOfRightTable(result_column_count, output_chunks);
  }

  return std::make_shared<Table>(definitions, std::move(output_chunks));
}

void AbstractJoinOperator::MaterializeMatches(const size_t result_column_count, const size_t result_row_count,
                                              std::vector<std::shared_ptr<Chunk>>& output_chunks) {
  Assert((table_A_matches_left && dangling_tuples_table_B_offsets_.size() == RightInputTable()->ChunkCount()) ||
             (!table_A_matches_left && dangling_tuples_table_B_offsets_.size() == LeftInputTable()->ChunkCount()),
         "PositionLists must be filled before materialization.");

  Segments output_segments;
  output_segments.reserve(result_column_count);

  MaterializeLeftSideOfMatchedTuples(result_row_count, output_segments);
  MaterializeRightSideOfMatchedTuples(result_row_count, output_segments);

  output_chunks.emplace_back(std::make_shared<Chunk>(std::move(output_segments)));
}

void AbstractJoinOperator::MaterializeLeftSideOfMatchedTuples(const size_t result_row_count,
                                                              Segments& output_segments) {
  if (table_A_matches_left) {
    MaterializeTableASideOfMatchedTuples(result_row_count, output_segments);
  } else {
    MaterializeTableBSideOfMatchedTuples(result_row_count, output_segments);
  }
}

void AbstractJoinOperator::MaterializeRightSideOfMatchedTuples(const size_t result_row_count,
                                                               Segments& output_segments) {
  if (table_A_matches_left) {
    MaterializeTableBSideOfMatchedTuples(result_row_count, output_segments);
  } else {
    MaterializeTableASideOfMatchedTuples(result_row_count, output_segments);
  }
}

void AbstractJoinOperator::MaterializeTableASideOfMatchedTuples(const size_t result_row_count,
                                                                Segments& output_segments) {
  for (ColumnCount i = 0; i < table_A->GetColumnCount(); ++i) {
    // NOLINTNEXTLINE(performance-unnecessary-value-param)
    ResolveDataType(table_A->ColumnDataType(i), [&](auto data_type) {
      using ColumnDataType = decltype(data_type);

      std::vector<std::vector<ColumnDataType>*> input_segments;
      input_segments.reserve(table_A->ChunkCount());

      for (ChunkId j = 0; j < table_A->ChunkCount(); ++j) {
        const auto abstract_segment = table_A->GetChunk(j)->GetSegment(i);
        const auto typed_segment = std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(abstract_segment);
        input_segments.push_back(&typed_segment->Values());
      }

      std::vector<ColumnDataType> output_segment_values;
      output_segment_values.reserve(result_row_count);

      for (const auto& position_list : position_lists_) {
        for (const auto& position : position_list) {
          const auto& input_segment = *input_segments[position.first.chunk_id];
          output_segment_values.push_back(input_segment[position.first.chunk_offset]);
        }
      }

      output_segments.push_back(std::make_shared<ValueSegment<ColumnDataType>>(std::move(output_segment_values)));
    });
  }
}

void AbstractJoinOperator::MaterializeTableBSideOfMatchedTuples(const size_t result_row_count,
                                                                Segments& output_segments) {
  for (ColumnCount i = 0; i < table_B->GetColumnCount(); ++i) {
    // NOLINTNEXTLINE(performance-unnecessary-value-param)
    ResolveDataType(table_B->ColumnDataType(i), [&](auto data_type) {
      using ColumnDataType = decltype(data_type);

      std::vector<ColumnDataType> output_segment_values;
      output_segment_values.reserve(result_row_count);

      for (ChunkId j = 0; j < table_B->ChunkCount(); ++j) {
        const auto abstract_segment = table_B->GetChunk(j)->GetSegment(i);
        const auto typed_segment = std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(abstract_segment);
        const auto segment_values = typed_segment->Values();

        for (const auto& position : position_lists_[j]) {
          output_segment_values.push_back(segment_values[position.second]);
        }
      }

      output_segments.push_back(std::make_shared<ValueSegment<ColumnDataType>>(std::move(output_segment_values)));
    });
  }
}

void AbstractJoinOperator::MaterializeDanglingTuplesOfLeftTable(const size_t result_column_count,
                                                                std::vector<std::shared_ptr<Chunk>>& output_chunks) {
  if (table_A_matches_left) {
    MaterializeDanglingTuplesOfTableA(result_column_count, output_chunks);
  } else {
    MaterializeDanglingTuplesOfTableB(result_column_count, output_chunks);
  }
}

void AbstractJoinOperator::MaterializeDanglingTuplesOfRightTable(const size_t result_column_count,
                                                                 std::vector<std::shared_ptr<Chunk>>& output_chunks) {
  if (table_A_matches_left) {
    MaterializeDanglingTuplesOfTableB(result_column_count, output_chunks);
  } else {
    MaterializeDanglingTuplesOfTableA(result_column_count, output_chunks);
  }
}

void AbstractJoinOperator::MaterializeDanglingTuplesOfTableA(const size_t result_column_count,
                                                             std::vector<std::shared_ptr<Chunk>>& output_chunks) {
  Segments segments;
  segments.reserve(result_column_count);

  if (!table_A_matches_left) {
    AppendNullFilledSegmentsForRowsOfTable(segments, table_B, dangling_tuples_table_A_count_);
  }

  for (ColumnCount i = 0; i < LeftInputTable()->GetColumnCount(); ++i) {
    // NOLINTNEXTLINE(performance-unnecessary-value-param)
    ResolveDataType(LeftInputTable()->ColumnDataType(i), [&](auto data_type) {
      using ColumnDataType = decltype(data_type);

      std::vector<ColumnDataType> dangling_segment_values;
      dangling_segment_values.reserve(dangling_tuples_table_A_count_);

      for (ChunkId j = 0; j < LeftInputTable()->ChunkCount(); ++j) {
        const auto& abstract_segment = LeftInputTable()->GetChunk(j)->GetSegment(i);
        const auto& typed_segment = std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(abstract_segment);
        const auto& segment_values = typed_segment->Values();

        for (ChunkOffset k = 0; k < LeftInputTable()->GetChunk(j)->Size(); ++k) {
          if (dangling_tuples_table_A_[j][k]) {
            dangling_segment_values.push_back(segment_values[k]);
          }
        }
      }

      segments.push_back(std::make_shared<ValueSegment<ColumnDataType>>(std::move(dangling_segment_values)));
    });
  }

  if (table_A_matches_left) {
    AppendNullFilledSegmentsForRowsOfTable(segments, table_B, dangling_tuples_table_A_count_);
  }

  output_chunks.emplace_back(std::make_shared<Chunk>(std::move(segments)));
}

void AbstractJoinOperator::MaterializeDanglingTuplesOfTableB(const size_t result_column_count,
                                                             std::vector<std::shared_ptr<Chunk>>& output_chunks) {
  Segments segments;
  segments.reserve(result_column_count);

  if (table_A_matches_left) {
    AppendNullFilledSegmentsForRowsOfTable(segments, table_A, dangling_tuples_table_B_count_);
  }

  for (ColumnCount i = 0; i < RightInputTable()->GetColumnCount(); ++i) {
    // NOLINTNEXTLINE(performance-unnecessary-value-param)
    ResolveDataType(RightInputTable()->ColumnDataType(i), [&](auto data_type) {
      using ColumnDataType = decltype(data_type);

      std::vector<ColumnDataType> dangling_segment_values;

      for (ChunkId k = 0; k < RightInputTable()->ChunkCount(); ++k) {
        const auto& abstract_segment = RightInputTable()->GetChunk(k)->GetSegment(i);
        const auto& typed_segment = std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(abstract_segment);
        const auto& segment_values = typed_segment->Values();

        for (ColumnId& offset : dangling_tuples_table_B_offsets_[k]) {
          dangling_segment_values.push_back(segment_values[offset]);
        }
      }

      segments.push_back(std::make_shared<ValueSegment<ColumnDataType>>(std::move(dangling_segment_values)));
    });
  }

  if (!table_A_matches_left) {
    AppendNullFilledSegmentsForRowsOfTable(segments, table_A, dangling_tuples_table_B_count_);
  }

  output_chunks.emplace_back(std::make_shared<Chunk>(std::move(segments)));
}

void AbstractJoinOperator::AppendNullFilledSegmentsForRowsOfTable(Segments& segments,
                                                                  const std::shared_ptr<const Table>& table,
                                                                  size_t row_count) {
  for (ColumnCount i = 0; i < table->GetColumnCount(); ++i) {
    // NOLINTNEXTLINE(performance-unnecessary-value-param)
    ResolveDataType(table->ColumnDataType(i), [&](auto data_type) {
      using ColumnDataType = decltype(data_type);
      segments.push_back(std::make_shared<ValueSegment<ColumnDataType>>(std::vector<ColumnDataType>(row_count),
                                                                        std::vector<bool>(row_count, true)));
    });
  }
}

int AbstractJoinOperator::DetermineOutputChunkCount() const {
  switch (join_mode_) {
    case JoinMode::kInner:
      return 1;
    case JoinMode::kLeftOuter:
    case JoinMode::kRightOuter:
      return 2;
    case JoinMode::kFullOuter:
      return 3;
    default:
      Fail("Unsupported join mode. Implemented join modes are Inner, LeftOuter, RightOuter and FullOuter.");
  }
}

int AbstractJoinOperator::ComputeResultRowCount() const {
  return std::accumulate(position_lists_.cbegin(), position_lists_.cend(), 0,
                         [&](const auto sum, const auto& position_list) { return sum + position_list.size(); });
}

ColumnCount AbstractJoinOperator::ComputeResultColumnCount() const {
  return LeftInputTable()->GetColumnCount() + RightInputTable()->GetColumnCount();
}

TableColumnDefinitions AbstractJoinOperator::BuildRightSchema() const {
  auto right_schema = RightInputTable()->ColumnDefinitions();
  if (join_mode_ == JoinMode::kLeftOuter || join_mode_ == JoinMode::kFullOuter) {
    for (auto& column : right_schema) {
      column.nullable = true;
    }
  }
  return right_schema;
}

TableColumnDefinitions AbstractJoinOperator::BuildLeftSchema() const {
  auto left_schema = LeftInputTable()->ColumnDefinitions();
  if (join_mode_ == JoinMode::kRightOuter || join_mode_ == JoinMode::kFullOuter) {
    for (auto& column : left_schema) {
      column.nullable = true;
    }
  }
  return left_schema;
}

}  // namespace skyrise