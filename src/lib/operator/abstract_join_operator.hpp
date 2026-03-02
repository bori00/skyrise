#pragma once

#include "abstract_operator.hpp"
#include "join_operator_predicate.hpp"
#include "types.hpp"

namespace skyrise {

class AbstractJoinOperator : public AbstractOperator {
  /**
   * Note: for efficiency reasons, as long as the PQP optimiser is not integrated in the codebase, we let the
   * JoinOperator decide at execution time whether they want to switch the left and right input sides.
   *
   * Independently of the internal implementation and whether the tables get switched internally, it is guaranteed that
   * the output will contain the columns of the left input table first and the columns of the right input table second.
   *
   * In the following we refer to the tables as table _A_ and table _B_. The matching is defined by the flag
   * @table_A_matches_left:
   * --> table_A_matches_left == True <=> table_A == left input table && table_B == right input table
   * --> table_B_matches_left == True <=> table_A == right input table && table_B == left input table
   */

 public:
  /**
   * PositionLists is a two-dimensional vector that stores a list of matching tuple-pairs for each chunk of table _B_.
   * Hence, the first dimension is formed by the chunks of table _B_. PositionLists[0] holds all matches
   * (according to the join predicate) of tuples if the _B_ tuple is stored in the first chunk of its table.
   * A join match is stated as Pair composed by a RowId and ChunkOffset. The RowId identifies which tuple of the _A_
   * table is involved while the ChunkOffset indicates the tuple of the _B_ table (as the ChunkId is the key for the
   * PositionList).
   *
   * Each PositionList has the following structure.
   *    [ChunkIndex of R] => [(RowId of S, ChunkOffset of R); ...]
   * Specifically the row of S and R joined marked with the # in the example will produce the following entry:
   *    [0] => [...; ((0,1), 0); ...]
   *
   * Hence, the PositionLists object in this scenario looks as follows.
   *    [0] => [((0,1), 0); ((0,1), 1); ((1,0), 0); ((1,0), 1); ((1,2), 2)]
   *    [1] => [((0,0), 0); ((1,2), 1)]
   */
  using PositionLists = std::vector<std::vector<std::pair<RowId, ChunkOffset>>>;

  AbstractJoinOperator(OperatorType operator_type, std::shared_ptr<const AbstractOperator> left_input,
                       std::shared_ptr<const AbstractOperator> right_input,
                       std::shared_ptr<JoinOperatorPredicate> predicate, const JoinMode join_mode);

 protected:
  std::shared_ptr<const Table> OnExecute(
      const std::shared_ptr<OperatorExecutionContext>& operator_execution_context) override;

  TableColumnDefinitions BuildLeftSchema() const;
  TableColumnDefinitions BuildRightSchema() const;

  ColumnCount ComputeResultColumnCount() const;
  int ComputeResultRowCount() const;
  int DetermineOutputChunkCount() const;

  virtual void FillPositionLists() = 0;

  void MaterializeMatches(const size_t result_column_count, const size_t result_row_count,
                          std::vector<std::shared_ptr<Chunk>>& output_chunks);
  void MaterializeLeftSideOfMatchedTuples(const size_t result_row_count, Segments& output_segments);
  void MaterializeRightSideOfMatchedTuples(const size_t result_row_count, Segments& output_segments);

  void MaterializeTableASideOfMatchedTuples(const size_t result_row_count, Segments& output_segments);
  void MaterializeTableBSideOfMatchedTuples(const size_t result_row_count, Segments& output_segments);

  void MaterializeDanglingTuplesOfLeftTable(const size_t result_column_count,
                                            std::vector<std::shared_ptr<Chunk>>& output_chunks);

  void MaterializeDanglingTuplesOfTableA(const size_t result_column_count,
                                         std::vector<std::shared_ptr<Chunk>>& output_chunks);

  void MaterializeDanglingTuplesOfRightTable(const size_t result_column_count,
                                             std::vector<std::shared_ptr<Chunk>>& output_chunks);

  void MaterializeDanglingTuplesOfTableB(const size_t result_column_count,
                                         std::vector<std::shared_ptr<Chunk>>& output_chunks);

  static void AppendNullFilledSegmentsForRowsOfTable(Segments& segments, const std::shared_ptr<const Table>& table,
                                                     size_t row_count);

  const std::shared_ptr<JoinOperatorPredicate> predicate_;
  const JoinMode join_mode_;

  PositionLists position_lists_;

  /**
   * This bitmap plays a central role for outer joins. The two-dimensional vector is filled as follows:
   *
   *              dangling_tuples_table_A_[i][j] := Tuple with RowId (ChunkIndex=i,ChunkOffset=j) of table_A
   *                                          is matched with any tuple of table_B.
   *
   * Thus, the bitmap states whether a tuple is dangling and must be considered for that reason
   * in a further step for Outer Joins or not.
   */
  std::vector<std::vector<bool>> dangling_tuples_table_A_;
  size_t dangling_tuples_table_A_count_ = 0;

  /**
   * Auxiliary data structures that are used to identify dangling tuples of table_B in outer joins.
   */
  std::vector<std::vector<ChunkOffset>> dangling_tuples_table_B_offsets_;
  size_t dangling_tuples_table_B_count_ = 0;

  bool table_A_matches_left = true;
  std::shared_ptr<const Table> table_A = std::shared_ptr<const Table>(nullptr);
  std::shared_ptr<const Table> table_B = std::shared_ptr<const Table>(nullptr);
};

}  // namespace skyrise
