#pragma once

#include "abstract_join_operator.hpp"
#include "join_operator_predicate.hpp"
#include "types.hpp"

namespace skyrise {

/**
 * SortedMergeJoinOperator assumes that within each CHUNK of the left input and right input, the rows are already sorted
 * by the column of the join predicate. But it doesn't assume any global sorting.
 *
 * TODO(bori00): warning: outer joins are not tested.
 */
class SortedMergeJoinOperator : public AbstractJoinOperator {
 public:
  SortedMergeJoinOperator(std::shared_ptr<const AbstractOperator> left_input,
                          std::shared_ptr<const AbstractOperator> right_input,
                          std::shared_ptr<JoinOperatorPredicate> predicate, const JoinMode join_mode);

  const std::string& Name() const override;

 private:
  void FillPositionLists() override;
};

}  // namespace skyrise
