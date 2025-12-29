/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "expression/abstract_expression.hpp"
#include "filter/abstract_filter_implementation.hpp"
#include "operator/abstract_operator.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace skyrise {

class LimitOperator : public AbstractOperator {
 public:
  LimitOperator(std::shared_ptr<const AbstractOperator> input_operator, std::shared_ptr<AbstractExpression> row_count);

  const std::shared_ptr<AbstractExpression>& RowCount() const;

  const std::string& Name() const override;

 protected:
  std::shared_ptr<const Table> OnExecute(
      const std::shared_ptr<OperatorExecutionContext>& operator_execution_context) override;

 private:
  const std::shared_ptr<AbstractExpression> row_count_;
};

}  // namespace skyrise
