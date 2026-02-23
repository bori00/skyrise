#pragma once

#include "function/function.hpp"
#include "scheduler/coordinator/pqp_pipeline_scheduler.hpp"

namespace skyrise {

class CoordinatorFunction : public Function {
 protected:
  static bool ValidateRequest(const Aws::Utils::Json::JsonView& request);
  aws::lambda_runtime::invocation_response OnHandleRequest(
      const Aws::Utils::Json::JsonView& request,
      std::optional<std::shared_ptr<RequestTracker>> request_tracker) const override;
};

}  // namespace skyrise
