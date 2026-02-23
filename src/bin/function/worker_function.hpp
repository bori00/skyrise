#pragma once

#include "client/base_client.hpp"
#include "function/function.hpp"

namespace skyrise {

class WorkerFunction : public Function {
 protected:
  static bool ValidateRequest(const Aws::Utils::Json::JsonView& request);
  static void InvokeRecursive(const Aws::Utils::Json::JsonView& request,
                              const std::shared_ptr<skyrise::BaseClient>& client);
  aws::lambda_runtime::invocation_response OnHandleRequest(
      const Aws::Utils::Json::JsonView& request,
      std::optional<std::shared_ptr<RequestTracker>> request_tracker) const override;
};

}  // namespace skyrise
