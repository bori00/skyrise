#pragma once

#include "function/function.hpp"

namespace skyrise {

class StorageIoFunction : public Function {
 protected:
  aws::lambda_runtime::invocation_response OnHandleRequest(
      const Aws::Utils::Json::JsonView& request,
      std::optional<std::shared_ptr<RequestTracker>> request_tracker) const override;
};

}  // namespace skyrise
