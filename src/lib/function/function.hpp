#pragma once

#include <optional>

#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/lambda-runtime/runtime.h>
#include <metering/request_tracker/request_tracker.hpp>

namespace skyrise {

class Function {
 public:
  virtual ~Function() = default;

  void HandleRequest() const;

 protected:
  static void MemoryAllocationExceptionHandler();
  aws::lambda_runtime::invocation_response HandlerFunction(
      const aws::lambda_runtime::invocation_request& request,
      std::optional<std::shared_ptr<RequestTracker>> request_tracker) const;
  virtual aws::lambda_runtime::invocation_response OnHandleRequest(
      const Aws::Utils::Json::JsonView& request,
      std::optional<std::shared_ptr<RequestTracker>> request_tracker) const = 0;
};

}  // namespace skyrise
