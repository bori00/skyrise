#pragma once

#include "abstract_benchmark_runner.hpp"
#include "client/coordinator_client.hpp"
#include "function/function_config.hpp"
#include "system_benchmark_config.hpp"
#include "system_benchmark_result.hpp"
#include "utils/costs/cost_calculator.hpp"

namespace skyrise {

class SystemBenchmarkRunner : public AbstractBenchmarkRunner {
 public:
  SystemBenchmarkRunner(std::shared_ptr<const Aws::IAM::IAMClient> iam_client,
                        std::shared_ptr<const Aws::Lambda::LambdaClient> lambda_client,
                        std::shared_ptr<const Aws::S3::S3Client> s3_client,
                        std::shared_ptr<const CostCalculator> cost_calculator, const Aws::String& client_region,
                        const bool metering = true, const bool introspection = true);

  std::shared_ptr<SystemBenchmarkResult> RunSystemConfig(const std::shared_ptr<SystemBenchmarkConfig>& config);

 protected:
  void Setup() override;
  void Teardown() override;

  void ConstructWorkerFunctionNames();

  std::vector<FunctionConfig> ConstructWorkerFunctionConfigsToUpload();

  std::shared_ptr<AbstractBenchmarkResult> OnRunConfig() override;

  static Aws::Utils::Json::JsonValue MapToJsonList(const std::map<size_t, std::string>& inputMap);

  const std::shared_ptr<const Aws::IAM::IAMClient> iam_client_;
  const std::shared_ptr<const Aws::Lambda::LambdaClient> lambda_client_;
  const std::shared_ptr<const Aws::S3::S3Client> s3_client_;
  const std::shared_ptr<const CostCalculator> cost_calculator_;
  const Aws::String client_region_;

  std::string coordinator_function_name_;
  std::map<size_t, std::string> worker_memory_size_mb_to_worker_function_name_;
  std::string shuffle_storage_identifier_;

  std::shared_ptr<SystemBenchmarkConfig> typed_config_;
  std::shared_ptr<SystemBenchmarkResult> benchmark_result_;

 private:
  static constexpr size_t kDefaultWorkerVCPUCount = 4;
};

}  // namespace skyrise
