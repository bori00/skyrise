#include "system_benchmark_runner.hpp"

#include <aws/lambda/model/InvokeRequest.h>
#include <magic_enum/magic_enum.hpp>

#include "function/function_utils.hpp"

namespace skyrise {

SystemBenchmarkRunner::SystemBenchmarkRunner(std::shared_ptr<const Aws::IAM::IAMClient> iam_client,
                                             std::shared_ptr<const Aws::Lambda::LambdaClient> lambda_client,
                                             std::shared_ptr<const Aws::S3::S3Client> s3_client,
                                             std::shared_ptr<const CostCalculator> cost_calculator,
                                             const Aws::String& client_region, const bool metering,
                                             const bool introspection)
    : AbstractBenchmarkRunner(metering, introspection),
      iam_client_(std::move(iam_client)),
      lambda_client_(std::move(lambda_client)),
      s3_client_(std::move(s3_client)),
      cost_calculator_(std::move(cost_calculator)),
      client_region_(client_region) {}

std::shared_ptr<SystemBenchmarkResult> SystemBenchmarkRunner::RunSystemConfig(
    const std::shared_ptr<SystemBenchmarkConfig>& config) {
  return std::dynamic_pointer_cast<SystemBenchmarkResult>(RunConfig(config));
}

void SystemBenchmarkRunner::Setup() {
  typed_config_ = std::dynamic_pointer_cast<SystemBenchmarkConfig>(config_);
  Assert(typed_config_, "SystemBenchmarkRunner can only consume SystemBenchmarkConfigs.");

  coordinator_function_name_ =
      config_->GetBenchmarkTimestamp() + "-" + kCoordinatorFunctionName + "-" + config_->GetBenchmarkId();
  shuffle_storage_identifier_ =
      config_->GetBenchmarkTimestamp() + "-" + "systemBenchmark" + "-" + config_->GetBenchmarkId();
  ConstructWorkerFunctionNames();

  std::vector<FunctionConfig> function_configs = ConstructWorkerFunctionConfigsToUpload();

  function_configs.push_back({GetFunctionZipFilePath(kCoordinatorBinaryName), coordinator_function_name_,
                              kLambdaMaximumMemorySizeMb, true, false});

  UploadFunctions(iam_client_, lambda_client_, function_configs, false);

  std::cout << "Uploaded functions: " << coordinator_function_name_;
  for (auto& [worker_memory_size_mb, worker_function_name] : worker_memory_size_mb_to_worker_function_name_) {
    std::cout << "\n Worker of size " << worker_memory_size_mb << " MB: " << worker_function_name;
    ;
  }
  std::cout << std::endl;
}

void SystemBenchmarkRunner::Teardown() {
  DeleteFunction(lambda_client_, coordinator_function_name_);
  for (auto& [worker_memory_size_mb, worker_function_name] : worker_memory_size_mb_to_worker_function_name_) {
    DeleteFunction(lambda_client_, worker_function_name);
  }
}

std::vector<FunctionConfig> SystemBenchmarkRunner::ConstructWorkerFunctionConfigsToUpload() {
  std::vector<FunctionConfig> worker_functions_to_upload;
  for (auto& [worker_memory_size_mb, worker_function_name] : worker_memory_size_mb_to_worker_function_name_) {
    worker_functions_to_upload.push_back(
        {GetFunctionZipFilePath(kWorkerBinaryName), worker_function_name, worker_memory_size_mb, true, false});
  }
  return worker_functions_to_upload;
}

void SystemBenchmarkRunner::ConstructWorkerFunctionNames() {
  int base_worker_memory_size_mb =
      typed_config_->GetWorkerMemorySizeMb().value_or(kDefaultWorkerVCPUCount * kLambdaVcpuEquivalentMemorySizeMb);
  Assert(base_worker_memory_size_mb <= kLambdaMaximumMemorySizeMb,
         "The worker's memory cannot be larger than the maximum allowed by AWS");
  int worker_memory_size_mb;
  for (const auto& [pipeline_id, pipeline_config] : typed_config_->GetQueryConfiguration().View().GetAllObjects()) {
    if (pipeline_config.KeyExists("worker_memory_size_mb")) {
      worker_memory_size_mb = pipeline_config.GetInt64("worker_memory_size_mb");
    } else {
      worker_memory_size_mb = base_worker_memory_size_mb;
    }
    Assert(worker_memory_size_mb <= kLambdaMaximumMemorySizeMb,
           "The worker's memory cannot be larger than the maximum allowed by AWS");
    if (!worker_memory_size_mb_to_worker_function_name_.contains(worker_memory_size_mb)) {
      worker_memory_size_mb_to_worker_function_name_[worker_memory_size_mb] =
          config_->GetBenchmarkTimestamp() + "-" + kWorkerFunctionName + "-" + std::to_string(worker_memory_size_mb) +
          "-" + config_->GetBenchmarkId();
    }
  }

  if (worker_memory_size_mb_to_worker_function_name_.empty()) {
    // if no pipelines are defined in the config, then assume that all pipelines will run with the base size.
    worker_memory_size_mb_to_worker_function_name_[base_worker_memory_size_mb] =
        config_->GetBenchmarkTimestamp() + "-" + kWorkerFunctionName + "-" +
        std::to_string(base_worker_memory_size_mb) + "-" + config_->GetBenchmarkId();
  }
}

Aws::Utils::Json::JsonValue SystemBenchmarkRunner::MapToJsonList(const std::map<size_t, std::string>& inputMap) {
  Aws::Utils::Json::JsonValue root;
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> jsonArray(inputMap.size());

  size_t index = 0;
  for (auto const& [size, name] : inputMap) {
    Aws::Utils::Json::JsonValue entry;
    entry.WithInteger("worker_memory_size_mb", size);
    entry.WithString("worker_function_name", name);
    jsonArray[index++] = entry;
  }
  root.AsArray(std::move(jsonArray));
  return root;
}

std::shared_ptr<AbstractBenchmarkResult> SystemBenchmarkRunner::OnRunConfig() {
  typed_config_ = std::dynamic_pointer_cast<SystemBenchmarkConfig>(config_);
  Assert(typed_config_, "SystemBenchmarkRunner can only consume SystemBenchmarkConfigs.");

  size_t results_count = typed_config_->GetRepetitionCount();
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> results(results_count);
  for (size_t i = 0; i < results_count; ++i) {
    auto invoke_request = Aws::Lambda::Model::InvokeRequest()
                              .WithFunctionName(coordinator_function_name_)
                              .WithInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);
    Aws::Utils::Json::JsonValue payload =
        Aws::Utils::Json::JsonValue()
            .WithString(kCoordinatorRequestCompilerNameAttribute,
                        std::string(magic_enum::enum_name(typed_config_->GetCompilerName())))
            .WithString(kCoordinatorRequestQueryPlanAttribute,
                        std::string(magic_enum::enum_name(typed_config_->GetQueryId())))
            .WithString(kCoordinatorRequestScaleFactorAttribute,
                        std::string(magic_enum::enum_name(typed_config_->GetScaleFactor())))
            .WithObject(kCoordinatorRequestShuffleStorageAttribute,
                        ObjectReference(kSkyriseBenchmarkContainer,
                                        shuffle_storage_identifier_ + "/repetition-" + std::to_string(i))
                            .ToJson())
            .WithObject(kCoordinatorRequestWorkerFunctionAttribute,
                        MapToJsonList(worker_memory_size_mb_to_worker_function_name_))
            .WithObject(kCoordinatorRequestQueryConfigurationAttribute, typed_config_->GetQueryConfiguration());
    const auto request_payload_stream = std::make_shared<std::stringstream>(payload.View().WriteCompact());
    invoke_request.SetBody(request_payload_stream);

    const auto outcome = lambda_client_->Invoke(invoke_request);
    if (outcome.IsSuccess()) {
      auto& response_payload_stream = outcome.GetResult().GetPayload();
      results[i] = Aws::Utils::Json::JsonValue(response_payload_stream);
      results[i].WithObject("WorkerFunctions", MapToJsonList(worker_memory_size_mb_to_worker_function_name_));
      results[i].WithString("CoordinatorFunction", coordinator_function_name_);
    } else {
      AWS_LOGSTREAM_ERROR(kBenchmarkTag.c_str(), outcome.GetError().GetMessage());
    }
  }
  return std::make_shared<SystemBenchmarkResult>(results);
}

}  // namespace skyrise
