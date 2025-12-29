#include "coordinator_function.hpp"

#include <vector>

#include <aws/core/utils/json/JsonSerializer.h>
#include <boost/math/tools/config.hpp>
#include <boost/mpl/min_max.hpp>

#include "client/coordinator_client.hpp"
#include "compiler/abstract_compiler.hpp"
#include "compiler/physical_query_plan/pqp_pipeline.hpp"
#include "constants.hpp"
#include "scheduler/coordinator/lambda_executor.hpp"
#include "utils/costs/cost_calculator.hpp"

namespace {

struct PipelineWorkerStatistics final {
  explicit PipelineWorkerStatistics() = default;

  size_t fragment_count;

  std::vector<size_t> fragment_runtimes;
  size_t average_runtime_ms;
  size_t max_fragment_runtime_ms;

  std::vector<size_t> fragment_bytes_consumed;
  size_t average_bytes_consumed;
  size_t max_bytes_consumed;

  std::vector<size_t> fragment_bytes_written;
  size_t average_bytes_written;
  size_t max_bytes_written;

  Aws::Utils::Json::JsonValue ToJson() const {
    Aws::Utils::Json::JsonValue pipeline_json;

    pipeline_json.WithInteger("fragment_count", fragment_count)
        .WithInt64("avg_runtime", average_runtime_ms)
        .WithInt64("max_fragment_runtime", max_fragment_runtime_ms)
        .WithInt64("avg_bytes_consumed", average_bytes_consumed)
        .WithInt64("max_bytes_consumed", max_bytes_consumed)
        .WithInt64("avg_bytes_written", average_bytes_written)
        .WithInt64("max_bytes_written", max_bytes_written);

    Aws::Utils::Array<Aws::Utils::Json::JsonValue> json_fragment_runtimes(fragment_count);
    Aws::Utils::Array<Aws::Utils::Json::JsonValue> json_fragment_bytes_consumed(fragment_count);
    Aws::Utils::Array<Aws::Utils::Json::JsonValue> json_fragment_bytes_written(fragment_count);

    for (size_t i = 0; i < fragment_count; ++i) {
      Aws::Utils::Json::JsonValue runtime_json;
      json_fragment_runtimes[i] = runtime_json.AsInteger(fragment_runtimes[i]);

      Aws::Utils::Json::JsonValue bytes_consumed_json;
      json_fragment_bytes_consumed[i] = bytes_consumed_json.AsInteger(fragment_bytes_consumed[i]);

      Aws::Utils::Json::JsonValue bytes_written_json;
      json_fragment_bytes_written[i] = bytes_written_json.AsInteger(fragment_bytes_written[i]);
    }

    pipeline_json.WithArray("fragment_runtimes", json_fragment_runtimes);
    pipeline_json.WithArray("fragment_bytes_consumed", json_fragment_bytes_consumed);
    pipeline_json.WithArray("fragment_bytes_written", json_fragment_bytes_written);

    return pipeline_json;
  }
};

PipelineWorkerStatistics GetPipelineWorkerStatistics(const std::shared_ptr<skyrise::PqpPipelineTask>& pipeline_task) {
  PipelineWorkerStatistics statistics;
  statistics.fragment_count = pipeline_task->fragment_execution_results.size();
  statistics.max_fragment_runtime_ms = 0;
  statistics.average_runtime_ms = 0;
  statistics.max_bytes_consumed = 0;
  statistics.average_bytes_consumed = 0;
  statistics.max_bytes_written = 0;
  statistics.average_bytes_written = 0;
  for (const auto& fragment_execution_result : pipeline_task->fragment_execution_results) {
    statistics.fragment_runtimes.push_back(fragment_execution_result->runtime_ms);
    statistics.fragment_bytes_consumed.push_back(fragment_execution_result->import_data_size_bytes);
    statistics.fragment_bytes_written.push_back(fragment_execution_result->export_data_size_bytes);
    statistics.average_runtime_ms += fragment_execution_result->runtime_ms;
    statistics.max_fragment_runtime_ms =
        std::max(statistics.max_fragment_runtime_ms, fragment_execution_result->runtime_ms);
    statistics.average_bytes_consumed += fragment_execution_result->import_data_size_bytes;
    statistics.max_bytes_consumed =
        std::max(statistics.max_bytes_consumed, fragment_execution_result->import_data_size_bytes);
    statistics.average_bytes_written += fragment_execution_result->export_data_size_bytes;
    statistics.max_bytes_written =
        std::max(statistics.max_bytes_written, fragment_execution_result->export_data_size_bytes);
  }
  statistics.average_runtime_ms /= statistics.fragment_count;
  statistics.average_bytes_consumed /= statistics.fragment_count;
  statistics.average_bytes_written /= statistics.fragment_count;
  return statistics;
}

struct WorkerStatistics final {
  explicit WorkerStatistics() = default;

  size_t worker_memory_size_mb{0};
  size_t worker_invocation_count{0};
  size_t worker_accumulated_runtime_ms{0};
  std::map<std::string, size_t> worker_accumulated_request_counts;
  std::map<std::string, PipelineWorkerStatistics> pipeline_workers_statistics;
};

WorkerStatistics GetWorkerStatistics(const std::vector<std::shared_ptr<skyrise::PqpPipelineTask>>& pipeline_tasks) {
  WorkerStatistics statistics;

  for (const auto& pipeline_task : pipeline_tasks) {
    const std::vector<std::shared_ptr<skyrise::PqpPipelineFragmentExecutionResult>>& fragment_execution_results =
        pipeline_task->fragment_execution_results;
    if (fragment_execution_results.empty()) {
      continue;
    }

    if (statistics.worker_memory_size_mb == 0) {
      statistics.worker_memory_size_mb = fragment_execution_results.front()->function_instance_size_mb;
    }

    statistics.worker_invocation_count += fragment_execution_results.size();

    statistics.pipeline_workers_statistics[pipeline_task->pqp_pipeline->Identity()] =
        GetPipelineWorkerStatistics(pipeline_task);

    for (const auto& fragment_execution_result : fragment_execution_results) {
      statistics.worker_accumulated_runtime_ms += fragment_execution_result->runtime_ms;

      const auto& metering = fragment_execution_result->metering.View();
      for (const auto& request_type : metering.GetAllObjects()) {
        statistics.worker_accumulated_request_counts[request_type.first] += request_type.second.GetInteger("finished");
      }
    }
  }

  return statistics;
}

Aws::Utils::Json::JsonValue GetPipelineWorkerStatisticsJson(const WorkerStatistics& worker_statistics) {
  Aws::Utils::Json::JsonValue json;

  for (const auto& [pipeline_name, pipeline_stats] : worker_statistics.pipeline_workers_statistics) {
    Aws::Utils::Json::JsonValue pipeline_stats_json = pipeline_stats.ToJson();
    json.WithObject(pipeline_name, pipeline_stats_json);
  }

  return json;
}

}  // namespace

namespace skyrise {

bool CoordinatorFunction::ValidateRequest(const Aws::Utils::Json::JsonView& request) {
  return request.KeyExists(kCoordinatorRequestCompilerNameAttribute) &&
         (request.KeyExists(kCoordinatorRequestQueryPlanAttribute) ||
          request.KeyExists(kCoordinatorRequestQueryStringAttribute)) &&
         request.KeyExists(kCoordinatorRequestScaleFactorAttribute) &&
         request.KeyExists(kCoordinatorRequestShuffleStorageAttribute) &&
         request.KeyExists(kCoordinatorRequestWorkerFunctionAttribute);
}

aws::lambda_runtime::invocation_response CoordinatorFunction::OnHandleRequest(
    const Aws::Utils::Json::JsonView& request) const {
  AWS_LOGSTREAM_DEBUG(kCoordinatorTag.c_str(), std::string("Coordinator request: ") + request.WriteCompact());
  if (!ValidateRequest(request)) {
    return aws::lambda_runtime::invocation_response::failure("Invalid parameters provided.", "text/plain");
  }

  Aws::Utils::Json::JsonValue response;

  const auto compiler_config = AbstractCompilerConfig::FromJson(request);
  const auto compiler = compiler_config->GenerateCompiler();
  const std::vector<std::shared_ptr<PqpPipeline>> pipelines = compiler->GeneratePqp();
  const std::string worker_function_name = request.GetString(kCoordinatorRequestWorkerFunctionAttribute);

  const auto client = std::make_shared<CoordinatorClient>();
  const auto executor = std::make_shared<LambdaExecutor>(client, worker_function_name);

  try {
    PqpPipelineScheduler scheduler(executor, pipelines);
    AWS_LOGSTREAM_INFO(kCoordinatorTag.c_str(), "Starting execution.");
    const auto execution_start = std::chrono::steady_clock::now();
    const auto [result, tasks] = scheduler.Execute();
    if (scheduler.HasError()) {
      return aws::lambda_runtime::invocation_response::failure(scheduler.GetError(), "text/plain");
    }
    AWS_LOGSTREAM_INFO(kCoordinatorTag.c_str(), "Finished execution.");

    std::string result_url = client->GetMutableS3Client()->GeneratePresignedUrl(result->bucket_name, result->identifier,
                                                                                Aws::Http::HttpMethod::HTTP_GET, 3600);
    const size_t execution_runtime_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - execution_start)
            .count();

    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    const char* coordinator_memory_size_mb_string = std::getenv("AWS_LAMBDA_FUNCTION_MEMORY_SIZE");
    const size_t coordinator_memory_size_mb =
        coordinator_memory_size_mb_string != nullptr ? std::stoull(coordinator_memory_size_mb_string, nullptr, 10) : 0;

    WorkerStatistics worker_statistics = GetWorkerStatistics(tasks);

    auto cost_calculator =
        std::make_shared<const CostCalculator>(client->GetPricingClient(), client->GetClientRegion());
    double coordinator_cost = cost_calculator->CalculateCostLambda(execution_runtime_ms, coordinator_memory_size_mb);
    double worker_cost = cost_calculator->CalculateCostLambda(worker_statistics.worker_accumulated_runtime_ms,
                                                              worker_statistics.worker_memory_size_mb);
    double compute_cost = coordinator_cost + worker_cost;

    Aws::Utils::Json::JsonValue serialized_statistics;
    serialized_statistics.WithInteger(kCoordinatorResponseExecutionRuntimeAttribute, execution_runtime_ms)
        .WithInteger(kCoordinatorResponseMemorySizeAttribute, coordinator_memory_size_mb)
        .WithInteger(kCoordinatorResponseWorkerMemorySizeAttribute, worker_statistics.worker_memory_size_mb)
        .WithInteger(kCoordinatorResponseWorkerInvocationCountAttribute, worker_statistics.worker_invocation_count)
        .WithInteger(kCoordinatorResponseWorkerAccumulatedRuntimeAttribute,
                     worker_statistics.worker_accumulated_runtime_ms)
        .WithDouble("execution_cost_cent", compute_cost)
        .WithObject("pipelines_stats", GetPipelineWorkerStatisticsJson(worker_statistics));

    response.WithObject(kCoordinatorResponseResultObjectAttribute, result->ToJson())
        .WithString(kCoordinatorResponseResultUrlStringAttribute, result_url)
        .WithObject(kCoordinatorResponseExecutionStatisticsAttribute, serialized_statistics);

  } catch (const std::exception& exception) {
    AWS_LOGSTREAM_INFO(kCoordinatorTag.c_str(), exception.what());
  }

  return aws::lambda_runtime::invocation_response::success(response.View().WriteCompact(), "application/json");
}

}  // namespace skyrise

int main() {
  const skyrise::CoordinatorFunction coordinator;
  coordinator.HandleRequest();

  return 0;
}
