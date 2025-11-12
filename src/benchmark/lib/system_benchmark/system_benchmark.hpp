#pragma once

#include <aws/core/Aws.h>

#include "abstract_benchmark.hpp"
#include "system_benchmark/system_benchmark_config.hpp"
#include "system_benchmark/system_benchmark_runner.hpp"

namespace skyrise {

class SystemBenchmark : public AbstractBenchmark {
 public:
  SystemBenchmark(
      std::shared_ptr<const Aws::IAM::IAMClient> iam_client,
      std::shared_ptr<const Aws::Lambda::LambdaClient> lambda_client,
      std::shared_ptr<const Aws::S3::S3Client> s3_client, const CompilerName& compiler_name, const QueryId& query_id,
      const ScaleFactor& scale_factor, const size_t concurrent_instance_count, const size_t repetition_count,
      const std::vector<size_t>& after_repetition_delays_min,
      const std::optional<size_t> stage_1_partitions_per_worker_count = std::nullopt
      /* The number of .parquet or .csv files of the partitioned input table read by a single stage 1 worker.
       * If empty, then the hardcoded values will be used. */
      ,
      const std::optional<size_t> shuffle_partitions_count = std::nullopt
      /* The number of partitions applied in any shuffle operation. If empty, then the hardcoded values will be used. */
      ,
      const std::optional<size_t> worker_memory_size_mb = std::nullopt
      /* The maximum memory assigned to each worker. If empty, then the hardcoded values will be used. */);

  const Aws::String& Name() const override;

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> Run(
      const std::shared_ptr<AbstractBenchmarkRunner>& benchmark_runner) override;

 private:
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> OnRun(const std::shared_ptr<SystemBenchmarkRunner>& runner);

  const std::shared_ptr<const Aws::IAM::IAMClient> iam_client_;
  const std::shared_ptr<const Aws::Lambda::LambdaClient> lambda_client_;
  const std::shared_ptr<const Aws::S3::S3Client> s3_client_;
  std::shared_ptr<SystemBenchmarkConfig> config_;
};

}  // namespace skyrise
