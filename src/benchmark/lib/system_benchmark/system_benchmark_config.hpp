#pragma once

#include "abstract_benchmark_config.hpp"
#include "types.hpp"

namespace skyrise {

class SystemBenchmarkConfig : public AbstractBenchmarkConfig {
 public:
  SystemBenchmarkConfig(
      const CompilerName& compiler_name, const QueryId& query_id, const ScaleFactor& scale_factor,
      const size_t concurrent_instance_count, const size_t repetition_count,
      const std::vector<std::function<void()>>& after_repetition_callbacks = {},
      const std::optional<size_t> stage_1_partitions_per_worker_count = std::nullopt
      /* The number of .parquet or .csv files of the partitioned input table read by a single stage 1 worker. If empty,
         then the hardcoded values will be used. */
      ,
      const std::optional<size_t> shuffle_partitions_count = std::nullopt
      /* The number of partitions applied in any shuffle operation. If empty, then the hardcoded values will be used. */
      ,
      const std::optional<size_t> worker_memory_size_mb = std::nullopt
      /* The memory assigned to each worker. If empty, then the hardcoded values will be used. */
      ,
      const Aws::Utils::Json::JsonValue query_configuration = Aws::Utils::Json::JsonValue()
      /* The JSON configuration defining the join algorithm and resource allocation for all pipelines. */
  );

  CompilerName GetCompilerName() const;
  QueryId GetQueryId() const;
  ScaleFactor GetScaleFactor() const;
  std::optional<size_t> GetStage1PartitionsPerWorkerCount() const;
  std::optional<size_t> GetShufflePartitionsCount() const;
  std::optional<size_t> GetWorkerMemorySizeMb() const;
  Aws::Utils::Json::JsonValue GetQueryConfiguration() const;

 protected:
  const CompilerName compiler_name_;
  const QueryId query_id_;
  const ScaleFactor scale_factor_;
  const std::optional<int> stage_1_partitions_per_worker_count_;
  const std::optional<int> shuffle_partitions_count_;
  const std::optional<int> worker_memory_size_mb_;
  const Aws::Utils::Json::JsonValue query_configuration_;
};

}  // namespace skyrise
