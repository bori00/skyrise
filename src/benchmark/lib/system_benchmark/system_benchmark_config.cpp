#include "system_benchmark_config.hpp"

namespace skyrise {

SystemBenchmarkConfig::SystemBenchmarkConfig(const CompilerName& compiler_name, const QueryId& query_id,
                                             const ScaleFactor& scale_factor, const size_t concurrent_instance_count,
                                             const size_t repetition_count,
                                             const std::vector<std::function<void()>>& after_repetition_callbacks,
                                             const std::optional<size_t> stage_1_partitions_per_worker_count,
                                             const std::optional<size_t> shuffle_partitions_count,
                                             const std::optional<size_t> worker_memory_size_mb,
                                             const Aws::Utils::Json::JsonValue query_configuration)
    : AbstractBenchmarkConfig(concurrent_instance_count, repetition_count, after_repetition_callbacks),
      compiler_name_(compiler_name),
      query_id_(query_id),
      scale_factor_(scale_factor),
      stage_1_partitions_per_worker_count_(stage_1_partitions_per_worker_count),
      shuffle_partitions_count_(shuffle_partitions_count),
      worker_memory_size_mb_(worker_memory_size_mb),
      query_configuration_(query_configuration) {}

CompilerName SystemBenchmarkConfig::GetCompilerName() const { return compiler_name_; }
QueryId SystemBenchmarkConfig::GetQueryId() const { return query_id_; }
ScaleFactor SystemBenchmarkConfig::GetScaleFactor() const { return scale_factor_; }
std::optional<size_t> SystemBenchmarkConfig::GetStage1PartitionsPerWorkerCount() const {
  return stage_1_partitions_per_worker_count_;
};
std::optional<size_t> SystemBenchmarkConfig::GetShufflePartitionsCount() const { return shuffle_partitions_count_; };
std::optional<size_t> SystemBenchmarkConfig::GetWorkerMemorySizeMb() const { return worker_memory_size_mb_; };
Aws::Utils::Json::JsonValue SystemBenchmarkConfig::GetQueryConfiguration() const { return query_configuration_; };

}  // namespace skyrise
