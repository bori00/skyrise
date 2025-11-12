#pragma once

#include "compiler/abstract_compiler.hpp"

namespace skyrise {

class TpchPqpGenerator : public AbstractCompiler {
 public:
  TpchPqpGenerator(const QueryId& query_id, const ScaleFactor& scale_factor,
                   const ObjectReference& shuffle_storage_prefix,
                   const std::optional<size_t> stage_1_partitions_per_worker_count = std::nullopt
                    /* The number of .parquet or .csv files of the partitioned input table read by a single stage 1 worker. If empty,
                       then the hardcoded values will be used. */,
                    const std::optional<size_t> shuffle_partitions_count = std::nullopt
                    /* The number of partitions applied in any shuffle operation. If empty, then the hardcoded values will be used. */);

  std::vector<std::shared_ptr<PqpPipeline>> GeneratePqp() const final;

 private:
  const std::optional<size_t> stage_1_partitions_per_worker_count_;
  const std::optional<size_t> shuffle_partitions_count_;

  std::vector<ObjectReference> ListTableObjects(const std::string& table_name, const FileFormat& import_format) const;

  std::vector<ObjectReference> GenerateOutputObjectIds(size_t count, const std::string& prefix,
                                                       const FileFormat export_format) const;

  static std::shared_ptr<PqpPipeline> GeneratePipeline(const std::string& pipeline_id, const std::string& import_id,
                                                       const std::shared_ptr<AbstractOperatorProxy>& pqp,
                                                       const FileFormat& export_format,
                                                       std::vector<ObjectReference> input_objects,
                                                       std::vector<ObjectReference> output_objects);

  size_t GetStage1PartitionsPerWorkerCount() const;

  size_t GetShufflePartitionsCount() const;

  // Query 1.
  static std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> GenerateQ1Pipeline1(
      const std::vector<ObjectReference>& input_objects);
  static std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> GenerateQ1Pipeline2(
      const std::vector<ObjectReference>& input_objects);
  static std::vector<std::shared_ptr<PqpPipeline>> GenerateQ1();

  // Query 6.
  std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> GenerateQ6Pipeline1(
      const std::vector<ObjectReference>& input_objects) const;
  std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> GenerateQ6Pipeline2(
      const std::vector<ObjectReference>& input_objects) const;
  std::vector<std::shared_ptr<PqpPipeline>> GenerateQ6() const;

  // Query 12.
  std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> GenerateQ12Pipeline1(
      const size_t partition_count, const std::vector<ObjectReference>& input_objects) const;
  std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> GenerateQ12Pipeline2(
      const size_t partition_count, const std::vector<ObjectReference>& input_objects) const;
  std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> GenerateQ12Pipeline3(
      const size_t partition_count, const std::vector<ObjectReference>& input_objects_left,
      const std::vector<ObjectReference>& input_objects_right) const;
  std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> GenerateQ12Pipeline4(
      const size_t partition_count, const std::vector<ObjectReference>& input_objects) const;
  std::vector<std::shared_ptr<PqpPipeline>> GenerateQ12() const;
};

class TpchPqpGeneratorConfig : public AbstractCompilerConfig {
 public:
  TpchPqpGeneratorConfig(const CompilerName& compiler_name, const QueryId& query_id, const ScaleFactor& scale_factor,
                         const ObjectReference& shuffle_storage_prefix,
                         const std::optional<size_t> stage_1_partitions_per_worker_count = std::nullopt
                    /* The number of .parquet or .csv files of the partitioned input table read by a single stage 1 worker. If empty,
                       then the hardcoded values will be used. */,
                    const std::optional<size_t> shuffle_partitions_count = std::nullopt
                    /* The number of partitions applied in any shuffle operation. If empty, then the hardcoded values will be used. */);

  std::shared_ptr<AbstractCompiler> GenerateCompiler() const final;
  bool operator==(const TpchPqpGeneratorConfig& other) const;

 private:
  const std::optional<size_t> stage_1_partitions_per_worker_count_;
  const std::optional<size_t> shuffle_partitions_count_;
};

}  // namespace skyrise
