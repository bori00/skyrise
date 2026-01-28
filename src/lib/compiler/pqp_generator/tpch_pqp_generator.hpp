#pragma once

#include "compiler/abstract_compiler.hpp"

namespace skyrise {

class TpchPqpGenerator : public AbstractCompiler {
 public:
  TpchPqpGenerator(
      const QueryId& query_id, const ScaleFactor& scale_factor, const ObjectReference& shuffle_storage_prefix,
      const std::optional<size_t> stage_1_partitions_per_worker_count = std::nullopt
      /* The number of .parquet or .csv files of the partitioned input table read by a single stage 1 worker. If empty,
         then the hardcoded values will be used. */
      ,
      const std::optional<size_t> shuffle_partitions_count = std::nullopt,
      /* The number of partitions applied in any shuffle operation. If empty, then the hardcoded values will be used. */
      const Aws::Utils::Json::JsonValue& join_configuration = Aws::Utils::Json::JsonValue());

  std::vector<std::shared_ptr<PqpPipeline>> GeneratePqp() const final;

  struct PipelineData {
    std::vector<ObjectReference> output_objects;
    std::optional<std::shared_ptr<PqpPipeline>> pqp_pipeline;
    std::vector<ColumnId> columns_to_read_in_successor;
  };

 private:
  const std::optional<size_t> stage_1_partitions_per_worker_count_;
  const std::optional<size_t> shuffle_partitions_count_;
  const Aws::Utils::Json::JsonValue join_configuration_;

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

  JoinAlgorithm GetJoinAlgorithmForPipeline(const std::string pipeline_id) const;

  static void SetAsPredecessorOf(std::optional<std::shared_ptr<PqpPipeline>> predecessor,
                                 std::optional<std::shared_ptr<PqpPipeline>> successor);

  static std::vector<std::shared_ptr<PqpPipeline>> FilterNonEmptyPipelines(
      const std::vector<std::optional<std::shared_ptr<PqpPipeline>>>& pipelines);

  // Query 1.
  std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> GenerateQ1Pipeline1(
      const std::vector<ObjectReference>& input_objects) const;
  std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> GenerateQ1Pipeline2(
      const std::vector<ObjectReference>& input_objects) const;
  std::vector<std::shared_ptr<PqpPipeline>> GenerateQ1() const;

  // Query 3.
  std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> GenerateQ3Pipeline1(
      const size_t partition_count, const std::vector<ObjectReference>& input_objects) const;
  std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> GenerateQ3Pipeline2(
      const size_t partition_count, const std::vector<ObjectReference>& input_objects) const;
  std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> GenerateQ3Pipeline3(
      const size_t partition_count, const std::vector<ObjectReference>& input_objects) const;
  std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> GenerateQ3Pipeline4(
      const size_t partition_count, const std::vector<ObjectReference>& input_objects_left,
      const std::vector<ObjectReference>& input_objects_right) const;
  std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> GenerateQ3Pipeline5(
      const size_t partition_count, const std::vector<ObjectReference>& input_objects_left,
      const std::vector<ObjectReference>& input_objects_right) const;
  std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> GenerateQ3Pipeline6(
      const std::vector<ObjectReference>& input_objects) const;
  std::vector<std::shared_ptr<PqpPipeline>> GenerateQ3() const;

  // Query 5.
  // Region scan + Nation Scan + Join + Supplier Scan + Join.
  PipelineData GenerateQ5Pipeline1(const size_t partitions_count,
                                   const std::vector<ObjectReference>& region_input_objects,
                                   const std::vector<ObjectReference>& nation_input_objects,
                                   const std::vector<ObjectReference>& supplier_input_objects) const;
  // Partition lineitems.
  PipelineData GenerateQ5Pipeline2(const size_t partition_count,
                                   const std::vector<ObjectReference>& input_objects) const;
  // Join suppliers with lineitems.
  PipelineData GenerateQ5Pipeline3(const size_t partition_count,
                                   const std::vector<ObjectReference>& supplier_input_objects,
                                   const std::vector<ObjectReference>& lineitem_input_objects,
                                   const std::vector<ColumnId>& supplier_column_indices,
                                   const std::vector<ColumnId>& lineitem_column_indices) const;
  // Scan, filter and partition orders.
  PipelineData GenerateQ5Pipeline4(const size_t partition_count,
                                   const std::vector<ObjectReference>& input_objects) const;
  // Join orders with lineitems.
  PipelineData GenerateQ5Pipeline5(const size_t partition_count,
                                   const std::vector<ObjectReference>& supplier_input_objects,
                                   const std::vector<ObjectReference>& lineitem_input_objects) const;
  // Scan and partition customers.
  PipelineData GenerateQ5Pipeline6(const size_t partition_count,
                                   const std::vector<ObjectReference>& input_objects) const;
  // Join customers with lineitems.
  PipelineData GenerateQ5Pipeline7(size_t partition_count, const std::vector<ObjectReference>& customer_input_objects,
                                   const std::vector<ObjectReference>& lineitem_input_objects,
                                   const std::vector<ColumnId>& customer_column_indices,
                                   const std::vector<ColumnId>& lineitem_column_indices) const;
  // Final aggregation.
  PipelineData GenerateQ5Pipeline8(const std::vector<ObjectReference>& input_objects) const;
  std::vector<std::shared_ptr<PqpPipeline>> GenerateQ5() const;

  // Query 6.
  std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> GenerateQ6Pipeline1(
      const std::vector<ObjectReference>& input_objects) const;
  std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> GenerateQ6Pipeline2(
      const std::vector<ObjectReference>& input_objects) const;
  std::vector<std::shared_ptr<PqpPipeline>> GenerateQ6() const;

  // Query 12.
  PipelineData GenerateQ12Pipeline1(const size_t partition_count,
                                    const std::vector<ObjectReference>& input_objects) const;
  PipelineData GenerateQ12Pipeline2(const size_t partition_count,
                                    const std::vector<ObjectReference>& input_objects) const;
  PipelineData GenerateQ12Pipeline3(const size_t partition_count,
                                    const std::vector<ObjectReference>& input_objects_left,
                                    const std::vector<ObjectReference>& input_objects_right,
                                    const std::vector<ColumnId>& left_column_indices,
                                    const std::vector<ColumnId>& right_column_indices) const;
  PipelineData GenerateQ12Pipeline4(const size_t partition_count, const std::vector<ObjectReference>& input_objects,
                                    const std::vector<ColumnId>& right_column_indices) const;
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
                    /* The number of partitions applied in any shuffle operation. If empty, then the hardcoded values will be used. */,
                    const Aws::Utils::Json::JsonValue& join_configuration = Aws::Utils::Json::JsonValue()
                    /* Contains the join algorithm for all join operators of the query. OR can be an empty json object,
                     * in which case the partitioned hash join algorithm will be applied everywhere. */);

  std::shared_ptr<AbstractCompiler> GenerateCompiler() const final;
  bool operator==(const TpchPqpGeneratorConfig& other) const;

 private:
  const std::optional<size_t> stage_1_partitions_per_worker_count_;
  const std::optional<size_t> shuffle_partitions_count_;
  const Aws::Utils::Json::JsonValue join_configuration_;
};

}  // namespace skyrise
