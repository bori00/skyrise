#include "abstract_compiler.hpp"

#include <magic_enum/magic_enum.hpp>

#include "compiler/pqp_generator/etl_pqp_generator.hpp"
#include "compiler/pqp_generator/tpch_pqp_generator.hpp"
#include "utils/time.hpp"

namespace skyrise {

AbstractCompiler::AbstractCompiler(const QueryId& query_id, const ScaleFactor& scale_factor,
                                   const ObjectReference& shuffle_storage)
    : query_id_(query_id), scale_factor_(scale_factor), shuffle_storage_(shuffle_storage) {}

AbstractCompilerConfig::AbstractCompilerConfig(const CompilerName& compiler_name, const QueryId& query_id,
                                               const ScaleFactor& scale_factor, const ObjectReference& shuffle_storage)
    : compiler_name_(compiler_name),
      query_id_(query_id),
      scale_factor_(scale_factor),
      shuffle_storage_(shuffle_storage) {}

std::shared_ptr<AbstractCompilerConfig> AbstractCompilerConfig::FromJson(const Aws::Utils::Json::JsonView& json) {
  const CompilerName compiler_name =
      magic_enum::enum_cast<CompilerName>(json.GetString(kCoordinatorRequestCompilerNameAttribute)).value();
  const QueryId query_id =
      magic_enum::enum_cast<QueryId>(json.GetString(kCoordinatorRequestQueryPlanAttribute)).value();
  const ScaleFactor scale_factor =
      magic_enum::enum_cast<ScaleFactor>(json.GetString(kCoordinatorRequestScaleFactorAttribute)).value();
  const ObjectReference shuffle_storage =
      ObjectReference::FromJson(json.GetObject(kCoordinatorRequestShuffleStorageAttribute));
  const Aws::Utils::Json::JsonValue& join_configuration =
      json.GetObject(kCoordinatorRequestJoinConfigurationAttribute).Materialize();
  if (join_configuration.View().KeyExists("pipeline3")) {
    AWS_LOGSTREAM_INFO(kCoordinatorTag.c_str(), "FromJson Has key")
  } else {
    AWS_LOGSTREAM_INFO(kCoordinatorTag.c_str(), "FromJson No key")
  }
  const std::optional<size_t> stage_1_partitions_per_worker_count =
      json.KeyExists(kCoordinatorRequestStage1PartitionsPerWorkerCountAttribute)
          ? std::optional<size_t>(json.GetInt64(kCoordinatorRequestStage1PartitionsPerWorkerCountAttribute))
          : std::nullopt;
  const std::optional<int> shuffle_partitions_count =
      json.KeyExists(kCoordinatorRequestShufflePartitionsCountAttribute)
          ? std::optional<size_t>(json.GetInt64(kCoordinatorRequestShufflePartitionsCountAttribute))
          : std::nullopt;

  switch (compiler_name) {
    case CompilerName::kEtl:
      return std::make_shared<EtlPqpGeneratorConfig>(compiler_name, query_id, scale_factor, shuffle_storage);
    case CompilerName::kProcessMining:
      Fail("PQP generator for process mining queries is not implemented.");
    case CompilerName::kSql:
      Fail("Compiler for SQL queries is not implemented.");
    case CompilerName::kTpch:
      return std::make_shared<TpchPqpGeneratorConfig>(compiler_name, query_id, scale_factor, shuffle_storage,
                                                      stage_1_partitions_per_worker_count, shuffle_partitions_count,
                                                      join_configuration);
    case CompilerName::kTpcxbb:
      Fail("PQP generator for TPCx-BB queries is not implemented.");
    default:
      Fail("Unknown compiler configuration.");
  }
}

// TODO(fbori): the ToJson should be overwritten for the subclasses.
Aws::Utils::Json::JsonValue AbstractCompilerConfig::ToJson() const {
  Aws::Utils::Json::JsonValue serialized_compiler_config;
  serialized_compiler_config
      .WithString(kCoordinatorRequestCompilerNameAttribute, std::string(magic_enum::enum_name(compiler_name_)))
      .WithString(kCoordinatorRequestQueryPlanAttribute, std::string(magic_enum::enum_name(query_id_)))
      .WithString(kCoordinatorRequestScaleFactorAttribute, std::string(magic_enum::enum_name(scale_factor_)))
      .WithObject(kCoordinatorRequestShuffleStorageAttribute, shuffle_storage_.ToJson());

  return serialized_compiler_config;
}

}  // namespace skyrise
