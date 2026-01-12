#include "tpch_pqp_generator.hpp"

#include <ranges>

#include <boost/parameter/aux_/pack/parameter_requirements.hpp>
#include <magic_enum/magic_enum.hpp>

#include "compiler/abstract_compiler.hpp"
#include "compiler/physical_query_plan/operator_proxy/aggregate_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/alias_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/export_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/filter_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/join_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/limit_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/partition_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/projection_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/sort_operator_proxy.hpp"
#include "compiler/physical_query_plan/pqp_pipeline.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "storage/backend/s3_utils.hpp"
#include "storage/formats/csv_reader.hpp"
#include "utils/pipeline_generator.hpp"

namespace skyrise {

TpchPqpGenerator::TpchPqpGenerator(const QueryId& query_id, const ScaleFactor& scale_factor,
                                   const ObjectReference& shuffle_storage_prefix,
                                   const std::optional<size_t> stage_1_partitions_per_worker_count,
                                   const std::optional<size_t> shuffle_partitions_count,
                                   const Aws::Utils::Json::JsonValue& join_configuration)
    : AbstractCompiler(query_id, scale_factor, shuffle_storage_prefix),
      stage_1_partitions_per_worker_count_(stage_1_partitions_per_worker_count),
      shuffle_partitions_count_(shuffle_partitions_count),
      join_configuration_(join_configuration) {}

std::vector<std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GeneratePqp() const {
  std::vector<std::shared_ptr<PqpPipeline>> result;
  switch (query_id_) {
    case QueryId::kTpchQ1:
      result = GenerateQ1();
      break;
    case QueryId::kTpchQ3:
      result = GenerateQ3();
      break;
    case QueryId::kTpchQ5:
      result = GenerateQ5();
      break;
    case QueryId::kTpchQ6:
      result = GenerateQ6();
      break;
    case QueryId::kTpchQ12:
      result = GenerateQ12();
      break;
    default:
      Fail("Unknown query.");
  }
  return result;
}

std::vector<ObjectReference> TpchPqpGenerator::ListTableObjects(const std::string& table_name,
                                                                const FileFormat& import_format) const {
  std::string scale_factor_infix;
  switch (scale_factor_) {
    case ScaleFactor::kSf1: {
      scale_factor_infix = "1";
    } break;
    case ScaleFactor::kSf10: {
      scale_factor_infix = "10";
    } break;
    case ScaleFactor::kSf100: {
      scale_factor_infix = "100";
    } break;
    case ScaleFactor::kSf1000: {
      scale_factor_infix = "1000";
    }
  }
  const std::string table_prefix = std::string("tpc-h/standard/") + GetFormatName(import_format) + "/sf" +
                                   scale_factor_infix + "/" + table_name + "/";

  // TODO(tobodner): Use base client for this.
  const auto s3_client = std::make_shared<Aws::S3::S3Client>();
  S3Storage s3_storage(s3_client, kS3BenchmarkDatasetsBucket);
  const auto outcome = s3_storage.List(table_prefix);
  Assert(!outcome.second.IsError(), outcome.second.GetMessage());

  std::vector<ObjectReference> objects;
  objects.reserve(outcome.first.size());
  for (const auto& object : outcome.first) {
    objects.emplace_back(kS3BenchmarkDatasetsBucket, object.GetIdentifier(), object.GetChecksum());
  }
  return objects;
}

std::vector<ObjectReference> TpchPqpGenerator::GenerateOutputObjectIds(size_t count, const std::string& prefix,
                                                                       const FileFormat export_format) const {
  std::vector<ObjectReference> object_ids;
  object_ids.reserve(count);
  while (count-- > 0) {
    const std::string object_id =
        shuffle_storage_.identifier + "/" + prefix + "/" + RandomString(8) + GetFormatExtension(export_format);
    object_ids.emplace_back(shuffle_storage_.bucket_name, object_id);
  }
  return object_ids;
}

std::shared_ptr<PqpPipeline> TpchPqpGenerator::GeneratePipeline(const std::string& pipeline_id,
                                                                const std::string& import_id,
                                                                const std::shared_ptr<AbstractOperatorProxy>& pqp,
                                                                const FileFormat& export_format,
                                                                std::vector<ObjectReference> input_objects,
                                                                std::vector<ObjectReference> output_objects) {
  std::vector<std::unordered_map<std::string, std::vector<ObjectReference>>> worker_input_mappings;
  worker_input_mappings.resize(output_objects.size());
  std::string input_id;
  input_id.append(pipeline_id).append("-").append(import_id);
  for (size_t i = 0; i < input_objects.size(); ++i) {
    worker_input_mappings[i % worker_input_mappings.size()][input_id].push_back(input_objects[i]);
  }

  auto pipeline = std::make_shared<PqpPipeline>(pipeline_id, pqp);
  for (size_t i = 0; i < output_objects.size(); ++i) {
    const PipelineFragmentDefinition fragment(worker_input_mappings[i], output_objects[i], export_format);
    pipeline->AddFragmentDefinition(fragment);
  }

  return pipeline;
}

size_t TpchPqpGenerator::GetStage1PartitionsPerWorkerCount() const {
  if (stage_1_partitions_per_worker_count_.has_value()) {
    return stage_1_partitions_per_worker_count_.value();
  }
  switch (query_id_) {
    case QueryId::kTpchQ1:
      return 1;
    case QueryId::kTpchQ3:
      return 1;
    case QueryId::kTpchQ5:
      return 1;
    case QueryId::kTpchQ6:
      return 5;
    case QueryId::kTpchQ12:
      return 3;
    default:
      Fail("Unknown query.");
  }
}

size_t TpchPqpGenerator::GetShufflePartitionsCount() const {
  if (shuffle_partitions_count_.has_value()) {
    return shuffle_partitions_count_.value();
  }
  switch (query_id_) {
    case QueryId::kTpchQ1:
      return 1;
    case QueryId::kTpchQ3:
      return 5;
    case QueryId::kTpchQ5:
      return 1;
    case QueryId::kTpchQ6:
      return 1;
    case QueryId::kTpchQ12:
      switch (scale_factor_) {
        case ScaleFactor::kSf1: {
          return 3;
        } break;
        case ScaleFactor::kSf10: {
          return 10;
        } break;
        case ScaleFactor::kSf100: {
          return 100;
        } break;
        case ScaleFactor::kSf1000: {
          return 1000;
        }
        default:
          Fail("Unknown scale factor.");
      }
    default:
      Fail("Unknown query.");
  }
}

JoinAlgorithm TpchPqpGenerator::GetJoinAlgorithmForPipeline(const std::string pipeline_id) const {
  if (join_configuration_.View().KeyExists(pipeline_id)) {
    return StringToJoinAlgorithm(join_configuration_.View().GetString(pipeline_id))
        .value_or(JoinAlgorithm::kBroadcastHashJoin);
  } else {
    return JoinAlgorithm::kBroadcastHashJoin;
  }
}

void TpchPqpGenerator::SetAsPredecessorOf(std::optional<std::shared_ptr<PqpPipeline>> predecessor,
                                          std::optional<std::shared_ptr<PqpPipeline>> successor) {
  if (predecessor.has_value() && successor.has_value()) {
    predecessor.value()->SetAsPredecessorOf(successor.value());
  }
}

std::vector<std::shared_ptr<PqpPipeline>> TpchPqpGenerator::FilterNonEmptyPipelines(
    const std::vector<std::optional<std::shared_ptr<PqpPipeline>>>& pipelines) {
  auto pipelines_range = pipelines | std::views::filter([](const auto& opt) { return opt.has_value(); }) |
                         std::views::transform([](const auto& opt) { return opt.value(); });
  std::vector<std::shared_ptr<PqpPipeline>> result;
  result.reserve(pipelines.size());
  std::ranges::copy(pipelines_range, std::back_inserter(result));
  return result;
}

std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ1Pipeline1(
    const std::vector<ObjectReference>& input_objects) const {
  using namespace expression_functional;

  // Read lineitems
  // Column 4: quantity. Column 5: extendedprice, Column 6: discount. Column 7: tax. Column 8: returnflag. Column 9:
  // linestatus. Column 10: shipdate.
  const std::string import_id = "import";
  const auto import_operator = std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{},
                                                                     std::vector<ColumnId>{4, 5, 6, 8, 9, 10, 7});
  import_operator->SetIdentity(import_id);

  // Filter based on l_shipdate
  // Column 0: quantity. Column 1: extendedprice, Column 2: discount. Column 3: returnflag. Column 4: linestatus. Column
  // 5: shipdate. Column 6: tax.
  const auto predicate1 = std::make_shared<BinaryPredicateExpression>(
      PredicateCondition::kLessThanEquals, PqpColumn_(5, DataType::kString, false, "l_shipdate"), Value_("1998-09-02"));
  const auto filter_operator1 = std::make_shared<FilterOperatorProxy>(predicate1);
  filter_operator1->SetLeftInput(import_operator);

  const std::vector<std::shared_ptr<AbstractExpression>> expressions{
      Mul_(PqpColumn_(1, DataType::kFloat, false, "l_extendedprice"),
           Sub_(Value_(1), PqpColumn_(2, DataType::kFloat, false, "l_discount"))),
      // New Column 1: l_quantity
      PqpColumn_(0, DataType::kFloat, false, "l_quantity"),
      // New Column 2: l_extendedprice
      PqpColumn_(1, DataType::kFloat, false, "l_extendedprice"),
      // New Column 3: l_discount
      PqpColumn_(2, DataType::kFloat, false, "l_discount"),
      // New Column 4: l_returnflag (Grouping Column)
      PqpColumn_(3, DataType::kString, false, "l_returnflag"),
      // New Column 5: l_linestatus (Grouping Column)
      PqpColumn_(4, DataType::kString, false, "l_linestatus"),
      Mul_(PqpColumn_(1, DataType::kFloat, false, "l_extendedprice"),
           Mul_(Sub_(Value_(1), PqpColumn_(2, DataType::kFloat, false, "l_discount")),
                Add_(Value_(1), PqpColumn_(6, DataType::kFloat, false, "l_tax"))))};
  const auto projection_operator = std::make_shared<ProjectionOperatorProxy>(expressions);
  projection_operator->SetLeftInput(filter_operator1);

  // Column 0: l_extendedprice * (1 - l_discount). Column 1: quantity. Column 2: extendedprice, Column 3: discount.
  // Column 4: returnflag. Column 5: linestatus. Column 6: charge
  std::vector<std::shared_ptr<AbstractExpression>> aggregates = {
      Sum_(PqpColumn_(0, DataType::kFloat, false, "disc_price")),
      Sum_(PqpColumn_(1, DataType::kFloat, false, "l_quantity")),
      Sum_(PqpColumn_(2, DataType::kFloat, false, "l_extendedprice")),
      Sum_(PqpColumn_(3, DataType::kFloat, false, "l_discount")),
      Count_(PqpColumn_(kInvalidColumnId, DataType::kFloat, false, "*")),  // invalid column for count(*)
      Sum_(PqpColumn_(6, DataType::kFloat, false, "charge"))};
  const std::vector<ColumnId> groupby_column_ids{4, 5};  // l_returnflag, l_linestatus
  const auto aggregate_operator = std::make_shared<AggregateOperatorProxy>(groupby_column_ids, aggregates);
  aggregate_operator->SetLeftInput(projection_operator);

  const auto export_operator =
      std::make_shared<ExportOperatorProxy>(ObjectReference("mock", "mock"), kIntermediateResultsExportFormat);
  export_operator->SetLeftInput(aggregate_operator);

  const std::string pipeline_id = "pipeline-1";
  const size_t stage_1_partitions_per_worker_count = GetStage1PartitionsPerWorkerCount();
  const size_t worker_count = (input_objects.size() / stage_1_partitions_per_worker_count) +
                              (input_objects.size() % stage_1_partitions_per_worker_count);
  auto output_objects = GenerateOutputObjectIds(worker_count, pipeline_id, kIntermediateResultsExportFormat);
  return {output_objects, GeneratePipeline(pipeline_id, import_id, export_operator, kIntermediateResultsExportFormat,
                                           input_objects, output_objects)};
}

std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ1Pipeline2(
    const std::vector<ObjectReference>& input_objects) const {
  using namespace expression_functional;
  // Read partial aggregates
  // Column 0: returnflag. Column 1: linestatus, Column 2: sum(disc price). Column 3: sum(quantity). Column 4:
  // sum(extendedprice). Column 5: sum(discount). Column 6: count(*). Column 7: sum(charge)
  const std::string import_id = "import";
  const auto import_operator = std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{},
                                                                     std::vector<ColumnId>{0, 1, 2, 3, 4, 5, 6, 7});
  import_operator->SetIdentity(import_id);

  // Aggregate the partial aggregates
  std::vector<std::shared_ptr<AbstractExpression>> aggregates = {
      Sum_(PqpColumn_(2, DataType::kDouble, false, "sum_disc_price")),
      Sum_(PqpColumn_(3, DataType::kDouble, false, "sum_qty")),
      Sum_(PqpColumn_(4, DataType::kDouble, false, "sum_extendedprice")),
      Sum_(PqpColumn_(6, DataType::kLong, false, "count")),
      Sum_(PqpColumn_(7, DataType::kDouble, false, "charge")),
      Sum_(PqpColumn_(5, DataType::kDouble, false, "discount")),
  };
  const std::vector<ColumnId> groupby_column_ids{0, 1};  // l_returnflag, l_linestatus
  const auto aggregate_operator = std::make_shared<AggregateOperatorProxy>(groupby_column_ids, aggregates);
  aggregate_operator->SetLeftInput(import_operator);

  // Get the averages as Sum / Count.
  const std::vector<std::shared_ptr<AbstractExpression>> expressions{
      PqpColumn_(0, DataType::kString, false, "l_returnflag"),
      PqpColumn_(1, DataType::kString, false, "l_linestatus"),
      PqpColumn_(2, DataType::kDouble, false, "sum_disc_price"),
      PqpColumn_(3, DataType::kDouble, false, "sum_qty"),
      PqpColumn_(4, DataType::kDouble, false, "sum_price"),
      PqpColumn_(5, DataType::kLong, false, "count"),
      PqpColumn_(6, DataType::kDouble, false, "sum_charge"),
      Div_(PqpColumn_(3, DataType::kDouble, false, "sum_qty"), PqpColumn_(5, DataType::kLong, false, "count")),
      Div_(PqpColumn_(4, DataType::kDouble, false, "sum_price"), PqpColumn_(5, DataType::kLong, false, "count")),
      Div_(PqpColumn_(7, DataType::kDouble, false, "sum_disc"), PqpColumn_(5, DataType::kLong, false, "count"))};
  const auto projection_operator = std::make_shared<ProjectionOperatorProxy>(expressions);
  projection_operator->SetLeftInput(aggregate_operator);

  // Sort by returnflag and lineitem.
  const auto sort_operator = std::make_shared<SortOperatorProxy>(
      std::vector<SortColumnDefinition>{SortColumnDefinition(0), SortColumnDefinition(1)});
  sort_operator->SetLeftInput(projection_operator);

  // Rename columns
  const std::vector<ColumnId>& column_ids_alias = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  const std::vector<std::string>& aliases = {"l_returnflag",   "l_linestatus", "sum_disc_price", "sum_qty",
                                             "sum_base_price", "count_order",  "sum_charge",     "avg_qty",
                                             "avg_price",      "avg_disc"};
  const auto alias_operator = std::make_shared<AliasOperatorProxy>(column_ids_alias, aliases);
  alias_operator->SetLeftInput(sort_operator);

  const auto export_operator = std::make_shared<ExportOperatorProxy>(ObjectReference("mock", "mock"), FileFormat::kCsv);
  export_operator->SetLeftInput(alias_operator);

  const std::string pipeline_id = "pipeline-2";
  auto output_objects = GenerateOutputObjectIds(1, pipeline_id, FileFormat::kCsv);
  return {output_objects,
          GeneratePipeline(pipeline_id, import_id, export_operator, FileFormat::kCsv, input_objects, output_objects)};
}

// select
//   l_returnflag,
//   l_linestatus,
//   sum(l_quantity) as sum_qty,
//   sum(l_extendedprice) as sum_base_price,
//   sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
//   sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
//   avg(l_quantity) as avg_qty,
//   avg(l_extendedprice) as avg_price,
//   avg(l_discount) as avg_disc,
//   count(*) as count_order
// from
//   lineitem
// where
//   l_shipdate <= date '1998-12-01' - interval '90' day
// group by
//   l_returnflag,
//   l_linestatus
// order by
//   l_returnflag,
//   l_linestatus;
std::vector<std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ1() const {
  const auto pipeline1 = GenerateQ1Pipeline1(ListTableObjects("lineitem", FileFormat::kParquet));
  const auto pipeline2 = GenerateQ1Pipeline2(pipeline1.first);

  pipeline1.second->SetAsPredecessorOf(pipeline2.second);

  return {pipeline1.second, pipeline2.second};
}

std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ3Pipeline1(
    const size_t partition_count, const std::vector<ObjectReference>& input_objects) const {
  using namespace expression_functional;
  const std::string left_import_id = "left_import";
  // Column 0: orderkey. Column 1: custkey. Column 4: orderdate. Column 7: shippriority
  const auto import_operator =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0, 1, 4, 7});
  import_operator->SetIdentity(left_import_id);

  // Filter based on o_orderdate
  const auto predicate1 = std::make_shared<BinaryPredicateExpression>(
      PredicateCondition::kLessThan, PqpColumn_(2, DataType::kString, false, "o_orderdate"), Value_("1995-03-15"));
  const auto filter_operator1 = std::make_shared<FilterOperatorProxy>(predicate1);
  filter_operator1->SetLeftInput(import_operator);

  std::shared_ptr<ExportOperatorProxy> export_operator =
      std::make_shared<ExportOperatorProxy>(ObjectReference("mock", "mock"), kIntermediateResultsExportFormat);
  if (GetJoinAlgorithmForPipeline("pipeline-4") == JoinAlgorithm::kPartitionedHashJoin) {
    // Partition by custkey.
    const auto partition_operator = std::make_shared<PartitionOperatorProxy>(
        std::make_shared<HashPartitioningFunction>(std::set<ColumnId>{1}, partition_count), false);
    partition_operator->SetLeftInput(filter_operator1);
    export_operator->SetLeftInput(partition_operator);
  } else {
    export_operator->SetLeftInput(filter_operator1);
  }

  const std::string pipeline_id = "pipeline-1";
  const size_t stage_1_partitions_per_worker_count = GetStage1PartitionsPerWorkerCount();
  const size_t worker_count = (input_objects.size() / stage_1_partitions_per_worker_count) +
                              (input_objects.size() % stage_1_partitions_per_worker_count);
  auto output_objects = GenerateOutputObjectIds(worker_count, pipeline_id, kIntermediateResultsExportFormat);
  const auto pipeline1 = GeneratePipeline(pipeline_id, left_import_id, export_operator,
                                          kIntermediateResultsExportFormat, input_objects, output_objects);
  return {output_objects, pipeline1};
}

std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ3Pipeline2(
    const size_t partition_count, const std::vector<ObjectReference>& input_objects) const {
  using namespace expression_functional;
  const std::string left_import_id = "left_import";
  // Column 0: custkey. Column 6: mktsegment.
  const auto import_operator =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0, 6});
  import_operator->SetIdentity(left_import_id);

  // Filter based on mktsegment
  const auto predicate1 = std::make_shared<BinaryPredicateExpression>(
      PredicateCondition::kEquals, PqpColumn_(1, DataType::kString, false, "c_mktsegment"), Value_("BUILDING"));
  const auto filter_operator1 = std::make_shared<FilterOperatorProxy>(predicate1);
  filter_operator1->SetLeftInput(import_operator);

  // Keep custkey only.
  const std::vector<std::shared_ptr<AbstractExpression>> expressions{PqpColumn_(0, DataType::kInt, false, "c_custkey")};
  const auto projection_operator = std::make_shared<ProjectionOperatorProxy>(expressions);
  projection_operator->SetLeftInput(filter_operator1);

  std::shared_ptr<ExportOperatorProxy> export_operator =
      std::make_shared<ExportOperatorProxy>(ObjectReference("mock", "mock"), kIntermediateResultsExportFormat);
  if (GetJoinAlgorithmForPipeline("pipeline-4") == JoinAlgorithm::kPartitionedHashJoin) {
    // Partition by custkey.
    const auto partition_operator = std::make_shared<PartitionOperatorProxy>(
        std::make_shared<HashPartitioningFunction>(std::set<ColumnId>{0}, partition_count), false);
    partition_operator->SetLeftInput(projection_operator);
    export_operator->SetLeftInput(partition_operator);
  } else {
    export_operator->SetLeftInput(projection_operator);
  }

  const std::string pipeline_id = "pipeline-2";
  const size_t stage_1_partitions_per_worker_count = GetStage1PartitionsPerWorkerCount();
  const size_t worker_count = (input_objects.size() / stage_1_partitions_per_worker_count) +
                              (input_objects.size() % stage_1_partitions_per_worker_count);
  auto output_objects = GenerateOutputObjectIds(worker_count, pipeline_id, kIntermediateResultsExportFormat);
  const auto pipeline2 = GeneratePipeline(pipeline_id, left_import_id, export_operator,
                                          kIntermediateResultsExportFormat, input_objects, output_objects);
  return {output_objects, pipeline2};
}

std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ3Pipeline3(
    const size_t partition_count, const std::vector<ObjectReference>& input_objects) const {
  using namespace expression_functional;
  const std::string left_import_id = "left_import";
  // Column 0: orderkey. Column 5: extendedprice. Column 6: discount. Column 10: shipdate.
  const auto import_operator =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0, 5, 6, 10});
  import_operator->SetIdentity(left_import_id);

  // Filter based on l_shipdate
  const auto predicate1 = std::make_shared<BinaryPredicateExpression>(
      PredicateCondition::kGreaterThan, PqpColumn_(3, DataType::kString, false, "l_shipdate"), Value_("1995-03-15"));
  const auto filter_operator1 = std::make_shared<FilterOperatorProxy>(predicate1);
  filter_operator1->SetLeftInput(import_operator);

  // Keep only relevant columns
  const std::vector<std::shared_ptr<AbstractExpression>> expressions{
      PqpColumn_(0, DataType::kInt, false, "o_orderkey"), PqpColumn_(1, DataType::kFloat, false, "extendedprice"),
      PqpColumn_(2, DataType::kFloat, false, "discount")};
  const auto projection_operator = std::make_shared<ProjectionOperatorProxy>(expressions);
  projection_operator->SetLeftInput(filter_operator1);

  std::shared_ptr<ExportOperatorProxy> export_operator =
      std::make_shared<ExportOperatorProxy>(ObjectReference("mock", "mock"), kIntermediateResultsExportFormat);
  if (GetJoinAlgorithmForPipeline("pipeline-5") == JoinAlgorithm::kPartitionedHashJoin) {
    // Partition by orderkey.
    const auto partition_operator = std::make_shared<PartitionOperatorProxy>(
        std::make_shared<HashPartitioningFunction>(std::set<ColumnId>{0}, partition_count), false);
    partition_operator->SetLeftInput(projection_operator);
    export_operator->SetLeftInput(partition_operator);
  } else {
    export_operator->SetLeftInput(projection_operator);
  }

  const std::string pipeline_id = "pipeline-3";
  const size_t stage_1_partitions_per_worker_count = GetStage1PartitionsPerWorkerCount();
  const size_t worker_count = (input_objects.size() / stage_1_partitions_per_worker_count) +
                              (input_objects.size() % stage_1_partitions_per_worker_count);
  auto output_objects = GenerateOutputObjectIds(worker_count, pipeline_id, kIntermediateResultsExportFormat);
  const auto pipeline3 = GeneratePipeline(pipeline_id, left_import_id, export_operator,
                                          kIntermediateResultsExportFormat, input_objects, output_objects);
  return {output_objects, pipeline3};
}

std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ3Pipeline4(
    const size_t partition_count, const std::vector<ObjectReference>& input_objects_left,
    const std::vector<ObjectReference>& input_objects_right) const {
  // NOLINTNEXTLINE(google-build-using-namespace)
  using namespace expression_functional;

  // The in-memory hashtable is built from the left-input, so we set the left input to be the smaller one, namely the
  // filtered customers. Table: customers.
  const std::string left_import_id = "left_import";
  auto import_operator_left =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0});
  import_operator_left->SetIdentity(left_import_id);

  // Table: orders. Column 0: orderkey. Column 1: custkey. Column 2: orderdate. Column 3: shippriority
  const std::string right_import_id = "right_import";
  auto import_operator_right =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0, 1, 2, 3});
  import_operator_right->SetIdentity(right_import_id);

  std::vector<std::shared_ptr<JoinOperatorPredicate>> empty_secondary_predicates;
  const auto predicate = std::make_shared<JoinOperatorPredicate>(JoinOperatorPredicate{
      .column_id_left = 0, .column_id_right = 1, .predicate_condition = PredicateCondition::kEquals});
  const auto join_operator = std::make_shared<JoinOperatorProxy>(JoinMode::kInner, predicate,
                                                                 empty_secondary_predicates, OperatorType::kHashJoin);
  join_operator->SetLeftInput(import_operator_left);
  join_operator->SetRightInput(import_operator_right);

  // Keep only the relevant columns.
  // Input: Column 0: c_custkey --> dropped. Column 2 : o_custkey --> dropped.
  const std::vector<std::shared_ptr<AbstractExpression>> expressions{
      PqpColumn_(1, DataType::kInt, false, "o_orderkey"), PqpColumn_(3, DataType::kString, false, "o_orderdate"),
      PqpColumn_(4, DataType::kInt, false, "o_shippriority")};
  const auto projection_operator = std::make_shared<ProjectionOperatorProxy>(expressions);
  projection_operator->SetLeftInput(join_operator);

  auto export_operator =
      std::make_shared<ExportOperatorProxy>(ObjectReference("mock", "mock"), kIntermediateResultsExportFormat);
  if (GetJoinAlgorithmForPipeline("pipeline-5") == JoinAlgorithm::kPartitionedHashJoin) {
    // Partition by orderkey.
    const auto partition_operator = std::make_shared<PartitionOperatorProxy>(
        std::make_shared<HashPartitioningFunction>(std::set<ColumnId>{0}, partition_count), false);
    partition_operator->SetLeftInput(projection_operator);
    export_operator->SetLeftInput(partition_operator);
  } else {
    export_operator->SetLeftInput(projection_operator);
  }

  const std::string pipeline_id = "pipeline-4";
  const auto output_objects = GenerateOutputObjectIds(partition_count, pipeline_id, kIntermediateResultsExportFormat);

  auto pipeline4 = std::make_shared<PqpPipeline>(pipeline_id, export_operator);

  std::string left_input_id;
  left_input_id.append(pipeline_id).append("-").append(left_import_id);
  std::string right_input_id;
  right_input_id.append(pipeline_id).append("-").append(right_import_id);

  pqp_utils::PipelineInput left_input =
      pqp_utils::PipelineInput(left_input_id,
                               GetJoinAlgorithmForPipeline(pipeline_id) == JoinAlgorithm::kBroadcastHashJoin
                                   ? pqp_utils::PipelineInput::InputShareType::kBroadcastedRead
                                   : pqp_utils::PipelineInput::InputShareType::kPartitionedRead,
                               input_objects_left, shuffle_storage_.bucket_name);
  pqp_utils::PipelineInput right_input =
      pqp_utils::PipelineInput(right_input_id,
                               GetJoinAlgorithmForPipeline(pipeline_id) == JoinAlgorithm::kBroadcastHashJoin
                                   ? pqp_utils::PipelineInput::InputShareType::kPartitionedByFileRead
                                   : pqp_utils::PipelineInput::InputShareType::kPartitionedRead,
                               input_objects_right, shuffle_storage_.bucket_name);
  std::vector<std::unordered_map<std::string, std::vector<ObjectReference>>> fragment_to_inputs =
      pqp_utils::BuildPipelineFragmentsInputsMap({left_input, right_input}, partition_count);

  for (size_t i = 0; i < partition_count; ++i) {
    const PipelineFragmentDefinition fragment(fragment_to_inputs[i], output_objects[i], FileFormat::kParquet);
    pipeline4->AddFragmentDefinition(fragment);
  }
  return {output_objects, pipeline4};
}

std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ3Pipeline5(
    const size_t partition_count, const std::vector<ObjectReference>& input_objects_left,
    const std::vector<ObjectReference>& input_objects_right) const {
  // NOLINTNEXTLINE(google-build-using-namespace)
  using namespace expression_functional;

  // The in-memory hashtable is built from the left-input, so we set the left input to be the smaller one, namely the
  // filtered orders from pipeline 4. Column 0: orderkey. Column 1: orderdate. Column 2: shippriority.
  const std::string left_import_id = "left_import";
  auto import_operator_left =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0, 1, 2});
  import_operator_left->SetIdentity(left_import_id);

  // Table: lineitems from pipeline 3. Column 0: orderkey. Column 1: extendedprice. Column 2: discount
  const std::string right_import_id = "right_import";
  auto import_operator_right =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0, 1, 2});
  import_operator_right->SetIdentity(right_import_id);

  // Join orders with lineitems.
  std::vector<std::shared_ptr<JoinOperatorPredicate>> empty_secondary_predicates;
  const auto predicate = std::make_shared<JoinOperatorPredicate>(JoinOperatorPredicate{
      .column_id_left = 0, .column_id_right = 0, .predicate_condition = PredicateCondition::kEquals});
  const auto join_operator = std::make_shared<JoinOperatorProxy>(JoinMode::kInner, predicate,
                                                                 empty_secondary_predicates, OperatorType::kHashJoin);
  join_operator->SetLeftInput(import_operator_left);
  join_operator->SetRightInput(import_operator_right);

  // Calculate l_extendedprice * (1 - l_discount)
  // Input: Column 0: o_orderkey. Column 1: o_orderdate. Column 2: o_shippriority. Column 3: l_orderkey. Column 4:
  // l_extendedprice. Column 5: l_discount.
  const std::vector<std::shared_ptr<AbstractExpression>> expressions{
      PqpColumn_(0, DataType::kInt, false, "o_orderkey"), PqpColumn_(1, DataType::kString, false, "o_orderdate"),
      PqpColumn_(2, DataType::kInt, false, "o_shoppriority"),
      Mul_(PqpColumn_(4, DataType::kFloat, false, "l_extendedprice"),
           Sub_(Value_(1), PqpColumn_(5, DataType::kFloat, false, "l_discount")))};
  const auto projection_operator = std::make_shared<ProjectionOperatorProxy>(expressions);
  projection_operator->SetLeftInput(join_operator);

  // Group by orderkey, orderdate and shippriority.
  std::vector<std::shared_ptr<AbstractExpression>> aggregates = {
      Sum_(PqpColumn_(3, DataType::kFloat, false, "revenue"))};
  const std::vector<ColumnId> groupby_column_ids{0, 1, 2};
  const auto aggregate_operator = std::make_shared<AggregateOperatorProxy>(groupby_column_ids, aggregates);
  aggregate_operator->SetLeftInput(projection_operator);

  // Order by revenue desc, o_orderdate.
  const auto sort_operator = std::make_shared<SortOperatorProxy>(std::vector<SortColumnDefinition>{
      SortColumnDefinition(3, SortMode::kDescending) /* revenue */, SortColumnDefinition(1) /* orderdate */});
  sort_operator->SetLeftInput(aggregate_operator);

  // TODO: implement a sort-limit operator
  // Keep the top 10 only
  const auto limit_operator = std::make_shared<LimitOperatorProxy>(Value_(int32_t{10}));
  limit_operator->SetLeftInput(sort_operator);

  auto export_operator =
      std::make_shared<ExportOperatorProxy>(ObjectReference("mock", "mock"), kIntermediateResultsExportFormat);
  export_operator->SetLeftInput(limit_operator);

  const std::string pipeline_id = "pipeline-5";
  const auto output_objects = GenerateOutputObjectIds(partition_count, pipeline_id, kIntermediateResultsExportFormat);

  auto pipeline5 = std::make_shared<PqpPipeline>(pipeline_id, export_operator);

  std::string left_input_id;
  left_input_id.append("pipeline-5").append("-").append(left_import_id);
  std::string right_input_id;
  right_input_id.append("pipeline-5").append("-").append(right_import_id);

  pqp_utils::PipelineInput left_input =
      pqp_utils::PipelineInput(left_input_id,
                               GetJoinAlgorithmForPipeline(pipeline_id) == JoinAlgorithm::kBroadcastHashJoin
                                   ? pqp_utils::PipelineInput::InputShareType::kBroadcastedRead
                                   : pqp_utils::PipelineInput::InputShareType::kPartitionedRead,
                               input_objects_left, shuffle_storage_.bucket_name);
  pqp_utils::PipelineInput right_input =
      pqp_utils::PipelineInput(right_input_id,
                               GetJoinAlgorithmForPipeline(pipeline_id) == JoinAlgorithm::kBroadcastHashJoin
                                   ? pqp_utils::PipelineInput::InputShareType::kPartitionedByFileRead
                                   : pqp_utils::PipelineInput::InputShareType::kPartitionedRead,
                               input_objects_right, shuffle_storage_.bucket_name);
  std::vector<std::unordered_map<std::string, std::vector<ObjectReference>>> fragment_to_inputs =
      pqp_utils::BuildPipelineFragmentsInputsMap({left_input, right_input}, partition_count);

  for (size_t i = 0; i < partition_count; ++i) {
    const PipelineFragmentDefinition fragment(fragment_to_inputs[i], output_objects[i], FileFormat::kParquet);
    pipeline5->AddFragmentDefinition(fragment);
  }
  return {output_objects, pipeline5};
}

std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ3Pipeline6(
    const std::vector<ObjectReference>& input_objects) const {
  using namespace expression_functional;

  // Column 0: orderkey, Column 1: orderdate. Column 2: shippriority. Column 3: revenue.
  const std::string left_import_id = "left_import";
  auto import_operator_left =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0, 1, 2, 3});
  import_operator_left->SetIdentity(left_import_id);

  // Order by revenue desc, o_orderdate.
  const auto sort_operator = std::make_shared<SortOperatorProxy>(std::vector<SortColumnDefinition>{
      SortColumnDefinition(3, SortMode::kDescending) /* revenue */, SortColumnDefinition(1) /* orderdate */});
  sort_operator->SetLeftInput(import_operator_left);

  // TODO: implement a sort-limit operator
  const auto limit_operator = std::make_shared<LimitOperatorProxy>(Value_(int32_t{10}));
  limit_operator->SetLeftInput(sort_operator);

  const auto alias_operator = std::make_shared<AliasOperatorProxy>(
      std::vector<ColumnId>{0, 1, 2, 3},
      std::vector<std::string>{"l_orderkey", "o_orderdate", "o_shippriority", "revenue"});
  alias_operator->SetLeftInput(limit_operator);

  auto export_operator =
      std::make_shared<ExportOperatorProxy>(ObjectReference("mock", "mock"), kFinalResultsExportFormat);
  export_operator->SetLeftInput(alias_operator);

  const std::string pipeline_id = "pipeline-6";
  auto output_objects = GenerateOutputObjectIds(1, pipeline_id, kFinalResultsExportFormat);
  const auto pipeline6 = GeneratePipeline(pipeline_id, left_import_id, export_operator, kFinalResultsExportFormat,
                                          input_objects, output_objects);

  return {output_objects, pipeline6};
}

std::vector<std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ3() const {
  size_t partition_count = GetShufflePartitionsCount();

  const auto pipeline1 = GenerateQ3Pipeline1(partition_count, ListTableObjects("orders", FileFormat::kParquet));
  const auto pipeline2 = GenerateQ3Pipeline2(partition_count, ListTableObjects("customer", FileFormat::kParquet));
  const auto pipeline3 = GenerateQ3Pipeline3(partition_count, ListTableObjects("lineitem", FileFormat::kParquet));

  // left input is expected to be smaller, for the internal hash table.
  const auto pipeline4 = GenerateQ3Pipeline4(partition_count, pipeline2.first, pipeline1.first);
  const auto pipeline5 = GenerateQ3Pipeline5(partition_count, pipeline4.first, pipeline3.first);

  const auto pipeline6 = GenerateQ3Pipeline6(pipeline5.first);

  pipeline1.second->SetAsPredecessorOf(pipeline4.second);
  pipeline2.second->SetAsPredecessorOf(pipeline4.second);
  pipeline4.second->SetAsPredecessorOf(pipeline5.second);
  pipeline3.second->SetAsPredecessorOf(pipeline5.second);
  pipeline5.second->SetAsPredecessorOf(pipeline6.second);

  return {pipeline1.second, pipeline2.second, pipeline3.second, pipeline4.second, pipeline5.second, pipeline6.second};
}

// Region scan + Nation Scan + Join + Supplier Scan + Join.
std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ5Pipeline1(
    size_t partitions_count, const std::vector<ObjectReference>& region_input_objects,
    const std::vector<ObjectReference>& nation_input_objects,
    const std::vector<ObjectReference>& supplier_input_objects) const {
  using namespace expression_functional;
  const std::string region_import_id = "left_import";
  // Column 0: regionkey. Column 1: name.
  const auto region_import_operator =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0, 1});
  region_import_operator->SetIdentity(region_import_id);

  // Filter regions based on name.
  const auto predicate1 = std::make_shared<BinaryPredicateExpression>(
      PredicateCondition::kEquals, PqpColumn_(1, DataType::kString, false, "r_name"), Value_("ASIA"));
  const auto region_filter_operator1 = std::make_shared<FilterOperatorProxy>(predicate1);
  region_filter_operator1->SetLeftInput(region_import_operator);

  const std::string nation_import_id = "right_import";
  // Column 0: nationkey. Column 1: name. Column 2: regionkey.
  const auto nation_import_operator =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0, 1, 2});
  nation_import_operator->SetIdentity(nation_import_id);

  // Join regions (left) with nations.
  std::vector<std::shared_ptr<JoinOperatorPredicate>> empty_secondary_predicates;
  const auto predicate = std::make_shared<JoinOperatorPredicate>(JoinOperatorPredicate{
      .column_id_left = 0, .column_id_right = 2, .predicate_condition = PredicateCondition::kEquals});
  const auto join_operator = std::make_shared<JoinOperatorProxy>(JoinMode::kInner, predicate,
                                                                 empty_secondary_predicates, OperatorType::kHashJoin);
  join_operator->SetLeftInput(region_filter_operator1);
  join_operator->SetRightInput(nation_import_operator);

  // Scan suppliers.
  const std::string supplier_import_id = "supplier_import";
  // Column 0: suppkey. Column 3: nationkey.
  const auto supplier_import_operator =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0, 3});
  supplier_import_operator->SetIdentity(supplier_import_id);

  // Join nations (left) with suppliers.
  // Left: Column 0: regionkey. Column 1: r_name. Column 2: n_nationkey. Column 3: n_name. Column 4: n_regionkey.
  const auto predicate2 = std::make_shared<JoinOperatorPredicate>(JoinOperatorPredicate{
      .column_id_left = 2, .column_id_right = 1, .predicate_condition = PredicateCondition::kEquals});
  const auto join_operator2 = std::make_shared<JoinOperatorProxy>(JoinMode::kInner, predicate2,
                                                                  empty_secondary_predicates, OperatorType::kHashJoin);
  join_operator2->SetLeftInput(join_operator);
  join_operator2->SetRightInput(supplier_import_operator);

  const std::vector<std::shared_ptr<AbstractExpression>> expressions{
      PqpColumn_(3, DataType::kString, false, "n_name"), PqpColumn_(5, DataType::kInt, false, "s_suppkey"),
      PqpColumn_(6, DataType::kInt, false, "s_nationkey")};
  const auto projection_operator = std::make_shared<ProjectionOperatorProxy>(expressions);
  projection_operator->SetLeftInput(join_operator2);

  // Partition by suppkey.
  const auto partition_operator = std::make_shared<PartitionOperatorProxy>(
      std::make_shared<HashPartitioningFunction>(std::set<ColumnId>{1}, partitions_count), false);
  partition_operator->SetLeftInput(projection_operator);

  std::shared_ptr<ExportOperatorProxy> export_operator =
      std::make_shared<ExportOperatorProxy>(ObjectReference("mock", "mock"), kIntermediateResultsExportFormat);
  export_operator->SetLeftInput(partition_operator);

  const std::string pipeline_id = "pipeline-1";
  const size_t worker_count = 1;
  auto output_objects = GenerateOutputObjectIds(worker_count, pipeline_id, kIntermediateResultsExportFormat);
  std::unordered_map<std::string, std::vector<ObjectReference>> worker_input_mappings;
  std::string region_input_id, nation_input_id, supplier_input_id;
  region_input_id.append(pipeline_id).append("-").append(region_import_id);
  nation_input_id.append(pipeline_id).append("-").append(nation_import_id);
  supplier_input_id.append(pipeline_id).append("-").append(supplier_import_id);
  for (size_t i = 0; i < region_input_objects.size(); ++i) {
    worker_input_mappings[region_input_id].push_back(region_input_objects[i]);
  }
  for (size_t i = 0; i < nation_input_objects.size(); ++i) {
    worker_input_mappings[nation_input_id].push_back(nation_input_objects[i]);
  }
  for (size_t i = 0; i < supplier_input_objects.size(); ++i) {
    worker_input_mappings[supplier_input_id].push_back(supplier_input_objects[i]);
  }
  auto pipeline1 = std::make_shared<PqpPipeline>(pipeline_id, export_operator);
  const PipelineFragmentDefinition fragment(worker_input_mappings, output_objects[0], kIntermediateResultsExportFormat);
  pipeline1->AddFragmentDefinition(fragment);

  return {output_objects, pipeline1};
}

// Scan and partition lineitems.
std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ5Pipeline2(
    const size_t partition_count, const std::vector<ObjectReference>& input_objects) const {
  // NOLINTNEXTLINE(google-build-using-namespace)
  using namespace expression_functional;

  // Column 0: orderkey. Column 2: suppkey. Column 5: extendedprice. Column 6: discount.
  const std::string left_import_id = "left_import";
  const auto import_operator =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0, 2, 5, 6});
  import_operator->SetIdentity(left_import_id);

  const auto partition_operator = std::make_shared<PartitionOperatorProxy>(
      std::make_shared<HashPartitioningFunction>(std::set<ColumnId>{1}, partition_count), false);
  partition_operator->SetLeftInput(import_operator);

  const auto export_operator =
      std::make_shared<ExportOperatorProxy>(ObjectReference("mock", "mock"), kIntermediateResultsExportFormat);

  export_operator->SetLeftInput(partition_operator);

  const std::string pipeline_id = "pipeline-2";
  const size_t stage_1_partitions_per_worker_count = GetStage1PartitionsPerWorkerCount();
  const size_t worker_count = (input_objects.size() / stage_1_partitions_per_worker_count) +
                              (input_objects.size() % stage_1_partitions_per_worker_count);
  auto output_objects = GenerateOutputObjectIds(worker_count, pipeline_id, kIntermediateResultsExportFormat);
  const auto pipeline2 = GeneratePipeline(pipeline_id, left_import_id, export_operator,
                                          kIntermediateResultsExportFormat, input_objects, output_objects);
  return {output_objects, pipeline2};
}

// Join suppliers with lineitems.
std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ5Pipeline3(
    size_t partition_count, const std::vector<ObjectReference>& supplier_input_objects,
    const std::vector<ObjectReference>& lineitem_input_objects) const {
  using namespace expression_functional;
  const std::string supplier_import_id = "left_import";
  // From pipeline 1. Column 0: n_name. Column 1: s_suppkey. Column 2: s_nationkey.
  const auto supplier_import_operator =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0, 1, 2});
  supplier_import_operator->SetIdentity(supplier_import_id);

  const std::string lineitem_import_id = "right_import";
  // Column 0: orderkey. Column 1: suppkey. Column 2: extendedprice. Column 3: discount.
  const auto lineitem_import_operator =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0, 1, 2, 3});
  lineitem_import_operator->SetIdentity(lineitem_import_id);

  // Join suppliers (left) with lineitems.
  std::vector<std::shared_ptr<JoinOperatorPredicate>> empty_secondary_predicates;
  const auto predicate = std::make_shared<JoinOperatorPredicate>(JoinOperatorPredicate{
      .column_id_left = 1, .column_id_right = 1, .predicate_condition = PredicateCondition::kEquals});
  const auto join_operator = std::make_shared<JoinOperatorProxy>(JoinMode::kInner, predicate,
                                                                 empty_secondary_predicates, OperatorType::kHashJoin);
  join_operator->SetLeftInput(supplier_import_operator);
  join_operator->SetRightInput(lineitem_import_operator);

  const std::vector<std::shared_ptr<AbstractExpression>> expressions{
      PqpColumn_(0, DataType::kString, false, "n_name"),         PqpColumn_(3, DataType::kInt, false, "l_orderkey"),
      PqpColumn_(5, DataType::kFloat, false, "l_extendedprice"), PqpColumn_(6, DataType::kFloat, false, "l_discount"),
      PqpColumn_(2, DataType::kInt, false, "n_nationkey"),
  };
  const auto projection_operator = std::make_shared<ProjectionOperatorProxy>(expressions);
  projection_operator->SetLeftInput(join_operator);

  // Partition by orderkey.
  const auto partition_operator = std::make_shared<PartitionOperatorProxy>(
      std::make_shared<HashPartitioningFunction>(std::set<ColumnId>{1}, partition_count), false);
  partition_operator->SetLeftInput(projection_operator);

  std::shared_ptr<ExportOperatorProxy> export_operator =
      std::make_shared<ExportOperatorProxy>(ObjectReference("mock", "mock"), kIntermediateResultsExportFormat);
  export_operator->SetLeftInput(partition_operator);

  const std::string pipeline_id = "pipeline-3";
  const auto output_objects = GenerateOutputObjectIds(partition_count, pipeline_id, kIntermediateResultsExportFormat);

  auto pipeline3 = std::make_shared<PqpPipeline>(pipeline_id, export_operator);

  std::string left_input_id;
  left_input_id.append("pipeline-3").append("-").append(supplier_import_id);
  std::string right_input_id;
  right_input_id.append("pipeline-3").append("-").append(lineitem_import_id);
  for (size_t i = 0; i < partition_count; ++i) {
    std::unordered_map<std::string, std::vector<ObjectReference>> map;
    map[left_input_id] = std::vector<ObjectReference>{};
    for (const auto& left_input : supplier_input_objects) {
      map[left_input_id].emplace_back(shuffle_storage_.bucket_name, left_input.identifier, "",
                                      std::vector<int32_t>{static_cast<int32_t>(i)});
    }
    map[right_input_id] = std::vector<ObjectReference>{};
    for (const auto& right_input : lineitem_input_objects) {
      map[right_input_id].emplace_back(shuffle_storage_.bucket_name, right_input.identifier, "",
                                       std::vector<int32_t>{static_cast<int32_t>(i)});
    }
    const PipelineFragmentDefinition fragment(map, output_objects[i], FileFormat::kParquet);
    pipeline3->AddFragmentDefinition(fragment);
  }
  return {output_objects, pipeline3};
}

std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ5Pipeline4(
    const size_t partition_count, const std::vector<ObjectReference>& input_objects) const {
  using namespace expression_functional;

  // orders. Column 0: orderkey. Column 1: custkey. Column 4: orderdate.
  const std::string left_import_id = "left_import";
  const auto import_operator =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0, 1, 4});
  import_operator->SetIdentity(left_import_id);

  const std::string l_orderdate_start = "1994-01-01";
  std::string l_orderdate_end = l_orderdate_start;
  l_orderdate_end[3] += 1;
  const auto predicate1 = std::make_shared<BetweenExpression>(PredicateCondition::kBetweenUpperExclusive,
                                                              PqpColumn_(2, DataType::kString, false, "l_orderdate"),
                                                              Value_(l_orderdate_start), Value_(l_orderdate_end));
  const auto filter_operator1 = std::make_shared<FilterOperatorProxy>(predicate1);
  filter_operator1->SetLeftInput(import_operator);

  // Keep only relevant columns
  const std::vector<std::shared_ptr<AbstractExpression>> expressions{PqpColumn_(0, DataType::kInt, false, "o_orderkey"),
                                                                     PqpColumn_(1, DataType::kInt, false, "o_custkey")};
  const auto projection_operator = std::make_shared<ProjectionOperatorProxy>(expressions);
  projection_operator->SetLeftInput(filter_operator1);

  // Partition by orderkey.
  const auto partition_operator = std::make_shared<PartitionOperatorProxy>(
      std::make_shared<HashPartitioningFunction>(std::set<ColumnId>{0}, partition_count), false);
  partition_operator->SetLeftInput(projection_operator);

  std::shared_ptr<ExportOperatorProxy> export_operator =
      std::make_shared<ExportOperatorProxy>(ObjectReference("mock", "mock"), kIntermediateResultsExportFormat);
  export_operator->SetLeftInput(partition_operator);

  const std::string pipeline_id = "pipeline-4";
  const size_t stage_1_partitions_per_worker_count = GetStage1PartitionsPerWorkerCount();
  const size_t worker_count = (input_objects.size() / stage_1_partitions_per_worker_count) +
                              (input_objects.size() % stage_1_partitions_per_worker_count);
  auto output_objects = GenerateOutputObjectIds(worker_count, pipeline_id, kIntermediateResultsExportFormat);
  const auto pipeline4 = GeneratePipeline(pipeline_id, left_import_id, export_operator,
                                          kIntermediateResultsExportFormat, input_objects, output_objects);
  return {output_objects, pipeline4};
}

// Join orders with lineitems.
std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ5Pipeline5(
    size_t partition_count, const std::vector<ObjectReference>& orders_input_objects,
    const std::vector<ObjectReference>& lineitem_input_objects) const {
  using namespace expression_functional;
  const std::string orders_import_id = "left_import";
  // From pipeline 1. Column 0: orderkey. Column 1: custkey.
  const auto orders_import_operator =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0, 1});
  orders_import_operator->SetIdentity(orders_import_id);

  const std::string lineitem_import_id = "right_import";
  // Column 0: n_name. Column 1: l_orderkey. Column 2: l_extendedprice. Column 3: l_discount. Column 4: s_nationkey.
  const auto lineitem_import_operator =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0, 1, 2, 3, 4});
  lineitem_import_operator->SetIdentity(lineitem_import_id);

  // Join orders (left) with lineitems.
  std::vector<std::shared_ptr<JoinOperatorPredicate>> empty_secondary_predicates;
  const auto predicate = std::make_shared<JoinOperatorPredicate>(JoinOperatorPredicate{
      .column_id_left = 0, .column_id_right = 1, .predicate_condition = PredicateCondition::kEquals});
  const auto join_operator = std::make_shared<JoinOperatorProxy>(JoinMode::kInner, predicate,
                                                                 empty_secondary_predicates, OperatorType::kHashJoin);
  join_operator->SetLeftInput(orders_import_operator);
  join_operator->SetRightInput(lineitem_import_operator);

  const std::vector<std::shared_ptr<AbstractExpression>> expressions{
      PqpColumn_(1, DataType::kInt, false, "o_custkey"), PqpColumn_(2, DataType::kString, false, "n_name"),
      PqpColumn_(4, DataType::kFloat, false, "l_extendedprice"), PqpColumn_(5, DataType::kFloat, false, "l_discount"),
      PqpColumn_(6, DataType::kInt, false, "s_nationkey")};
  const auto projection_operator = std::make_shared<ProjectionOperatorProxy>(expressions);
  projection_operator->SetLeftInput(join_operator);

  // Partition by custkey.
  const auto partition_operator = std::make_shared<PartitionOperatorProxy>(
      std::make_shared<HashPartitioningFunction>(std::set<ColumnId>{0}, partition_count), false);
  partition_operator->SetLeftInput(projection_operator);

  std::shared_ptr<ExportOperatorProxy> export_operator =
      std::make_shared<ExportOperatorProxy>(ObjectReference("mock", "mock"), kIntermediateResultsExportFormat);
  export_operator->SetLeftInput(partition_operator);

  const std::string pipeline_id = "pipeline-5";
  const auto output_objects = GenerateOutputObjectIds(partition_count, pipeline_id, kIntermediateResultsExportFormat);

  auto pipeline5 = std::make_shared<PqpPipeline>(pipeline_id, export_operator);

  std::string left_input_id;
  left_input_id.append("pipeline-5").append("-").append(orders_import_id);
  std::string right_input_id;
  right_input_id.append("pipeline-5").append("-").append(lineitem_import_id);
  for (size_t i = 0; i < partition_count; ++i) {
    std::unordered_map<std::string, std::vector<ObjectReference>> map;
    map[left_input_id] = std::vector<ObjectReference>{};
    for (const auto& left_input : orders_input_objects) {
      map[left_input_id].emplace_back(shuffle_storage_.bucket_name, left_input.identifier, "",
                                      std::vector<int32_t>{static_cast<int32_t>(i)});
    }
    map[right_input_id] = std::vector<ObjectReference>{};
    for (const auto& right_input : lineitem_input_objects) {
      map[right_input_id].emplace_back(shuffle_storage_.bucket_name, right_input.identifier, "",
                                       std::vector<int32_t>{static_cast<int32_t>(i)});
    }
    const PipelineFragmentDefinition fragment(map, output_objects[i], FileFormat::kParquet);
    pipeline5->AddFragmentDefinition(fragment);
  }
  return {output_objects, pipeline5};
}

// Scan and partition customers.
std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ5Pipeline6(
    const size_t partition_count, const std::vector<ObjectReference>& input_objects) const {
  using namespace expression_functional;

  // customers. Column 0: custkey. Column 3: nationkey.
  const std::string left_import_id = "left_import";
  const auto import_operator =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0, 3});
  import_operator->SetIdentity(left_import_id);

  // Partition by custkey.
  const auto partition_operator = std::make_shared<PartitionOperatorProxy>(
      std::make_shared<HashPartitioningFunction>(std::set<ColumnId>{0}, partition_count), false);
  partition_operator->SetLeftInput(import_operator);

  std::shared_ptr<ExportOperatorProxy> export_operator =
      std::make_shared<ExportOperatorProxy>(ObjectReference("mock", "mock"), kIntermediateResultsExportFormat);
  export_operator->SetLeftInput(partition_operator);

  const std::string pipeline_id = "pipeline-6";
  const size_t stage_1_partitions_per_worker_count = GetStage1PartitionsPerWorkerCount();
  const size_t worker_count = (input_objects.size() / stage_1_partitions_per_worker_count) +
                              (input_objects.size() % stage_1_partitions_per_worker_count);
  auto output_objects = GenerateOutputObjectIds(worker_count, pipeline_id, kIntermediateResultsExportFormat);
  const auto pipeline6 = GeneratePipeline(pipeline_id, left_import_id, export_operator,
                                          kIntermediateResultsExportFormat, input_objects, output_objects);
  return {output_objects, pipeline6};
}

// Join customers with lineitems.
std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ5Pipeline7(
    size_t partition_count, const std::vector<ObjectReference>& customer_input_objects,
    const std::vector<ObjectReference>& lineitem_input_objects) const {
  using namespace expression_functional;
  const std::string orders_import_id = "left_import";
  // customers. Column 0: c_custkey. Column 1: nationkey.
  const auto customers_import_operator =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0, 1});
  customers_import_operator->SetIdentity(orders_import_id);

  const std::string lineitem_import_id = "right_import";
  // Column 0: o_custkey. Column 1: n_name. Column 2: l_extendedprice. Column 3: l_discount. Column 4: s_nationkey.
  const auto lineitem_import_operator =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0, 1, 2, 3, 4});
  lineitem_import_operator->SetIdentity(lineitem_import_id);

  // Join customers (left) with lineitems.
  std::vector<std::shared_ptr<JoinOperatorPredicate>> empty_secondary_predicates;
  const auto predicate = std::make_shared<JoinOperatorPredicate>(JoinOperatorPredicate{
      .column_id_left = 0, .column_id_right = 0, .predicate_condition = PredicateCondition::kEquals});
  const auto join_operator = std::make_shared<JoinOperatorProxy>(JoinMode::kInner, predicate,
                                                                 empty_secondary_predicates, OperatorType::kHashJoin);
  join_operator->SetLeftInput(customers_import_operator);
  join_operator->SetRightInput(lineitem_import_operator);

  // Filter based on nationkey match.
  // Column 0: c_custkey. Column 1: c_nationkey. Column 2: o_custkey. Column 3: n_name. Column 4: l_extendedprice.
  // Column 5: l_discount. Column 6: s_nationkey.
  const auto predicate1 = std::make_shared<BinaryPredicateExpression>(
      PredicateCondition::kEquals, PqpColumn_(1, DataType::kInt, false, "c_nationkey"),
      PqpColumn_(6, DataType::kInt, false, "s_nationkey"));
  const auto nation_filter_operator1 = std::make_shared<FilterOperatorProxy>(predicate1);
  nation_filter_operator1->SetLeftInput(join_operator);

  const std::vector<std::shared_ptr<AbstractExpression>> expressions{
      PqpColumn_(3, DataType::kString, false, "n_name"),
      Mul_(PqpColumn_(4, DataType::kFloat, false, "l_extendedprice"),
           Sub_(Value_(1), PqpColumn_(5, DataType::kFloat, false, "l_discount")))};
  const auto projection_operator = std::make_shared<ProjectionOperatorProxy>(expressions);
  projection_operator->SetLeftInput(nation_filter_operator1);

  std::vector<std::shared_ptr<AbstractExpression>> aggregates = {
      Sum_(PqpColumn_(1, DataType::kFloat, false, "revenue"))};
  const std::vector<ColumnId> groupby_column_ids{0};
  const auto aggregate_operator = std::make_shared<AggregateOperatorProxy>(groupby_column_ids, aggregates);
  aggregate_operator->SetLeftInput(projection_operator);

  std::shared_ptr<ExportOperatorProxy> export_operator =
      std::make_shared<ExportOperatorProxy>(ObjectReference("mock", "mock"), kIntermediateResultsExportFormat);
  export_operator->SetLeftInput(aggregate_operator);

  const std::string pipeline_id = "pipeline-7";
  const auto output_objects = GenerateOutputObjectIds(partition_count, pipeline_id, kIntermediateResultsExportFormat);

  auto pipeline7 = std::make_shared<PqpPipeline>(pipeline_id, export_operator);

  std::string left_input_id;
  left_input_id.append("pipeline-7").append("-").append(orders_import_id);
  std::string right_input_id;
  right_input_id.append("pipeline-7").append("-").append(lineitem_import_id);
  for (size_t i = 0; i < partition_count; ++i) {
    std::unordered_map<std::string, std::vector<ObjectReference>> map;
    map[left_input_id] = std::vector<ObjectReference>{};
    for (const auto& left_input : customer_input_objects) {
      map[left_input_id].emplace_back(shuffle_storage_.bucket_name, left_input.identifier, "",
                                      std::vector<int32_t>{static_cast<int32_t>(i)});
    }
    map[right_input_id] = std::vector<ObjectReference>{};
    for (const auto& right_input : lineitem_input_objects) {
      map[right_input_id].emplace_back(shuffle_storage_.bucket_name, right_input.identifier, "",
                                       std::vector<int32_t>{static_cast<int32_t>(i)});
    }
    const PipelineFragmentDefinition fragment(map, output_objects[i], FileFormat::kParquet);
    pipeline7->AddFragmentDefinition(fragment);
  }
  return {output_objects, pipeline7};
}

// Merge final results of
std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ5Pipeline8(
    const std::vector<ObjectReference>& input_objects) const {
  using namespace expression_functional;

  // Column 0: n_name. Column 1: revenue
  const std::string left_import_id = "left_import";
  auto import_operator_left =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0, 1});
  import_operator_left->SetIdentity(left_import_id);

  std::vector<std::shared_ptr<AbstractExpression>> aggregates = {
      Sum_(PqpColumn_(1, DataType::kDouble, false, "revenue"))};
  const std::vector<ColumnId> groupby_column_ids{0};
  const auto aggregate_operator = std::make_shared<AggregateOperatorProxy>(groupby_column_ids, aggregates);
  aggregate_operator->SetLeftInput(import_operator_left);

  // Order by revenue desc
  const auto sort_operator = std::make_shared<SortOperatorProxy>(
      std::vector<SortColumnDefinition>{SortColumnDefinition(1, SortMode::kDescending)});
  sort_operator->SetLeftInput(aggregate_operator);

  auto export_operator =
      std::make_shared<ExportOperatorProxy>(ObjectReference("mock", "mock"), kFinalResultsExportFormat);
  export_operator->SetLeftInput(sort_operator);

  const std::string pipeline_id = "pipeline-8";
  auto output_objects = GenerateOutputObjectIds(1, pipeline_id, kFinalResultsExportFormat);
  const auto pipeline8 = GeneratePipeline(pipeline_id, left_import_id, export_operator, kFinalResultsExportFormat,
                                          input_objects, output_objects);

  return {output_objects, pipeline8};
}

std::vector<std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ5() const {
  int partitions_count = GetShufflePartitionsCount();
  const auto pipeline1 = GenerateQ5Pipeline1(partitions_count, ListTableObjects("region", FileFormat::kParquet),
                                             ListTableObjects("nation", FileFormat::kParquet),
                                             ListTableObjects("supplier", FileFormat::kParquet));

  const auto pipeline2 = GenerateQ5Pipeline2(partitions_count, ListTableObjects("lineitem", FileFormat::kParquet));

  // join lineitem with suppliers
  const auto pipeline3 = GenerateQ5Pipeline3(partitions_count, pipeline1.first, pipeline2.first);

  // scan orders
  const auto pipeline4 = GenerateQ5Pipeline4(partitions_count, ListTableObjects("orders", FileFormat::kParquet));

  // join orders with lineitems
  const auto pipeline5 = GenerateQ5Pipeline5(partitions_count, pipeline4.first, pipeline3.first);

  // scan customers
  const auto pipeline6 = GenerateQ5Pipeline6(partitions_count, ListTableObjects("customer", FileFormat::kParquet));

  // join lineitems with customers
  const auto pipeline7 = GenerateQ5Pipeline7(partitions_count, pipeline6.first, pipeline5.first);

  // final aggregation
  const auto pipeline8 = GenerateQ5Pipeline8(pipeline7.first);

  pipeline1.second->SetAsPredecessorOf(pipeline3.second);
  pipeline2.second->SetAsPredecessorOf(pipeline3.second);

  pipeline4.second->SetAsPredecessorOf(pipeline5.second);
  pipeline3.second->SetAsPredecessorOf(pipeline5.second);

  pipeline5.second->SetAsPredecessorOf(pipeline7.second);
  pipeline6.second->SetAsPredecessorOf(pipeline7.second);

  pipeline7.second->SetAsPredecessorOf(pipeline8.second);

  return {pipeline1.second, pipeline2.second, pipeline3.second, pipeline4.second,
          pipeline5.second, pipeline6.second, pipeline7.second, pipeline8.second};
}

std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ6Pipeline1(
    const std::vector<ObjectReference>& input_objects) const {
  // NOLINTNEXTLINE(google-build-using-namespace)
  using namespace expression_functional;

  const std::string l_shipdate_start = "1994-01-01";
  const float l_discount = 0.06;
  const int32_t l_quantity = 24;

  const std::string import_id = "import";
  const auto import_operator =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{4, 5, 6, 10});
  import_operator->SetIdentity(import_id);

  auto l_shipdate_end = l_shipdate_start;
  l_shipdate_end[3] += 1;
  const auto predicate1 = std::make_shared<BetweenExpression>(PredicateCondition::kBetweenUpperExclusive,
                                                              PqpColumn_(3, DataType::kString, false, "l_shipdate"),
                                                              Value_(l_shipdate_start), Value_(l_shipdate_end));
  const auto filter_operator1 = std::make_shared<FilterOperatorProxy>(predicate1);
  filter_operator1->SetLeftInput(import_operator);

  const auto predicate2 = std::make_shared<BetweenExpression>(PredicateCondition::kBetweenInclusive,
                                                              PqpColumn_(2, DataType::kFloat, false, "l_discount"),
                                                              Value_(l_discount - 0.01f), Value_(l_discount + 0.01f));
  const auto filter_operator2 = std::make_shared<FilterOperatorProxy>(predicate2);
  filter_operator2->SetLeftInput(filter_operator1);

  const auto predicate3 = std::make_shared<BinaryPredicateExpression>(
      PredicateCondition::kLessThan, PqpColumn_(0, DataType::kFloat, false, "l_quantity"), Value_(l_quantity));
  const auto filter_operator3 = std::make_shared<FilterOperatorProxy>(predicate3);
  filter_operator3->SetLeftInput(filter_operator2);

  const std::vector<std::shared_ptr<AbstractExpression>> expressions{Mul_(
      PqpColumn_(1, DataType::kFloat, false, "l_extendedprice"), PqpColumn_(2, DataType::kFloat, false, "l_discount"))};
  const auto projection_operator = std::make_shared<ProjectionOperatorProxy>(expressions);
  projection_operator->SetLeftInput(filter_operator3);

  std::vector<std::shared_ptr<AbstractExpression>> aggregates = {
      Sum_(PqpColumn_(0, DataType::kFloat, false, "revenue"))};
  const std::vector<ColumnId> groupby_column_ids{};
  const auto aggregate_operator = std::make_shared<AggregateOperatorProxy>(groupby_column_ids, aggregates);
  aggregate_operator->SetLeftInput(projection_operator);

  const auto export_operator =
      std::make_shared<ExportOperatorProxy>(ObjectReference("mock", "mock"), kIntermediateResultsExportFormat);
  export_operator->SetLeftInput(aggregate_operator);

  const std::string pipeline_id = "pipeline-1";
  const size_t stage_1_partitions_per_worker_count = GetStage1PartitionsPerWorkerCount();
  const size_t worker_count = (input_objects.size() / stage_1_partitions_per_worker_count) +
                              (input_objects.size() % stage_1_partitions_per_worker_count);
  auto output_objects = GenerateOutputObjectIds(worker_count, pipeline_id, kIntermediateResultsExportFormat);
  return {output_objects, GeneratePipeline(pipeline_id, import_id, export_operator, kIntermediateResultsExportFormat,
                                           input_objects, output_objects)};
}

std::pair<std::vector<ObjectReference>, std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ6Pipeline2(
    const std::vector<ObjectReference>& input_objects) const {
  // NOLINTNEXTLINE(google-build-using-namespace)
  using namespace expression_functional;

  const std::string import_id = "import";
  const auto import_operator =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0});
  import_operator->SetIdentity(import_id);

  std::vector<std::shared_ptr<AbstractExpression>> aggregates = {std::make_shared<AggregateExpression>(
      AggregateFunction::kSum, PqpColumn_(0, DataType::kDouble, false, "revenue"))};
  const std::vector<ColumnId> groupby_column_ids{};
  const auto aggregate_operator = std::make_shared<AggregateOperatorProxy>(groupby_column_ids, aggregates);
  aggregate_operator->SetLeftInput(import_operator);

  const std::vector<ColumnId>& column_ids_alias = {0};
  const std::vector<std::string>& aliases = {"revenue"};
  const auto alias_operator = std::make_shared<AliasOperatorProxy>(column_ids_alias, aliases);
  alias_operator->SetLeftInput(aggregate_operator);

  const auto export_operator = std::make_shared<ExportOperatorProxy>(ObjectReference("mock", "mock"), FileFormat::kCsv);
  export_operator->SetLeftInput(alias_operator);

  const std::string pipeline_id = "pipeline-2";
  auto output_objects = GenerateOutputObjectIds(1, pipeline_id, FileFormat::kCsv);
  return {output_objects,
          GeneratePipeline(pipeline_id, import_id, export_operator, FileFormat::kCsv, input_objects, output_objects)};
}

std::vector<std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ6() const {
  const auto pipeline1 = GenerateQ6Pipeline1(ListTableObjects("lineitem", FileFormat::kParquet));
  const auto pipeline2 = GenerateQ6Pipeline2(pipeline1.first);

  pipeline1.second->SetAsPredecessorOf(pipeline2.second);

  return {pipeline1.second, pipeline2.second};
}

TpchPqpGenerator::PipelineData TpchPqpGenerator::GenerateQ12Pipeline1(
    const size_t partition_count, const std::vector<ObjectReference>& input_objects) const {
  // NOLINTNEXTLINE(google-build-using-namespace)
  using namespace expression_functional;

  const std::vector<std::string> l_shipmode_in = {"MAIL", "SHIP"};
  const std::string l_receiptdate_start = "1994-01-01";  // inclusive
  const std::string l_receiptdate_end = "1995-01-01";    // exclusive

  const std::string left_import_id = "left_import";
  const auto import_operator =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0, 10, 11, 12, 14});
  import_operator->SetIdentity(left_import_id);

  const auto predicate1 = std::make_shared<BetweenExpression>(PredicateCondition::kBetweenUpperExclusive,
                                                              PqpColumn_(3, DataType::kString, false, "l_receiptdate"),
                                                              Value_(l_receiptdate_start), Value_(l_receiptdate_end));
  const auto filter_operator1 = std::make_shared<FilterOperatorProxy>(predicate1);
  filter_operator1->SetLeftInput(import_operator);

  const auto predicate2 =
      std::make_shared<InExpression>(PredicateCondition::kIn, PqpColumn_(4, DataType::kString, false, "l_shipmode"),
                                     List_(Value_(l_shipmode_in.front()), Value_(l_shipmode_in.back())));
  const auto filter_operator2 = std::make_shared<FilterOperatorProxy>(predicate2);
  filter_operator2->SetLeftInput(filter_operator1);

  const auto predicate3 = std::make_shared<BinaryPredicateExpression>(
      PredicateCondition::kLessThan, PqpColumn_(1, DataType::kString, false, "l_shipdate"),
      PqpColumn_(2, DataType::kString, false, "l_commitdate"));
  const auto filter_operator3 = std::make_shared<FilterOperatorProxy>(predicate3);
  filter_operator3->SetLeftInput(filter_operator2);

  const auto predicate4 = std::make_shared<BinaryPredicateExpression>(
      PredicateCondition::kLessThan, PqpColumn_(2, DataType::kString, false, "l_commitdate"),
      PqpColumn_(3, DataType::kString, false, "l_receiptdate"));
  const auto filter_operator4 = std::make_shared<FilterOperatorProxy>(predicate4);
  filter_operator4->SetLeftInput(filter_operator3);

  const std::vector<std::shared_ptr<AbstractExpression>> expressions{
      PqpColumn_(0, DataType::kInt, false, "l_orderkey"), PqpColumn_(4, DataType::kString, false, "l_shipmode")};
  const auto projection_operator = std::make_shared<ProjectionOperatorProxy>(expressions);
  projection_operator->SetLeftInput(filter_operator4);

  const auto export_operator =
      std::make_shared<ExportOperatorProxy>(ObjectReference("mock", "mock"), kIntermediateResultsExportFormat);
  if (GetJoinAlgorithmForPipeline("pipeline-3") == JoinAlgorithm::kPartitionedHashJoin ||
      GetJoinAlgorithmForPipeline("pipeline-3") == JoinAlgorithm::kSortMergeJoin) {
    const auto partition_operator = std::make_shared<PartitionOperatorProxy>(
        std::make_shared<HashPartitioningFunction>(std::set<ColumnId>{0}, partition_count),
        /* sort_within_partition */ GetJoinAlgorithmForPipeline("pipeline-3") == JoinAlgorithm::kSortMergeJoin);
    partition_operator->SetLeftInput(projection_operator);
    export_operator->SetLeftInput(partition_operator);
  } else {
    export_operator->SetLeftInput(projection_operator);
  }

  const std::string pipeline_id = "pipeline-1";
  const size_t stage_1_partitions_per_worker_count = GetStage1PartitionsPerWorkerCount();
  const size_t worker_count = (input_objects.size() / stage_1_partitions_per_worker_count) +
                              (input_objects.size() % stage_1_partitions_per_worker_count);
  auto output_objects = GenerateOutputObjectIds(worker_count, pipeline_id, kIntermediateResultsExportFormat);
  const auto pipeline1 = GeneratePipeline(pipeline_id, left_import_id, export_operator,
                                          kIntermediateResultsExportFormat, input_objects, output_objects);
  return {output_objects, pipeline1, {0, 1}};
}

TpchPqpGenerator::PipelineData TpchPqpGenerator::GenerateQ12Pipeline2(
    const size_t partition_count, const std::vector<ObjectReference>& input_objects) const {
  if (GetJoinAlgorithmForPipeline("pipeline-3") == JoinAlgorithm::kBroadcastHashJoin) {
    return {input_objects, std::nullopt, {0, 5}};
  }
  AWS_LOGSTREAM_INFO(kCoordinatorTag.c_str(),
                     "JOIN ALGORITHM: " << JoinAlgorithmToString(GetJoinAlgorithmForPipeline("pipeline-3")));
  Assert(GetJoinAlgorithmForPipeline("pipeline-3") == JoinAlgorithm::kPartitionedHashJoin ||
             GetJoinAlgorithmForPipeline("pipeline-3") == JoinAlgorithm::kSortMergeJoin,
         "Unhandled join algorithm for pipeline 3 of Query 12");
  const std::string left_import_id = "left_import";
  const auto import_operator =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, std::vector<ColumnId>{0, 5});
  import_operator->SetIdentity(left_import_id);

  auto partition_operator = std::make_shared<PartitionOperatorProxy>(
      std::make_shared<HashPartitioningFunction>(std::set<ColumnId>{0}, partition_count),
      /* sort_within_partition */ GetJoinAlgorithmForPipeline("pipeline-3") == JoinAlgorithm::kSortMergeJoin);
  partition_operator->SetLeftInput(import_operator);

  std::shared_ptr<ExportOperatorProxy> export_operator =
      std::make_shared<ExportOperatorProxy>(ObjectReference("mock", "mock"), kIntermediateResultsExportFormat);
  export_operator->SetLeftInput(partition_operator);

  const std::string pipeline_id = "pipeline-2";
  const size_t stage_1_partitions_per_worker_count = GetStage1PartitionsPerWorkerCount();
  const size_t worker_count = (input_objects.size() / stage_1_partitions_per_worker_count) +
                              (input_objects.size() % stage_1_partitions_per_worker_count);
  auto output_objects = GenerateOutputObjectIds(worker_count, pipeline_id, kIntermediateResultsExportFormat);
  const auto pipeline2 = GeneratePipeline(pipeline_id, left_import_id, export_operator,
                                          kIntermediateResultsExportFormat, input_objects, output_objects);
  return {output_objects, pipeline2, {0, 1}};
}

TpchPqpGenerator::PipelineData TpchPqpGenerator::GenerateQ12Pipeline3(
    const size_t partition_count, const std::vector<ObjectReference>& input_objects_left,
    const std::vector<ObjectReference>& input_objects_right, const std::vector<ColumnId>& left_column_indices,
    const std::vector<ColumnId>& right_column_indices) const {
  // NOLINTNEXTLINE(google-build-using-namespace)
  using namespace expression_functional;

  const std::string pipeline_id = "pipeline-3";

  const std::string left_import_id = "left_import";
  auto import_operator_left =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, left_column_indices);
  import_operator_left->SetIdentity(left_import_id);

  const std::string right_import_id = "right_import";
  auto import_operator_right =
      std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, right_column_indices);
  import_operator_right->SetIdentity(right_import_id);

  std::vector<std::shared_ptr<JoinOperatorPredicate>> empty_secondary_predicates;
  const auto predicate = std::make_shared<JoinOperatorPredicate>(JoinOperatorPredicate{
      .column_id_left = 0, .column_id_right = 0, .predicate_condition = PredicateCondition::kEquals});
  const auto join_operator = std::make_shared<JoinOperatorProxy>(
      JoinMode::kInner, predicate, empty_secondary_predicates,
      GetJoinAlgorithmForPipeline(pipeline_id) == JoinAlgorithm::kSortMergeJoin ? OperatorType::kSortMergeJoin
                                                                                : OperatorType::kHashJoin);
  join_operator->SetLeftInput(import_operator_left);
  join_operator->SetRightInput(import_operator_right);

  const std::vector<std::shared_ptr<AbstractExpression>> expressions{
      PqpColumn_(1, DataType::kString, false, "l_shipmode"),
      Case_(Or_(Equals_(PqpColumn_(3, DataType::kString, false, "o_orderpriority"), Value_("1-URGENT")),
                Equals_(PqpColumn_(3, DataType::kString, false, "o_orderpriority"), Value_("2-HIGH"))),
            1, 0),
      Case_(And_(NotEquals_(PqpColumn_(3, DataType::kString, false, "o_orderpriority"), Value_("1-URGENT")),
                 NotEquals_(PqpColumn_(3, DataType::kString, false, "o_orderpriority"), Value_("2-HIGH"))),
            1, 0)};

  const auto projection_operator = std::make_shared<ProjectionOperatorProxy>(expressions);
  projection_operator->SetLeftInput(join_operator);

  std::vector<std::shared_ptr<AbstractExpression>> aggregates = {
      Sum_(PqpColumn_(1, DataType::kInt, false, "high_line_count")),
      Sum_(PqpColumn_(2, DataType::kInt, false, "low_line_count"))};
  const auto aggregate_operator = std::make_shared<AggregateOperatorProxy>(std::vector<ColumnId>{0}, aggregates);
  aggregate_operator->SetLeftInput(projection_operator);

  auto export_operator =
      std::make_shared<ExportOperatorProxy>(ObjectReference("mock", "mock"), kIntermediateResultsExportFormat);
  export_operator->SetLeftInput(aggregate_operator);

  const auto output_objects = GenerateOutputObjectIds(partition_count, pipeline_id, kIntermediateResultsExportFormat);

  auto pipeline3 = std::make_shared<PqpPipeline>(pipeline_id, export_operator);

  std::string left_input_id;
  left_input_id.append("pipeline-3").append("-").append(left_import_id);
  std::string right_input_id;
  right_input_id.append("pipeline-3").append("-").append(right_import_id);

  pqp_utils::PipelineInput left_input =
      pqp_utils::PipelineInput(left_input_id,
                               GetJoinAlgorithmForPipeline(pipeline_id) == JoinAlgorithm::kBroadcastHashJoin
                                   ? pqp_utils::PipelineInput::InputShareType::kPartitionedByFileRead
                                   : pqp_utils::PipelineInput::InputShareType::kPartitionedRead,
                               input_objects_left, shuffle_storage_.bucket_name);
  pqp_utils::PipelineInput right_input =
      pqp_utils::PipelineInput(right_input_id,
                               GetJoinAlgorithmForPipeline(pipeline_id) == JoinAlgorithm::kBroadcastHashJoin
                                   ? pqp_utils::PipelineInput::InputShareType::kBroadcastedRead
                                   : pqp_utils::PipelineInput::InputShareType::kPartitionedRead,
                               input_objects_right, shuffle_storage_.bucket_name);
  std::vector<std::unordered_map<std::string, std::vector<ObjectReference>>> fragment_to_inputs =
      pqp_utils::BuildPipelineFragmentsInputsMap({left_input, right_input}, partition_count);

  for (size_t i = 0; i < partition_count; ++i) {
    const PipelineFragmentDefinition fragment(fragment_to_inputs[i], output_objects[i], FileFormat::kParquet);
    pipeline3->AddFragmentDefinition(fragment);
  }
  return {output_objects, pipeline3, {0, 1, 2}};
}

TpchPqpGenerator::PipelineData TpchPqpGenerator::GenerateQ12Pipeline4(
    const size_t partition_count, const std::vector<ObjectReference>& input_objects,
    const std::vector<ColumnId>& column_indices) const {
  // NOLINTNEXTLINE(google-build-using-namespace)
  using namespace expression_functional;

  const std::string left_import_id = "left_import";
  const auto import_operator = std::make_shared<ImportOperatorProxy>(std::vector<ObjectReference>{}, column_indices);
  import_operator->SetIdentity(left_import_id);

  std::vector<std::shared_ptr<AbstractExpression>> aggregates = {
      Sum_(PqpColumn_(1, DataType::kLong, false, "high_line_count")),
      Sum_(PqpColumn_(2, DataType::kLong, false, "low_line_count"))};
  const auto aggregate_operator = std::make_shared<AggregateOperatorProxy>(std::vector<ColumnId>{0}, aggregates);
  aggregate_operator->SetLeftInput(import_operator);

  const auto sort_operator =
      std::make_shared<SortOperatorProxy>(std::vector<SortColumnDefinition>{SortColumnDefinition(0)});
  sort_operator->SetLeftInput(aggregate_operator);

  const auto alias_operator = std::make_shared<AliasOperatorProxy>(
      std::vector<ColumnId>{0, 1, 2}, std::vector<std::string>{"l_shipmode", "high_line_count", "low_line_count"});
  alias_operator->SetLeftInput(sort_operator);

  auto export_operator =
      std::make_shared<ExportOperatorProxy>(ObjectReference("mock", "mock"), kIntermediateResultsExportFormat);
  export_operator->SetLeftInput(alias_operator);

  const std::string pipeline_id = "pipeline-4";
  auto output_objects = GenerateOutputObjectIds(partition_count, pipeline_id, FileFormat::kCsv);
  const auto pipeline4 =
      GeneratePipeline(pipeline_id, left_import_id, export_operator, FileFormat::kCsv, input_objects, output_objects);
  return {output_objects, pipeline4, {0, 1, 2}};
}

std::vector<std::shared_ptr<PqpPipeline>> TpchPqpGenerator::GenerateQ12() const {
  size_t partition_count = GetShufflePartitionsCount();

  AWS_LOGSTREAM_INFO(kCoordinatorTag.c_str(),
                     "Executing join configuration " << join_configuration_.View().WriteReadable());

  const auto pipeline1_data = GenerateQ12Pipeline1(partition_count, ListTableObjects("lineitem", FileFormat::kParquet));
  const auto pipeline2_data = GenerateQ12Pipeline2(partition_count, ListTableObjects("orders", FileFormat::kParquet));
  const auto pipeline3_data =
      GenerateQ12Pipeline3(partition_count, pipeline1_data.output_objects, pipeline2_data.output_objects,
                           pipeline1_data.columns_to_read_in_successor, pipeline2_data.columns_to_read_in_successor);
  const auto pipeline4_data =
      GenerateQ12Pipeline4(1, pipeline3_data.output_objects, pipeline3_data.columns_to_read_in_successor);

  SetAsPredecessorOf(pipeline1_data.pqp_pipeline, pipeline3_data.pqp_pipeline);
  SetAsPredecessorOf(pipeline2_data.pqp_pipeline, pipeline3_data.pqp_pipeline);
  SetAsPredecessorOf(pipeline3_data.pqp_pipeline, pipeline4_data.pqp_pipeline);

  return FilterNonEmptyPipelines({pipeline1_data.pqp_pipeline, pipeline2_data.pqp_pipeline, pipeline3_data.pqp_pipeline,
                                  pipeline4_data.pqp_pipeline});
}

TpchPqpGeneratorConfig::TpchPqpGeneratorConfig(const CompilerName& compiler_name, const QueryId& query_id,
                                               const ScaleFactor& scale_factor,
                                               const ObjectReference& shuffle_storage_prefix,
                                               const std::optional<size_t> stage_1_partitions_per_worker_count,
                                               const std::optional<size_t> shuffle_partitions_count,
                                               const Aws::Utils::Json::JsonValue& join_configuration)
    : AbstractCompilerConfig(compiler_name, query_id, scale_factor, shuffle_storage_prefix),
      stage_1_partitions_per_worker_count_(stage_1_partitions_per_worker_count),
      shuffle_partitions_count_(shuffle_partitions_count),
      join_configuration_(join_configuration) {}

std::shared_ptr<AbstractCompiler> TpchPqpGeneratorConfig::GenerateCompiler() const {
  return std::make_shared<TpchPqpGenerator>(query_id_, scale_factor_, shuffle_storage_,
                                            stage_1_partitions_per_worker_count_, shuffle_partitions_count_,
                                            join_configuration_);
}

bool TpchPqpGeneratorConfig::operator==(const TpchPqpGeneratorConfig& other) const {
  return query_id_ == other.query_id_ && scale_factor_ == other.scale_factor_ &&
         shuffle_storage_ == other.shuffle_storage_ &&
         stage_1_partitions_per_worker_count_ == other.stage_1_partitions_per_worker_count_ &&
         shuffle_partitions_count_ == other.shuffle_partitions_count_ &&
         join_configuration_ == other.join_configuration_;
}

}  // namespace skyrise
