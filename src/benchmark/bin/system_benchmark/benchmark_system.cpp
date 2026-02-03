#include "../benchmark_executable.hpp"
#include "system_benchmark/system_benchmark.hpp"
#include "system_benchmark/system_benchmark_utils.hpp"

int main(int argc, char** argv) {
  try {
    BenchmarkExecutable executable("systemBenchmark", "System Benchmark");

    cxxopts::OptionAdder& option_adder = executable.GetOptionAdder();
    option_adder("compiler_name", "The name of the compiler [kTpch, kTpcxbb, ...]", cxxopts::value<std::string>());
    option_adder("query_id", "The IDs of the queries [kTpchQ1, kTpchQ6, ...]", cxxopts::value<std::string>());
    option_adder("scale_factor", "The scale factor of the dataset [1, 10, ...]", cxxopts::value<std::string>());
    option_adder("concurrent_instance_count", "The concurrent instance count", cxxopts::value<size_t>());
    option_adder("repetition_count", "The repetition count", cxxopts::value<size_t>());
    option_adder("after_repetition_delays_min", "The after repetition delays [min]",
                 cxxopts::value<std::vector<size_t>>());
    option_adder("worker_memory_size_mb", "The memory assigned to each worker, in MB.", cxxopts::value<size_t>());
    option_adder(
        "query_config_filepath",
        "The filepath to a json file defining the join algorithm and resource allocation used in each "
        "pipeline of the query. "
        "If no file is provided, or no parameter is specified for a pipeline, then "
        "* for the join algorithm, a a Partitioned Hash Join will be applied"
        "* for the worker_count, a default of 1 worker will be applied"
        "Note: for the worker count, pipeline N has a join algorithm which requires partitioning, then its predecessor "
        "pipelines will partition data data worker_count-ways. Final pipelines cannot have more than 1 worker."
        "Format: {"
        "'pipeline-<id>': "
        "{"
        "'join_algorithm': 'BroadcastHashJoin' / 'PartitionedHashJoin'"
        "'worker_count': <int> "
        "}",
        cxxopts::value<std::string>());

    const cxxopts::ParseResult& parse_result = executable.GetParseResult(argc, argv);

    auto benchmark = std::make_shared<skyrise::SystemBenchmark>(
        executable.GetClient().GetIamClient(), executable.GetClient().GetLambdaClient(),
        executable.GetClient().GetS3Client(),
        skyrise::ParseCompilerName(parse_result["compiler_name"].as<std::string>()),
        skyrise::ParseQueryId(parse_result["query_id"].as<std::string>()),
        skyrise::ParseScaleFactor(parse_result["scale_factor"].as<std::string>()),
        parse_result["concurrent_instance_count"].as<size_t>(), parse_result["repetition_count"].as<size_t>(),
        parse_result["after_repetition_delays_min"].as<std::vector<size_t>>(),
        parse_result.as_optional<size_t>("worker_memory_size_mb"),
        skyrise::ParseJoinConfigurationFilePath(parse_result.as_optional<std::string>("query_config_filepath")));
    executable.ExecuteBenchmark(benchmark);
  } catch (const std::exception& exception) {
    std::cout << exception.what() << "\n";

    return 1;
  }
  return 0;
}
