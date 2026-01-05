#include "system_benchmark_utils.hpp"

#include <fstream>

#include <magic_enum/magic_enum.hpp>

namespace skyrise {

CompilerName ParseCompilerName(const std::string& compiler_name_string) {
  const auto compiler_name = magic_enum::enum_cast<skyrise::CompilerName>(compiler_name_string);
  if (!compiler_name.has_value()) {
    Fail("Compiler name '" + compiler_name_string + "' cannot be parsed.");
  }
  return compiler_name.value();
}

QueryId ParseQueryId(const std::string& query_id_string) {
  const auto query_id = magic_enum::enum_cast<skyrise::QueryId>(query_id_string);
  if (!query_id.has_value()) {
    Fail("Query ID '" + query_id_string + "' cannot be parsed.");
  }
  return query_id.value();
}

ScaleFactor ParseScaleFactor(const std::string& scale_factor_string) {
  const auto scale_factor = magic_enum::enum_cast<skyrise::ScaleFactor>(scale_factor_string);
  if (!scale_factor.has_value()) {
    Fail("Scale factor '" + scale_factor_string + "' cannot be parsed.");
  }
  return scale_factor.value();
}

/* Returns the JSON configuration from the file, if a file is provided. Else, returns an empty JSON object. */
Aws::Utils::Json::JsonValue ParseJoinConfigurationFilePath(const std::optional<std::string>& config_filepath) {
  if (config_filepath.has_value()) {
    std::ifstream inputFile(config_filepath.value().c_str());

    if (!inputFile.is_open()) {
      Fail("Error: Unable to open file " + config_filepath.value());
    }

    Aws::Utils::Json::JsonValue jsonValue(inputFile);

    if (!jsonValue.WasParseSuccessful()) {
      Fail("Error: Failed to parse join configuration JSON. " + jsonValue.GetErrorMessage() + " from file " +
           config_filepath.value());
    }

    return jsonValue;
  }
  return Aws::Utils::Json::JsonValue();
}

}  // namespace skyrise
