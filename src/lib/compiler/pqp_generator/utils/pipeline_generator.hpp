#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "types.hpp"

namespace skyrise {

namespace pqp_utils {

class PipelineInput {
 public:
  enum class InputShareType : std::uint8_t {
    kBroadcastedRead, /* Each worker needs to read all input files fully. */
    kPartitionedRead  /* Assumes that the input parquet files are partitioned. Each worker needs to a specific partition
                         from each input file. */
    ,
    kPartitionedByFileRead /* Each worker needs to read a subset of the input files. */
  };

  PipelineInput(std::string input_id, InputShareType input_share_type,
                const std::vector<ObjectReference>& input_objects);

  [[nodiscard]] std::string input_id() const { return input_id_; }
  [[nodiscard]] InputShareType input_share_type() const { return input_share_type_; }
  [[nodiscard]] const std::vector<ObjectReference>& input_objects() const { return input_objects_; }

 private:
  const std::string input_id_;
  const InputShareType input_share_type_;
  const std::vector<ObjectReference>& input_objects_;
};

std::vector<std::unordered_map<std::string, std::vector<ObjectReference>>> BuildPipelineFragmentsInputsMap(
    const std::vector<PipelineInput>& pipeline_inputs, int fragments_count);

}  // namespace pqp_utils
}  // namespace skyrise