#include "pipeline_generator.hpp"

#include <map>

#include <compiler/abstract_compiler.hpp>

namespace skyrise {

namespace pqp_utils {

PipelineInput::PipelineInput(const std::string input_id, InputShareType input_share_type,
                             const std::vector<ObjectReference>& input_objects, std::string bucket_name)
    : input_id_(input_id),
      input_share_type_(input_share_type),
      input_objects_(input_objects),
      bucket_name_(bucket_name) {}

void AddPipelineInputToMap(
    std::vector<std::unordered_map<std::string, std::vector<ObjectReference>>>& fragment_to_inputs,
    const PipelineInput& pipeline_input) {
  size_t fragments_count = fragment_to_inputs.size();
  switch (pipeline_input.input_share_type()) {
    case PipelineInput::InputShareType::kBroadcastedRead:
      // Each fragment (worker) reads the whole of the input.
      for (size_t fragment_id = 0; fragment_id < fragments_count; ++fragment_id) {
        for (const ObjectReference& obj_reference : pipeline_input.input_objects()) {
          fragment_to_inputs[fragment_id][pipeline_input.input_id()].push_back(obj_reference);
        }
      }
      break;
    case PipelineInput::InputShareType::kPartitionedRead:
      // Each fragment (worker) reads its own partition of each input object.
      for (size_t fragment_id = 0; fragment_id < fragments_count; ++fragment_id) {
        fragment_to_inputs[fragment_id][pipeline_input.input_id()] = std::vector<ObjectReference>{};
        for (const auto& input_object : pipeline_input.input_objects()) {
          fragment_to_inputs[fragment_id][pipeline_input.input_id()].emplace_back(
              pipeline_input.bucket_name(), input_object.identifier, "",
              std::vector<int32_t>{static_cast<int32_t>(fragment_id)});
        }
      }
      break;
    case PipelineInput::InputShareType::kPartitionedByFileRead:
      Assert(pipeline_input.input_objects().size() >= fragments_count,
             "For kPartitionedByFileRead, each fragment must get at least one file.");
      for (size_t i = 0; i < pipeline_input.input_objects().size(); ++i) {
        fragment_to_inputs[i % fragments_count][pipeline_input.input_id()].emplace_back(
            pipeline_input.input_objects()[i]);
      }
  }
}

std::vector<std::unordered_map<std::string, std::vector<ObjectReference>>> BuildPipelineFragmentsInputsMap(
    const std::vector<PipelineInput>& pipeline_inputs, int fragments_count) {
  std::vector<std::unordered_map<std::string, std::vector<ObjectReference>>> fragment_input_mappings;
  fragment_input_mappings.resize(fragments_count);
  for (const PipelineInput& pipeline_input : pipeline_inputs) {
    AddPipelineInputToMap(fragment_input_mappings, pipeline_input);
  }
  return fragment_input_mappings;
}

}  // namespace pqp_utils

}  // namespace skyrise