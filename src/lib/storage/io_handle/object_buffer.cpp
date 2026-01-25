#include "object_buffer.hpp"

#include <algorithm>

#include <utils/assert.hpp>

namespace skyrise {

ObjectBuffer::ObjectBuffer(const std::map<Range, std::shared_ptr<ByteBuffer>>& request_buffer)
    : request_buffer_(request_buffer) {}

void ObjectBuffer::AddBuffer(const std::pair<Range, std::shared_ptr<ByteBuffer>>& buffer) {
  std::lock_guard<std::mutex> lock(buffer_mutex_);
  request_buffer_.emplace(buffer);
}

std::shared_ptr<ByteBuffer> ObjectBuffer::MergeBuffers(
    const std::vector<std::pair<Range, std::shared_ptr<ByteBuffer>>>& buffers_to_merge, const size_t offset,
    const size_t n_bytes) {
  if (buffers_to_merge.empty()) {
    return std::make_shared<ByteBuffer>(0);
  }

  Assert(offset >= buffers_to_merge.front().first.first, "Offset must be >= first start.");
  size_t end_offset = 0;
  for (const auto& [range, _] : buffers_to_merge) {
    end_offset = std::max(end_offset, range.second);
  }
  Assert(end_offset >= offset, "End_offset must be >= last range offset");
  const size_t capacity = std::min(n_bytes, end_offset - offset);
  auto result_buffer = std::make_shared<ByteBuffer>(capacity);
  result_buffer->Resize(capacity);
  std::fill(result_buffer->Data(), result_buffer->Data() + capacity, 0);

  size_t start_offset = 0;
  size_t write_position = 0;
  size_t bytes_to_write = 0;

  for (const auto& [range, buffer] : buffers_to_merge) {
    Assert(buffer->Size() == range.second - range.first, "Buffer size != than its declared range");
    Assert(range.second <= end_offset, "The ranges are not properly sorted by end position");
    start_offset = offset > range.first ? offset - range.first : 0;
    Assert(start_offset < buffer->Size(), "Start offset must be within buffer");
    write_position = offset > range.first ? 0 : range.first - offset;
    bytes_to_write = std::min(buffer->Size() - start_offset, capacity - write_position);
    Assert(write_position + bytes_to_write <= capacity, "Write end position must be within buffer.");
    Assert(start_offset + bytes_to_write <= buffer->Size(), "Read end position must be within read buffer.");

    std::copy_n(buffer->CharData() + start_offset, bytes_to_write, result_buffer->Data() + write_position);
  }
  return result_buffer;
}

// TODO(tobodner): Check if this needs to be thread safe.
std::shared_ptr<ByteBuffer> ObjectBuffer::Read(const size_t offset, const size_t n_bytes) {
  std::vector<std::pair<Range, std::shared_ptr<ByteBuffer>>> merge_buffers;

  for (const auto& [range, request_buffer] : request_buffer_) {
    if (range.first <= offset) {
      if (range.second >= offset + n_bytes) {
        // The request can be served from the current buffer.
        auto byte_buffer = std::make_shared<ByteBuffer>(request_buffer->Data() + offset - range.first, n_bytes);
        byte_buffer->Resize(n_bytes);
        return byte_buffer;
      } else if (range.second > offset) {
        // The current buffer must be merged with M of the following buffers to serve the request for N bytes.
        merge_buffers.emplace_back(range, request_buffer);
      }
    } else if (!merge_buffers.empty() && !(range.first >= offset + n_bytes)) {
      merge_buffers.emplace_back(range, request_buffer);
    }
  }

  return MergeBuffers(merge_buffers, offset, n_bytes);
}

}  // namespace skyrise
