#include "object_buffer.hpp"

#include <algorithm>

#include <utils/assert.hpp>

#include "../backend/s3_storage.hpp"

namespace skyrise {

ObjectBuffer::ObjectBuffer(const std::map<Range, std::shared_ptr<ByteBuffer>>& request_buffer)
    : request_buffer_(request_buffer) {}

void ObjectBuffer::AddBuffer(const std::pair<Range, std::shared_ptr<ByteBuffer>>& buffer) {
  std::lock_guard<std::mutex> lock(buffer_mutex_);
  request_buffer_.emplace(buffer);
}

//
// [0 - 1000]
// [100 - 1500]
// Read: 500 - 1400
// --> 500-100 + 500-1400 does not fit

std::shared_ptr<ByteBuffer> ObjectBuffer::MergeBuffers(
    const std::vector<std::pair<Range, std::shared_ptr<ByteBuffer>>>& buffers_to_merge, const size_t offset,
    const size_t n_bytes) {
  // TODO(bori00): calling front on an empty vector would cause a segfault
  if (buffers_to_merge.empty()) {
    return std::make_shared<ByteBuffer>(0);
  }
  AWS_LOGSTREAM_INFO("ObjectBuffer-Merge", "Inside");

  Assert(offset >= buffers_to_merge.front().first.first, "Offset must be >= first start.");
  size_t start_offset = offset - buffers_to_merge.front().first.first;
  const size_t end_offset = buffers_to_merge.back().first.second;
  Assert(end_offset >= offset, "End_offset must be >= last range offset");
  const size_t capacity = std::min(n_bytes, end_offset - offset);
  auto result_buffer = std::make_shared<ByteBuffer>(capacity);
  result_buffer->Resize(capacity);
  AWS_LOGSTREAM_INFO("ObjectBuffer-Merge", "Resized result to " << capacity << " for requested n_bytes=" << n_bytes);

  size_t write_position = 0;
  size_t bytes_to_write = 0;

  for (const auto& [range, buffer] : buffers_to_merge) {
    Assert(range.second <= end_offset, "The ranges are not properly sorted by end position");
    start_offset = offset > range.first ? offset - range.first : 0;
    Assert(start_offset < buffer->Size(), "Start offset must be within buffer");
    write_position = offset > range.first ? 0 : range.first - offset;
    bytes_to_write = std::min(buffer->Size() - start_offset, capacity - write_position);
    Assert(write_position + bytes_to_write <= capacity, "Write end position must be within buffer.");
    Assert(start_offset + bytes_to_write <= buffer->Size(), "Read end position must be within read buffer.");

    std::copy_n(buffer->CharData() + start_offset, bytes_to_write, result_buffer->Data() + write_position);

    // if (buffer == buffers_to_merge.back().second) {
    //   AWS_LOGSTREAM_INFO("ObjectBuffer-Merge", "END Copying from buffer of size "
    //                                                << buffer->Size() << " from offset 0 "
    //                                                << " #bytes=" << capacity - write_position << " to position "
    //                                                << write_position << " with end position at " << capacity);
    //   // TODO(bori00): change - if there is just one buffer to merge, i.e. first one is the last one
    //   std::copy_n(buffer->CharData() + start_offset, capacity - write_position, result_buffer->Data() +
    //   write_position); break;
    // }
    // AWS_LOGSTREAM_INFO("ObjectBuffer-Merge", "Copying from buffer of size "
    //                                              << buffer->Size() << " from offset " << start_offset
    //                                              << " #bytes=" << buffer->Size() - start_offset << " to position "
    //                                              << write_position << " with end position at "
    //                                              << write_position + buffer->Size() - start_offset);
    // // Assert(start_offset <= buffer->Size(), "Start_offset must be within the source buffer size");
    // // Assert(write_position < result_buffer->Size(), "Cannot write beyond the size of the result buffer");
    // // Assert(write_position + buffer->Size() - start_offset <= result_buffer->Size(),
    // //        "Cannot write beyond the size of the result buffer");
    // std::copy_n(buffer->CharData() + start_offset, buffer->Size() - start_offset,
    //             result_buffer->Data() + write_position);
    // // write_position += buffer->Size() - start_offset;
    // // start_offset = 0;
  }

  AWS_LOGSTREAM_INFO("ObjectBuffer-Merge", "Written results to buffer");
  return result_buffer;
}

// TODO(tobodner): Check if this needs to be thread safe.
std::shared_ptr<ByteBuffer> ObjectBuffer::Read(const size_t offset, const size_t n_bytes) {
  // TODO(bori00): remove?
  std::lock_guard<std::mutex> lock(buffer_mutex_);
  std::vector<std::pair<Range, std::shared_ptr<ByteBuffer>>> merge_buffers;

  // std::optional<std::pair<unsigned long, unsigned long>> prev_range;
  // for (const auto& [range, request_buffer] : request_buffer_) {
  //   if (prev_range) {
  //     Assert(prev_range->second == range.first, "Invalid range");
  //   }
  //   prev_range = range;
  // }
  size_t total_size_added = 0;

  for (const auto& [range, request_buffer] : request_buffer_) {
    AWS_LOGSTREAM_INFO("ObjectBuffer-Read", "Reading from offset " << offset << " and n_bytes=" << n_bytes
                                                                   << " testing range " << range.first << " "
                                                                   << range.second << " of size "
                                                                   << request_buffer->Size());
    if (range.first <= offset) {
      if (range.second >= offset + n_bytes) {
        // The request can be served from the current buffer.
        auto byte_buffer = std::make_shared<ByteBuffer>(request_buffer->Data() + offset - range.first, n_bytes);
        byte_buffer->Resize(n_bytes);
        AWS_LOGSTREAM_INFO("ObjectBuffer-Read", "Serving from one buffer");
        return byte_buffer;
      } else if (range.second >= offset) {
        // The current buffer must be merged with M of the following buffers to serve the request for N bytes.
        merge_buffers.emplace_back(range, request_buffer);
        total_size_added += request_buffer->Size();
        AWS_LOGSTREAM_INFO("ObjectBuffer-Read",
                           "Added to buffers to merge, with total_size_added=" << total_size_added);
      }
    } else if (!merge_buffers.empty() && !(range.first > offset + n_bytes)) {
      total_size_added += request_buffer->Size();
      AWS_LOGSTREAM_INFO("ObjectBuffer-Read", "Added to buffers to merge, total_size_added=" << total_size_added);
      merge_buffers.emplace_back(range, request_buffer);
    }
  }

  return MergeBuffers(merge_buffers, offset, n_bytes);
}

}  // namespace skyrise
