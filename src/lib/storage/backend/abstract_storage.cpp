#include "abstract_storage.hpp"

#include "utils/assert.hpp"

namespace skyrise {

using Range = std::pair<size_t, size_t>;

// This is a generic implementation for storage backends that do not wish to override this function.
StorageError ObjectReader::ReadTail(size_t num_last_bytes, ByteBuffer* buffer) {
  const ObjectStatus status = GetStatus();
  if (status.GetError().IsError()) {
    return status.GetError();
  }

  const size_t first_byte = num_last_bytes < status.GetSize() ? status.GetSize() - num_last_bytes : 0;

  return Read(first_byte, status.GetSize() - 1, buffer);
}

// NOLINTBEGIN(performance-unnecessary-value-param)
StorageError ObjectReader::ReadObjectAsync(const std::shared_ptr<ObjectBuffer>& /*object_buffer*/) {
  Fail("ReadObjectAsync called by an ObjectReader that does not provide an implementation.");
}
// NOLINTEND(performance-unnecessary-value-param)

}  // namespace skyrise
