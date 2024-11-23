#pragma once

#include "storage/lsmtree/Slice.hpp"

#include <cstdint>
#include <memory>
#include <vector>

namespace DB {
// clang-format off
// +----------------------------------------------+-----------------+-----------------------------+
// |                Data Section                  |      Extra      |       Offset Section        |
// +----------------------------------------------+-----------------+-----------------------------+
// | entry[0] | entry[1] | ... | entry[n] | empty | num_of_elements | offset[0] | offset[1] | ... |
// +----------------------------------------------+-----------------+-----------------------------+
// size: Extra(4 bytes), Offset Section(num_of_elements * 4 bytes), Data Section(4M - other)

// +---------------------------------------------------+
// |                       Entry                       |
// +------------+----------+--------------+------------+
// | key_length | key_data | value_length | value_data |
// +------------+----------+--------------+------------+
// size: key_length(2 bytes), key_data(key_length bytes), value_length(2 bytes), value_data(value_length bytes)
// clang-format on
struct SSTable {
  uint64_t sstable_id_;
  uint32_t num_of_elements_;
  std::vector<uint32_t> offsets_;
  Slice first_key_;
  Slice last_key_;
};

using SSTableRef = std::shared_ptr<SSTable>;
} // namespace DB