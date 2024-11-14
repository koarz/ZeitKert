#pragma once

#include "common/Config.hpp"
#include "storage/lsmtree/Slice.hpp"
#include <cstdint>
#include <memory>
#include <vector>

namespace DB {
// clang-format off
// +-----------------+-----------------------------+----------------------------------------------+
// |      Extra      |       Offset Section        |                Data Section                  |
// +-----------------+-----------------------------+----------------------------------------------+
// | num_of_elements | offset[0] | offset[1] | ... | entry[0] | entry[1] | ... | entry[n] | empty |
// +-----------------+-----------------------------+----------------------------------------------+
// size: Extra(4 bytes), Offset Section(num_of_elements * 4 bytes), Data Section(4M - other)

// +---------------------------------------------------+
// |                       Entry                       |
// +------------+----------+--------------+------------+
// | key_length | key_data | value_length | value_data |
// +------------+----------+--------------+------------+
// size: key_length(2 bytes), key_data(key_length bytes), value_length(2 bytes), value_data(value_length bytes)
// clang-format on
struct SSTable {
  page_id_t page_id_;
  uint32_t num_of_elements_;
  std::vector<uint32_t> offsets_;
  Slice first_key_;
  Slice last_key_;
};

using SSTableRef = std::shared_ptr<SSTable>;
} // namespace DB