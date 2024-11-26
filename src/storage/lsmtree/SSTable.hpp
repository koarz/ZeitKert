#pragma once

#include "storage/lsmtree/Slice.hpp"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <sys/types.h>
#include <vector>

namespace DB {
// clang-format off
// +----------------------------------------------+-------------------------------------------+-----------------------------+---------------+
// |                Data Section                  |                Index Section              |       Offset Section        |     Extra     |
// +----------------------------------------------+-------------------------------------------+-----------------------------+---------------+
// | block[0] | block[1] | ... | block[n] | empty | block[1] max key | block[2] max key | ... | offset[0] | offset[1] | ... | num_of_blocks |
// +----------------------------------------------+-------------------------------------------+-----------------------------+---------------+
// size: Extra(2 bytes), Offset Section(num_of_elements * 4 bytes), Data Section(32KB * num_of_blocks)

// +---------------------------------------------------+
// |                       Entry                       |
// +------------+----------+--------------+------------+
// | key_length | key_data | value_length | value_data |
// +------------+----------+--------------+------------+
// size: key_length(2 bytes), key_data(key_length bytes), value_length(2 bytes), value_data(value_length bytes)
// clang-format on

// Every SSTable always 4MB
// level0 max sstable num: 4
// level1 max sstable num: 10
// level2 max sstable num: 100
// ...
// so if we know how many sstable num is
// we can directly caculate max level
constexpr uint LEVEL[]{0, 4, 14, 114, 1114, 11114 /* 40GB */};

inline uint32_t GetLevel(uint32_t table_id, uint32_t total_table) {
  if (table_id >= total_table) {
    return 0xffffffff;
  }
  int max_level =
      std::lower_bound(LEVEL, LEVEL + sizeof(LEVEL) / 4, total_table) - LEVEL;
  int t = table_id + 1;
  for (int i = max_level; i > 0; i--) {
    t -= (LEVEL[i] - LEVEL[i - 1]);
    if (t <= 0) {
      return i - 1;
    }
  }
  return 0;
}

struct SSTable {
  uint32_t sstable_id_;
  uint32_t num_of_blocks_;
  std::vector<Slice> index_;
  std::vector<uint32_t> offsets_;
};

using SSTableRef = std::shared_ptr<SSTable>;
} // namespace DB