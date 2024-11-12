#pragma once

#include "storage/lsmtree/SkipList.hpp"

namespace DB {
template <typename Key, typename Value, typename KeyCompare> class MemTable {
  int refs_;
  SkipList<Key, Value, KeyCompare> skip_list_;

public:
};
} // namespace DB