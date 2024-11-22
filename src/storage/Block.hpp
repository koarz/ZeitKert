#pragma once

#include "storage/column/ColumnWithNameType.hpp"

#include <cstddef>
#include <vector>
namespace DB {
class Block {
public:
  ColumnWithNameTypeRef GetColumn(size_t idx) { return data_[idx]; }

  void ReplaceByPosition(size_t idx, ColumnWithNameTypeRef col) {
    data_[idx] = col;
  }

  void PushColumn(ColumnWithNameTypeRef col) { data_.push_back(col); }

  size_t Size() { return data_.size(); }

  std::vector<ColumnWithNameTypeRef> &GetData() { return data_; }

private:
  std::vector<ColumnWithNameTypeRef> data_;
};
} // namespace DB