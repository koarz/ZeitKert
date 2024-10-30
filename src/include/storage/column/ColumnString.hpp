#pragma once

#include "storage/column/Column.hpp"

#include <string>
#include <vector>

namespace DB {
class ColumnString final : public Column {
public:
  ColumnString() = default;
  bool IsConstColumn() override { return false; }
  bool IsNullable() override { return true; }

  void Insert(std::string &&v) {
    offset_.push_back(v.size());
    if (v.size() + data_.size() > data_.capacity()) {
      data_.reserve((data_.size() + v.size()) << 1);
    }
    data_.append(v);
  }

protected:
  std::string data_;

  std::vector<int> offset_;
};
} // namespace DB