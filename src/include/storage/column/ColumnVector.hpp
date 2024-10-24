#pragma once

#include "storage/column/Column.hpp"
#include <vector>

namespace DB {
template <typename T> class ColumnVector final : public Column {
public:
  ColumnVector() = default;
  bool IsConstColumn() override { return false; }
  bool IsNullable() override { return true; }

  void Insert(T v) { data_.insert(v); }

protected:
  std::vector<T> data_;
};
} // namespace DB