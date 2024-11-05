#pragma once

#include "storage/column/Column.hpp"

#include <string>
#include <vector>

namespace DB {
template <typename T> class ColumnVector final : public Column {
public:
  ColumnVector() = default;
  bool IsConstColumn() override { return false; }
  bool IsNullable() override { return true; }

  void Insert(T v) { data_.push_back(v); }

  std::string GetStrElement(size_t idx) override {
    if (idx >= data_.size()) {
      return "Null";
    }
    return std::to_string(data_[idx]);
  }

  size_t Size() override { return data_.size(); }

protected:
  std::vector<T> data_;
};
} // namespace DB