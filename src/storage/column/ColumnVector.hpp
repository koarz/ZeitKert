#pragma once

#include "storage/column/Column.hpp"

#include <cstddef>
#include <string>
#include <vector>

namespace DB {
template <typename T> class ColumnVector final : public Column {
public:
  ColumnVector() = default;
  bool IsConstColumn() override { return false; }
  bool IsNullable() override { return true; }

  void Reserve(size_t n) { data_.reserve(n); }

  void InsertBulk(const T *data, size_t count) {
    data_.insert(data_.end(), data, data + count);
  }

  void Insert(T v) { data_.push_back(v); }

  std::string GetStrElement(size_t idx) override {
    if (idx >= data_.size()) {
      return "Null";
    }
    return std::to_string(data_[idx]);
  }

  size_t Size() override { return data_.size(); }

  size_t GetMaxElementSize() override {
    // 4 is "Null"
    return 4;
  }

  T &operator[](size_t idx) { return data_[idx]; }

  const std::vector<T> &Data() const { return data_; }

private:
  std::vector<T> data_;
};
} // namespace DB