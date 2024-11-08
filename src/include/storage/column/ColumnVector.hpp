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

  void Insert(T v) {
    element_str_.push_back(std::to_string(v));
    if (element_str_.rbegin()->size() > max_element_size_) {
      max_element_size_ = element_str_.rbegin()->size();
    }
    data_.push_back(v);
  }

  std::string GetStrElement(size_t idx) override {
    if (idx >= data_.size()) {
      return "Null";
    }
    return element_str_[idx];
  }

  size_t Size() override { return data_.size(); }

  size_t GetMaxElementSize() override { return max_element_size_; }

  T &operator[](size_t idx) { return data_[idx]; }

private:
  size_t max_element_size_{};

  std::vector<T> data_;

  std::vector<std::string> element_str_;
};
} // namespace DB