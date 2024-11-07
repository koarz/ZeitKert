#pragma once

#include "storage/column/Column.hpp"

#include <cstddef>
#include <string>
#include <vector>

namespace DB {
class ColumnString final : public Column {
public:
  ColumnString() = default;
  bool IsConstColumn() override { return false; }
  bool IsNullable() override { return true; }

  void Insert(std::string &&v) {
    if (v.size() > max_element_size_) {
      max_element_size_ = v.size();
    }

    offset_.push_back(data_.size());
    if (v.size() + data_.size() > data_.capacity()) {
      data_.reserve((data_.size() + v.size()) << 1);
    }
    data_.append(v);
  }

  std::string GetStrElement(size_t idx) override {
    if (idx == offset_.size() - 1) {
      return data_.substr(offset_[idx]);
    }
    if (idx >= offset_.size()) {
      return "Null";
    }
    return data_.substr(offset_[idx], offset_[idx + 1] - offset_[idx]);
  }

  size_t Size() override { return offset_.size(); }

  size_t GetMaxElementSize() override { return max_element_size_; }

private:
  size_t max_element_size_{};

  std::string data_;

  std::vector<int> offset_;
};
} // namespace DB