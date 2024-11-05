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

protected:
  std::string data_;

  std::vector<int> offset_;
};
} // namespace DB