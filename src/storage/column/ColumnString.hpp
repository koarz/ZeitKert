#pragma once

#include "storage/column/Column.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace DB {
class ColumnString final : public Column {
public:
  ColumnString() = default;
  bool IsConstColumn() override { return false; }
  bool IsNullable() override { return true; }

  // 预分配空间
  void Reserve(size_t n) { offset_.reserve(n + 1); }

  void Insert(std::string &&v) {
    if (v.size() > max_element_size_) {
      max_element_size_ = v.size();
    }

    offset_.push_back(static_cast<uint32_t>(data_.size()));
    if (v.size() + data_.size() > data_.capacity()) {
      data_.reserve((data_.size() + v.size()) << 1);
    }
    data_.append(v);
  }

  // 批量设置（直接接管 offset 和 data）
  void SetBulk(std::vector<uint32_t> &&offsets, std::string &&data) {
    offset_.clear();
    offset_.reserve(offsets.size());
    for (auto off : offsets) {
      offset_.push_back(static_cast<int>(off));
    }
    data_ = std::move(data);
    // 更新 max_element_size_
    max_element_size_ = 0;
    for (size_t i = 0; i + 1 < offset_.size(); ++i) {
      size_t len = offset_[i + 1] - offset_[i];
      if (len > max_element_size_) {
        max_element_size_ = len;
      }
    }
  }

  // 批量追加
  void AppendBulk(const uint32_t *offsets, size_t offset_count,
                  const char *str_data, size_t data_size) {
    if (offset_count == 0) {
      return;
    }
    // offset_count = row_count + 1 (每个字符串的起始偏移 + 末尾偏移)
    size_t row_count = offset_count - 1;
    uint32_t base_offset = static_cast<uint32_t>(data_.size());

    // 追加每行的偏移（转换为相对于 data_ 的绝对偏移）
    uint32_t src_base = offsets[0];
    for (size_t i = 0; i < row_count; ++i) {
      offset_.push_back(static_cast<int>(base_offset + offsets[i] - src_base));
      size_t len = offsets[i + 1] - offsets[i];
      if (len > max_element_size_) {
        max_element_size_ = len;
      }
    }

    // 追加字符串数据
    data_.append(str_data, data_size);
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

  size_t GetMaxElementSize() override {
    // 4 is Null
    return std::max(max_element_size_, 4UL);
  }

  std::string operator[](size_t idx) { return GetStrElement(idx); }

  const std::string &Data() const { return data_; }
  const std::vector<int> &Offsets() const { return offset_; }

private:
  size_t max_element_size_{};

  std::string data_;

  std::vector<int> offset_;
};
} // namespace DB