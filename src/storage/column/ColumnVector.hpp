#pragma once

#include "storage/column/Column.hpp"

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

namespace DB {
template <typename T> class ColumnVector final : public Column {
public:
  // 零拷贝数据段：指向 mmap 内存的指针
  struct DataSpan {
    const T *ptr;
    size_t count;
    std::shared_ptr<void> ref; // 持有 MMapFile 引用，防止释放
  };

  ColumnVector() = default;
  bool IsConstColumn() override { return false; }
  bool IsNullable() override { return true; }

  void Reserve(size_t n) { data_.reserve(n); }

  void InsertBulk(const T *data, size_t count) {
    data_.insert(data_.end(), data, data + count);
  }

  void Insert(T v) { data_.push_back(v); }

  // 添加零拷贝 span（mmap 指针），ref 保持 MMapFile 存活
  void AddSpan(const T *data, size_t count, std::shared_ptr<void> ref) {
    spans_.push_back({data, count, std::move(ref)});
    total_span_rows_ += count;
  }

  bool HasSpans() const { return !spans_.empty(); }
  const std::vector<DataSpan> &Spans() const { return spans_; }

  std::string GetStrElement(size_t idx) override {
    size_t owned = data_.size();
    if (idx < owned) {
      return std::to_string(data_[idx]);
    }
    size_t span_idx = idx - owned;
    for (const auto &s : spans_) {
      if (span_idx < s.count) {
        return std::to_string(s.ptr[span_idx]);
      }
      span_idx -= s.count;
    }
    return "Null";
  }

  size_t Size() override { return data_.size() + total_span_rows_; }

  size_t GetMaxElementSize() override {
    // 4 is "Null"
    return 4;
  }

  T &operator[](size_t idx) {
    if (!spans_.empty()) {
      Materialize();
    }
    return data_[idx];
  }

  // const 版本：直接返回已有数据（不触发物化）
  const std::vector<T> &Data() const { return data_; }

  // 非 const 版本：自动物化 span 到 data_
  const std::vector<T> &Data() {
    if (!spans_.empty()) {
      Materialize();
    }
    return data_;
  }

  // 将所有 span 拷贝到 data_ 中（惰性物化）
  void Materialize() {
    if (spans_.empty())
      return;
    data_.reserve(data_.size() + total_span_rows_);
    for (const auto &s : spans_) {
      data_.insert(data_.end(), s.ptr, s.ptr + s.count);
    }
    spans_.clear();
    total_span_rows_ = 0;
  }

private:
  std::vector<T> data_;
  std::vector<DataSpan> spans_;
  size_t total_span_rows_ = 0;
};
} // namespace DB