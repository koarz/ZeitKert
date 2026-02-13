#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

namespace DB {
class Column {

public:
  virtual bool IsConstColumn() { return false; }
  virtual bool IsNullable() { return true; }
  virtual std::string GetStrElement(size_t idx) = 0;
  virtual size_t Size() = 0;
  virtual size_t GetMaxElementSize() = 0;

  bool IsNull(size_t idx) const {
    if (!has_nulls_)
      return false;
    size_t byte_idx = idx / 8;
    if (byte_idx >= null_bitmap_.size())
      return false;
    return (null_bitmap_[byte_idx] >> (idx % 8)) & 1;
  }

  void SetNull(size_t idx) {
    size_t byte_idx = idx / 8;
    if (byte_idx >= null_bitmap_.size()) {
      null_bitmap_.resize(byte_idx + 1, 0);
    }
    null_bitmap_[byte_idx] |= (1 << (idx % 8));
    has_nulls_ = true;
  }

  bool HasNulls() const { return has_nulls_; }

  const std::vector<uint8_t> &NullBitmap() const { return null_bitmap_; }

  void ResizeNullBitmap(size_t n) { null_bitmap_.resize((n + 7) / 8, 0); }

  void SetNullBitmapRaw(const uint8_t *data, size_t size) {
    null_bitmap_.assign(data, data + size);
    has_nulls_ = true;
  }

protected:
  std::vector<uint8_t> null_bitmap_;
  bool has_nulls_ = false;
};
using ColumnPtr = std::shared_ptr<Column>;
} // namespace DB