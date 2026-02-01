#pragma once

#include "common/Config.hpp"
#include "storage/lsmtree/Slice.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

namespace DB {

class SliceRef {
  const Byte *data_{nullptr};
  uint32_t size_{0};

public:
  SliceRef() = default;

  SliceRef(const Byte *data, uint32_t size) : data_(data), size_(size) {}

  explicit SliceRef(const Slice &s)
      : data_(s.GetData()), size_(static_cast<uint32_t>(s.Size())) {}

  SliceRef(const SliceRef &) = default;
  SliceRef &operator=(const SliceRef &) = default;

  SliceRef(SliceRef &&) = default;
  SliceRef &operator=(SliceRef &&) = default;

  ~SliceRef() = default;

  const Byte *GetData() const { return data_; }

  uint32_t Size() const { return size_; }

  bool IsEmpty() const { return size_ == 0; }

  std::string_view ToStringView() const {
    return std::string_view(data_, size_);
  }

  std::string ToString() const { return std::string(data_, size_); }

  Slice ToSlice() const {
    if (data_ == nullptr || size_ == 0) {
      return Slice{};
    }
    return Slice{const_cast<void *>(static_cast<const void *>(data_)),
                 static_cast<uint16_t>(size_)};
  }

  std::string Serilize() const {
    Byte s[sizeof(uint16_t)]{};
    uint16_t sz = static_cast<uint16_t>(size_);
    std::memcpy(s, &sz, sizeof(sz));
    return std::string(s, sizeof(s)) + ToString();
  }
};

struct SliceRefCompare {
  int operator()(const SliceRef &l, const SliceRef &r) const {
    size_t min_size = std::min(l.Size(), r.Size());
    int res = std::memcmp(l.GetData(), r.GetData(), min_size);
    if (res == 0) {
      return static_cast<int>(r.Size()) - static_cast<int>(l.Size());
    }
    return res;
  }
};

} // namespace DB
