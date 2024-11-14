#pragma once

#include "common/util/SliceUtil.hpp"
#include <cstddef>
#include <cstring>
#include <string>

namespace DB {
class Slice {
  const char *data_;
  uint16_t size_;

public:
  Slice() : data_(nullptr), size_(0) {}

  Slice(const char *data, uint16_t size) : data_(data), size_(size) {}

  Slice(const std::string &s) : data_(s.data()), size_(s.size()) {}

  Slice(const char *s) : data_(s), size_(strlen(s)) {}

  Slice(const Slice &) = default;
  Slice &operator=(const Slice &) = default;

  std::string Serilize() const {
    uint8_t s[sizeof(size_)]{};
    SliceUtil::EncodeUint16(s, size_);
    return std::string(reinterpret_cast<char *>(s), sizeof(s)) + ToString();
  }

  size_t Size() const { return size_; }

  const char *GetData() const { return data_; }

  bool IsEmpty() const { return size_ == 0; }

  std::string ToString() const {
    return std::string(reinterpret_cast<const char *>(data_), size_);
  }
};

struct SliceCompare {
  int operator()(const Slice &l, const Slice &r) {
    size_t min_size = std::min(l.Size(), r.Size());
    int res = memcmp(l.GetData(), r.GetData(), min_size);
    if (res == 0) {
      return r.Size() - l.Size();
    }
    return res;
  }
};
} // namespace DB