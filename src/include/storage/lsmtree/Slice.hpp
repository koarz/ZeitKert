#pragma once

#include "common/Config.hpp"
#include "common/util/SliceUtil.hpp"

#include <cstddef>
#include <cstring>
#include <string>

namespace DB {
class Slice {
  Byte *data_;
  uint16_t size_;

public:
  Slice() : data_(nullptr), size_(0) {}

  Slice(const char *s) : data_(nullptr), size_(strlen(s)) {
    data_ = new Byte[size_];
    memcpy(data_, s, size_);
  }

  Slice(void *data, uint16_t size) : data_(new Byte[size]()), size_(size) {
    memcpy(data_, data, size_);
  }

  Slice(std::string str) : data_(new Byte[str.size()]), size_(str.size()) {
    memcpy(data_, str.data(), size_);
  }

  Slice(int i) : data_(new Byte[4]), size_(4) { memcpy(data_, &i, size_); }

  Slice(double d) : data_(new Byte[8]), size_(8) { memcpy(data_, &d, size_); }

  Slice(const Slice &other) {
    data_ = new Byte[other.size_];
    size_ = other.size_;
    memcpy(data_, other.data_, size_);
  }

  Slice &operator=(const Slice &other) {
    if (data_)
      delete[] data_;
    data_ = new Byte[other.size_];
    size_ = other.size_;
    memcpy(data_, other.data_, size_);
    return *this;
  }

  Slice(Slice &&other) {
    if (data_)
      delete[] data_;
    data_ = other.data_;
    size_ = other.size_;
    other.data_ = nullptr;
    other.size_ = 0;
  }

  Slice &operator=(Slice &&other) {
    if (data_)
      delete[] data_;
    data_ = other.data_;
    size_ = other.size_;
    other.data_ = nullptr;
    other.size_ = 0;
    return *this;
  }

  ~Slice() { delete[] data_; }

  std::string Serilize() const {
    Byte s[sizeof(size_)]{};
    SliceUtil::EncodeUint16(s, size_);
    return std::string(s, sizeof(s)) + ToString();
  }

  size_t Size() const { return size_; }

  Byte *GetData() const { return data_; }

  bool IsEmpty() const { return size_ == 0; }

  std::string ToString() const { return std::string(data_, size_); }
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