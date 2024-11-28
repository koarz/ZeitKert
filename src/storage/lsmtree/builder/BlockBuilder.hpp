#pragma once

#include "common/Config.hpp"
#include "storage/lsmtree/Coding.hpp"
#include "storage/lsmtree/Slice.hpp"

#include <memory>
#include <string>

namespace DB {
class BlockBuilder {
  Slice last_key_;
  std::string buf_;

public:
  uint32_t Add(Slice &key, Slice &value) {
    auto size = key.Size() + value.Size() + 8;
    if (buf_.size() + size > DEFAULT_PAGE_SIZE) {
      return -1;
    }
    auto buffer = std::make_unique<char[]>(size);
    std::ignore = ParseSliceToEntry(key, value, buffer.get());
    buf_.append(buffer.get(), size);
    last_key_ = key;
    // for offset
    return buf_.size() - size;
  }

  bool IsEmpty() { return buf_.empty(); }

  std::string &GetData() { return buf_; }

  Slice &GetLastKey() { return last_key_; }
};
} // namespace DB