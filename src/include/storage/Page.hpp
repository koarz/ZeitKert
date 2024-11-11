#pragma once

#include "common/Config.hpp"

#include <cstdint>
#include <shared_mutex>

namespace DB {
class Page {
  uint8_t *data_;
  bool is_dirty_ = false;
  page_id_t page_id_ = INVALID_PAGE_ID;
  std::shared_mutex rw_lock_;

public:
  Page() : data_(new uint8_t[DEFAULT_PAGE_SIZE]) {}

  ~Page() { delete[] data_; }

  bool IsDirty() { return is_dirty_; }

  void SetDirty(bool dirty) { is_dirty_ = dirty; }

  ReadLock GetReadLock() { return ReadLock{rw_lock_}; }

  WriteLock GetWriteLock() { return WriteLock{rw_lock_}; }

  uint8_t *GetData() { return data_; }

  page_id_t GetPageId() { return page_id_; }
};
} // namespace DB