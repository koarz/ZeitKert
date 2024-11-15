#pragma once

#include "common/Config.hpp"

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <shared_mutex>

namespace DB {
class Page {
  friend class BufferPoolManager;

  Byte *const data_;
  bool is_dirty_ = false;
  std::shared_mutex rw_lock_;
  std::filesystem::path path_;
  page_id_t page_id_ = INVALID_PAGE_ID;

public:
  Page() : data_(new Byte[DEFAULT_PAGE_SIZE]) {
    memset(data_, 0, DEFAULT_PAGE_SIZE);
  }

  ~Page() { delete[] data_; }

  bool IsDirty() { return is_dirty_; }

  void SetDirty(bool dirty) { is_dirty_ = dirty; }

  ReadLock GetReadLock() { return ReadLock{rw_lock_}; }

  WriteLock GetWriteLock() { return WriteLock{rw_lock_}; }

  Byte *GetData() { return data_; }

  page_id_t GetPageId() { return page_id_; }

  std::filesystem::path GetPath() { return path_; }
};
} // namespace DB