#pragma once

#include "buffer/BufferPoolManager.hpp"
#include "common/Config.hpp"
#include "storage/lsmtree/Coding.hpp"
#include "storage/lsmtree/iterator/Iterator.hpp"

#include <filesystem>
#include <vector>

namespace DB {
class SSTableIterator : public Iterator {
  Slice key_;
  Slice value_;
  uint32_t currnt_index_{0};
  std::filesystem::path path_;
  std::vector<uint32_t> offsets_;
  std::shared_ptr<BufferPoolManager> buffer_pool_;

public:
  SSTableIterator(std::filesystem::path path, std::vector<uint32_t> offsets,
                  std::shared_ptr<BufferPoolManager> buffer_pool)
      : path_(path), offsets_(std::move(offsets)), buffer_pool_(buffer_pool) {
    Page *page;
    auto s = buffer_pool_->FetchPage(path_, 0, page);
    auto lock = page->GetReadLock();
    std::ignore = ParseEntryToSlice(key_, value_, page->GetData());
    std::ignore = buffer_pool_->UnPinPage(path_, 0);
  }

  SSTableIterator(SSTableIterator &&other) {
    key_ = std::move(other.key_);
    value_ = std::move(other.value_);
    currnt_index_ = other.currnt_index_;
    offsets_ = std::move(other.offsets_);

    other.currnt_index_ = 0;
  }

  SSTableIterator &operator=(SSTableIterator &&other) {
    key_ = std::move(other.key_);
    value_ = std::move(other.value_);
    currnt_index_ = other.currnt_index_;
    offsets_ = std::move(other.offsets_);

    other.currnt_index_ = 0;
    return *this;
  }

  ~SSTableIterator() override = default;

  void Next() override {
    if (Valid()) {
      currnt_index_++;
    }
    if (Valid()) {
      auto offset = offsets_[currnt_index_];
      Page *page;
      auto s = buffer_pool_->FetchPage(path_, offset / DEFAULT_PAGE_SIZE, page);
      auto lock = page->GetReadLock();
      std::ignore = ParseEntryToSlice(
          key_, value_, page->GetData() + offset % DEFAULT_PAGE_SIZE);
      std::ignore = buffer_pool_->UnPinPage(path_, offset / DEFAULT_PAGE_SIZE);
    }
  }

  bool Valid() override { return currnt_index_ < offsets_.size(); }

  Slice &GetKey() override { return key_; }

  Slice &GetValue() override { return value_; }
};
} // namespace DB