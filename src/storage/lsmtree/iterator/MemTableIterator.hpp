#pragma once

#include "storage/lsmtree/Slice.hpp"
#include "storage/lsmtree/VectorizedMemTable.hpp"
#include "storage/lsmtree/iterator/Iterator.hpp"

namespace DB {

// 适配器：将 VectorizedMemTable::Iterator 适配到 Iterator 接口
class MemTableIterator : public Iterator {
  VectorizedMemTable::Iterator impl_;
  mutable Slice key_cache_;
  mutable Slice value_cache_;
  mutable bool cache_valid_{false};

  void RebuildCache() const {
    if (impl_.Valid() && !cache_valid_) {
      // 使用 impl_ 的 GetKey/GetValue 获取缓存
      auto &key = const_cast<VectorizedMemTable::Iterator &>(impl_).GetKey();
      auto &val = const_cast<VectorizedMemTable::Iterator &>(impl_).GetValue();
      key_cache_ = key;
      value_cache_ = val;
      cache_valid_ = true;
    }
  }

public:
  explicit MemTableIterator(VectorizedMemTable::Iterator impl)
      : impl_(std::move(impl)) {}

  MemTableIterator(MemTableIterator &&other) noexcept
      : impl_(std::move(other.impl_)), key_cache_(std::move(other.key_cache_)),
        value_cache_(std::move(other.value_cache_)),
        cache_valid_(other.cache_valid_) {
    other.cache_valid_ = false;
  }

  MemTableIterator &operator=(MemTableIterator &&other) noexcept {
    impl_ = std::move(other.impl_);
    key_cache_ = std::move(other.key_cache_);
    value_cache_ = std::move(other.value_cache_);
    cache_valid_ = other.cache_valid_;
    other.cache_valid_ = false;
    return *this;
  }

  ~MemTableIterator() override = default;

  bool Valid() override { return impl_.Valid(); }

  void Next() override {
    impl_.Next();
    cache_valid_ = false;
  }

  Slice &GetKey() override {
    RebuildCache();
    return key_cache_;
  }

  Slice &GetValue() override {
    RebuildCache();
    return value_cache_;
  }

  // 零拷贝访问（给 BuildSelectionVector 用）
  std::string_view GetKeyView() const { return impl_.GetKeyView(); }
  std::string_view GetValueView() const { return impl_.GetValueView(); }
};

} // namespace DB
