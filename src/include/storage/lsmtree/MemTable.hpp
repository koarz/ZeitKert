#pragma once

#include "common/Status.hpp"
#include "storage/lsmtree/SkipList.hpp"
#include "storage/lsmtree/Slice.hpp"
#include "storage/lsmtree/WAL.hpp"

#include <atomic>
#include <cstddef>
#include <memory>

namespace DB {

constexpr size_t DEFAULT_SKIP_LIST_LEVEL = 8;

class MemTable {
  WAL wal_;
  std::atomic_uint32_t approximate_size_;
  SkipList<Slice, Slice, SliceCompare> skip_list_;

public:
  MemTable() : skip_list_(DEFAULT_SKIP_LIST_LEVEL, SliceCompare{}) {}
  MemTable(std::filesystem::path path)
      : wal_(path), skip_list_(DEFAULT_SKIP_LIST_LEVEL, SliceCompare{}) {
    RecoverFromWal();
  }

  // the function serilize all skiplist's data to string
  // and write them to immutable page
  std::string Serilize();

  Status Put(Slice key, Slice value);

  Status Get(Slice key, Slice *value);

  void RecoverFromWal();

  size_t GetApproximateSize() { return approximate_size_; }

  class Iterator {
    SkipList<Slice, Slice, SliceCompare>::Iterator it_;

  public:
    Iterator(SkipList<Slice, Slice, SliceCompare>::Iterator it) : it_(it) {}
    void Next() {
      if (Valid())
        ++it_;
    }

    bool Valid() {
      return it_ != SkipList<Slice, Slice, SliceCompare>::Iterator(nullptr);
    }

    Slice &GetKey() { return (*it_).first; }

    Slice &GetValue() { return (*it_).second; }
  };

  Iterator MakeNewIterator() { return Iterator{skip_list_.Begin()}; }
};

using MemTableRef = std::shared_ptr<MemTable>;
} // namespace DB