#pragma once

#include "common/Status.hpp"
#include "storage/lsmtree/SkipList.hpp"
#include "storage/lsmtree/Slice.hpp"
#include "storage/lsmtree/WAL.hpp"
#include "storage/lsmtree/iterator/MemTableIterator.hpp"

#include <atomic>
#include <cstddef>
#include <memory>

namespace DB {
// the skip list time complexity is log(n) every memtable size approximate 4MB
// if just int value the key size + value size + length size is large than 12
// byes so the data num must less than 500'000 for every memtable
// log(500'000) about equal 19, so 8 level is big enough
constexpr size_t DEFAULT_SKIP_LIST_LEVEL = 8;

class MemTable {
  WAL wal_;
  std::atomic_uint32_t approximate_size_;
  SkipList<Slice, Slice, SliceCompare> skip_list_;

public:
  MemTable() : skip_list_(DEFAULT_SKIP_LIST_LEVEL, SliceCompare{}) {}
  MemTable(std::filesystem::path path, bool write_log)
      : wal_(path, write_log),
        skip_list_(DEFAULT_SKIP_LIST_LEVEL, SliceCompare{}) {
    RecoverFromWal();
  }

  void ToImmutable() { wal_.Finish(); }

  // the function serilize all skiplist's data to string
  // and write them to immutable page
  std::string Serilize();

  Status Put(const Slice &key, const Slice &value);

  Status Get(const Slice &key, Slice *value);

  void RecoverFromWal();

  size_t GetApproximateSize() { return approximate_size_; }

  MemTableIterator MakeNewIterator() {
    return MemTableIterator{skip_list_.Begin()};
  }
};

using MemTableRef = std::unique_ptr<MemTable>;
} // namespace DB