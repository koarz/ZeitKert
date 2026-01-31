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
// 跳表时间复杂度为 log(n)，每个 memtable 大小约为 64MB
// 如果只存 int 值，key size + value size + length size 大于 12 字节
// 所以每个 memtable 的数据数量必须小于 5'600'000
// log(5'600'000) 约等于 22.4，因此 24 层足够
constexpr size_t DEFAULT_SKIP_LIST_LEVEL = 24;

class MemTable {
  WAL wal_;
  std::atomic_uint32_t approximate_size_{};
  SkipList<Slice, Slice, SliceCompare> skip_list_;

public:
  MemTable() : skip_list_(DEFAULT_SKIP_LIST_LEVEL, SliceCompare{}) {}
  // wal_path 应该是完整的 WAL 文件路径（如 "table_path/0.wal"）
  MemTable(std::filesystem::path wal_path, bool write_log, bool recover = true)
      : wal_(std::move(wal_path), write_log),
        skip_list_(DEFAULT_SKIP_LIST_LEVEL, SliceCompare{}) {
    if (recover) {
      RecoverFromWal();
    }
  }

  // 关闭 WAL 文件流
  void ToImmutable() { wal_.Finish(); }

  // 获取 WAL 文件路径
  std::filesystem::path GetWalPath() const { return wal_.GetPath(); }

  // 删除 WAL 文件
  void DeleteWal() { WAL::Remove(wal_.GetPath()); }

  // 将跳表中所有数据序列化为字符串
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