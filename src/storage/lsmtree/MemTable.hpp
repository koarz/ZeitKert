#pragma once

#include "common/Status.hpp"
#include "storage/lsmtree/Slice.hpp"
#include "storage/lsmtree/VectorizedMemTable.hpp"
#include "storage/lsmtree/iterator/MemTableIterator.hpp"
#include "type/ValueType.hpp"

#include <cstddef>
#include <filesystem>
#include <memory>

namespace DB {

constexpr size_t DEFAULT_SKIP_LIST_LEVEL = 24;

class MemTable {
  VectorizedMemTable impl_;

public:
  MemTable() = default;

  // 新构造函数：支持主键类型
  MemTable(std::filesystem::path wal_path, bool write_log,
           ValueType::Type key_type, bool recover = true)
      : impl_(std::move(wal_path), write_log, key_type, recover) {}

  void ToImmutable() { impl_.ToImmutable(); }

  std::filesystem::path GetWalPath() const { return impl_.GetWalPath(); }

  void DeleteWal() { impl_.DeleteWal(); }

  std::string Serilize() { return impl_.Serilize(); }

  Status Put(const Slice &key, const Slice &value) {
    return impl_.Put(key, value);
  }

  void SetDeferFlush(bool defer) { impl_.SetDeferFlush(defer); }

  void FlushWal() { impl_.FlushWal(); }

  Status Get(const Slice &key, Slice *value) { return impl_.Get(key, value); }

  void RecoverFromWal() {} // VectorizedMemTable 在构造时自动恢复

  size_t GetApproximateSize() { return impl_.GetApproximateSize(); }

  ValueType::Type GetKeyType() const { return impl_.GetKeyType(); }

  // 返回适配器迭代器（兼容 Iterator 接口）
  MemTableIterator MakeNewIterator() {
    return MemTableIterator{impl_.MakeIterator()};
  }

  // 直接访问 VectorizedMemTable（给 BuildSelectionVector 零拷贝用）
  VectorizedMemTable &GetImpl() { return impl_; }
  const VectorizedMemTable &GetImpl() const { return impl_; }
};

using MemTableRef = std::unique_ptr<MemTable>;

} // namespace DB
