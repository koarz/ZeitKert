#include "storage/lsmtree/LSMTree.hpp"
#include "common/Config.hpp"
#include "common/Status.hpp"
#include "storage/column/Column.hpp"
#include "storage/column/ColumnString.hpp"
#include "storage/column/ColumnVector.hpp"
#include "storage/lsmtree/MemTable.hpp"
#include "storage/lsmtree/SkipList.hpp"
#include "storage/lsmtree/Slice.hpp"
#include "storage/lsmtree/TableOperator.hpp"
#include "storage/lsmtree/iterator/MemTableIterator.hpp"
#include "storage/lsmtree/iterator/MergeIterator.hpp"

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>

namespace DB {
void LSMTree::ReadSSTableMeta() {}

Status LSMTree::Insert(const Slice &key, const Slice &value) {
  if (value.Size() > 0x4000) {
    // make life easy
    // every block have 32k bytes data
    // if store too large data, we need splite it to another block
    // it will be introduce new flag for data block
    // i dont want to do it at now
    // IMPORTANT: Don't use string type as primary key, ban it!
    return Status::Error(
        ErrorCode::InsertError,
        "Your data too large, please split it to less than 16384 bytes");
  }
  std::unique_lock lock(latch_);
  auto size = memtable_->GetApproximateSize();
  if (size >= SSTABLE_SIZE) {
    std::unique_lock lock(immutable_latch_);
    memtable_->ToImmutable();
    immutable_table_.push_back(std::move(memtable_));
    // start TableOperator check
    // when wal file size >= 4MB start cpmpaction
    // 4MB / 32KB = 128 so if immutable num = 128 start it
    if (immutable_table_.size() == 128) {
      // TODO: build sstable become async operate with write new memtable
      // maybe we can insert new data first?
      auto s = TableOperator::BuildSSTable(column_path_, table_number_,
                                           immutable_table_);
      if (!s.ok()) {
        return s;
      }
    }
    memtable_ = std::make_unique<MemTable>(column_path_, write_log_);
  }
  return memtable_->Put(key, value);
}

Status LSMTree::Remove(const Slice &key) {
  return Insert(key, Slice{});
}

Status LSMTree::GetValue(const Slice &key, Slice *value) {
  std::shared_lock lock(latch_);
  Status status = memtable_->Get(key, value);
  if (!status.ok()) {
    lock.unlock();
    std::shared_lock lock(immutable_latch_);
    for (auto it = immutable_table_.rbegin(); it != immutable_table_.rend();
         it++) {
      status = (*it)->Get(key, value);
      if (status.ok()) {
        if (value->Size() == 0) {
          // The key was deleted
          return Status::Error(ErrorCode::NotFound,
                               "The key no mapping any value");
        }
        return Status::OK();
      }
    }
    return status;
  }
  // check sstable release lock first
  if (value->Size() == 0) {
    return Status::Error(ErrorCode::NotFound, "The key no mapping any value");
  }
  return Status::OK();
}

Status LSMTree::ScanColumn(ColumnPtr &res) {
  // TODO: add lock for every memtable
  std::shared_lock lock1(latch_), lock2(immutable_latch_);
  std::vector<std::shared_ptr<Iterator>> iters;
  SkipList<Slice, Slice, SliceCompare> temp_list{8, SliceCompare{}};
  iters.push_back(
      std::make_shared<MemTableIterator>(memtable_->MakeNewIterator()));
  for (auto it = immutable_table_.rbegin(); it != immutable_table_.rend();
       it++) {
    iters.push_back(
        std::make_shared<MemTableIterator>((*it)->MakeNewIterator()));
  }
  MergeIterator it(std::move(iters));
  switch (value_type_->GetType()) {
  case ValueType::Type::Int: {
    auto col = std::make_shared<ColumnVector<int>>();
    while (it.Valid()) {
      col->Insert(*reinterpret_cast<int *>(it.GetValue().GetData()));
      it.Next();
    }
    res = col;
    break;
  }
  case ValueType::Type::String: {
    auto col = std::make_shared<ColumnString>();
    while (it.Valid()) {
      col->Insert(it.GetValue().ToString());
      it.Next();
    }
    res = col;
    break;
  }
  case ValueType::Type::Double: {
    auto col = std::make_shared<ColumnVector<double>>();
    while (it.Valid()) {
      col->Insert(*reinterpret_cast<double *>(it.GetValue().GetData()));
      it.Next();
    }
    res = col;
    break;
  }
  case ValueType::Type::Null: break;
  }
  return Status::OK();
}
} // namespace DB