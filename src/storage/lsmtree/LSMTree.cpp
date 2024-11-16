#include "storage/lsmtree/LSMTree.hpp"
#include "common/Status.hpp"
#include "storage/column/Column.hpp"
#include "storage/column/ColumnString.hpp"
#include "storage/column/ColumnVector.hpp"
#include "storage/lsmtree/MemTable.hpp"
#include "storage/lsmtree/SkipList.hpp"
#include "storage/lsmtree/Slice.hpp"
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>

namespace DB {
Status LSMTree::Insert(const Slice &key, const Slice &value) {
  std::unique_lock lock(latch_);
  auto size = memtable_->GetApproximateSize();
  if (size >= 4096) {
    std::unique_lock lock(immutable_latch_);
    immutable_table_.push_back(std::move(memtable_));
    memtable_ = std::make_shared<MemTable>();
    // start checkpoint check
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
  std::shared_lock lock(latch_);
  SkipList<Slice, Slice, SliceCompare> temp_list{16, SliceCompare{}};
  auto iterator = memtable_->MakeNewIterator();
  while (iterator.Valid()) {
    temp_list.Insert(iterator.GetKey(), iterator.GetValue(), false);
    iterator.Next();
  }
  lock.unlock();
  lock = std::shared_lock(immutable_latch_);
  for (auto it = immutable_table_.rbegin(); it != immutable_table_.rend();
       it++) {
    iterator = (*it)->MakeNewIterator();
    while (iterator.Valid()) {
      temp_list.Insert(iterator.GetKey(), iterator.GetValue(), false);
      iterator.Next();
    }
  }
  auto it = temp_list.Begin();
  switch (value_type_->GetType()) {
  case ValueType::Type::Int: {
    auto col = std::make_shared<ColumnVector<int>>();
    while (it != temp_list.End()) {
      col->Insert(std::stoi((*it).second.ToString()));
      ++it;
    }
    res = col;
    break;
  }
  case ValueType::Type::String: {
    auto col = std::make_shared<ColumnString>();
    while (it != temp_list.End()) {
      col->Insert((*it).second.ToString());
      ++it;
    }
    res = col;
    break;
  }
  case ValueType::Type::Double: {
    auto col = std::make_shared<ColumnVector<double>>();
    while (it != temp_list.End()) {
      col->Insert(std::stod((*it).second.ToString()));
      ++it;
    }
    res = col;
    break;
  }
  case ValueType::Type::Null: break;
  }
  return Status::OK();
}
} // namespace DB