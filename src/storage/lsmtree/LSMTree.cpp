#include "storage/lsmtree/LSMTree.hpp"
#include "common/Status.hpp"
#include "storage/lsmtree/MemTable.hpp"
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
    immutable_table_.push_back(std::move(memtable_));
    memtable_ = std::make_shared<MemTable>();
    // start checkpoint check
  }
  return memtable_->Put(key, value);
}

Status LSMTree::Remove(const Slice &key) {
  return Insert(key, "");
}

Status LSMTree::GetValue(const Slice &key, Slice *value) {
  std::shared_lock lock(latch_);
  Status status = memtable_->Get(key, value);
  if (!status.ok()) {
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
  lock.unlock();
  if (value->Size() == 0) {
    return Status::Error(ErrorCode::NotFound, "The key no mapping any value");
  }
  return Status::OK();
}
} // namespace DB