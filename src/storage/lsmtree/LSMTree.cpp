#include "storage/lsmtree/LSMTree.hpp"
#include "common/Config.hpp"
#include "common/Status.hpp"
#include "storage/column/Column.hpp"
#include "storage/column/ColumnString.hpp"
#include "storage/column/ColumnVector.hpp"
#include "storage/lsmtree/BloomFilter.hpp"
#include "storage/lsmtree/ColumnReader.hpp"
#include "storage/lsmtree/MemTable.hpp"
#include "storage/lsmtree/RowCodec.hpp"
#include "storage/lsmtree/Slice.hpp"
#include "storage/lsmtree/TableOperator.hpp"
#include "storage/lsmtree/iterator/MemTableIterator.hpp"

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <tuple>
#include <vector>

namespace DB {
static bool GetColumnValuePointer(const Byte *base, const RowGroupMeta &rg,
                                  uint32_t row_idx, size_t col_idx,
                                  ValueType::Type type, const Byte *&ptr,
                                  uint32_t &len) {
  // 从 PAX RowGroup 计算列值指针
  if (row_idx >= rg.row_count) {
    return false;
  }
  const auto &col = rg.columns[col_idx];
  switch (type) {
  case ValueType::Type::Int: {
    len = sizeof(int);
    ptr = base + col.offset + row_idx * len;
    return true;
  }
  case ValueType::Type::Double: {
    len = sizeof(double);
    ptr = base + col.offset + row_idx * len;
    return true;
  }
  case ValueType::Type::String: {
    // 字符串 offsets 后接 data 区
    const Byte *offsets_base = base + col.offset;
    uint32_t start = 0;
    uint32_t end = 0;
    std::memcpy(&start, offsets_base + row_idx * sizeof(uint32_t),
                sizeof(uint32_t));
    std::memcpy(&end, offsets_base + (row_idx + 1) * sizeof(uint32_t),
                sizeof(uint32_t));
    const Byte *data_base =
        offsets_base + (rg.row_count + 1) * sizeof(uint32_t);
    len = end - start;
    ptr = data_base + start;
    return true;
  }
  case ValueType::Type::Null:
    len = 0;
    ptr = nullptr;
    return true;
  }
  return false;
}

static bool BuildRowFromRowGroup(
    const Byte *base, const RowGroupMeta &rg, uint32_t row_idx,
    const std::vector<std::shared_ptr<ValueType>> &column_types, Slice *row) {
  if (!row) {
    return false;
  }
  // 从 RowGroup 还原行编码
  std::string buffer;
  buffer.reserve(128);
  for (size_t col_idx = 0; col_idx < column_types.size(); col_idx++) {
    const Byte *ptr = nullptr;
    uint32_t len = 0;
    if (!GetColumnValuePointer(base, rg, row_idx, col_idx,
                               column_types[col_idx]->GetType(), ptr, len)) {
      return false;
    }
    buffer.append(reinterpret_cast<const char *>(&len), sizeof(len));
    if (len > 0) {
      buffer.append(reinterpret_cast<const char *>(ptr), len);
    }
  }
  *row = Slice{buffer};
  return true;
}

static bool FindRowIndex(const Byte *base, const RowGroupMeta &rg,
                         const Slice &key, ValueType::Type key_type,
                         uint16_t key_idx, uint32_t &row_idx) {
  if (rg.row_count == 0) {
    return false;
  }
  // RowGroup 内按主键二分查找
  SliceCompare cmp;
  int left = 0;
  int right = static_cast<int>(rg.row_count) - 1;
  while (left <= right) {
    int mid = left + (right - left) / 2;
    const Byte *ptr = nullptr;
    uint32_t len = 0;
    if (!GetColumnValuePointer(base, rg, mid, key_idx, key_type, ptr, len)) {
      return false;
    }
    Slice mid_key{const_cast<Byte *>(ptr), static_cast<uint16_t>(len)};
    int res = cmp(mid_key, key);
    if (res == 0) {
      row_idx = static_cast<uint32_t>(mid);
      return true;
    }
    if (res < 0) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  return false;
}

LSMTree::LSMTree(std::filesystem::path table_path,
                 std::shared_ptr<BufferPoolManager> buffer_pool_manager,
                 std::vector<std::shared_ptr<ValueType>> column_types,
                 uint16_t primary_key_idx, bool write_log)
    : IndexEngine(SliceCompare{}, std::move(table_path),
                  std::move(buffer_pool_manager)),
      write_log_(write_log), table_number_(0),
      column_types_(std::move(column_types)), primary_key_idx_(primary_key_idx),
      memtable_(std::make_unique<MemTable>(column_path_, write_log_)) {
  if (column_types_.empty()) {
    primary_key_idx_ = 0;
  } else if (primary_key_idx_ >= column_types_.size()) {
    primary_key_idx_ = 0;
  }
  if (std::filesystem::exists(column_path_)) {
    for (const auto &entry :
         std::filesystem::directory_iterator(column_path_)) {
      if (!entry.is_regular_file()) {
        continue;
      }
      if (entry.path().extension() != ".sst") {
        continue;
      }
      auto stem = entry.path().stem().string();
      uint32_t id = 0;
      try {
        id = static_cast<uint32_t>(std::stoul(stem));
      } catch (...) {
        continue;
      }
      table_number_ = std::max(table_number_, id + 1);
      auto &table_meta = sstables_[id] = std::make_shared<SSTable>();
      table_meta->sstable_id_ = id;
      std::ignore = TableOperator::ReadSSTable(
          column_path_, table_meta, column_types_, buffer_pool_manager_);
    }
  }
}

LSMTree::~LSMTree() {
  // 将当前 memtable 加入刷盘队列
  if (memtable_ && memtable_->GetApproximateSize() > 0) {
    memtable_->ToImmutable();
    immutable_table_.push_back(std::move(memtable_));
  }
  // 逐个刷盘所有 immutable tables
  while (!immutable_table_.empty()) {
    std::vector<MemTableRef> to_flush;
    to_flush.push_back(std::move(immutable_table_.front()));
    immutable_table_.erase(immutable_table_.begin());
    auto &table_meta = sstables_[table_number_] = std::make_shared<SSTable>();
    table_meta->sstable_id_ = table_number_;
    std::ignore = TableOperator::BuildSSTable(column_path_, table_number_,
                                              to_flush, column_types_,
                                              primary_key_idx_, table_meta);
  }
}

Status LSMTree::Insert(const Slice &key, const Slice &value) {
  if (value.Size() > SSTABLE_SIZE) {
    return Status::Error(
        ErrorCode::InsertError,
        "Your row data too large, please split it to less than 64MB");
  }
  bool recover{};
  std::unique_lock lock(latch_);
  auto size = memtable_->GetApproximateSize();
  // MemTable 到达 SSTable 大小后转不可变
  if (size >= SSTABLE_SIZE) {
    std::unique_lock lock(immutable_latch_);
    memtable_->ToImmutable();
    immutable_table_.push_back(std::move(memtable_));
    if (immutable_table_.size() >= MAX_IMMUTABLE_COUNT) {
      // 缓冲区满，刷最老的 immutable 为 SSTable
      std::vector<MemTableRef> to_flush;
      to_flush.push_back(std::move(immutable_table_.front()));
      immutable_table_.erase(immutable_table_.begin());
      auto sstable_id = table_number_;
      auto &table_meta = sstables_[sstable_id] = std::make_shared<SSTable>();
      table_meta->sstable_id_ = sstable_id;
      auto s = TableOperator::BuildSSTable(column_path_, table_number_,
                                           to_flush, column_types_,
                                           primary_key_idx_, table_meta);
      if (!s.ok()) {
        return s;
      }
      recover = true;
    }
    memtable_ = std::make_unique<MemTable>(column_path_, write_log_, recover);
  }
  return memtable_->Put(key, value);
}

Status LSMTree::Remove(const Slice &key) {
  return Insert(key, Slice{});
}

Status LSMTree::GetValue(const Slice &key, Slice *value) {
  std::shared_lock lock(latch_);
  Status status = memtable_->Get(key, value);
  if (status.ok()) {
    if (value->Size() == 0) {
      return Status::Error(ErrorCode::NotFound, "The key no mapping any value");
    }
    return Status::OK();
  }
  lock.unlock();
  std::shared_lock lock2(immutable_latch_);
  for (auto it = immutable_table_.rbegin(); it != immutable_table_.rend();
       it++) {
    status = (*it)->Get(key, value);
    if (status.ok()) {
      if (value->Size() == 0) {
        return Status::Error(ErrorCode::NotFound,
                             "The key no mapping any value");
      }
      return Status::OK();
    }
  }

  SliceCompare cmp;
  // SSTable 按 max_key 二分定位 RowGroup
  for (int i = static_cast<int>(table_number_) - 1; i >= 0; i--) {
    auto it = sstables_.find(i);
    if (it == sstables_.end()) {
      continue;
    }
    auto &table = *it->second;
    if (!table.data_file_ || !table.data_file_->Valid()) {
      continue;
    }
    if (table.rowgroups_.empty()) {
      continue;
    }
    int left = 0;
    int right = static_cast<int>(table.rowgroups_.size()) - 1;
    int candidate = -1;
    while (left <= right) {
      int mid = left + (right - left) / 2;
      Slice max_key{table.rowgroups_[mid].max_key};
      int res = cmp(max_key, key);
      if (res >= 0) {
        candidate = mid;
        right = mid - 1;
      } else {
        left = mid + 1;
      }
    }
    if (candidate < 0) {
      continue;
    }
    const auto &rg = table.rowgroups_[candidate];
    // bloom 先判定再查找
    if (!rg.bloom.empty()) {
      BloomFilter bloom(rg.bloom);
      if (!bloom.MayContain(key)) {
        continue;
      }
    }
    // mmap 读取 RowGroup 数据
    const Byte *base =
        table.data_file_->Data() + static_cast<size_t>(rg.offset);
    uint32_t row_idx = 0;
    if (!FindRowIndex(base, rg, key, column_types_[primary_key_idx_]->GetType(),
                      primary_key_idx_, row_idx)) {
      continue;
    }
    if (!BuildRowFromRowGroup(base, rg, row_idx, column_types_, value)) {
      return Status::Error(ErrorCode::IOError, "Failed to read row");
    }
    if (value->Size() == 0) {
      return Status::Error(ErrorCode::NotFound, "The key no mapping any value");
    }
    return Status::OK();
  }
  return Status::Error(ErrorCode::NotFound, "The key no mapping any value");
}

Status LSMTree::ScanColumn(size_t column_idx, ColumnPtr &res) {
  if (column_idx >= column_types_.size()) {
    return Status::Error(ErrorCode::NotFound, "Column index out of range");
  }

  auto type = column_types_[column_idx];

  // 创建列容器
  switch (type->GetType()) {
  case ValueType::Type::Int: res = std::make_shared<ColumnVector<int>>(); break;
  case ValueType::Type::String: res = std::make_shared<ColumnString>(); break;
  case ValueType::Type::Double:
    res = std::make_shared<ColumnVector<double>>();
    break;
  case ValueType::Type::Null: return Status::OK();
  }

  std::shared_lock lock1(latch_), lock2(immutable_latch_);

  // 1. 从 MemTable 读取（仍需迭代，行存结构无法直接列读取）
  auto scan_memtable = [&](const MemTableRef &mt) {
    auto iter = mt->MakeNewIterator();
    MemTableIterator mem_iter(std::move(iter));
    while (mem_iter.Valid()) {
      if (mem_iter.GetValue().Size() == 0) {
        mem_iter.Next();
        continue;
      }
      Slice val;
      if (!RowCodec::DecodeColumn(mem_iter.GetValue(), column_idx, &val)) {
        mem_iter.Next();
        continue;
      }
      switch (type->GetType()) {
      case ValueType::Type::Int: {
        if (val.Size() == sizeof(int)) {
          int v = 0;
          std::memcpy(&v, val.GetData(), sizeof(int));
          static_cast<ColumnVector<int> *>(res.get())->Insert(v);
        }
        break;
      }
      case ValueType::Type::Double: {
        if (val.Size() == sizeof(double)) {
          double v = 0.0;
          std::memcpy(&v, val.GetData(), sizeof(double));
          static_cast<ColumnVector<double> *>(res.get())->Insert(v);
        }
        break;
      }
      case ValueType::Type::String: {
        static_cast<ColumnString *>(res.get())->Insert(val.ToString());
        break;
      }
      case ValueType::Type::Null: break;
      }
      mem_iter.Next();
    }
  };

  scan_memtable(memtable_);

  // 2. 从 Immutable Tables 读取（仍需迭代）
  for (auto it = immutable_table_.rbegin(); it != immutable_table_.rend();
       ++it) {
    scan_memtable(*it);
  }

  // 3. 从 SSTable 直接批量读取（利用 PAX 列布局）
  for (auto &[id, sstable] : sstables_) {
    ColumnReader::ReadColumnFromSSTable(sstable, column_idx, type, res);
  }

  return Status::OK();
}
} // namespace DB
