#include "storage/lsmtree/LSMTree.hpp"
#include "common/Config.hpp"
#include "common/Status.hpp"
#include "fmt/format.h"
#include "storage/column/Column.hpp"
#include "storage/column/ColumnString.hpp"
#include "storage/column/ColumnVector.hpp"
#include "storage/lsmtree/BloomFilter.hpp"
#include "storage/lsmtree/ColumnReader.hpp"
#include "storage/lsmtree/MemTable.hpp"
#include "storage/lsmtree/RowCodec.hpp"
#include "storage/lsmtree/SelectionVector.hpp"
#include "storage/lsmtree/Slice.hpp"
#include "storage/lsmtree/TableOperator.hpp"

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
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

// 根据 id 生成 WAL 文件路径
static std::filesystem::path MakeWalPath(const std::filesystem::path &base,
                                         uint32_t id) {
  return base / fmt::format("{}.wal", id);
}

LSMTree::LSMTree(std::filesystem::path table_path,
                 std::shared_ptr<BufferPoolManager> buffer_pool_manager,
                 std::vector<std::shared_ptr<ValueType>> column_types,
                 uint16_t primary_key_idx, bool write_log)
    : IndexEngine(SliceCompare{}, std::move(table_path),
                  std::move(buffer_pool_manager)),
      write_log_(write_log), table_number_(0),
      column_types_(std::move(column_types)),
      primary_key_idx_(primary_key_idx) {
  if (column_types_.empty()) {
    primary_key_idx_ = 0;
  } else if (primary_key_idx_ >= column_types_.size()) {
    primary_key_idx_ = 0;
  }

  // 收集所有 WAL 文件的 id
  std::vector<std::pair<uint32_t, std::filesystem::path>> wal_files;

  if (std::filesystem::exists(column_path_)) {
    // 扫描目录中的 .sst 和 .wal 文件
    for (const auto &entry :
         std::filesystem::directory_iterator(column_path_)) {
      if (!entry.is_regular_file()) {
        continue;
      }
      if (entry.path().extension() == ".sst") {
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
      } else if (entry.path().extension() == ".wal") {
        auto stem = entry.path().stem().string();
        uint32_t id = 0;
        try {
          id = static_cast<uint32_t>(std::stoul(stem));
        } catch (...) {
          continue;
        }
        wal_files.emplace_back(id, entry.path());
        wal_number_ = std::max(wal_number_, id + 1);
      }
    }
  }

  // 按 id 排序 WAL 文件
  std::sort(wal_files.begin(), wal_files.end());

  // 获取主键类型
  auto pk_type = column_types_.empty()
                     ? ValueType::Type::String
                     : column_types_[primary_key_idx_]->GetType();

  if (wal_files.empty()) {
    // 没有 WAL 文件，创建新的 MemTable
    memtable_ = std::make_unique<MemTable>(
        MakeWalPath(column_path_, wal_number_++), write_log_, pk_type, false);
  } else {
    // 最后一个 WAL（id 最大）恢复到 MemTable
    auto &[last_id, last_path] = wal_files.back();
    memtable_ =
        std::make_unique<MemTable>(last_path, write_log_, pk_type, true);

    // 其他 WAL 恢复到 immutable
    for (size_t i = 0; i + 1 < wal_files.size(); i++) {
      auto &[id, path] = wal_files[i];
      auto mem = std::make_unique<MemTable>(path, write_log_, pk_type, true);
      mem->ToImmutable();
      immutable_table_.push_back(std::move(mem));
    }
  }
}

LSMTree::~LSMTree() {
  // 逐个刷盘所有 immutable tables，并删除对应的 WAL 文件
  while (!immutable_table_.empty()) {
    auto &imm = immutable_table_.front();
    // 跳过空的 immutable table
    if (imm->GetApproximateSize() == 0) {
      imm->DeleteWal();
      immutable_table_.erase(immutable_table_.begin());
      continue;
    }
    std::vector<MemTableRef> to_flush;
    to_flush.push_back(std::move(imm));
    immutable_table_.erase(immutable_table_.begin());
    auto &table_meta = sstables_[table_number_] = std::make_shared<SSTable>();
    table_meta->sstable_id_ = table_number_;
    std::ignore = TableOperator::BuildSSTable(column_path_, table_number_,
                                              to_flush, column_types_,
                                              primary_key_idx_, table_meta);
    // 刷盘后删除 WAL 文件
    for (auto &mem : to_flush) {
      mem->DeleteWal();
    }
  }
}

Status LSMTree::Insert(const Slice &key, const Slice &value) {
  if (value.Size() > SSTABLE_SIZE) {
    return Status::Error(
        ErrorCode::InsertError,
        "Your row data too large, please split it to less than 64MB");
  }
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
      // 刷盘后删除 WAL 文件
      for (auto &mem : to_flush) {
        mem->DeleteWal();
      }
    }
    // 创建新的 memtable，使用新的 WAL 序号
    auto pk_type = column_types_.empty()
                       ? ValueType::Type::String
                       : column_types_[primary_key_idx_]->GetType();
    memtable_ = std::make_unique<MemTable>(
        MakeWalPath(column_path_, wal_number_++), write_log_, pk_type, false);
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

  // 构建去重后的 SelectionVector
  auto sv = BuildSelectionVector();

  // 预分配容量减少重分配
  size_t total_rows = sv.TotalRows();
  switch (type->GetType()) {
  case ValueType::Type::Int:
    static_cast<ColumnVector<int> *>(res.get())->Reserve(total_rows);
    break;
  case ValueType::Type::Double:
    static_cast<ColumnVector<double> *>(res.get())->Reserve(total_rows);
    break;
  case ValueType::Type::String:
    static_cast<ColumnString *>(res.get())->Reserve(total_rows);
    break;
  case ValueType::Type::Null: break;
  }

  // 按 SelectionVector 读取数据
  for (const auto &sel : sv.GetSelections()) {
    switch (sel.source) {
    case DataSource::MemTable: {
      // 零拷贝直接访问 Arena
      auto &impl = memtable_->GetImpl();
      auto read_mem_row = [&](uint32_t target_idx) {
        const Byte *value_ptr = nullptr;
        uint32_t value_len = 0;
        if (!impl.GetValueRawByIndex(target_idx, value_ptr, value_len))
          return;

        // 零拷贝解析列
        const Byte *col_ptr = nullptr;
        uint32_t col_len = 0;
        if (!RowCodec::DecodeColumnRaw(value_ptr, value_len, column_idx,
                                       col_ptr, col_len))
          return;

        switch (type->GetType()) {
        case ValueType::Type::Int: {
          if (col_len == sizeof(int)) {
            int v = 0;
            std::memcpy(&v, col_ptr, sizeof(int));
            static_cast<ColumnVector<int> *>(res.get())->Insert(v);
          }
          break;
        }
        case ValueType::Type::Double: {
          if (col_len == sizeof(double)) {
            double v = 0.0;
            std::memcpy(&v, col_ptr, sizeof(double));
            static_cast<ColumnVector<double> *>(res.get())->Insert(v);
          }
          break;
        }
        case ValueType::Type::String: {
          static_cast<ColumnString *>(res.get())->Insert(
              std::string(col_ptr, col_len));
          break;
        }
        case ValueType::Type::Null: break;
        }
      };

      if (sel.IsContiguous()) {
        for (uint32_t i = 0; i < sel.count; i++) {
          read_mem_row(sel.start_row + i);
        }
      } else {
        for (uint32_t idx : sel.rows) {
          read_mem_row(idx);
        }
      }
      break;
    }
    case DataSource::Immutable: {
      // 零拷贝直接访问 Immutable Arena
      if (sel.source_id >= immutable_table_.size())
        break;
      auto &impl = immutable_table_[sel.source_id]->GetImpl();

      auto read_imm_row = [&](uint32_t target_idx) {
        const Byte *value_ptr = nullptr;
        uint32_t value_len = 0;
        if (!impl.GetValueRawByIndex(target_idx, value_ptr, value_len))
          return;

        const Byte *col_ptr = nullptr;
        uint32_t col_len = 0;
        if (!RowCodec::DecodeColumnRaw(value_ptr, value_len, column_idx,
                                       col_ptr, col_len))
          return;

        switch (type->GetType()) {
        case ValueType::Type::Int: {
          if (col_len == sizeof(int)) {
            int v = 0;
            std::memcpy(&v, col_ptr, sizeof(int));
            static_cast<ColumnVector<int> *>(res.get())->Insert(v);
          }
          break;
        }
        case ValueType::Type::Double: {
          if (col_len == sizeof(double)) {
            double v = 0.0;
            std::memcpy(&v, col_ptr, sizeof(double));
            static_cast<ColumnVector<double> *>(res.get())->Insert(v);
          }
          break;
        }
        case ValueType::Type::String: {
          static_cast<ColumnString *>(res.get())->Insert(
              std::string(col_ptr, col_len));
          break;
        }
        case ValueType::Type::Null: break;
        }
      };

      if (sel.IsContiguous()) {
        for (uint32_t i = 0; i < sel.count; i++) {
          read_imm_row(sel.start_row + i);
        }
      } else {
        for (uint32_t idx : sel.rows) {
          read_imm_row(idx);
        }
      }
      break;
    }
    case DataSource::SSTable: {
      // 从 SSTable 利用 ColumnReader 批量读取
      auto it = sstables_.find(sel.source_id);
      if (it == sstables_.end())
        break;
      auto &sstable = it->second;
      if (!sstable->data_file_ || !sstable->data_file_->Valid())
        break;
      if (sel.rowgroup_idx >= sstable->rowgroups_.size())
        break;

      const auto &rg = sstable->rowgroups_[sel.rowgroup_idx];
      const Byte *rg_base =
          sstable->data_file_->Data() + static_cast<size_t>(rg.offset);
      ColumnReader::ReadColumnWithSelection(rg, rg_base, column_idx, type, sel,
                                            res);
      break;
    }
    }
  }

  return Status::OK();
}

SelectionVector LSMTree::BuildSelectionVector() {
  SelectionVector sv;

  auto pk_type = column_types_.empty()
                     ? ValueType::Type::String
                     : column_types_[primary_key_idx_]->GetType();

  // 根据主键类型分派不同实现
  switch (pk_type) {
  case ValueType::Type::Int: return BuildSelectionVectorInt();
  default: return BuildSelectionVectorString();
  }
}

// 整数主键特化：排序时直接比较 int，极度高效
SelectionVector LSMTree::BuildSelectionVectorInt() {
  SelectionVector sv;

  struct IntKeyLoc {
    int key;
    DataSource source;
    uint32_t source_id;
    uint32_t row_idx;
  };

  // 预估容量
  size_t estimated_count = memtable_->GetImpl().Count();
  for (auto &imm : immutable_table_) {
    estimated_count += imm->GetImpl().Count();
  }

  std::vector<IntKeyLoc> key_locations;
  key_locations.reserve(estimated_count);

  // 收集 MemTable
  {
    const auto &entries = memtable_->GetImpl().GetIntEntries();
    for (size_t i = 0; i < entries.size(); ++i) {
      const auto &e = entries[i];
      if (e.value_len == 0)
        continue; // 跳过删除
      key_locations.push_back(
          {e.key, DataSource::MemTable, 0, static_cast<uint32_t>(i)});
    }
  }

  // 收集 Immutable（从新到旧）
  for (size_t imm_idx = immutable_table_.size(); imm_idx > 0; --imm_idx) {
    const auto &entries =
        immutable_table_[imm_idx - 1]->GetImpl().GetIntEntries();
    for (size_t i = 0; i < entries.size(); ++i) {
      const auto &e = entries[i];
      if (e.value_len == 0)
        continue;
      key_locations.push_back({e.key, DataSource::Immutable,
                               static_cast<uint32_t>(imm_idx - 1),
                               static_cast<uint32_t>(i)});
    }
  }

  // 排序：同 key 时 row_idx 大的（新版本）排前面
  std::stable_sort(
      key_locations.begin(), key_locations.end(),
      [](const IntKeyLoc &a, const IntKeyLoc &b) {
        if (a.key != b.key) return a.key < b.key;
        return a.row_idx > b.row_idx;
      });

  // 去重并分组（取第一个即最新版本）
  std::vector<int> mem_keys;
  mem_keys.reserve(key_locations.size());

  std::vector<uint32_t> memtable_rows;
  std::vector<std::vector<uint32_t>> immutable_rows(immutable_table_.size());

  int last_key = 0;
  bool first = true;
  for (const auto &loc : key_locations) {
    if (first || loc.key != last_key) {
      mem_keys.push_back(loc.key);
      if (loc.source == DataSource::MemTable) {
        memtable_rows.push_back(loc.row_idx);
      } else {
        immutable_rows[loc.source_id].push_back(loc.row_idx);
      }
      last_key = loc.key;
      first = false;
    }
  }

  // 批量添加
  if (!memtable_rows.empty()) {
    sv.AddRows(DataSource::MemTable, 0, 0, std::move(memtable_rows));
  }
  for (size_t i = 0; i < immutable_rows.size(); ++i) {
    if (!immutable_rows[i].empty()) {
      sv.AddRows(DataSource::Immutable, static_cast<uint32_t>(i), 0,
                 std::move(immutable_rows[i]));
    }
  }

  // SSTable 去重
  if (mem_keys.empty()) {
    for (auto it = sstables_.rbegin(); it != sstables_.rend(); ++it) {
      auto &[id, sstable] = *it;
      if (!sstable->data_file_ || !sstable->data_file_->Valid())
        continue;
      for (uint32_t rg_idx = 0; rg_idx < sstable->rowgroups_.size(); ++rg_idx) {
        sv.AddContiguous(DataSource::SSTable, id, rg_idx, 0,
                         sstable->rowgroups_[rg_idx].row_count);
      }
    }
    return sv;
  }

  int mem_min = mem_keys.front();
  int mem_max = mem_keys.back();

  for (auto it = sstables_.rbegin(); it != sstables_.rend(); ++it) {
    auto &[id, sstable] = *it;
    if (!sstable->data_file_ || !sstable->data_file_->Valid())
      continue;

    const Byte *sst_base = sstable->data_file_->Data();

    for (uint32_t rg_idx = 0; rg_idx < sstable->rowgroups_.size(); ++rg_idx) {
      const auto &rg = sstable->rowgroups_[rg_idx];
      const auto &pk_zone = rg.columns[primary_key_idx_].zone;

      if (pk_zone.has_value && pk_zone.min.size() == sizeof(int)) {
        int zone_min = *reinterpret_cast<const int *>(pk_zone.min.data());
        int zone_max = *reinterpret_cast<const int *>(pk_zone.max.data());

        if (mem_max < zone_min || zone_max < mem_min) {
          sv.AddContiguous(DataSource::SSTable, id, rg_idx, 0, rg.row_count);
          continue;
        }

        auto candidate_start =
            std::lower_bound(mem_keys.begin(), mem_keys.end(), zone_min);
        auto candidate_end =
            std::upper_bound(candidate_start, mem_keys.end(), zone_max);

        if (candidate_start == candidate_end) {
          sv.AddContiguous(DataSource::SSTable, id, rg_idx, 0, rg.row_count);
          continue;
        }

        std::vector<uint32_t> valid_rows;
        valid_rows.reserve(rg.row_count);

        const Byte *rg_base = sst_base + static_cast<size_t>(rg.offset);
        const Byte *col_base = rg_base + rg.columns[primary_key_idx_].offset;
        auto search_it = candidate_start;

        for (uint32_t row_idx = 0; row_idx < rg.row_count; ++row_idx) {
          int row_key =
              *reinterpret_cast<const int *>(col_base + row_idx * sizeof(int));

          while (search_it != candidate_end && *search_it < row_key) {
            ++search_it;
          }

          if (!(search_it != candidate_end && *search_it == row_key)) {
            valid_rows.push_back(row_idx);
          }
        }

        if (valid_rows.size() == rg.row_count) {
          sv.AddContiguous(DataSource::SSTable, id, rg_idx, 0, rg.row_count);
        } else if (!valid_rows.empty()) {
          sv.AddRows(DataSource::SSTable, id, rg_idx, std::move(valid_rows));
        }
      } else {
        // 无 ZoneMap，全量扫描
        std::vector<uint32_t> valid_rows;
        valid_rows.reserve(rg.row_count);

        const Byte *rg_base = sst_base + static_cast<size_t>(rg.offset);
        const Byte *col_base = rg_base + rg.columns[primary_key_idx_].offset;
        auto search_it = mem_keys.begin();

        for (uint32_t row_idx = 0; row_idx < rg.row_count; ++row_idx) {
          int row_key =
              *reinterpret_cast<const int *>(col_base + row_idx * sizeof(int));

          while (search_it != mem_keys.end() && *search_it < row_key) {
            ++search_it;
          }

          if (!(search_it != mem_keys.end() && *search_it == row_key)) {
            valid_rows.push_back(row_idx);
          }
        }

        if (valid_rows.size() == rg.row_count) {
          sv.AddContiguous(DataSource::SSTable, id, rg_idx, 0, rg.row_count);
        } else if (!valid_rows.empty()) {
          sv.AddRows(DataSource::SSTable, id, rg_idx, std::move(valid_rows));
        }
      }
    }
  }

  return sv;
}

// 字符串主键（通用实现）
SelectionVector LSMTree::BuildSelectionVectorString() {
  SelectionVector sv;

  struct KeyRef {
    const Byte *ptr;
    uint32_t len;
    bool operator<(const KeyRef &other) const {
      size_t min_len = std::min(len, other.len);
      int cmp = std::memcmp(ptr, other.ptr, min_len);
      return cmp != 0 ? cmp < 0 : len < other.len;
    }
    bool operator==(const KeyRef &other) const {
      return len == other.len && std::memcmp(ptr, other.ptr, len) == 0;
    }
  };

  struct KeyLocation {
    KeyRef key;
    DataSource source;
    uint32_t source_id;
    uint32_t row_idx;
  };

  size_t estimated_count = memtable_->GetImpl().Count();
  for (auto &imm : immutable_table_) {
    estimated_count += imm->GetImpl().Count();
  }

  std::vector<KeyLocation> key_locations;
  key_locations.reserve(estimated_count);

  // 收集 MemTable
  {
    auto &impl = memtable_->GetImpl();
    const auto &entries = impl.GetStringEntries();
    const Byte *key_base = impl.KeyData();

    for (size_t i = 0; i < entries.size(); ++i) {
      const auto &e = entries[i];
      if (e.value_len == 0)
        continue;
      KeyRef k{key_base + e.key_offset, e.key_len};
      key_locations.push_back(
          {k, DataSource::MemTable, 0, static_cast<uint32_t>(i)});
    }
  }

  // 收集 Immutable
  for (size_t imm_idx = immutable_table_.size(); imm_idx > 0; --imm_idx) {
    auto &impl = immutable_table_[imm_idx - 1]->GetImpl();
    const auto &entries = impl.GetStringEntries();
    const Byte *key_base = impl.KeyData();

    for (size_t i = 0; i < entries.size(); ++i) {
      const auto &e = entries[i];
      if (e.value_len == 0)
        continue;
      KeyRef k{key_base + e.key_offset, e.key_len};
      key_locations.push_back({k, DataSource::Immutable,
                               static_cast<uint32_t>(imm_idx - 1),
                               static_cast<uint32_t>(i)});
    }
  }

  // 排序：同 key 时 row_idx 大的（新版本）排前面
  std::stable_sort(
      key_locations.begin(), key_locations.end(),
      [](const KeyLocation &a, const KeyLocation &b) {
        if (!(a.key == b.key)) return a.key < b.key;
        return a.row_idx > b.row_idx;
      });

  std::vector<KeyRef> mem_keys;
  mem_keys.reserve(key_locations.size());

  std::vector<uint32_t> memtable_rows;
  std::vector<std::vector<uint32_t>> immutable_rows(immutable_table_.size());

  KeyRef last_key{nullptr, 0};
  bool first = true;
  for (const auto &loc : key_locations) {
    if (first || !(loc.key == last_key)) {
      mem_keys.push_back(loc.key);
      if (loc.source == DataSource::MemTable) {
        memtable_rows.push_back(loc.row_idx);
      } else {
        immutable_rows[loc.source_id].push_back(loc.row_idx);
      }
      last_key = loc.key;
      first = false;
    }
  }

  if (!memtable_rows.empty()) {
    sv.AddRows(DataSource::MemTable, 0, 0, std::move(memtable_rows));
  }
  for (size_t i = 0; i < immutable_rows.size(); ++i) {
    if (!immutable_rows[i].empty()) {
      sv.AddRows(DataSource::Immutable, static_cast<uint32_t>(i), 0,
                 std::move(immutable_rows[i]));
    }
  }

  // SSTable 去重
  if (mem_keys.empty()) {
    for (auto it = sstables_.rbegin(); it != sstables_.rend(); ++it) {
      auto &[id, sstable] = *it;
      if (!sstable->data_file_ || !sstable->data_file_->Valid())
        continue;
      for (uint32_t rg_idx = 0; rg_idx < sstable->rowgroups_.size(); ++rg_idx) {
        sv.AddContiguous(DataSource::SSTable, id, rg_idx, 0,
                         sstable->rowgroups_[rg_idx].row_count);
      }
    }
    return sv;
  }

  KeyRef mem_min = mem_keys.front();
  KeyRef mem_max = mem_keys.back();

  for (auto it = sstables_.rbegin(); it != sstables_.rend(); ++it) {
    auto &[id, sstable] = *it;
    if (!sstable->data_file_ || !sstable->data_file_->Valid())
      continue;

    const Byte *sst_base = sstable->data_file_->Data();

    for (uint32_t rg_idx = 0; rg_idx < sstable->rowgroups_.size(); ++rg_idx) {
      const auto &rg = sstable->rowgroups_[rg_idx];
      const auto &pk_zone = rg.columns[primary_key_idx_].zone;

      if (pk_zone.has_value) {
        KeyRef zone_min{reinterpret_cast<const Byte *>(pk_zone.min.data()),
                        static_cast<uint32_t>(pk_zone.min.size())};
        KeyRef zone_max{reinterpret_cast<const Byte *>(pk_zone.max.data()),
                        static_cast<uint32_t>(pk_zone.max.size())};

        if (mem_max < zone_min || zone_max < mem_min) {
          sv.AddContiguous(DataSource::SSTable, id, rg_idx, 0, rg.row_count);
          continue;
        }

        auto candidate_start =
            std::lower_bound(mem_keys.begin(), mem_keys.end(), zone_min);
        auto candidate_end =
            std::upper_bound(candidate_start, mem_keys.end(), zone_max);

        if (candidate_start == candidate_end) {
          sv.AddContiguous(DataSource::SSTable, id, rg_idx, 0, rg.row_count);
          continue;
        }

        std::vector<uint32_t> valid_rows;
        valid_rows.reserve(rg.row_count);

        const Byte *rg_base = sst_base + static_cast<size_t>(rg.offset);
        auto key_type = column_types_[primary_key_idx_]->GetType();
        auto search_it = candidate_start;

        for (uint32_t row_idx = 0; row_idx < rg.row_count; ++row_idx) {
          const Byte *key_ptr = nullptr;
          uint32_t key_len = 0;
          if (!GetColumnValuePointer(rg_base, rg, row_idx, primary_key_idx_,
                                     key_type, key_ptr, key_len)) {
            continue;
          }

          KeyRef row_key{key_ptr, key_len};
          while (search_it != candidate_end && *search_it < row_key) {
            ++search_it;
          }

          if (!(search_it != candidate_end && *search_it == row_key)) {
            valid_rows.push_back(row_idx);
          }
        }

        if (valid_rows.size() == rg.row_count) {
          sv.AddContiguous(DataSource::SSTable, id, rg_idx, 0, rg.row_count);
        } else if (!valid_rows.empty()) {
          sv.AddRows(DataSource::SSTable, id, rg_idx, std::move(valid_rows));
        }
      } else {
        std::vector<uint32_t> valid_rows;
        valid_rows.reserve(rg.row_count);

        const Byte *rg_base = sst_base + static_cast<size_t>(rg.offset);
        auto key_type = column_types_[primary_key_idx_]->GetType();
        auto search_it = mem_keys.begin();

        for (uint32_t row_idx = 0; row_idx < rg.row_count; ++row_idx) {
          const Byte *key_ptr = nullptr;
          uint32_t key_len = 0;
          if (!GetColumnValuePointer(rg_base, rg, row_idx, primary_key_idx_,
                                     key_type, key_ptr, key_len)) {
            continue;
          }

          KeyRef row_key{key_ptr, key_len};
          while (search_it != mem_keys.end() && *search_it < row_key) {
            ++search_it;
          }

          if (!(search_it != mem_keys.end() && *search_it == row_key)) {
            valid_rows.push_back(row_idx);
          }
        }

        if (valid_rows.size() == rg.row_count) {
          sv.AddContiguous(DataSource::SSTable, id, rg_idx, 0, rg.row_count);
        } else if (!valid_rows.empty()) {
          sv.AddRows(DataSource::SSTable, id, rg_idx, std::move(valid_rows));
        }
      }
    }
  }

  return sv;
}
} // namespace DB
