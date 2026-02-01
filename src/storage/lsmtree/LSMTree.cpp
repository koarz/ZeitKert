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

  if (wal_files.empty()) {
    // 没有 WAL 文件，创建新的 MemTable
    memtable_ = std::make_unique<MemTable>(
        MakeWalPath(column_path_, wal_number_++), write_log_, false);
  } else {
    // 最后一个 WAL（id 最大）恢复到 MemTable
    auto &[last_id, last_path] = wal_files.back();
    memtable_ = std::make_unique<MemTable>(last_path, write_log_, true);

    // 其他 WAL 恢复到 immutable
    for (size_t i = 0; i + 1 < wal_files.size(); i++) {
      auto &[id, path] = wal_files[i];
      auto mem = std::make_unique<MemTable>(path, write_log_, true);
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
    memtable_ = std::make_unique<MemTable>(
        MakeWalPath(column_path_, wal_number_++), write_log_, false);
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

  // 按 SelectionVector 读取数据
  for (const auto &sel : sv.GetSelections()) {
    switch (sel.source) {
    case DataSource::MemTable: {
      // 从 MemTable 按行读取
      auto iter = memtable_->MakeNewIterator();
      uint32_t row_idx = 0;
      auto read_mem_row = [&](uint32_t target_idx) {
        while (row_idx < target_idx && iter.Valid()) {
          iter.Next();
          row_idx++;
        }
        if (!iter.Valid())
          return;
        Slice val;
        if (!RowCodec::DecodeColumn(iter.GetValue(), column_idx, &val))
          return;
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
      // 从 Immutable Table 按行读取
      if (sel.source_id >= immutable_table_.size())
        break;
      auto &imm = immutable_table_[sel.source_id];
      auto iter = imm->MakeNewIterator();
      uint32_t row_idx = 0;
      auto read_imm_row = [&](uint32_t target_idx) {
        while (row_idx < target_idx && iter.Valid()) {
          iter.Next();
          row_idx++;
        }
        if (!iter.Valid())
          return;
        Slice val;
        if (!RowCodec::DecodeColumn(iter.GetValue(), column_idx, &val))
          return;
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

  // --- 阶段 1: 收集 MemTable/Immutable 的 key 并记录位置 ---
  // 使用有序 vector 以支持二分查找，同时记录每个 key 对应的位置信息用于去重
  struct KeyLocation {
    std::string key;
    DataSource source;
    uint32_t source_id;
    uint32_t row_idx;
  };
  std::vector<KeyLocation> key_locations;

  // 1. 扫描 MemTable（最新数据）
  {
    auto iter = memtable_->MakeNewIterator();
    uint32_t row_idx = 0;
    while (iter.Valid()) {
      if (iter.GetValue().Size() > 0) {
        key_locations.push_back(
            {iter.GetKey().ToString(), DataSource::MemTable, 0, row_idx});
      }
      iter.Next();
      row_idx++;
    }
  }

  // 2. 扫描 Immutable Tables（从新到旧）
  for (size_t i = immutable_table_.size(); i > 0; i--) {
    auto &imm = immutable_table_[i - 1];
    auto iter = imm->MakeNewIterator();
    uint32_t row_idx = 0;
    while (iter.Valid()) {
      if (iter.GetValue().Size() > 0) {
        key_locations.push_back({iter.GetKey().ToString(),
                                 DataSource::Immutable,
                                 static_cast<uint32_t>(i - 1), row_idx});
      }
      iter.Next();
      row_idx++;
    }
  }

  // 按 key 排序，相同 key 的保持原顺序（稳定排序），先出现的优先级更高
  std::stable_sort(
      key_locations.begin(), key_locations.end(),
      [](const KeyLocation &a, const KeyLocation &b) { return a.key < b.key; });

  // 去重：只保留每个 key 的第一个出现（即最新版本）
  std::vector<std::string> mem_keys; // 有序的去重后 key 列表
  {
    std::string last_key;
    bool first = true;
    for (const auto &loc : key_locations) {
      if (first || loc.key != last_key) {
        mem_keys.push_back(loc.key);
        sv.AddRow(loc.source, loc.source_id, 0, loc.row_idx);
        last_key = loc.key;
        first = false;
      }
    }
  }

  // --- 阶段 2: 扫描 SSTable (高性能版) ---
  // mem_keys 现在是有序的，可用二分查找和双指针

  // 全局边界，用于 O(1) 快速跳过
  std::string mem_min_key, mem_max_key;
  if (!mem_keys.empty()) {
    mem_min_key = mem_keys.front();
    mem_max_key = mem_keys.back();
  }

  for (auto it = sstables_.rbegin(); it != sstables_.rend(); ++it) {
    auto &[id, sstable] = *it;
    if (!sstable->data_file_ || !sstable->data_file_->Valid()) {
      continue;
    }

    for (uint32_t rg_idx = 0; rg_idx < sstable->rowgroups_.size(); rg_idx++) {
      const auto &rg = sstable->rowgroups_[rg_idx];

      // [Fast Path 1] MemTable 为空，SSTable 全部有效
      if (mem_keys.empty()) {
        sv.AddContiguous(DataSource::SSTable, id, rg_idx, 0, rg.row_count);
        continue;
      }

      // [Fast Path 2] 全局 ZoneMap 剪枝 O(1)
      const auto &pk_zone = rg.columns[primary_key_idx_].zone;
      bool range_overlap = true;

      if (pk_zone.has_value) {
        // 比较 RowGroup 范围 [rg_min, rg_max] 与 MemTable 范围 [mem_min,
        // mem_max] 如果完全不相交，直接全选
        if (mem_max_key < std::string(pk_zone.min.begin(), pk_zone.min.end()) ||
            mem_min_key > std::string(pk_zone.max.begin(), pk_zone.max.end())) {
          range_overlap = false;
        }
      }

      if (!range_overlap) {
        sv.AddContiguous(DataSource::SSTable, id, rg_idx, 0, rg.row_count);
        continue;
      }

      // [Slow Path] 范围有重叠，用二分查找定位候选区间
      auto candidate_start = mem_keys.begin();
      auto candidate_end = mem_keys.end();

      if (pk_zone.has_value) {
        std::string rg_min_str(pk_zone.min.begin(), pk_zone.min.end());
        std::string rg_max_str(pk_zone.max.begin(), pk_zone.max.end());
        // 找到第一个 >= rg_min 的位置
        candidate_start =
            std::lower_bound(mem_keys.begin(), mem_keys.end(), rg_min_str);
        // 找到第一个 > rg_max 的位置
        candidate_end =
            std::upper_bound(candidate_start, mem_keys.end(), rg_max_str);
      }

      // 区间内没有 mem_key，说明无重叠
      if (candidate_start == candidate_end) {
        sv.AddContiguous(DataSource::SSTable, id, rg_idx, 0, rg.row_count);
        continue;
      }

      // 逐行检查，用双指针加速
      const Byte *base =
          sstable->data_file_->Data() + static_cast<size_t>(rg.offset);
      auto key_type = column_types_[primary_key_idx_]->GetType();

      // 双指针：search_it 只往前走，不回退
      auto search_it = candidate_start;

      for (uint32_t row_idx = 0; row_idx < rg.row_count; row_idx++) {
        const Byte *key_ptr = nullptr;
        uint32_t key_len = 0;
        if (!GetColumnValuePointer(base, rg, row_idx, primary_key_idx_,
                                   key_type, key_ptr, key_len)) {
          continue;
        }

        std::string row_key_str(reinterpret_cast<const char *>(key_ptr),
                                key_len);

        // 双指针推进：跳过小于当前行 key 的 mem_keys
        while (search_it != candidate_end && *search_it < row_key_str) {
          ++search_it;
        }

        // 检查是否命中
        if (search_it != candidate_end && *search_it == row_key_str) {
          // key 在 MemTable/Immutable 中存在，跳过 SSTable 的旧版本
        } else {
          // 有效数据，加入 SV
          sv.AddRow(DataSource::SSTable, id, rg_idx, row_idx);
        }
      }
    }
  }

  return sv;
}
} // namespace DB
