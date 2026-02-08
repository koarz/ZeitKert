#pragma once

#include "common/Config.hpp"
#include "common/Status.hpp"
#include "storage/IndexEngine.hpp"
#include "storage/column/Column.hpp"
#include "storage/lsmtree/LevelMeta.hpp"
#include "storage/lsmtree/MemTable.hpp"
#include "storage/lsmtree/SSTable.hpp"
#include "storage/lsmtree/SelectionVector.hpp"
#include "storage/lsmtree/Slice.hpp"
#include "storage/lsmtree/TableOperator.hpp"
#include "type/ValueType.hpp"

#include <atomic>
#include <cstddef>
#include <map>
#include <memory>
#include <shared_mutex>
#include <vector>

namespace DB {

// 前向声明
class Manifest;
class CompactionScheduler;

class LSMTree : public IndexEngine<Slice, Slice, SliceCompare> {
  std::shared_mutex latch_;
  std::shared_mutex immutable_latch_;
  std::shared_mutex level_latch_;

  bool write_log_;
  MemTableRef memtable_;
  uint32_t table_number_;
  uint32_t wal_number_{0};
  std::vector<std::shared_ptr<ValueType>> column_types_;
  uint16_t primary_key_idx_{};
  // TODO:
  // the immutable table may reading by other threads
  // so we need safely delete them when immutable was full
  std::vector<MemTableRef> immutable_table_;
  std::map<uint32_t, SSTableRef> sstables_;

  // 分层 compaction 支持
  std::vector<LevelMeta> levels_;
  std::unique_ptr<Manifest> manifest_;
  std::unique_ptr<CompactionScheduler> compaction_scheduler_;
  std::atomic<uint32_t> next_table_id_{0};

  // 主键类型特化的 BuildSelectionVector 实现
  SelectionVector BuildSelectionVectorInt();
  SelectionVector BuildSelectionVectorString();

  // 刷盘后将新 SSTable 加入 L0
  void AddToL0(uint32_t sstable_id, const SSTableRef &sstable);

public:
  LSMTree(std::filesystem::path table_path,
          std::shared_ptr<BufferPoolManager> buffer_pool_manager,
          std::vector<std::shared_ptr<ValueType>> column_types,
          uint16_t primary_key_idx, bool write_log = true);

  ~LSMTree() override;

  Status Insert(const Slice &key, const Slice &value) override;

  Status Remove(const Slice &key) override;

  Status GetValue(const Slice &key, Slice *column) override;

  Status ScanColumn(size_t column_idx, ColumnPtr &res);

  // 构建去重后的 SelectionVector
  SelectionVector BuildSelectionVector();

  size_t GetImmutableSize() { return immutable_table_.size(); }

  uint32_t GetTableNum() { return table_number_; }

  // 手动刷盘：将当前 MemTable 和所有 Immutable tables 刷盘为 SST 文件
  Status FlushToSST();

  uint16_t GetPrimaryKeyIndex() const { return primary_key_idx_; }

  const std::vector<std::shared_ptr<ValueType>> &GetColumnTypes() const {
    return column_types_;
  }

  // compaction 用：获取路径
  const std::filesystem::path &GetPath() const { return column_path_; }

  // Level 管理
  std::vector<LevelMeta> &GetLevels() { return levels_; }
  const std::vector<LevelMeta> &GetLevels() const { return levels_; }

  // compaction 调度器用：获取锁
  std::shared_mutex &GetLatch() { return latch_; }
  std::shared_mutex &GetLevelLatch() { return level_latch_; }

  // 根据 ID 获取 SSTable
  SSTableRef GetSSTable(uint32_t id);

  // 注册新 SSTable（compaction 使用）
  void RegisterSSTable(uint32_t id, SSTableRef sstable);

  // 获取下一个可用的 table ID（线程安全）
  uint32_t GetNextTableId();

  // 原子安装 compaction 结果
  Status InstallCompactionResults(const CompactionJob &job,
                                  const std::vector<uint32_t> &new_sstable_ids);

  // 手动触发 compaction（测试用）
  void TriggerCompaction();

  // 获取 L0 文件数（测试用）
  size_t GetL0FileCount() const;
};
} // namespace DB
