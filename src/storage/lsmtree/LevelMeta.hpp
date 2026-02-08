#pragma once

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

namespace DB {

// 某层中 SSTable 的元数据
struct LeveledSSTableMeta {
  uint32_t sstable_id;
  uint32_t level;
  std::string min_key;
  std::string max_key;
  uint64_t file_size;
  bool being_compacted{false};

  LeveledSSTableMeta() = default;
  LeveledSSTableMeta(uint32_t id, uint32_t lvl, std::string min_k,
                     std::string max_k, uint64_t size)
      : sstable_id(id), level(lvl), min_key(std::move(min_k)),
        max_key(std::move(max_k)), file_size(size), being_compacted(false) {}
};

// LSM-Tree 中单层的元数据
struct LevelMeta {
  uint32_t level_num;
  std::vector<LeveledSSTableMeta> sstables;
  uint64_t total_size{0};

  LevelMeta() : level_num(0), total_size(0) {}
  explicit LevelMeta(uint32_t num) : level_num(num), total_size(0) {}

  void AddSSTable(const LeveledSSTableMeta &meta) {
    sstables.push_back(meta);
    total_size += meta.file_size;
    // L1+ 需要保持按 min_key 排序，以便二分查找
    if (level_num > 0) {
      std::sort(sstables.begin(), sstables.end(),
                [](const LeveledSSTableMeta &a, const LeveledSSTableMeta &b) {
                  return a.min_key < b.min_key;
                });
    }
  }

  void RemoveSSTable(uint32_t sstable_id) {
    for (auto it = sstables.begin(); it != sstables.end(); ++it) {
      if (it->sstable_id == sstable_id) {
        total_size -= it->file_size;
        sstables.erase(it);
        return;
      }
    }
  }

  void Clear() {
    sstables.clear();
    total_size = 0;
  }
};

// Compaction 任务定义
struct CompactionJob {
  uint32_t input_level;
  uint32_t output_level;
  std::vector<uint32_t> input_sstables;  // 输入层的 SSTable ID
  std::vector<uint32_t> output_sstables; // 输出层重叠的 SSTable ID
  bool is_trivial_move{false}; // 若为 true，直接移动文件无需重写
};

} // namespace DB
