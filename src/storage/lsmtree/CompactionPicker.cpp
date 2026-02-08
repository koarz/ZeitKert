#include "storage/lsmtree/CompactionPicker.hpp"

#include <algorithm>
#include <cstring>
#include <limits>

namespace DB {

// 类型感知的 key 比较
int CompactionPicker::CompareKeys(const std::string &a, const std::string &b) const {
  switch (key_type_) {
  case ValueType::Type::Int: {
    if (a.size() != sizeof(int) || b.size() != sizeof(int)) {
      return a.compare(b);
    }
    int a_val = 0, b_val = 0;
    std::memcpy(&a_val, a.data(), sizeof(int));
    std::memcpy(&b_val, b.data(), sizeof(int));
    if (a_val < b_val) return -1;
    if (a_val > b_val) return 1;
    return 0;
  }
  case ValueType::Type::Double: {
    if (a.size() != sizeof(double) || b.size() != sizeof(double)) {
      return a.compare(b);
    }
    double a_val = 0.0, b_val = 0.0;
    std::memcpy(&a_val, a.data(), sizeof(double));
    std::memcpy(&b_val, b.data(), sizeof(double));
    if (a_val < b_val) return -1;
    if (a_val > b_val) return 1;
    return 0;
  }
  case ValueType::Type::String:
  default:
    return a.compare(b);
  }
}

std::optional<CompactionJob>
CompactionPicker::PickCompaction(std::vector<LevelMeta> &levels) {
  // 优先级 1：L0 文件数超过阈值时触发 compaction
  auto l0_job = PickL0Compaction(levels);
  if (l0_job.has_value()) {
    return l0_job;
  }

  // 优先级 2：某层大小超过限制时触发 compaction
  for (uint32_t level = 1; level < levels.size() - 1; level++) {
    auto level_job = PickLevelCompaction(levels, level);
    if (level_job.has_value()) {
      return level_job;
    }
  }

  return std::nullopt;
}

std::optional<CompactionJob>
CompactionPicker::PickL0Compaction(std::vector<LevelMeta> &levels) {
  if (levels.empty() || levels[0].sstables.size() < L0_COMPACTION_THRESHOLD) {
    return std::nullopt;
  }

  // 检查是否有 L0 文件正在 compaction
  for (const auto &meta : levels[0].sstables) {
    if (meta.being_compacted) {
      return std::nullopt;
    }
  }

  CompactionJob job;
  job.input_level = 0;
  job.output_level = 1;

  // 所有 L0 文件参与 compaction，使用类型感知比较找 min/max key
  std::string min_key;
  std::string max_key;
  bool first = true;

  for (const auto &meta : levels[0].sstables) {
    job.input_sstables.push_back(meta.sstable_id);
    if (first) {
      min_key = meta.min_key;
      max_key = meta.max_key;
      first = false;
    } else {
      if (CompareKeys(meta.min_key, min_key) < 0)
        min_key = meta.min_key;
      if (CompareKeys(meta.max_key, max_key) > 0)
        max_key = meta.max_key;
    }
  }

  // 查找 L1 中重叠的文件
  if (levels.size() > 1) {
    job.output_sstables = FindOverlappingFiles(levels[1], min_key, max_key);

    // 检查 L1 重叠文件是否正在 compaction
    for (uint32_t id : job.output_sstables) {
      for (const auto &meta : levels[1].sstables) {
        if (meta.sstable_id == id && meta.being_compacted) {
          return std::nullopt;
        }
      }
    }
  }

  return job;
}

std::optional<CompactionJob>
CompactionPicker::PickLevelCompaction(std::vector<LevelMeta> &levels,
                                      uint32_t level) {
  if (level >= levels.size() - 1) {
    return std::nullopt;
  }

  uint64_t max_size = GetMaxLevelSize(level);
  if (levels[level].total_size <= max_size) {
    return std::nullopt;
  }

  // 选择一个文件进行 compaction（选 min_key 最小的以保证可预测性）
  const LeveledSSTableMeta *candidate = nullptr;
  for (const auto &meta : levels[level].sstables) {
    if (meta.being_compacted) {
      continue;
    }
    if (candidate == nullptr || CompareKeys(meta.min_key, candidate->min_key) < 0) {
      candidate = &meta;
    }
  }

  if (candidate == nullptr) {
    return std::nullopt;
  }

  CompactionJob job;
  job.input_level = level;
  job.output_level = level + 1;
  job.input_sstables.push_back(candidate->sstable_id);

  // 查找下一层中重叠的文件
  job.output_sstables =
      FindOverlappingFiles(levels[level + 1], candidate->min_key,
                           candidate->max_key);

  // 检查下一层重叠文件是否正在 compaction
  for (uint32_t id : job.output_sstables) {
    for (const auto &meta : levels[level + 1].sstables) {
      if (meta.sstable_id == id && meta.being_compacted) {
        return std::nullopt;
      }
    }
  }

  // 平凡移动：下一层无重叠且不是 L0
  if (job.output_sstables.empty() && level > 0) {
    job.is_trivial_move = true;
  }

  return job;
}

std::vector<uint32_t>
CompactionPicker::FindOverlappingFiles(const LevelMeta &level,
                                       const std::string &min_key,
                                       const std::string &max_key) {
  std::vector<uint32_t> result;

  for (const auto &meta : level.sstables) {
    if (KeyRangesOverlap(min_key, max_key, meta.min_key, meta.max_key)) {
      result.push_back(meta.sstable_id);
    }
  }

  return result;
}

uint64_t CompactionPicker::GetMaxLevelSize(uint32_t level) {
  if (level == 0) {
    // L0 由文件数控制，不按大小
    return std::numeric_limits<uint64_t>::max();
  }

  // L1 = L1_MAX_BYTES，每层 10 倍递增
  uint64_t size = L1_MAX_BYTES;
  for (uint32_t i = 1; i < level; i++) {
    size = static_cast<uint64_t>(size * LEVEL_SIZE_MULTIPLIER);
  }
  return size;
}

bool CompactionPicker::KeyRangesOverlap(const std::string &min1,
                                        const std::string &max1,
                                        const std::string &min2,
                                        const std::string &max2) const {
  // 两个范围 [min1, max1] 和 [min2, max2] 重叠当且仅当：
  // NOT (max1 < min2 OR max2 < min1)
  // 即：min1 <= max2 AND min2 <= max1
  return CompareKeys(min1, max2) <= 0 && CompareKeys(min2, max1) <= 0;
}

} // namespace DB
