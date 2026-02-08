#pragma once

#include "common/Config.hpp"
#include "storage/lsmtree/LevelMeta.hpp"
#include "type/ValueType.hpp"

#include <cstdint>
#include <cstring>
#include <optional>
#include <vector>

namespace DB {

class CompactionPicker {
public:
  CompactionPicker() = default;

  // 设置主键类型（用于正确比较 key）
  void SetKeyType(ValueType::Type type) { key_type_ = type; }

  // 根据 level 状态选择下一个 compaction 任务
  // 无需 compaction 时返回 nullopt
  std::optional<CompactionJob> PickCompaction(std::vector<LevelMeta> &levels);

private:
  ValueType::Type key_type_{ValueType::Type::String};

  // 类型感知的 key 比较：返回 -1, 0, 1
  int CompareKeys(const std::string &a, const std::string &b) const;

  // 检查 L0 是否需要 compaction（文件数 >= 阈值）
  std::optional<CompactionJob> PickL0Compaction(std::vector<LevelMeta> &levels);

  // 检查某层是否因大小超限需要 compaction
  std::optional<CompactionJob>
  PickLevelCompaction(std::vector<LevelMeta> &levels, uint32_t level);

  // 在目标层中查找与给定 key 范围重叠的 SSTable
  std::vector<uint32_t> FindOverlappingFiles(const LevelMeta &level,
                                             const std::string &min_key,
                                             const std::string &max_key);

  // 获取某层的最大容量
  uint64_t GetMaxLevelSize(uint32_t level);

public:
  // 判断两个 key 范围是否重叠（使用当前 key_type_）
  bool KeyRangesOverlap(const std::string &min1, const std::string &max1,
                        const std::string &min2, const std::string &max2) const;
};

} // namespace DB
