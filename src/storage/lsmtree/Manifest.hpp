#pragma once

#include "common/Status.hpp"
#include "storage/lsmtree/LevelMeta.hpp"

#include <cstdint>
#include <filesystem>
#include <mutex>
#include <string>
#include <vector>

namespace DB {

// MANIFEST 文件格式：
// 每条记录一行，格式如下：
//   ADD <level> <sstable_id> <file_size> <min_key_hex> <max_key_hex>
//   DEL <level> <sstable_id>
//
// 快照记录以 SNAPSHOT 开头，后接当前状态的所有 ADD 记录。

class Manifest {
public:
  explicit Manifest(std::filesystem::path dir);
  ~Manifest();

  // 从文件加载 manifest，重建 level 状态
  Status Load(std::vector<LevelMeta> &levels);

  // 保存当前 level 状态的完整快照
  Status Save(const std::vector<LevelMeta> &levels);

  // 追加增量变更
  Status AddSSTable(uint32_t level, const LeveledSSTableMeta &meta);
  Status RemoveSSTable(uint32_t level, uint32_t sstable_id);

  // 获取 manifest 文件路径
  std::filesystem::path GetPath() const { return manifest_path_; }

private:
  Status AppendRecord(const std::string &record);
  Status WriteSnapshot(const std::vector<LevelMeta> &levels);

  std::filesystem::path dir_;
  std::filesystem::path manifest_path_;
  std::mutex mutex_;
  size_t record_count_{0};
  static constexpr size_t kSnapshotThreshold =
      100; // 超过 100 条增量记录后重写快照
};

} // namespace DB
