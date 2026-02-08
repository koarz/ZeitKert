#include "storage/lsmtree/CompactionScheduler.hpp"
#include "common/Config.hpp"
#include "common/Logger.hpp"
#include "fmt/format.h"
#include "storage/lsmtree/LSMTree.hpp"
#include "storage/lsmtree/Manifest.hpp"
#include "storage/lsmtree/builder/SSTableBuilder.hpp"
#include "storage/lsmtree/iterator/MergeIterator.hpp"
#include "storage/lsmtree/iterator/SSTableIterator.hpp"

#include <algorithm>
#include <filesystem>
#include <functional>
#include <memory>

namespace DB {

CompactionScheduler::CompactionScheduler(LSMTree *tree) : tree_(tree) {
  // 设置 CompactionPicker 的 key 类型
  auto &column_types = tree_->GetColumnTypes();
  auto pk_idx = tree_->GetPrimaryKeyIndex();
  if (pk_idx < column_types.size()) {
    picker_.SetKeyType(column_types[pk_idx]->GetType());
  }
}

CompactionScheduler::~CompactionScheduler() {
  Stop();
}

void CompactionScheduler::Start() {
  if (running_.load()) {
    return;
  }

  stop_requested_.store(false);
  running_.store(true);

  background_thread_ = std::thread([this]() { BackgroundThread(); });
}

void CompactionScheduler::Stop() {
  if (!running_.load()) {
    return;
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    stop_requested_.store(true);
  }
  cv_.notify_all();

  if (background_thread_.joinable()) {
    background_thread_.join();
  }

  running_.store(false);
}

void CompactionScheduler::MaybeScheduleCompaction() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    compaction_pending_.store(true);
  }
  cv_.notify_one();
}

void CompactionScheduler::BackgroundThread() {
  LOG_INFO("Compaction background thread started");
  while (!stop_requested_.load()) {
    std::unique_lock<std::mutex> lock(mutex_);

    // 等待 compaction 信号或停止请求
    cv_.wait(lock, [this]() {
      return stop_requested_.load() || compaction_pending_.load();
    });

    if (stop_requested_.load()) {
      break;
    }

    compaction_pending_.store(false);
    lock.unlock();

    // 尝试选取并执行 compaction
    bool did_work = true;
    while (did_work && !stop_requested_.load()) {
      auto &levels = tree_->GetLevels();
      std::vector<LevelMeta> levels_copy;
      {
        std::shared_lock<std::shared_mutex> levels_lock(tree_->GetLevelLatch());
        levels_copy = levels;
      }

      auto job = picker_.PickCompaction(levels_copy);
      if (!job.has_value()) {
        did_work = false;
        break;
      }

      // 标记文件为正在 compaction
      {
        std::unique_lock<std::shared_mutex> levels_lock(tree_->GetLevelLatch());
        auto &real_levels = tree_->GetLevels();

        for (uint32_t id : job->input_sstables) {
          for (auto &meta : real_levels[job->input_level].sstables) {
            if (meta.sstable_id == id) {
              meta.being_compacted = true;
              break;
            }
          }
        }

        for (uint32_t id : job->output_sstables) {
          for (auto &meta : real_levels[job->output_level].sstables) {
            if (meta.sstable_id == id) {
              meta.being_compacted = true;
              break;
            }
          }
        }
      }

      auto status = DoCompaction(*job);
      if (!status.ok()) {
        // 失败时取消标记
        std::unique_lock<std::shared_mutex> levels_lock(tree_->GetLevelLatch());
        auto &real_levels = tree_->GetLevels();

        for (uint32_t id : job->input_sstables) {
          for (auto &meta : real_levels[job->input_level].sstables) {
            if (meta.sstable_id == id) {
              meta.being_compacted = false;
              break;
            }
          }
        }

        for (uint32_t id : job->output_sstables) {
          for (auto &meta : real_levels[job->output_level].sstables) {
            if (meta.sstable_id == id) {
              meta.being_compacted = false;
              break;
            }
          }
        }
        did_work = false;
      }
    }
  }
  LOG_INFO("Compaction background thread stopped");
}

Status CompactionScheduler::DoCompaction(CompactionJob &job) {
  LOG_INFO("DoCompaction: L{} -> L{}, input_files={}, output_files={}",
           job.input_level, job.output_level, job.input_sstables.size(),
           job.output_sstables.size());
  // 收集输入 SSTable 的迭代器
  std::vector<std::shared_ptr<Iterator>> iters;

  // 获取列类型和主键索引
  auto column_types = tree_->GetColumnTypes();
  auto primary_key_idx = tree_->GetPrimaryKeyIndex();
  auto &path = tree_->GetPath();

  // 输入层文件按 ID 降序排列（确保新数据在前）
  std::sort(job.input_sstables.begin(), job.input_sstables.end(),
            std::greater<uint32_t>());

  // 收集所有输入 SSTable
  std::vector<SSTableRef> input_tables;

  {
    std::shared_lock<std::shared_mutex> levels_lock(tree_->GetLevelLatch());

    // 输入层文件（按 ID 降序，新的在前）
    for (uint32_t id : job.input_sstables) {
      auto sstable = tree_->GetSSTable(id);
      if (sstable) {
        input_tables.push_back(sstable);
      }
    }

    // 输出层文件（重叠的，较旧的数据）
    for (uint32_t id : job.output_sstables) {
      auto sstable = tree_->GetSSTable(id);
      if (sstable) {
        input_tables.push_back(sstable);
      }
    }
  }

  if (input_tables.empty()) {
    return Status::Error(ErrorCode::IOError, "No input tables for compaction");
  }

  // 处理平凡移动（无需合并）
  if (job.is_trivial_move && job.input_sstables.size() == 1 &&
      job.output_sstables.empty()) {
    return tree_->InstallCompactionResults(job, job.input_sstables);
  }

  // 创建合并用的迭代器 - 较新的表在前（输入层的表较新）
  for (auto &sstable : input_tables) {
    iters.push_back(std::make_shared<SSTableIterator>(sstable, column_types));
  }

  // 创建合并迭代器，传入主键类型以进行正确比较
  auto pk_type = column_types[primary_key_idx]->GetType();
  MergeIterator merge_iter(std::move(iters), pk_type);

  // 判断是否可以删除 tombstone
  bool is_bottom_level = (job.output_level == MAX_LEVELS - 1);
  bool no_overlap_below = false;

  if (!is_bottom_level) {
    // 检查输出层以下是否有重叠文件
    std::string min_key, max_key;
    bool first = true;

    for (auto &table : input_tables) {
      for (auto &rg : table->rowgroups_) {
        if (first) {
          if (!rg.max_key.empty()) {
            max_key = rg.max_key;
          }
          first = false;
        } else {
          if (!rg.max_key.empty() && rg.max_key > max_key) {
            max_key = rg.max_key;
          }
        }
      }
    }

    // 检查下层是否有重叠
    {
      std::shared_lock<std::shared_mutex> levels_lock(tree_->GetLevelLatch());
      auto &levels = tree_->GetLevels();

      no_overlap_below = true;
      for (uint32_t level = job.output_level + 1; level < levels.size();
           level++) {
        for (auto &meta : levels[level].sstables) {
          if (picker_.KeyRangesOverlap(min_key, max_key, meta.min_key,
                                       meta.max_key)) {
            no_overlap_below = false;
            break;
          }
        }
        if (!no_overlap_below)
          break;
      }
    }
  }

  bool can_drop_tombstone = is_bottom_level || no_overlap_below;
  if (can_drop_tombstone) {
    LOG_INFO("DoCompaction: tombstone cleanup enabled (bottom_level={}, "
             "no_overlap_below={})",
             is_bottom_level, no_overlap_below);
  }

  // 构建新的 SSTable
  uint32_t new_table_id = tree_->GetNextTableId();
  std::vector<uint32_t> new_sstable_ids;

  auto builder = std::make_unique<SSTableBuilder>(
      path, new_table_id, column_types, primary_key_idx);

  std::string current_min_key;
  std::string current_max_key;
  uint64_t current_file_size = 0;
  bool has_data = false;

  while (merge_iter.Valid()) {
    auto &key = merge_iter.GetKey();
    auto &value = merge_iter.GetValue();

    // 可以删除 tombstone 时跳过
    if (can_drop_tombstone && value.Size() == 0) {
      merge_iter.Next();
      continue;
    }

    // 记录 key 范围
    std::string key_str = key.ToString();
    if (!has_data) {
      current_min_key = key_str;
      has_data = true;
    }
    current_max_key = key_str;

    if (!builder->Add(key, value)) {
      // 当前 SSTable 已满，完成并开始新的
      auto status = builder->Finish();
      if (!status.ok()) {
        return status;
      }

      auto sstable_meta = builder->BuildSSTableMeta();
      new_sstable_ids.push_back(new_table_id);

      // 计算文件大小
      auto file_path = path / fmt::format("{}.sst", new_table_id);
      if (std::filesystem::exists(file_path)) {
        current_file_size = std::filesystem::file_size(file_path);
      }

      // 注册新 SSTable
      tree_->RegisterSSTable(new_table_id, sstable_meta);

      // 注册 level 元数据
      {
        std::unique_lock<std::shared_mutex> levels_lock(tree_->GetLevelLatch());
        LeveledSSTableMeta level_meta(new_table_id, job.output_level,
                                      current_min_key, current_max_key,
                                      current_file_size);
        tree_->GetLevels()[job.output_level].AddSSTable(level_meta);
      }

      // 开始新 SSTable
      new_table_id = tree_->GetNextTableId();
      builder = std::make_unique<SSTableBuilder>(path, new_table_id,
                                                 column_types, primary_key_idx);

      current_min_key = key_str;
      current_max_key = key_str;
      has_data = true;

      // 重新添加当前行
      builder->Add(key, value);
    }

    merge_iter.Next();
  }

  // 完成最后一个 SSTable（如果有数据）
  if (has_data) {
    auto status = builder->Finish();
    if (!status.ok()) {
      return status;
    }

    auto sstable_meta = builder->BuildSSTableMeta();
    new_sstable_ids.push_back(new_table_id);

    auto file_path = path / fmt::format("{}.sst", new_table_id);
    if (std::filesystem::exists(file_path)) {
      current_file_size = std::filesystem::file_size(file_path);
    }

    // 注册新 SSTable
    tree_->RegisterSSTable(new_table_id, sstable_meta);

    // 注册 level 元数据
    {
      std::unique_lock<std::shared_mutex> levels_lock(tree_->GetLevelLatch());
      LeveledSSTableMeta level_meta(new_table_id, job.output_level,
                                    current_min_key, current_max_key,
                                    current_file_size);
      tree_->GetLevels()[job.output_level].AddSSTable(level_meta);
    }
  }

  // 安装 compaction 结果（删除旧文件，更新 manifest）
  LOG_INFO("DoCompaction: completed, produced {} new SSTable(s)",
           new_sstable_ids.size());
  return tree_->InstallCompactionResults(job, new_sstable_ids);
}

bool CompactionScheduler::CanDeleteTombstone(uint32_t level,
                                             const std::string &key) {
  if (level >= MAX_LEVELS - 1) {
    return true; // 最底层，可以删除
  }

  // 检查下层是否有重叠文件
  std::shared_lock<std::shared_mutex> levels_lock(tree_->GetLevelLatch());
  auto &levels = tree_->GetLevels();

  for (uint32_t l = level + 1; l < levels.size(); l++) {
    for (const auto &meta : levels[l].sstables) {
      if (key >= meta.min_key && key <= meta.max_key) {
        return false; // 有重叠，不能删除 tombstone
      }
    }
  }

  return true;
}

} // namespace DB
