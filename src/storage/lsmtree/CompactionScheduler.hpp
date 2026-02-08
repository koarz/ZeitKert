#pragma once

#include "common/Status.hpp"
#include "storage/lsmtree/CompactionPicker.hpp"
#include "storage/lsmtree/LevelMeta.hpp"

#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>

namespace DB {

// 前向声明
class LSMTree;

class CompactionScheduler {
public:
  using InstallCallback =
      std::function<Status(const CompactionJob &job,
                           const std::vector<uint32_t> &new_sstable_ids)>;

  CompactionScheduler(LSMTree *tree);
  ~CompactionScheduler();

  // 启动后台 compaction 线程
  void Start();

  // 停止后台线程并等待结束
  void Stop();

  // 发送信号表示可能需要 compaction
  void MaybeScheduleCompaction();

  // 检查 compaction 是否正在运行
  bool IsRunning() const { return running_.load(); }

private:
  // 后台线程函数
  void BackgroundThread();

  // 执行单个 compaction 任务
  Status DoCompaction(CompactionJob &job);

  // 检查是否可以安全删除 tombstone
  bool CanDeleteTombstone(uint32_t level, const std::string &key);

  LSMTree *tree_;
  CompactionPicker picker_;

  std::thread background_thread_;
  std::mutex mutex_;
  std::condition_variable cv_;
  std::atomic<bool> running_{false};
  std::atomic<bool> stop_requested_{false};
  std::atomic<bool> compaction_pending_{false};
};

} // namespace DB
