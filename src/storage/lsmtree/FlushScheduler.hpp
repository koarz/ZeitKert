#pragma once

#include "common/Status.hpp"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>

namespace DB {

class LSMTree;

// 异步刷盘调度器：把 ImmutableMemTable → SSTable 的耗时构建放到后台线程
//
// 关键不变量（与 LSMTree 的读路径配合）：
//   1. 后台线程构建 SSTable 期间，被刷盘的 MemTable 仍然保留在
//      LSMTree::immutable_table_ 中，读路径照常能查到这些 key
//   2. 只有当 SSTable 已注册到 sstables_/levels_ 后，才在同一个临界区里
//      把对应的 MemTable 从 immutable_table_ 移除并删除 WAL
//      读路径要么看到旧的 immutable，要么看到新的
//      SSTable，不会同时出现也不会都看不到
//
// 信号语义：
//   - NotifyFlush()        ：写路径加完 immutable 后唤醒后台线程
//   - WaitUntilDrained()   ：FlushToSST 等需要同步语义时使用
//   - Stop()               ：停止后会先把所有积压的 immutable 全部刷完再 join
class FlushScheduler {
public:
  explicit FlushScheduler(LSMTree *tree) : tree_(tree) {}

  ~FlushScheduler() { Stop(); }

  // 启动后台线程
  void Start();

  // 停止后台线程；停止前会把 immutable_table_ 里剩余的全部刷盘
  void Stop();

  // 写路径调用：通知后台线程有新的 immutable 等待刷盘
  void NotifyFlush();

  // 同步等待：阻塞到 immutable_table_ 全部刷盘完成
  void WaitUntilDrained();

  bool IsRunning() const { return running_.load(); }

private:
  // 后台线程主循环
  void BackgroundThread();

  // 把 immutable_table_ 队首的一个 MemTable 刷盘。返回 false 表示队列已空
  bool FlushOne();

  LSMTree *tree_;
  std::thread bg_thread_;
  std::mutex mutex_;
  std::condition_variable work_cv_;  // 唤醒后台线程
  std::condition_variable drain_cv_; // 唤醒等待 drain 的写线程
  std::atomic<bool> running_{false};
  std::atomic<bool> stop_requested_{false};
  // 后台线程当前是否正在执行刷盘（用于 WaitUntilDrained 判定真正空闲）
  std::atomic<bool> flush_in_progress_{false};
};

} // namespace DB
