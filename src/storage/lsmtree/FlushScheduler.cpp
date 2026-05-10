#include "storage/lsmtree/FlushScheduler.hpp"
#include "common/Logger.hpp"
#include "storage/lsmtree/LSMTree.hpp"

namespace DB {

void FlushScheduler::Start() {
  if (running_.load()) {
    return;
  }
  stop_requested_.store(false);
  running_.store(true);
  bg_thread_ = std::thread([this]() { BackgroundThread(); });
}

void FlushScheduler::Stop() {
  if (!running_.load()) {
    return;
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stop_requested_.store(true);
  }
  work_cv_.notify_all();
  if (bg_thread_.joinable()) {
    bg_thread_.join();
  }
  running_.store(false);
}

void FlushScheduler::NotifyFlush() {
  // 不需要持锁修改原子值，但 notify 前要确保被唤醒方能看到 predicate
  // 这里用 lock_guard 保证可见性
  {
    std::lock_guard<std::mutex> lock(mutex_);
  }
  work_cv_.notify_one();
}

void FlushScheduler::WaitUntilDrained() {
  std::unique_lock<std::mutex> lock(mutex_);
  drain_cv_.wait(lock, [this]() {
    return tree_->ImmutableQueueEmptyForFlush() && !flush_in_progress_.load();
  });
}

void FlushScheduler::BackgroundThread() {
  LOG_INFO("Flush background thread started");
  while (true) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      work_cv_.wait(lock, [this]() {
        return stop_requested_.load() || !tree_->ImmutableQueueEmptyForFlush();
      });

      if (stop_requested_.load() && tree_->ImmutableQueueEmptyForFlush()) {
        break;
      }
    }

    flush_in_progress_.store(true);
    bool did_work = FlushOne();
    flush_in_progress_.store(false);

    // 唤醒等待 drain 的写线程（back-pressure / FlushToSST）
    drain_cv_.notify_all();

    if (!did_work && stop_requested_.load()) {
      break;
    }
  }
  LOG_INFO("Flush background thread stopped");
}

bool FlushScheduler::FlushOne() {
  // 把实际的刷盘逻辑委托给 LSMTree（它持有所有需要的状态和锁）
  return tree_->FlushOldestImmutable();
}

} // namespace DB
