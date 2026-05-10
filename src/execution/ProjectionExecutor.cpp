#include "execution/ProjectionExecutor.hpp"
#include "common/Status.hpp"

#include <thread>
#include <vector>

namespace DB {

Status ProjectionExecutor::Init() {
  // 没有 WHERE 缓存且子节点 ≥2：并行 Init（典型场景：SELECT col1, col2, ...）
  // FilteredDataCache 是 thread_local，子线程看不到主线程缓存，所以
  // 一旦 Filter 激活了缓存就必须顺序 Init
  if (!FilteredDataCache::IsActive() && children_.size() >= 2) {
    std::vector<std::thread> threads;
    std::vector<Status> statuses(children_.size());

    threads.reserve(children_.size());
    for (size_t i = 0; i < children_.size(); i++) {
      threads.emplace_back([&, i]() { statuses[i] = children_[i]->Init(); });
    }
    for (auto &t : threads) {
      t.join();
    }
    for (auto &s : statuses) {
      if (!s.ok()) {
        return s;
      }
    }
    return Status::OK();
  }

  // 顺序 Init
  for (auto &child : children_) {
    auto status = child->Init();
    if (!status.ok()) {
      return status;
    }
  }
  return Status::OK();
}

Status ProjectionExecutor::Next(DataChunk &out) {
  // 从每个子节点拉一个 chunk，按列拼接到 out
  // 当前向量化模型下子节点都是 "一次发射全部 + EOF"，这里一次拉取就能拿到完整结果
  size_t row_count = 0;
  for (auto &child : children_) {
    DataChunk child_chunk;
    auto status = child->Next(child_chunk);
    if (!status.ok()) {
      return status;
    }
    if (child_chunk.IsEmpty()) {
      // 任一子节点 EOF 即整体 EOF
      out.Reset();
      return Status::OK();
    }
    row_count = std::max(row_count, child_chunk.row_count());
    for (auto &col : child_chunk.columns()) {
      out.AddColumn(col, col->Size());
    }
  }
  // 显式覆盖行数（DataChunk::AddColumn 默认按第一列长度推断）
  out.set_row_count(row_count);
  return Status::OK();
}

} // namespace DB
