#include "execution/ProjectionExecutor.hpp"
#include "common/Status.hpp"

#include <thread>
#include <vector>

namespace DB {
Status ProjectionExecutor::Execute() {
  // 当缓存未激活（无 WHERE 的纯 SELECT）且有 2+ children 时，并行执行
  if (!FilteredDataCache::IsActive() && children_.size() >= 2) {
    std::vector<std::thread> threads;
    std::vector<Status> statuses(children_.size());

    for (size_t i = 0; i < children_.size(); i++) {
      threads.emplace_back([&, i]() { statuses[i] = children_[i]->Execute(); });
    }

    for (auto &t : threads) {
      t.join();
    }

    // 按顺序检查错误并收集结果
    for (size_t i = 0; i < children_.size(); i++) {
      if (!statuses[i].ok()) {
        return statuses[i];
      }
      for (auto col : children_[i]->GetSchema()->GetColumns()) {
        schema_->GetColumns().push_back(col);
      }
    }
  } else {
    // 原有顺序逻辑（FilteredDataCache 是 thread_local，子线程看不到主线程缓存）
    Status status;
    for (auto &child : children_) {
      status = child->Execute();
      if (!status.ok()) {
        return status;
      }
      for (auto col : child->GetSchema()->GetColumns()) {
        this->schema_->GetColumns().push_back(col);
      }
    }
  }
  return Status::OK();
}
} // namespace DB
