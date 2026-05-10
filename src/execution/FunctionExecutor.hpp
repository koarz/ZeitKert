#pragma once

#include "catalog/Schema.hpp"
#include "common/Status.hpp"
#include "execution/AbstractExecutor.hpp"
#include "function/Function.hpp"

#include <memory>
#include <vector>

namespace DB {

// 表达式/聚合函数算子（向量化版本）
//
// Init：递归 Init 子节点
// Next：从每个子节点拉取数据 chunk 拼成 Block，调用 function->ExecuteImpl，
//       发射结果 chunk；后续调用返回空 chunk 表示 EOF
//
// 当前向量化设计中，子节点（Scan/Filter/Range/Function）会一次性发射完整数据
// 后续如果引入更细粒度的批次拼接，需要在此处累积多个 chunk
class FunctionExecutor : public AbstractExecutor {
  std::shared_ptr<Function> function_;
  std::vector<AbstractExecutorRef> children_;
  bool emitted_{false};

public:
  FunctionExecutor(SchemaRef schema, std::shared_ptr<Function> &function,
                   std::vector<AbstractExecutorRef> children)
      : AbstractExecutor(std::move(schema)), function_(function),
        children_(std::move(children)) {}

  Status Init() override;
  Status Next(DataChunk &out) override;
};

} // namespace DB
