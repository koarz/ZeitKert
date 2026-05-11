#pragma once

#include "common/Status.hpp"
#include "execution/DataChunk.hpp"
#include "execution/ExecutorFactory.hpp"
#include "planner/AbstractPlanNode.hpp"

namespace DB {

// 执行入口（向量化版本）
//
// 驱动方式：Init() 一次 + 反复 Next(chunk) 直到 chunk.IsEmpty()。
// 每个 chunk 的列直接追加到 plan 的 schema 上，让既有的
// `result_set.schema_ = plan->GetSchemaRef()` 打印路径无感知地继续工作。
//
// 当前所有算子都是 "一次性发射全部 +
// EOF"，循环最多走两次（一次拿数据，一次拿到空 chunk）
// 未来如果出现真正流式产出，需要在这里把同名列做拼接合并
class ExecutionEngine {
public:
  Status Execute(AbstractPlanNodeRef plan) {
    auto executor = ExecutorFactory::CreateExecutor(plan);
    if (!executor) {
      return Status::Error(ErrorCode::BindError, "failed to build executor");
    }

    auto status = executor->Init();
    if (!status.ok()) {
      return status;
    }

    auto out_schema = plan->GetSchemaRef();
    while (true) {
      DataChunk chunk;
      status = executor->Next(chunk);
      if (!status.ok()) {
        return status;
      }
      if (chunk.IsEmpty()) {
        break;
      }
      // 将本批次列追加到 plan 的 schema：当前算子均一次性发射，因此
      // 这里直接 append；将来若需要分批拼接同名列，应在此处合并
      auto &cols = out_schema->GetColumns();
      for (auto &col : chunk.columns()) {
        cols.push_back(col);
      }
    }
    return Status::OK();
  }
};
} // namespace DB
