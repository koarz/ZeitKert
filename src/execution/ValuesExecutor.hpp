#pragma once

#include "common/Status.hpp"
#include "execution/AbstractExecutor.hpp"
#include "planner/ValuePlanNode.hpp"

namespace DB {

// 常量值算子：把 SQL 文本中的字面量包装成单行列发射出去（向量化版本）
class ValuesExecutor : public AbstractExecutor {
  std::shared_ptr<ValuePlanNode> plan_;
  bool emitted_{false};

public:
  ValuesExecutor(SchemaRef schema, std::shared_ptr<ValuePlanNode> plan)
      : AbstractExecutor(std::move(schema)), plan_(plan) {}

  Status Init() override;
  Status Next(DataChunk &out) override;
};

} // namespace DB
