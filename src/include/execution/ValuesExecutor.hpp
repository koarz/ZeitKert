#pragma once

#include "common/Status.hpp"
#include "execution/AbstractExecutor.hpp"
#include "planner/ValuePlanNode.hpp"
#include <memory>

namespace DB {
class ValuesExecutor : public AbstractExecutor {
  std::shared_ptr<ValuePlanNode> plan_;

public:
  ValuesExecutor(SchemaRef schema, std::shared_ptr<ValuePlanNode> plan)
      : AbstractExecutor(std::move(schema)), plan_(plan) {}

  Status Init() override;

  Status Execute() override;
};
} // namespace DB