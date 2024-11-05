#pragma once

#include "common/Status.hpp"
#include "execution/AbstractExecutor.hpp"
#include "planner/ValuePlanNode.hpp"

namespace DB {
class ValuesExecutor : public AbstractExecutor {
  ValuePlanNode &plan_;

public:
  ValuesExecutor(ValuePlanNode &plan) : plan_(plan) {}

  Status Init() override;

  Status Execute() override;

  SchemaRef GetSchema() override { return plan_.GetSchemaRef(); }
};
} // namespace DB