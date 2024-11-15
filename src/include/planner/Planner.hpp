#pragma once

#include "common/Context.hpp"
#include "common/Status.hpp"
#include "parser/statement/SelectStatement.hpp"
#include "planner/AbstractPlanNode.hpp"

namespace DB {
class Planner {
  std::shared_ptr<QueryContext> context_;
  AbstractPlanNodeRef plan_;

public:
  explicit Planner(std::shared_ptr<QueryContext> context);

  AbstractPlanNodeRef GetPlan() { return plan_; }

  Status QueryPlan();

  Status PlanSelect(SelectStatement &statement);

  AbstractPlanNodeRef GetPlanNode(BoundExpressRef plan);
};
} // namespace DB