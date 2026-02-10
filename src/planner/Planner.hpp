#pragma once

#include "catalog/meta/TableMeta.hpp"
#include "common/Context.hpp"
#include "common/Status.hpp"
#include "parser/statement/InsertStatement.hpp"
#include "parser/statement/SelectStatement.hpp"
#include "planner/AbstractPlanNode.hpp"

#include <cstdint>

namespace DB {
class Planner {
  std::shared_ptr<QueryContext> context_;
  AbstractPlanNodeRef plan_;

  // range table function state
  TableMetaRef range_table_;
  int64_t range_start_{};
  int64_t range_stop_{};
  int64_t range_step_{1};

public:
  explicit Planner(std::shared_ptr<QueryContext> context);

  AbstractPlanNodeRef GetPlan() { return plan_; }

  Status QueryPlan();

  Status PlanSelect(SelectStatement &statement);

  Status PlanInsert(InsertStatement &statement);

  AbstractPlanNodeRef GetPlanNode(BoundExpressRef plan);
};
} // namespace DB