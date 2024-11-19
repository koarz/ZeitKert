#include "catalog/Schema.hpp"
#include "common/Status.hpp"
#include "parser/statement/InsertStatement.hpp"
#include "parser/statement/SelectStatement.hpp"
#include "planner/InsertPlanNode.hpp"
#include "planner/Planner.hpp"

#include <memory>

namespace DB {
Status Planner::PlanInsert(InsertStatement &statement) {
  if (auto select = statement.select_; select != nullptr) {
    auto status = PlanSelect(static_cast<SelectStatement &>(*select));
    if (!status.ok()) {
      return status;
    }
    plan_ = std::make_shared<InsertPlanNode>(
        std::make_shared<Schema>(),
        std::vector<AbstractPlanNodeRef>{std::move(plan_)}, statement.table_);
  } else {
    auto &tuples = statement.tuples_;
    std::vector<AbstractPlanNodeRef> children;
    for (auto &column : tuples) {
      children.push_back(GetPlanNode(column));
    }

    plan_ = std::make_shared<InsertPlanNode>(
        std::make_shared<Schema>(), std::move(children), statement.table_);
  }
  return Status::OK();
}
} // namespace DB