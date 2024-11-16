#include "catalog/Schema.hpp"
#include "common/EnumClass.hpp"
#include "common/Status.hpp"
#include "parser/binder/BoundExpress.hpp"
#include "parser/statement/SelectStatement.hpp"
#include "planner/AbstractPlanNode.hpp"
#include "planner/Planner.hpp"
#include "planner/ProjectionPlanNode.hpp"
#include "planner/ValuePlanNode.hpp"

#include <memory>

namespace DB {
Status Planner::PlanSelect(SelectStatement &satement) {
  std::vector<AbstractPlanNodeRef> columns;
  std::vector<BoundExpressRef> temp_columns;
  for (auto &column : satement.columns_) {
    if (column->expr_type_ == BoundExpressType::BoundConstant) {
      temp_columns.push_back(column);
      continue;
    } else {
      // if pre const value col is not empty
      if (!temp_columns.empty()) {
        columns.push_back(std::make_shared<ValuePlanNode>(
            std::make_shared<Schema>(), std::move(temp_columns)));
      }
      columns.push_back(GetPlanNode(column));
    }
  }
  if (!temp_columns.empty()) {
    columns.push_back(std::make_shared<ValuePlanNode>(
        std::make_shared<Schema>(), std::move(temp_columns)));
  }

  // normal select
  plan_ = std::make_shared<ProjectionPlanNode>(std::make_shared<Schema>(),
                                               std::move(columns));
  return Status::OK();
}
} // namespace DB