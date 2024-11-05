#include "catalog/Schema.hpp"
#include "common/EnumClass.hpp"
#include "common/Status.hpp"
#include "parser/statement/SelectStmt.hpp"
#include "planner/AbstractPlanNode.hpp"
#include "planner/Planner.hpp"
#include "planner/ProjectionPlanNode.hpp"
#include "planner/ValuePlanNode.hpp"

#include <memory>

namespace DB {
Status Planner::PlanSelect(SelectStmt &satement) {
  if (satement.from_.empty()) {
    // not from table, just value
    std::vector<BoundExpressRef> columns;
    for (auto &column : satement.columns_) {
      columns.push_back(column);
    }
    plan_ =
        std::make_shared<ValuePlanNode>(std::make_shared<Schema>(), columns);
  } else {
  }

  return Status::OK();
}
} // namespace DB