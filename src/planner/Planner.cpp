#include "planner/Planner.hpp"
#include "common/EnumClass.hpp"
#include "common/Status.hpp"
#include "parser/SQLStatement.hpp"
#include "parser/statement/SelectStmt.hpp"
#include "planner/AbstractPlanNode.hpp"

namespace DB {
Planner::Planner(std::shared_ptr<QueryContext> context) : context_(context) {}

Status Planner::QueryPlan() {
  auto statement = context_->sql_statement_;
  if (statement->type == StatementType::SELECT_STATEMENT) {
    return PlanSelect(dynamic_cast<SelectStmt &>(*statement));
  }
  return Status::OK();
}
} // namespace DB