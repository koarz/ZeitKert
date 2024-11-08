#include "planner/Planner.hpp"
#include "catalog/Schema.hpp"
#include "common/EnumClass.hpp"
#include "common/Status.hpp"
#include "parser/SQLStatement.hpp"
#include "parser/binder/BoundFunction.hpp"
#include "parser/statement/SelectStmt.hpp"
#include "planner/AbstractPlanNode.hpp"
#include "planner/FunctionPlanNode.hpp"
#include "planner/ValuePlanNode.hpp"
#include <memory>
#include <vector>

namespace DB {
Planner::Planner(std::shared_ptr<QueryContext> context) : context_(context) {}

Status Planner::QueryPlan() {
  auto statement = context_->sql_statement_;
  if (statement->type == StatementType::SELECT_STATEMENT) {
    return PlanSelect(dynamic_cast<SelectStmt &>(*statement));
  }
  return Status::OK();
}

AbstractPlanNodeRef Planner::GetPlanNode(BoundExpressRef expr) {
  std::vector<BoundExpressRef> expr_column;
  std::vector<AbstractPlanNodeRef> abst_column;
  switch (expr->expr_type_) {
  case BoundExpressType::BoundConstant: {
    expr_column.push_back(expr);
    return std::make_shared<ValuePlanNode>(std::make_shared<Schema>(),
                                           std::move(expr_column));
  }
  case BoundExpressType::BoundFunction: {
    auto exp = dynamic_cast<BoundFunction &>(*expr);
    for (auto col : exp.GetArguments()) {
      abst_column.push_back(GetPlanNode(col));
    }
    return std::make_shared<FunctionPlanNode>(
        std::make_shared<Schema>(), std::move(abst_column), exp.GetFunction());
  }
  }
  return nullptr;
}
} // namespace DB