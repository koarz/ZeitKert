#include "planner/Planner.hpp"
#include "catalog/Schema.hpp"
#include "common/EnumClass.hpp"
#include "common/Status.hpp"
#include "parser/binder/BoundColumnMeta.hpp"
#include "parser/binder/BoundFunction.hpp"
#include "parser/binder/BoundTuple.hpp"
#include "parser/statement/InsertStatement.hpp"
#include "parser/statement/SelectStatement.hpp"
#include "planner/AbstractPlanNode.hpp"
#include "planner/FunctionPlanNode.hpp"
#include "planner/RangePlanNode.hpp"
#include "planner/ScanColumnPlanNode.hpp"
#include "planner/TuplePlanNode.hpp"
#include "planner/ValuePlanNode.hpp"

#include <memory>
#include <vector>

namespace DB {
Planner::Planner(std::shared_ptr<QueryContext> context) : context_(context) {}

Status Planner::QueryPlan() {
  auto statement = context_->sql_statement_;
  if (statement->type == StatementType::SelectStatement) {
    return PlanSelect(static_cast<SelectStatement &>(*statement));
  } else if (statement->type == StatementType::InsertStatement) {
    return PlanInsert(static_cast<InsertStatement &>(*statement));
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
    auto &exp = static_cast<BoundFunction &>(*expr);
    for (auto col : exp.GetArguments()) {
      abst_column.push_back(GetPlanNode(col));
    }
    return std::make_shared<FunctionPlanNode>(
        std::make_shared<Schema>(), std::move(abst_column), exp.GetFunction());
  }
  case BoundExpressType::BoundTuple: {
    auto &exp = static_cast<BoundTuple &>(*expr);
    for (auto col : exp.elements_) {
      abst_column.push_back(GetPlanNode(col));
    }
    return std::make_shared<TuplePlanNode>(std::make_shared<Schema>(),
                                           std::move(abst_column));
  }
  case BoundExpressType::BoundColumnMeta: {
    auto &exp = static_cast<BoundColumnMeta &>(*expr);
    auto table_meta = exp.GetTableMeta();
    if (range_table_ && table_meta.get() == range_table_.get()) {
      return std::make_shared<RangePlanNode>(std::make_shared<Schema>(),
                                             range_start_, range_stop_,
                                             range_step_);
    }
    auto lsm_tree = context_->GetOrCreateLSMTree(table_meta);
    return std::make_shared<ScanColumnPlanNode>(std::make_shared<Schema>(),
                                                exp.GetColumnMeta(), lsm_tree,
                                                exp.GetColumnMeta()->index_);
  }
  }
  return nullptr;
}
} // namespace DB
