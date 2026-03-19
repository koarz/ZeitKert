#include "catalog/Schema.hpp"
#include "common/Status.hpp"
#include "parser/binder/BoundColumnMeta.hpp"
#include "parser/binder/BoundFunction.hpp"
#include "parser/statement/DeleteStatement.hpp"
#include "planner/DeletePlanNode.hpp"
#include "planner/FilterPlanNode.hpp"
#include "planner/Planner.hpp"

#include <memory>
#include <set>

namespace DB {

// 递归收集表达式中的所有列引用（复用 PlanSelect 的模式）
static void CollectColumnsForDelete(const BoundExpressRef &expr,
                                    std::set<BoundColumnMeta *> &columns) {
  if (!expr)
    return;

  switch (expr->expr_type_) {
  case BoundExpressType::BoundColumnMeta:
    columns.insert(static_cast<BoundColumnMeta *>(expr.get()));
    break;
  case BoundExpressType::BoundFunction: {
    auto &func = static_cast<BoundFunction &>(*expr);
    for (auto &arg : func.GetArguments()) {
      CollectColumnsForDelete(arg, columns);
    }
    break;
  }
  default: break;
  }
}

Status Planner::PlanDelete(DeleteStatement &statement) {
  auto lsm_tree = context_->GetOrCreateLSMTree(statement.table_);

  std::vector<FilterColumnScan> condition_columns;

  if (statement.where_condition_) {
    // 收集 WHERE 条件中引用的列
    std::set<BoundColumnMeta *> where_cols;
    CollectColumnsForDelete(statement.where_condition_, where_cols);

    auto table = statement.table_;
    uint32_t col_idx = 0;
    for (auto &col_meta : table->GetColumns()) {
      for (auto *col : where_cols) {
        if (col->GetColumnMeta()->name_ == col_meta->name_) {
          condition_columns.push_back({col_meta, lsm_tree, col_idx});
          break;
        }
      }
      col_idx++;
    }
  }

  plan_ = std::make_shared<DeletePlanNode>(
      std::make_shared<Schema>(), statement.table_, lsm_tree,
      statement.where_condition_, std::move(condition_columns));

  return Status::OK();
}
} // namespace DB
