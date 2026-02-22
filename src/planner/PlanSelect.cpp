#include "catalog/Schema.hpp"
#include "common/EnumClass.hpp"
#include "common/Status.hpp"
#include "parser/binder/BoundColumnMeta.hpp"
#include "parser/binder/BoundExpress.hpp"
#include "parser/binder/BoundFunction.hpp"
#include "parser/statement/SelectStatement.hpp"
#include "planner/AbstractPlanNode.hpp"
#include "planner/FilterPlanNode.hpp"
#include "planner/Planner.hpp"
#include "planner/ProjectionPlanNode.hpp"
#include "planner/ValuePlanNode.hpp"

#include <memory>
#include <set>
#include <utility>

namespace DB {

// 递归收集表达式中的所有列引用
static void CollectColumns(const BoundExpressRef &expr,
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
      CollectColumns(arg, columns);
    }
    break;
  }
  default: break;
  }
}

Status Planner::PlanSelect(SelectStatement &satement) {
  // 子查询透传：直接规划内层 statement，外层 select * 不做额外处理
  if (satement.subquery_) {
    return PlanSelect(*satement.subquery_);
  }

  // Transfer range info from statement to planner
  if (satement.range_info_) {
    range_table_ = satement.range_table_;
    range_start_ = satement.range_info_->start;
    range_stop_ = satement.range_info_->stop;
    range_step_ = satement.range_info_->step;
  }

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

  // 如果有 WHERE 子句，在 Projection 外面包一层 Filter
  if (satement.where_condition_) {
    // 只收集需要的列：WHERE 条件引用的列 + SELECT 表达式引用的列
    std::set<std::pair<std::string, std::string>>
        needed_cols; // (table_name, col_name)

    // 收集 WHERE 条件中的列
    std::set<BoundColumnMeta *> where_cols;
    CollectColumns(satement.where_condition_, where_cols);
    for (auto *col : where_cols) {
      needed_cols.insert(
          {col->GetTableMeta()->GetTableName(), col->GetColumnMeta()->name_});
    }

    // 收集 SELECT 表达式中的列
    for (auto &expr : satement.columns_) {
      std::set<BoundColumnMeta *> select_cols;
      CollectColumns(expr, select_cols);
      for (auto *col : select_cols) {
        needed_cols.insert(
            {col->GetTableMeta()->GetTableName(), col->GetColumnMeta()->name_});
      }
    }

    // 为需要的列创建扫描信息
    std::vector<FilterColumnScan> filter_columns;
    for (auto &table : satement.from_) {
      if (range_table_ && table.get() == range_table_.get()) {
        continue; // skip virtual range table
      }
      auto lsm_tree = context_->GetOrCreateLSMTree(table);
      uint32_t col_idx = 0;
      for (auto &col_meta : table->GetColumns()) {
        if (needed_cols.count({table->GetTableName(), col_meta->name_})) {
          filter_columns.push_back({col_meta, lsm_tree, col_idx});
        }
        col_idx++;
      }
    }

    auto projection = std::make_shared<ProjectionPlanNode>(
        std::make_shared<Schema>(), std::move(columns));
    std::vector<AbstractPlanNodeRef> filter_children;
    filter_children.push_back(projection);
    plan_ = std::make_shared<FilterPlanNode>(
        std::make_shared<Schema>(), std::move(filter_children),
        satement.where_condition_, std::move(filter_columns));
  } else {
    // normal select
    plan_ = std::make_shared<ProjectionPlanNode>(std::make_shared<Schema>(),
                                                 std::move(columns));
  }
  return Status::OK();
}
} // namespace DB