#pragma once

#include "catalog/Schema.hpp"
#include "common/Status.hpp"
#include "execution/AbstractExecutor.hpp"
#include "parser/binder/BoundExpress.hpp"
#include "planner/FilterPlanNode.hpp"
#include "storage/column/Column.hpp"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace DB {
class FilterExecutor : public AbstractExecutor {
  std::vector<AbstractExecutorRef> children_;
  BoundExpressRef condition_;
  std::vector<FilterColumnScan> condition_columns_;

  // WHERE 条件中列名到列数据的映射（独立扫描的列）
  std::unordered_map<std::string, ColumnPtr> condition_column_data_;

  // 递归求值 WHERE 条件表达式
  ColumnPtr EvalCondition(const BoundExpressRef &expr);

  // 扫描 WHERE 条件中需要的列
  Status ScanConditionColumns();

public:
  FilterExecutor(SchemaRef schema, std::vector<AbstractExecutorRef> children,
                 BoundExpressRef condition,
                 std::vector<FilterColumnScan> condition_columns = {})
      : AbstractExecutor(std::move(schema)), children_(std::move(children)),
        condition_(std::move(condition)),
        condition_columns_(std::move(condition_columns)) {}

  ~FilterExecutor() override = default;

  Status Execute() override;
};
} // namespace DB
