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

// 过滤算子（向量化版本）
//
// Init：
//   1. 扫描 WHERE 条件涉及的列（带谓词下推）
//   2. 求值条件得到 mask 并按 mask 过滤
//   3. 把过滤后的列放进 FilteredDataCache 并激活
//   4. 递归 Init 子节点（典型为 Projection）
//   子节点的 ScanColumnExecutor 在 Init 时会优先从缓存拿过滤后的数据
//
// Next：从子节点拉 chunk 并直接转发；任一子节点 EOF 即整体 EOF
// 析构：清理 FilteredDataCache，避免污染同线程后续查询
class FilterExecutor : public AbstractExecutor {
  std::vector<AbstractExecutorRef> children_;
  BoundExpressRef condition_;
  std::vector<FilterColumnScan> condition_columns_;

  // WHERE 条件中列名到列数据的映射（独立扫描的列）
  std::unordered_map<std::string, ColumnPtr> condition_column_data_;

  // 是否已经激活了 FilteredDataCache，析构时根据此标志决定是否 Clear
  bool cache_activated_{false};

  // 递归求值 WHERE 条件表达式
  ColumnPtr EvalCondition(const BoundExpressRef &expr);

  // 扫描 WHERE 条件中需要的列
  Status ScanConditionColumns();

  // Init 内部：执行谓词下推 + EvalCondition + 填充缓存
  Status PrepareFilteredData();

public:
  FilterExecutor(SchemaRef schema, std::vector<AbstractExecutorRef> children,
                 BoundExpressRef condition,
                 std::vector<FilterColumnScan> condition_columns = {})
      : AbstractExecutor(std::move(schema)), children_(std::move(children)),
        condition_(std::move(condition)),
        condition_columns_(std::move(condition_columns)) {}

  ~FilterExecutor() override {
    if (cache_activated_) {
      FilteredDataCache::Clear();
    }
  }

  Status Init() override;
  Status Next(DataChunk &out) override;
};
} // namespace DB
