#pragma once

#include "catalog/meta/TableMeta.hpp"
#include "common/Status.hpp"
#include "execution/AbstractExecutor.hpp"
#include "parser/binder/BoundExpress.hpp"
#include "planner/FilterPlanNode.hpp"
#include "storage/lsmtree/LSMTree.hpp"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace DB {
// 删除算子（向量化版本）
//
// 终结态算子：Init 阶段扫描主键 + 求值 WHERE + 执行 Remove，
// Next 一次性发射 "DeleteRows: N" 结果列后立即 EOF
class DeleteExecutor : public AbstractExecutor {
  TableMetaRef table_meta_;
  std::shared_ptr<LSMTree> lsm_tree_;
  BoundExpressRef where_condition_;
  std::vector<FilterColumnScan> condition_columns_;

  // WHERE 条件中列名到列数据的映射
  std::unordered_map<std::string, ColumnPtr> condition_column_data_;

  // 写入完成后缓存的删除行数
  uint32_t deleted_rows_{0};
  bool emitted_{false};

  // 递归求值 WHERE 条件表达式
  ColumnPtr EvalCondition(const BoundExpressRef &expr);

  // 扫描 WHERE 条件中需要的列
  Status ScanConditionColumns();

public:
  DeleteExecutor(SchemaRef schema, TableMetaRef table_meta,
                 std::shared_ptr<LSMTree> lsm_tree,
                 BoundExpressRef where_condition,
                 std::vector<FilterColumnScan> condition_columns)
      : AbstractExecutor(std::move(schema)),
        table_meta_(std::move(table_meta)), lsm_tree_(std::move(lsm_tree)),
        where_condition_(std::move(where_condition)),
        condition_columns_(std::move(condition_columns)) {}

  ~DeleteExecutor() override = default;

  Status Init() override;
  Status Next(DataChunk &out) override;
};
} // namespace DB
