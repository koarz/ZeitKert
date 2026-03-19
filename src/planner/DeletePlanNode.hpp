#pragma once

#include "catalog/meta/TableMeta.hpp"
#include "common/EnumClass.hpp"
#include "parser/binder/BoundExpress.hpp"
#include "planner/AbstractPlanNode.hpp"
#include "planner/FilterPlanNode.hpp"
#include "storage/lsmtree/LSMTree.hpp"

#include <memory>
#include <vector>

namespace DB {
class DeletePlanNode : public AbstractPlanNode {
  TableMetaRef table_meta_;
  std::shared_ptr<LSMTree> lsm_tree_;
  BoundExpressRef where_condition_;
  std::vector<FilterColumnScan> condition_columns_;

public:
  DeletePlanNode(SchemaRef schema, TableMetaRef table_meta,
                 std::shared_ptr<LSMTree> lsm_tree,
                 BoundExpressRef where_condition,
                 std::vector<FilterColumnScan> condition_columns)
      : AbstractPlanNode(std::move(schema), {}),
        table_meta_(std::move(table_meta)), lsm_tree_(std::move(lsm_tree)),
        where_condition_(std::move(where_condition)),
        condition_columns_(std::move(condition_columns)) {}

  ~DeletePlanNode() override = default;

  PlanType GetType() const override { return PlanType::Delete; }

  TableMetaRef GetTableMeta() { return table_meta_; }

  std::shared_ptr<LSMTree> GetLSMTree() { return lsm_tree_; }

  BoundExpressRef GetCondition() const { return where_condition_; }

  const std::vector<FilterColumnScan> &GetConditionColumns() const {
    return condition_columns_;
  }
};
} // namespace DB
