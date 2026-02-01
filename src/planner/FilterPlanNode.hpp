#pragma once

#include "catalog/Schema.hpp"
#include "catalog/meta/ColumnMeta.hpp"
#include "common/EnumClass.hpp"
#include "parser/binder/BoundExpress.hpp"
#include "planner/AbstractPlanNode.hpp"
#include "storage/lsmtree/LSMTree.hpp"

#include <memory>
#include <vector>

namespace DB {

// WHERE 条件中需要扫描的列信息
struct FilterColumnScan {
  ColumnMetaRef column_meta;
  std::shared_ptr<LSMTree> lsm_tree;
  uint32_t column_idx;
};

class FilterPlanNode : public AbstractPlanNode {
  BoundExpressRef condition_;
  std::vector<FilterColumnScan> condition_columns_;

public:
  FilterPlanNode(SchemaRef schema, std::vector<AbstractPlanNodeRef> children,
                 BoundExpressRef condition,
                 std::vector<FilterColumnScan> condition_columns = {})
      : AbstractPlanNode(std::move(schema), std::move(children)),
        condition_(std::move(condition)),
        condition_columns_(std::move(condition_columns)) {}

  ~FilterPlanNode() override = default;

  PlanType GetType() const override { return PlanType::Filter; }

  BoundExpressRef GetCondition() const { return condition_; }

  const std::vector<FilterColumnScan> &GetConditionColumns() const {
    return condition_columns_;
  }
};
} // namespace DB
