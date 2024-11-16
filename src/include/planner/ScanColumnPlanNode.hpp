#pragma once

#include "catalog/meta/ColumnMeta.hpp"
#include "planner/AbstractPlanNode.hpp"

namespace DB {
class ScanColumnPlanNode : public AbstractPlanNode {
  ColumnMetaRef column_meta_;

public:
  ScanColumnPlanNode(SchemaRef schema, ColumnMetaRef column_meta)
      : AbstractPlanNode(std::move(schema), {}), column_meta_(column_meta) {}
  ~ScanColumnPlanNode() override = default;

  ColumnMetaRef &GetColumnMeta() { return column_meta_; }

  PlanType GetType() const override { return PlanType::SeqScan; }
};
} // namespace DB