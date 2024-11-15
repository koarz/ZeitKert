#pragma once

#include "catalog/meta/TableMeta.hpp"
#include "common/EnumClass.hpp"
#include "planner/AbstractPlanNode.hpp"

namespace DB {
class InsertPlanNode : public AbstractPlanNode {
  TableMetaRef table_meta_;

public:
  InsertPlanNode(SchemaRef schema, std::vector<AbstractPlanNodeRef> children,
                 TableMetaRef table_meta)
      : AbstractPlanNode(std::move(schema), std::move(children)),
        table_meta_(table_meta) {}
  ~InsertPlanNode() override = default;

  TableMetaRef GetTableMeta() { return table_meta_; }

  PlanType GetType() const override { return PlanType::Insert; }
};
} // namespace DB