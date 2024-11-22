#pragma once

#include "planner/AbstractPlanNode.hpp"

namespace DB {
// Tuple plan node will be a tuple of values aggregated, in the
// insert plan should be the tuple of values by column aggregation, and then the
// data are inserted into the lsm tree
class TuplePlanNode : public AbstractPlanNode {
  // all data store in children
public:
  TuplePlanNode(SchemaRef schema, std::vector<AbstractPlanNodeRef> children)
      : AbstractPlanNode(std::move(schema), std::move(children)) {}
  ~TuplePlanNode() override = default;

  PlanType GetType() const override { return PlanType::Tuple; }
};
} // namespace DB