#pragma once

#include "catalog/Schema.hpp"
#include "common/EnumClass.hpp"
#include "parser/binder/BoundExpress.hpp"
#include "planner/AbstractPlanNode.hpp"

namespace DB {
class ValuePlanNode : public AbstractPlanNode {

public:
  ValuePlanNode(SchemaRef schema, std::vector<BoundExpressRef> values)
      : AbstractPlanNode(schema, {}), values_(std::move(values)) {}
  ~ValuePlanNode() override = default;

  PlanType GetType() const override { return PlanType::Values; }

  std::vector<BoundExpressRef> values_;
};
} // namespace DB