#pragma once

#include "catalog/Schema.hpp"
#include "execution/AbstractExecutor.hpp"
#include "function/Function.hpp"
#include "planner/AbstractPlanNode.hpp"

namespace DB {
class FunctionPlanNode : public AbstractPlanNode {
public:
  FunctionPlanNode(SchemaRef schema, std::vector<AbstractPlanNodeRef> children,
                   std::shared_ptr<Function> function)
      : AbstractPlanNode(std::move(schema), std::move(children)),
        function_(std::move(function)) {}
  ~FunctionPlanNode() override = default;

  PlanType GetType() const override { return PlanType::Function; }

  std::shared_ptr<Function> function_;
};
} // namespace DB