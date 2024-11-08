#pragma once

#include "catalog/Schema.hpp"
#include "common/EnumClass.hpp"
#include "parser/binder/BoundExpress.hpp"
#include "planner/AbstractPlanNode.hpp"

#include <memory>
#include <vector>

namespace DB {
class ProjectionPlanNode : public AbstractPlanNode {
public:
  ProjectionPlanNode(SchemaRef schema,
                     std::vector<AbstractPlanNodeRef> children)
      : AbstractPlanNode(std::move(schema), std::move(children)) {}
  ~ProjectionPlanNode() override = default;

  PlanType GetType() const override { return PlanType::Projection; }
};

using ProjectionPlanNodeRef = std::shared_ptr<ProjectionPlanNode>();
} // namespace DB