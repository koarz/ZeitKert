#pragma once

#include "catalog/Schema.hpp"
#include "common/EnumClass.hpp"
#include "parser/binder/BoundExpress.hpp"
#include "planner/AbstractPlanNode.hpp"

#include <memory>
#include <vector>

namespace DB {
class ProjectionPlanNode : public AbstractPlanNode {
  std::vector<BoundExpressRef> select_list_;

public:
  ProjectionPlanNode(SchemaRef schema,
                     std::vector<AbstractPlanNodeRef> children,
                     std::vector<BoundExpressRef> select_list)
      : AbstractPlanNode(schema, std::move(children)),
        select_list_(std::move(select_list)) {}
  ~ProjectionPlanNode() override = default;

  PlanType GetType() const override { return PlanType::Projection; }
};

using ProjectionPlanNodeRef = std::shared_ptr<ProjectionPlanNode>();
} // namespace DB