#pragma once

#include "catalog/Schema.hpp"
#include "common/EnumClass.hpp"

#include <memory>
#include <vector>

namespace DB {
class AbstractPlanNode;
using AbstractPlanNodeRef = std::shared_ptr<AbstractPlanNode>;

class AbstractPlanNode {
public:
  AbstractPlanNode() = default;
  AbstractPlanNode(SchemaRef schema, std::vector<AbstractPlanNodeRef> children)
      : schema_(std::move(schema)), children_(std::move(children)) {}

  virtual ~AbstractPlanNode() = default;

  AbstractPlanNodeRef GetChildAt(uint32_t child_idx) const {
    return children_[child_idx];
  }

  const std::vector<AbstractPlanNodeRef> &GetChildren() const {
    return children_;
  }

  virtual PlanType GetType() const = 0;

  SchemaRef GetSchemaRef() { return schema_; }

private:
  SchemaRef schema_;
  std::vector<AbstractPlanNodeRef> children_;
};
} // namespace DB