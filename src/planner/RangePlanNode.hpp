#pragma once

#include "planner/AbstractPlanNode.hpp"

#include <cstdint>

namespace DB {
class RangePlanNode : public AbstractPlanNode {
  int64_t start_;
  int64_t stop_;
  int64_t step_;

public:
  RangePlanNode(SchemaRef schema, int64_t start, int64_t stop, int64_t step)
      : AbstractPlanNode(std::move(schema), {}), start_(start), stop_(stop),
        step_(step) {}

  ~RangePlanNode() override = default;

  int64_t GetStart() const { return start_; }
  int64_t GetStop() const { return stop_; }
  int64_t GetStep() const { return step_; }

  PlanType GetType() const override { return PlanType::Range; }
};
} // namespace DB
