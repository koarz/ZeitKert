#pragma once

#include "execution/AbstractExecutor.hpp"
#include "execution/ValuesExecutor.hpp"
#include "planner/AbstractPlanNode.hpp"
#include "planner/ValuePlanNode.hpp"
#include <memory>

namespace DB {
struct ExecutorFactory {
  static std::unique_ptr<AbstractExecutor>
  CreateExecutor(const AbstractPlanNodeRef &plan) {
    switch (plan->GetType()) {
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(
          dynamic_cast<ValuePlanNode &>(*plan));
    }
    case PlanType::SeqScan:
    case PlanType::IndexScan:
    case PlanType::Insert:
    case PlanType::Update:
    case PlanType::Delete:
    case PlanType::Aggregation:
    case PlanType::Limit:
    case PlanType::NestedLoopJoin:
    case PlanType::NestedIndexJoin:
    case PlanType::HashJoin:
    case PlanType::Filter:
    case PlanType::Projection:
    case PlanType::Sort:
    case PlanType::TopN:
    case PlanType::MockScan:
    case PlanType::InitCheck: break;
    }
  }
};
} // namespace DB