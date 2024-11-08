#pragma once

#include "execution/AbstractExecutor.hpp"
#include "execution/FunctionExecutor.hpp"
#include "execution/ProjectionExecutor.hpp"
#include "execution/ValuesExecutor.hpp"
#include "planner/AbstractPlanNode.hpp"
#include "planner/FunctionPlanNode.hpp"
#include "planner/ProjectionPlanNode.hpp"
#include "planner/ValuePlanNode.hpp"
#include <memory>
#include <vector>

namespace DB {
struct ExecutorFactory {
  static std::unique_ptr<AbstractExecutor>
  CreateExecutor(const AbstractPlanNodeRef &plan) {
    switch (plan->GetType()) {
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(
          plan->GetSchemaRef(), std::dynamic_pointer_cast<ValuePlanNode>(plan));
    }
    case PlanType::Function: {
      auto p = dynamic_cast<FunctionPlanNode &>(*plan);
      std::vector<AbstractExecutorRef> children;
      for (auto child : p.GetChildren()) {
        children.push_back(CreateExecutor(child));
      }
      return std::make_unique<FunctionExecutor>(p.GetSchemaRef(), p.function_,
                                                std::move(children));
    }
    case PlanType::Projection: {
      auto p = dynamic_cast<ProjectionPlanNode &>(*plan);
      std::vector<AbstractExecutorRef> children;
      for (auto child : p.GetChildren()) {
        children.push_back(CreateExecutor(child));
      }
      return std::make_unique<ProjectionExecutor>(p.GetSchemaRef(),
                                                  std::move(children));
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
    case PlanType::Sort:
    case PlanType::TopN:
    case PlanType::MockScan:
    case PlanType::InitCheck: break;
    }
  }
};
} // namespace DB