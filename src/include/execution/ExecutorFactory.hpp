#pragma once

#include "execution/AbstractExecutor.hpp"
#include "execution/FunctionExecutor.hpp"
#include "execution/InsertExecutor.hpp"
#include "execution/ProjectionExecutor.hpp"
#include "execution/ScanColumnExecutor.hpp"
#include "execution/TupleExecutor.hpp"
#include "execution/ValuesExecutor.hpp"
#include "planner/AbstractPlanNode.hpp"
#include "planner/FunctionPlanNode.hpp"
#include "planner/InsertPlanNode.hpp"
#include "planner/ProjectionPlanNode.hpp"
#include "planner/ScanColumnPlanNode.hpp"
#include "planner/TuplePlanNode.hpp"
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
      auto &p = static_cast<FunctionPlanNode &>(*plan);
      std::vector<AbstractExecutorRef> children;
      for (auto child : p.GetChildren()) {
        children.push_back(CreateExecutor(child));
      }
      return std::make_unique<FunctionExecutor>(p.GetSchemaRef(), p.function_,
                                                std::move(children));
    }
    case PlanType::Projection: {
      auto &p = static_cast<ProjectionPlanNode &>(*plan);
      std::vector<AbstractExecutorRef> children;
      for (auto child : p.GetChildren()) {
        children.push_back(CreateExecutor(child));
      }
      return std::make_unique<ProjectionExecutor>(p.GetSchemaRef(),
                                                  std::move(children));
    }
    case PlanType::Insert: {
      auto &p = static_cast<InsertPlanNode &>(*plan);
      std::vector<AbstractExecutorRef> children;
      for (auto child : p.GetChildren()) {
        children.push_back(CreateExecutor(child));
      }
      return std::make_unique<InsertExecutor>(
          p.GetSchemaRef(), std::move(children), p.GetTableMeta());
    }
    case PlanType::Tuple: {
      auto &p = static_cast<TuplePlanNode &>(*plan);
      std::vector<AbstractExecutorRef> children;
      for (auto child : p.GetChildren()) {
        children.push_back(CreateExecutor(child));
      }
      return std::make_unique<TupleExecutor>(p.GetSchemaRef(),
                                             std::move(children));
    }
    case PlanType::SeqScan: {
      auto &p = static_cast<ScanColumnPlanNode &>(*plan);
      return std::make_unique<ScanColumnExecutor>(p.GetSchemaRef(),
                                                  p.GetColumnMeta());
    }
    case PlanType::IndexScan:
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