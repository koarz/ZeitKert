#pragma once

#include "common/Status.hpp"
#include "execution/ExecutorFactory.hpp"
#include "planner/AbstractPlanNode.hpp"

namespace DB {
class ExecutionEngine {
public:
  Status Execute(AbstractPlanNodeRef plan) {
    auto executor = ExecutorFactory::CreateExecutor(plan);
    Status status;
    status = executor->Init();
    status = executor->Execute();
    return status;
  }
};
} // namespace DB