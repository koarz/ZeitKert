#pragma once

#include "catalog/Schema.hpp"
#include "common/Status.hpp"
#include "execution/AbstractExecutor.hpp"
#include "function/Function.hpp"

#include <memory>
#include <vector>

namespace DB {
class FunctionExecutor : public AbstractExecutor {
  std::shared_ptr<Function> function_;
  std::vector<AbstractExecutorRef> children_;

public:
  FunctionExecutor(SchemaRef schema, std::shared_ptr<Function> &function,
                   std::vector<AbstractExecutorRef> children)
      : AbstractExecutor(std::move(schema)), function_(function),
        children_(std::move(children)) {}

  Status Init() override;

  Status Execute() override;
};
} // namespace DB