#pragma once

#include "catalog/Schema.hpp"
#include "common/Status.hpp"
#include "execution/AbstractExecutor.hpp"

#include <thread>
#include <vector>

namespace DB {
class ProjectionExecutor : public AbstractExecutor {
  std::vector<AbstractExecutorRef> children_;

public:
  ProjectionExecutor(SchemaRef schema,
                     std::vector<AbstractExecutorRef> children)
      : AbstractExecutor(schema), children_(std::move(children)) {}

  ~ProjectionExecutor() override = default;

  Status Execute() override;
};
} // namespace DB