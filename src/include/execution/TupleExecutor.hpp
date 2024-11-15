#pragma once

#include "common/Status.hpp"
#include "execution/AbstractExecutor.hpp"

namespace DB {
class TupleExecutor : public AbstractExecutor {
  std::vector<AbstractExecutorRef> children_;

public:
  TupleExecutor(SchemaRef schema, std::vector<AbstractExecutorRef> children)
      : AbstractExecutor(schema), children_(std::move(children)) {}

  ~TupleExecutor() override = default;

  Status Init() override;

  Status Execute() override;
};
} // namespace DB