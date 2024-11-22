#pragma once

#include "catalog/Schema.hpp"
#include "common/Status.hpp"
#include <memory>

namespace DB {
class AbstractExecutor {
protected:
  SchemaRef schema_;

public:
  AbstractExecutor(SchemaRef schema) : schema_(schema) {}

  virtual ~AbstractExecutor() = default;

  virtual Status Execute() = 0;

  SchemaRef GetSchema() { return schema_; };
};

using AbstractExecutorRef = std::unique_ptr<AbstractExecutor>;
} // namespace DB