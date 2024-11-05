#pragma once

#include "catalog/Schema.hpp"
#include "common/Status.hpp"

namespace DB {
class AbstractExecutor {
public:
  virtual ~AbstractExecutor() = default;

  virtual Status Init() = 0;

  virtual Status Execute() = 0;

  virtual SchemaRef GetSchema() = 0;
};
} // namespace DB