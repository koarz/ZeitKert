#pragma once

#include "execution/AbstractExecutor.hpp"
#include "planner/RangePlanNode.hpp"

#include <cstdint>
#include <memory>

namespace DB {
class RangeExecutor : public AbstractExecutor {
  int64_t start_;
  int64_t stop_;
  int64_t step_;

public:
  RangeExecutor(SchemaRef schema, int64_t start, int64_t stop, int64_t step)
      : AbstractExecutor(std::move(schema)), start_(start), stop_(stop),
        step_(step) {}

  ~RangeExecutor() override = default;

  Status Execute() override;
};
} // namespace DB
