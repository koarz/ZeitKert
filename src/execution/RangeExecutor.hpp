#pragma once

#include "execution/AbstractExecutor.hpp"
#include "planner/RangePlanNode.hpp"

#include <cstdint>
#include <memory>

namespace DB {

// range(start, stop, step) 序列生成算子（向量化版本）
// 整段序列在第一次 Next() 一次性发射完毕，与现有调用方期望一致
class RangeExecutor : public AbstractExecutor {
  int64_t start_;
  int64_t stop_;
  int64_t step_;
  bool emitted_{false};

public:
  RangeExecutor(SchemaRef schema, int64_t start, int64_t stop, int64_t step)
      : AbstractExecutor(std::move(schema)), start_(start), stop_(stop),
        step_(step) {}

  ~RangeExecutor() override = default;

  Status Init() override;
  Status Next(DataChunk &out) override;
};

} // namespace DB
