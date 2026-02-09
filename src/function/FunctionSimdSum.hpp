#pragma once

#include "common/Status.hpp"
#include "function/Function.hpp"
#include "type/Double.hpp"

#include <memory>

namespace DB {
class FunctionSimdSum final : public Function {
public:
  std::string GetName() const override { return "SIMD_SUM"; }

  std::shared_ptr<ValueType> GetResultType() const override {
    return std::make_shared<Double>();
  }

  Status
  ResolveResultType(Block &block,
                    std::shared_ptr<ValueType> &result_type) const override;

  Status ExecuteImpl(Block &block, size_t result_idx,
                     size_t input_rows_count) const override;
};
} // namespace DB
