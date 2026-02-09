#pragma once

#include "function/Function.hpp"
#include "type/Double.hpp"

#include <memory>
#include <string>

namespace DB {
class FunctionMath final : public Function {
public:
  enum class MathOp {
    Sqrt, Sin, Cos, Tan, Asin, Acos, Atan,
    Log, Log10, Exp, Ceil, Floor, Round
  };

  explicit FunctionMath(MathOp op);

  std::string GetName() const override { return name_; }

  std::shared_ptr<ValueType> GetResultType() const override {
    return std::make_shared<Double>();
  }

  Status ExecuteImpl(Block &block, size_t result_idx,
                     size_t input_rows_count) const override;

private:
  std::string name_;
  MathOp op_;
};
} // namespace DB
