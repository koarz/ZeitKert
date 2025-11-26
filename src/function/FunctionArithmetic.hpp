#pragma once

#include "function/Function.hpp"
#include "type/Double.hpp"
#include "type/Int.hpp"

#include <memory>
#include <string>

namespace DB {
class FunctionBinaryArithmetic final : public Function {
public:
  enum class Operator { Add, Sub, Mul, Div };

  FunctionBinaryArithmetic(Operator op, std::shared_ptr<ValueType> result_type);

  std::string GetName() const override { return name_; }

  std::shared_ptr<ValueType> GetResultType() const override {
    return result_type_;
  }

  Status ExecuteImpl(Block &block, size_t result_idx,
                     size_t input_rows_count) const override;

private:
  std::string name_;
  Operator op_;
  std::shared_ptr<ValueType> result_type_;
};
} // namespace DB
