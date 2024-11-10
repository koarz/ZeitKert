#pragma once

#include "common/Status.hpp"
#include "function/Function.hpp"
#include "type/String.hpp"

#include <memory>

namespace DB {
class FunctionToUpper final : public Function {
public:
  std::string GetName() const override { return "TO_UPPER"; }

  std::shared_ptr<ValueType> GetResultType() const override {
    return std::make_shared<String>();
  }

  Status ExecuteImpl(Block &block, size_t result_idx,
                     size_t input_rows_count) const override;
};

class FunctionToLower final : public Function {
public:
  std::string GetName() const override { return "TO_LOWER"; }

  std::shared_ptr<ValueType> GetResultType() const override {
    return std::make_shared<String>();
  }

  Status ExecuteImpl(Block &block, size_t result_idx,
                     size_t input_rows_count) const override;
};
} // namespace DB