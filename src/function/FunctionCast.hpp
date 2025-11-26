#pragma once

#include "function/Function.hpp"

#include <memory>
#include <string>

namespace DB {
class FunctionCast final : public Function {
public:
  FunctionCast() = default;

  std::string GetName() const override { return "CAST"; }

  std::shared_ptr<ValueType> GetResultType() const override { return nullptr; }

  Status
  ResolveResultType(Block &block,
                    std::shared_ptr<ValueType> &result_type) const override;

  Status ExecuteImpl(Block &block, size_t result_idx,
                     size_t input_rows_count) const override;
};
} // namespace DB
