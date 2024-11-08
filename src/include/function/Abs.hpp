#pragma once

#include "common/Status.hpp"
#include "function/Function.hpp"
#include "type/Int.hpp"

#include <memory>

namespace DB {
class FunctionAbs final : public Function {
public:
  std::string GetName() const override { return "ABS"; }

  std::shared_ptr<ValueType> GetResultType() const override {
    return std::make_shared<Int>();
  }

  Status ExecuteImpl(Block &block, size_t result_idx,
                     size_t input_rows_count) const override;
};
} // namespace DB