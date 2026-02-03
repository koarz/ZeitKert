#pragma once

#include "function/Function.hpp"
#include "type/Int.hpp"

#include <memory>
#include <string>

namespace DB {
class FunctionLogical final : public Function {
public:
  enum class Operator { And, Or };

  FunctionLogical(Operator op);

  std::string GetName() const override { return name_; }

  std::shared_ptr<ValueType> GetResultType() const override {
    return std::make_shared<Int>(); // 返回 0 或 1
  }

  Status ExecuteImpl(Block &block, size_t result_idx,
                     size_t input_rows_count) const override;

  Status
  ResolveResultType(Block &block,
                    std::shared_ptr<ValueType> &result_type) const override {
    result_type = GetResultType();
    return Status::OK();
  }

private:
  std::string name_;
  Operator op_;
};
} // namespace DB
