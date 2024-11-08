#pragma once

#include "common/EnumClass.hpp"
#include "function/Function.hpp"
#include "parser/binder/BoundExpress.hpp"

#include <memory>
#include <vector>

namespace DB {
class BoundFunction : public BoundExpress {
  std::shared_ptr<Function> function_;
  std::vector<BoundExpressRef> arguments_;

public:
  BoundFunction(std::shared_ptr<Function> function,
                std::vector<BoundExpressRef> arguments)
      : BoundExpress(BoundExpressType::BoundFunction), function_(function),
        arguments_(arguments) {}

  ~BoundFunction() override = default;

  std::shared_ptr<Function> GetFunction() { return function_; }

  std::vector<BoundExpressRef> GetArguments() { return arguments_; }
};
} // namespace DB