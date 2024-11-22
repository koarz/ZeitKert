#pragma once

#include "common/EnumClass.hpp"

#include <memory>

namespace DB {
struct BoundExpress {
  BoundExpress(BoundExpressType expr_type) : expr_type_(expr_type) {}
  virtual ~BoundExpress() = default;

  BoundExpressType expr_type_;
};
using BoundExpressRef = std::shared_ptr<BoundExpress>;
} // namespace DB