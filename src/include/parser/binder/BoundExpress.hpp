#pragma once

#include "common/EnumClass.hpp"
#include <memory>

namespace DB {
struct BoundExpress {
  BoundExpress(BindExpressType expr_type) : expr_type_(expr_type) {}
  virtual ~BoundExpress() = default;

  BindExpressType expr_type_;
};
using BoundExpressRef = std::shared_ptr<BoundExpress>;
} // namespace DB