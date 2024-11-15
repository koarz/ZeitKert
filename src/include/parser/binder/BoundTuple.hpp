#pragma once

#include "parser/binder/BoundExpress.hpp"
#include <vector>

namespace DB {
struct BoundTuple : public BoundExpress {
  std::vector<BoundExpressRef> elements_;

  BoundTuple() : BoundExpress(BoundExpressType::BoundTuple) {}
  ~BoundTuple() override = default;
};
} // namespace DB