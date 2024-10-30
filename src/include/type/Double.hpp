#pragma once

#include "type/ValueType.hpp"

namespace DB {
class Double final : public ValueType {
public:
  Double() : ValueType(Type::Double, sizeof(double)) {}

  std::string GetString() override { return "double"; }
};
} // namespace DB