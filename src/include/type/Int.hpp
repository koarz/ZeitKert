#pragma once

#include "type/ValueType.hpp"

namespace DB {
class Int final : public ValueType {
public:
  Int() : ValueType(Type::Int, sizeof(int)) {}

  bool IsVariableSize() override { return false; }
  std::string GetString() override { return "int"; }
};
} // namespace DB