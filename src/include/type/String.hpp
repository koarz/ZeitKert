#pragma once

#include "type/ValueType.hpp"

namespace DB {
class String final : public ValueType {
public:
  String() : ValueType(Type::String) {}

  bool IsVariableSize() override { return true; }
  std::string GetString() override { return "string"; }
};
} // namespace DB