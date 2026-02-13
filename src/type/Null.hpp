#pragma once

#include "type/ValueType.hpp"

namespace DB {
class Null final : public ValueType {
public:
  Null() : ValueType(Type::Null, 0) {}

  std::string ToString() override { return "null"; }
};
} // namespace DB
