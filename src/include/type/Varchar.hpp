#pragma once

#include "type/ValueType.hpp"
#include <cstddef>
#include <format>

namespace DB {
class Varchar final : public ValueType {

public:
  Varchar(size_t size) : ValueType(Type::Varchar, size) {}

  std::string GetString() override {
    return std::format("varchar({})", GetSize());
  }
};
} // namespace DB