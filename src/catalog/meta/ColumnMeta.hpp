#pragma once

#include "type/ValueType.hpp"

#include <cstdint>
#include <memory>
#include <string>
namespace DB {
struct ColumnMeta {
  std::string name_;
  std::shared_ptr<ValueType> type_;
  uint32_t index_{0};

  ColumnMeta() = default;
  ColumnMeta(std::string name, std::shared_ptr<ValueType> type,
             uint32_t index = 0)
      : name_(std::move(name)), type_(std::move(type)), index_(index) {}
};

using ColumnMetaRef = std::shared_ptr<ColumnMeta>;
} // namespace DB
