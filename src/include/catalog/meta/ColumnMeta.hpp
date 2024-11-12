#pragma once

#include "type/ValueType.hpp"

#include <atomic>
#include <memory>
#include <string>
namespace DB {
struct ColumnMeta {
  std::string name_;
  std::shared_ptr<ValueType> type_;
  std::atomic_uint64_t page_number_;
};

using ColumnMetaRef = std::shared_ptr<ColumnMeta>;
} // namespace DB