#pragma once

#include "function/FunctionComparison.hpp"
#include "storage/lsmtree/RowGroupMeta.hpp"
#include "type/ValueType.hpp"

#include <cstddef>
#include <cstring>
#include <string>

namespace DB {

// 可下推到扫描层的简单谓词: column[col_idx] <op> constant
struct ScanPredicate {
  size_t column_idx;
  ValueType::Type column_type; // Int / Double / String
  FunctionComparison::Operator op;
  int const_int = 0;
  double const_double = 0.0;
  std::string const_string;
};

// ZoneMap 裁剪：判断 RowGroup 是否可能包含满足谓词的行
// 返回 true 表示可能匹配（不可跳过），false 表示一定不匹配（可跳过）
inline bool ZoneMapMayMatch(const ZoneMap &zone, const ScanPredicate &pred) {
  if (!zone.has_value) {
    return true; // 无统计信息，不跳过
  }

  using Op = FunctionComparison::Operator;

  switch (pred.column_type) {
  case ValueType::Type::Int: {
    if (zone.min.size() != sizeof(int) || zone.max.size() != sizeof(int)) {
      return true;
    }
    int zone_min = 0, zone_max = 0;
    std::memcpy(&zone_min, zone.min.data(), sizeof(int));
    std::memcpy(&zone_max, zone.max.data(), sizeof(int));
    int c = pred.const_int;

    switch (pred.op) {
    case Op::Greater:         return zone_max > c;
    case Op::GreaterOrEquals: return zone_max >= c;
    case Op::Less:            return zone_min < c;
    case Op::LessOrEquals:    return zone_min <= c;
    case Op::Equals:          return c >= zone_min && c <= zone_max;
    case Op::NotEquals:       return !(zone_min == zone_max && zone_min == c);
    }
    break;
  }
  case ValueType::Type::Double: {
    if (zone.min.size() != sizeof(double) || zone.max.size() != sizeof(double)) {
      return true;
    }
    double zone_min = 0.0, zone_max = 0.0;
    std::memcpy(&zone_min, zone.min.data(), sizeof(double));
    std::memcpy(&zone_max, zone.max.data(), sizeof(double));
    double c = pred.const_double;

    switch (pred.op) {
    case Op::Greater:         return zone_max > c;
    case Op::GreaterOrEquals: return zone_max >= c;
    case Op::Less:            return zone_min < c;
    case Op::LessOrEquals:    return zone_min <= c;
    case Op::Equals:          return c >= zone_min && c <= zone_max;
    case Op::NotEquals:       return !(zone_min == zone_max && zone_min == c);
    }
    break;
  }
  case ValueType::Type::String: {
    const auto &c = pred.const_string;

    switch (pred.op) {
    case Op::Greater:         return zone.max > c;
    case Op::GreaterOrEquals: return zone.max >= c;
    case Op::Less:            return zone.min < c;
    case Op::LessOrEquals:    return zone.min <= c;
    case Op::Equals:          return c >= zone.min && c <= zone.max;
    case Op::NotEquals:       return !(zone.min == zone.max && zone.min == c);
    }
    break;
  }
  default:
    return true;
  }

  return true;
}

} // namespace DB
