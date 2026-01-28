#pragma once

#include "common/Config.hpp"
#include "storage/lsmtree/Slice.hpp"
#include "type/ValueType.hpp"

#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <string>
#include <string_view>

namespace DB {
class RowCodec {
public:
  static void AppendValue(std::string &buffer, ValueType::Type type,
                          std::string_view value) {
    // 行编码使用长度前缀按列追加
    uint32_t len = 0;
    char raw[sizeof(double)]{};
    switch (type) {
    case ValueType::Type::Int: {
      if (value == "Null") {
        // 数值空值用 len 为 0 表示
        len = 0;
        break;
      }
      int v = std::stoi(std::string(value));
      len = sizeof(v);
      std::memcpy(raw, &v, len);
      break;
    }
    case ValueType::Type::Double: {
      if (value == "Null") {
        // 数值空值用 len 为 0 表示
        len = 0;
        break;
      }
      double v = std::stod(std::string(value));
      len = sizeof(v);
      std::memcpy(raw, &v, len);
      break;
    }
    case ValueType::Type::String: {
      len = static_cast<uint32_t>(value.size());
      break;
    }
    case ValueType::Type::Null: len = 0; break;
    }

    buffer.append(reinterpret_cast<const char *>(&len), sizeof(len));
    if (len == 0) {
      return;
    }
    if (type == ValueType::Type::String) {
      buffer.append(value.data(), value.size());
    } else {
      buffer.append(raw, len);
    }
  }

  static bool DecodeColumn(const Slice &row, size_t column_idx, Slice *value) {
    if (!value) {
      return false;
    }
    const Byte *p = row.GetData();
    size_t remaining = row.Size();
    // 顺序跳过列直到目标列
    for (size_t i = 0; i <= column_idx; i++) {
      if (remaining < sizeof(uint32_t)) {
        return false;
      }
      uint32_t len{};
      std::memcpy(&len, p, sizeof(len));
      p += sizeof(len);
      remaining -= sizeof(len);
      if (remaining < len) {
        return false;
      }
      if (i == column_idx) {
        if (len > std::numeric_limits<uint16_t>::max()) {
          return false;
        }
        *value = Slice{const_cast<Byte *>(p), static_cast<uint16_t>(len)};
        return true;
      }
      p += len;
      remaining -= len;
    }
    return false;
  }

  static bool
  DecodeRow(const Slice &row, size_t column_count,
            const std::function<void(size_t, const Byte *, uint32_t)> &cb) {
    const Byte *p = row.GetData();
    size_t remaining = row.Size();
    // 按列顺序解码行
    for (size_t i = 0; i < column_count; i++) {
      if (remaining < sizeof(uint32_t)) {
        return false;
      }
      uint32_t len{};
      std::memcpy(&len, p, sizeof(len));
      p += sizeof(len);
      remaining -= sizeof(len);
      if (remaining < len) {
        return false;
      }
      cb(i, p, len);
      p += len;
      remaining -= len;
    }
    return true;
  }
};
} // namespace DB
