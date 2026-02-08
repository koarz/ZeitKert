#pragma once

#include "common/Config.hpp"
#include "type/ValueType.hpp"

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

namespace DB {
struct ZoneMap {
  bool has_value = false;
  std::string min;
  std::string max;
};

struct ColumnChunkMeta {
  uint32_t offset = 0;
  uint32_t size = 0;
  ZoneMap zone;
};

struct RowGroupMeta {
  uint32_t offset = 0;
  uint32_t row_count = 0;
  std::vector<ColumnChunkMeta> columns;
  std::string bloom;
  std::string max_key;
  // 新增：key 列的偏移和大小（相对于 RowGroup 数据起始）
  uint32_t key_column_offset = 0;
  uint32_t key_column_size = 0;

  void Serialize(const std::vector<std::shared_ptr<ValueType>> &types,
                 std::string &out) const {
    // 顺序写入 RowGroup 元数据到 blob
    auto append = [&](const auto &v) {
      out.append(reinterpret_cast<const char *>(&v), sizeof(v));
    };

    append(offset);
    append(row_count);
    for (size_t i = 0; i < columns.size(); i++) {
      const auto &col = columns[i];
      append(col.offset);
      append(col.size);
      uint8_t has_value = col.zone.has_value ? 1 : 0;
      append(has_value);
      switch (types[i]->GetType()) {
      case ValueType::Type::Int: {
        // 数值 zonemap 使用定长二进制
        int v_min = 0;
        int v_max = 0;
        if (col.zone.has_value && col.zone.min.size() == sizeof(int) &&
            col.zone.max.size() == sizeof(int)) {
          std::memcpy(&v_min, col.zone.min.data(), sizeof(int));
          std::memcpy(&v_max, col.zone.max.data(), sizeof(int));
        }
        append(v_min);
        append(v_max);
        break;
      }
      case ValueType::Type::Double: {
        // 数值 zonemap 使用定长二进制
        double v_min = 0.0;
        double v_max = 0.0;
        if (col.zone.has_value && col.zone.min.size() == sizeof(double) &&
            col.zone.max.size() == sizeof(double)) {
          std::memcpy(&v_min, col.zone.min.data(), sizeof(double));
          std::memcpy(&v_max, col.zone.max.data(), sizeof(double));
        }
        append(v_min);
        append(v_max);
        break;
      }
      case ValueType::Type::String: {
        // 字符串 zonemap 以长度加字节保存
        uint16_t min_len = 0;
        uint16_t max_len = 0;
        if (col.zone.has_value) {
          min_len = static_cast<uint16_t>(col.zone.min.size());
          max_len = static_cast<uint16_t>(col.zone.max.size());
        }
        append(min_len);
        if (min_len > 0) {
          out.append(col.zone.min.data(), min_len);
        }
        append(max_len);
        if (max_len > 0) {
          out.append(col.zone.max.data(), max_len);
        }
        break;
      }
      case ValueType::Type::Null: break;
      }
    }

    uint32_t bloom_size = static_cast<uint32_t>(bloom.size());
    // bloom 和 max_key 放在末尾
    append(bloom_size);
    if (bloom_size > 0) {
      out.append(bloom.data(), bloom.size());
    }
    uint32_t key_size = static_cast<uint32_t>(max_key.size());
    append(key_size);
    if (key_size > 0) {
      out.append(max_key.data(), max_key.size());
    }
    // 新增：key 列偏移和大小
    append(key_column_offset);
    append(key_column_size);
  }

  static bool Deserialize(const Byte *&p, const Byte *end,
                          const std::vector<std::shared_ptr<ValueType>> &types,
                          RowGroupMeta &out) {
    // 按固定顺序解码 RowGroup 元数据
    auto read = [&](auto &v) {
      if (p + sizeof(v) > end) {
        return false;
      }
      std::memcpy(&v, p, sizeof(v));
      p += sizeof(v);
      return true;
    };

    if (!read(out.offset)) {
      return false;
    }
    if (!read(out.row_count)) {
      return false;
    }
    out.columns.clear();
    out.columns.reserve(types.size());
    for (size_t i = 0; i < types.size(); i++) {
      ColumnChunkMeta col;
      if (!read(col.offset) || !read(col.size)) {
        return false;
      }
      uint8_t has_value = 0;
      if (!read(has_value)) {
        return false;
      }
      col.zone.has_value = has_value != 0;
      switch (types[i]->GetType()) {
      case ValueType::Type::Int: {
        // 数值 zonemap 固定长度读取
        int v_min = 0;
        int v_max = 0;
        if (!read(v_min) || !read(v_max)) {
          return false;
        }
        if (col.zone.has_value) {
          col.zone.min.assign(reinterpret_cast<const char *>(&v_min),
                              sizeof(v_min));
          col.zone.max.assign(reinterpret_cast<const char *>(&v_max),
                              sizeof(v_max));
        }
        break;
      }
      case ValueType::Type::Double: {
        // 数值 zonemap 固定长度读取
        double v_min = 0.0;
        double v_max = 0.0;
        if (!read(v_min) || !read(v_max)) {
          return false;
        }
        if (col.zone.has_value) {
          col.zone.min.assign(reinterpret_cast<const char *>(&v_min),
                              sizeof(v_min));
          col.zone.max.assign(reinterpret_cast<const char *>(&v_max),
                              sizeof(v_max));
        }
        break;
      }
      case ValueType::Type::String: {
        // 字符串 zonemap 读出长度和字节
        uint16_t min_len = 0;
        uint16_t max_len = 0;
        if (!read(min_len)) {
          return false;
        }
        if (p + min_len > end) {
          return false;
        }
        if (min_len > 0) {
          col.zone.min.assign(reinterpret_cast<const char *>(p), min_len);
          p += min_len;
        }
        if (!read(max_len)) {
          return false;
        }
        if (p + max_len > end) {
          return false;
        }
        if (max_len > 0) {
          col.zone.max.assign(reinterpret_cast<const char *>(p), max_len);
          p += max_len;
        }
        break;
      }
      case ValueType::Type::Null: break;
      }
      out.columns.emplace_back(std::move(col));
    }

    uint32_t bloom_size = 0;
    if (!read(bloom_size)) {
      return false;
    }
    if (p + bloom_size > end) {
      return false;
    }
    out.bloom.assign(reinterpret_cast<const char *>(p), bloom_size);
    p += bloom_size;

    uint32_t key_size = 0;
    if (!read(key_size)) {
      return false;
    }
    if (p + key_size > end) {
      return false;
    }
    out.max_key.assign(reinterpret_cast<const char *>(p), key_size);
    p += key_size;

    // 新增：读取 key 列偏移和大小
    if (!read(out.key_column_offset)) {
      return false;
    }
    if (!read(out.key_column_size)) {
      return false;
    }
    return true;
  }
};
} // namespace DB
