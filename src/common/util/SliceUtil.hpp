#pragma once

#include "common/Config.hpp"

#include <cstdint>
#include <cstring>

namespace DB {
struct SliceUtil {
  static void GetUint64(Byte *src, uint64_t *value) { memcpy(value, src, 8); }

  static void EncodeUint64(Byte *dst, uint64_t value) {
    memcpy(dst, &value, 8);
  }

  static void GetUint16(Byte *src, uint16_t *value) { memcpy(value, src, 2); }

  static void EncodeUint16(Byte *dst, uint16_t value) {
    memcpy(dst, &value, 2);
  }
};
} // namespace DB