#pragma once

#include <cstdint>
#include <cstring>

namespace DB {
struct SliceUtil {
  static void GetUint64(uint8_t *src, uint64_t *value) {
    memcpy(value, src, 8);
  }

  static void EncodeUint64(uint8_t *dst, uint64_t value) {
    memcpy(dst, &value, 8);
  }

  static void GetUint16(uint8_t *src, uint16_t *value) {
    memcpy(value, src, 2);
  }

  static void EncodeUint16(uint8_t *dst, uint16_t value) {
    memcpy(dst, &value, 2);
  }
};
} // namespace DB