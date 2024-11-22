#pragma once

#include <cstdint>
#include <cstring>

namespace DB {

inline char *EncodeUInt32(char *data, uint32_t n) {
  std::memcpy(data, &n, 4);
  return data + 4;
}

inline char *DecodeUint32(char *data, uint32_t &n) {
  std::memcpy(&n, data, 4);
  return data + 4;
}

} // namespace DB