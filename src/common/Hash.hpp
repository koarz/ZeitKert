#pragma once

#include "common/Config.hpp"
#include "xxhash.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <type_traits>

namespace DB {
inline uint64_t Hash64(Byte *data, size_t size, uint64_t seed = 0) noexcept {
  return XXH3_64bits_withSeed(data, size, seed);
}

inline uint64_t Hash64(const void *data, size_t size,
                       uint64_t seed = 0) noexcept {
  return XXH3_64bits_withSeed(data, size, seed);
}

inline uint64_t Hash64(std::string_view data, uint64_t seed = 0) noexcept {
  return Hash64(data.data(), data.size(), seed);
}

template <typename T>
inline uint64_t Hash64(const T &value, uint64_t seed = 0) noexcept {
  static_assert(std::is_trivially_copyable_v<T>,
                "Hash64 requires trivially copyable types.");
  return Hash64(std::addressof(value), sizeof(T), seed);
}
} // namespace DB
