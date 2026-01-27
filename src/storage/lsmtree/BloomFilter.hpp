#pragma once

#include "common/Hash.hpp"
#include "storage/lsmtree/Slice.hpp"

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace DB {
class BloomFilter {
  static constexpr size_t num_probes_ = 7;
  static constexpr size_t block_bytes_ = 64;

  const Byte *data_ = nullptr;
  size_t data_size_ = 0;
  size_t num_blocks_ = 0;

  bool IsValid() const {
    return data_ && data_size_ != 0 && data_size_ % block_bytes_ == 0 &&
           num_blocks_ != 0;
  }

  bool MayContainHash(uint64_t h) const {
    const uint32_t block_idx = (h >> 32) % num_blocks_;

    uint32_t current_h = static_cast<uint32_t>(h);
    const uint32_t delta = (current_h >> 17) | (current_h << 15);

    const auto *block = reinterpret_cast<const unsigned char *>(
        data_ + block_idx * block_bytes_);
    for (size_t i = 0; i < num_probes_; ++i) {
      const uint32_t bit_pos = current_h & 511;
      if ((block[bit_pos / 8] &
           static_cast<unsigned char>(1U << (bit_pos % 8))) == 0) {
        return false;
      }
      current_h += delta;
    }
    return true;
  }

public:
  BloomFilter() = default;

  BloomFilter(const Byte *data, size_t size) { Reset(data, size); }

  explicit BloomFilter(std::string_view data) { Reset(data); }

  void Reset(const Byte *data, size_t size) {
    if (!data) {
      data_ = nullptr;
      data_size_ = 0;
      num_blocks_ = 0;
      return;
    }
    data_ = data;
    data_size_ = size;
    num_blocks_ = data_size_ / block_bytes_;
  }

  void Reset(std::string_view data) {
    Reset(reinterpret_cast<const Byte *>(data.data()), data.size());
  }

  bool MayContain(const Slice &key) const {
    return MayContain(key.GetData(), key.Size());
  }

  bool MayContain(const Byte *key, size_t size) const {
    if (!IsValid()) {
      return false;
    }
    if (!key && size != 0) {
      return false;
    }

    static const Byte kEmpty = 0;
    const Byte *data = key ? key : &kEmpty;
    return MayContainHash(Hash64(static_cast<const void *>(data), size));
  }
};
} // namespace DB
