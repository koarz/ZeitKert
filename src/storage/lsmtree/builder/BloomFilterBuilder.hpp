#pragma once

#include "common/Hash.hpp"
#include "storage/lsmtree/Slice.hpp"

#include <cstddef>
#include <cstdint>

namespace DB {
class BloomFilterBuilder {
  static constexpr size_t num_probes_ = 7;
  static constexpr size_t block_bytes_ = 64;
  static constexpr size_t bits_per_key_ = 12;

  std::string data_;
  size_t num_blocks_;

public:
  explicit BloomFilterBuilder(size_t num_keys) {
    size_t total_bits = num_keys * bits_per_key_;
    num_blocks_ = (total_bits + (block_bytes_ * 8) - 1) / (block_bytes_ * 8);

    size_t total_bytes = num_blocks_ * block_bytes_;
    data_.resize(total_bytes, 0);
  }

  void AddKey(const Slice &key) {
    uint64_t h = Hash64(key.GetData(), key.Size());
    uint32_t block_idx = (h >> 32) % num_blocks_;

    uint32_t current_h = static_cast<uint32_t>(h);
    uint32_t delta = (current_h >> 17) | (current_h << 15);

    auto block = &data_.data()[block_idx * block_bytes_];
    for (int i = 0; i < num_probes_; i++) {
      uint32_t bit_pos = current_h & 511;
      block[bit_pos / 8] |= (1 << (bit_pos % 8));
      current_h += delta;
    }
  }

  std::string &GetData() { return data_; }
};
} // namespace DB
