#include "storage/lsmtree/builder/BloomFilterBuilder.hpp"
#include "storage/lsmtree/Slice.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <string>

namespace {
constexpr size_t kBlockBytes = 64;
constexpr size_t kBitsPerKey = 12;
constexpr size_t kNumProbes = 7;
} // namespace

TEST(BloomFilterBuilderTest, DataSizeMatchesKeys) {
  using namespace DB;
  BloomFilterBuilder one_key(1);
  EXPECT_EQ(one_key.GetData().size(), kBlockBytes);

  const size_t total_bits = 43 * kBitsPerKey;
  const size_t num_blocks =
      (total_bits + (kBlockBytes * 8) - 1) / (kBlockBytes * 8);
  BloomFilterBuilder boundary_keys(43);
  EXPECT_EQ(boundary_keys.GetData().size(), num_blocks * kBlockBytes);
}

TEST(BloomFilterBuilderTest, AddKeySetsExpectedBitsInBlock) {
  using namespace DB;
  BloomFilterBuilder builder(100);
  const std::string before = builder.GetData();
  for (unsigned char byte : before) {
    EXPECT_EQ(byte, 0);
  }

  Slice key("hello");
  builder.AddKey(&key);

  const std::string &data = builder.GetData();
  ASSERT_EQ(data.size() % kBlockBytes, 0U);
  const size_t num_blocks = data.size() / kBlockBytes;
  ASSERT_GT(num_blocks, 0U);

  const uint64_t h = Hash64(key.GetData(), key.Size());
  const uint32_t block_idx = (h >> 32) % num_blocks;
  uint32_t current_h = static_cast<uint32_t>(h);
  const uint32_t delta = (current_h >> 17) | (current_h << 15);

  std::array<unsigned char, kBlockBytes> expected{};
  for (size_t i = 0; i < kNumProbes; ++i) {
    const uint32_t bit_pos = current_h & 511;
    expected[bit_pos / 8] |=
        static_cast<unsigned char>(1U << (bit_pos % 8));
    current_h += delta;
  }

  const size_t block_start = block_idx * kBlockBytes;
  for (size_t i = 0; i < data.size(); ++i) {
    const unsigned char actual =
        static_cast<unsigned char>(data[i]);
    if (i < block_start || i >= block_start + kBlockBytes) {
      EXPECT_EQ(actual, 0);
      continue;
    }
    const unsigned char expected_byte = expected[i - block_start];
    EXPECT_EQ(actual & expected_byte, expected_byte);
    EXPECT_EQ(actual & static_cast<unsigned char>(~expected_byte), 0);
  }
}
