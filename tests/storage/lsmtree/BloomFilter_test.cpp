#include "common/Config.hpp"
#include "common/Hash.hpp"
#include "storage/lsmtree/BloomFilter.hpp"
#include "storage/lsmtree/Slice.hpp"
#include "storage/lsmtree/builder/BloomFilterBuilder.hpp"

#include <array>
#include <gtest/gtest.h>
#include <string>
#include <vector>

namespace {
constexpr size_t kBlockBytes = 64;
constexpr size_t kNumProbes = 7;
} // namespace

TEST(BloomFilterTest, AddedKeysMatch) {
  using namespace DB;
  std::vector<std::string> keys{"alpha", "bravo", "charlie", "delta"};
  BloomFilterBuilder builder(keys.size());
  for (const auto &key : keys) {
    Slice slice(key);
    builder.AddKey(&slice);
  }

  BloomFilter filter(builder.GetData());
  for (const auto &key : keys) {
    Slice slice(key);
    EXPECT_TRUE(filter.MayContain(slice));
  }
}

TEST(BloomFilterTest, ByteInterfaceMatches) {
  using namespace DB;
  std::vector<std::string> keys{"alpha", "bravo", "charlie", "delta"};
  BloomFilterBuilder builder(keys.size());
  for (const auto &key : keys) {
    Slice slice(key);
    builder.AddKey(&slice);
  }

  BloomFilter filter(reinterpret_cast<const Byte *>(builder.GetData().data()),
                     builder.GetData().size());
  for (const auto &key : keys) {
    EXPECT_TRUE(filter.MayContain(reinterpret_cast<const Byte *>(key.data()),
                                  key.size()));
  }
}

TEST(BloomFilterTest, EmptyFilterDoesNotMatch) {
  using namespace DB;
  BloomFilterBuilder builder(1);
  BloomFilter filter(builder.GetData());

  Slice missing("missing");
  EXPECT_FALSE(filter.MayContain(missing));
}

TEST(BloomFilterTest, InvalidDataReturnsFalse) {
  using namespace DB;
  std::string bad_data(10, '\0');
  BloomFilter filter(bad_data);
  Slice key("alpha");
  EXPECT_FALSE(filter.MayContain(key));

  BloomFilter empty_filter(std::string_view{});
  EXPECT_FALSE(empty_filter.MayContain(key));
}

TEST(BloomFilterTest, MissingBitReturnsFalse) {
  using namespace DB;
  std::string data(kBlockBytes, '\0');
  Slice key("alpha");

  const uint64_t h = Hash64(key.GetData(), key.Size());
  uint32_t current_h = static_cast<uint32_t>(h);
  const uint32_t delta = (current_h >> 17) | (current_h << 15);

  std::array<uint32_t, kNumProbes> bit_positions{};
  for (size_t i = 0; i < kNumProbes; ++i) {
    const uint32_t bit_pos = current_h & 511;
    bit_positions[i] = bit_pos;
    const unsigned char mask = static_cast<unsigned char>(1U << (bit_pos % 8));
    const unsigned char existing =
        static_cast<unsigned char>(data[bit_pos / 8]);
    data[bit_pos / 8] = static_cast<char>(existing | mask);
    current_h += delta;
  }

  const uint32_t missing_bit = bit_positions[0];
  const unsigned char missing_mask =
      static_cast<unsigned char>(1U << (missing_bit % 8));
  const unsigned char existing =
      static_cast<unsigned char>(data[missing_bit / 8]);
  data[missing_bit / 8] =
      static_cast<char>(existing & static_cast<unsigned char>(~missing_mask));

  BloomFilter filter(data);
  EXPECT_FALSE(filter.MayContain(key));
}

TEST(BloomFilterTest, ResetSwitchesData) {
  using namespace DB;
  BloomFilterBuilder builder(1);
  Slice key("alpha");
  builder.AddKey(&key);

  BloomFilter filter(builder.GetData());
  EXPECT_TRUE(filter.MayContain(key));

  std::string empty_data(builder.GetData().size(), '\0');
  filter.Reset(reinterpret_cast<const Byte *>(empty_data.data()),
               empty_data.size());
  EXPECT_FALSE(filter.MayContain(key));

  filter.Reset(reinterpret_cast<const Byte *>(builder.GetData().data()),
               builder.GetData().size());
  EXPECT_TRUE(filter.MayContain(key));
}
