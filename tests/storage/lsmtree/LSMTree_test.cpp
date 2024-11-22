#include "storage/lsmtree/LSMTree.hpp"
#include "storage/lsmtree/Slice.hpp"
#include "type/Int.hpp"
#include "type/ValueType.hpp"

#include <gtest/gtest.h>
#include <memory>
#include <string>

TEST(LSMTreeTest, BasicWriteReadTest) {
  using namespace DB;
  auto dm = std::make_shared<DiskManager>();
  auto bpm = std::make_shared<BufferPoolManager>(4, dm);
  std::filesystem::path path{"column"};
  auto value_type = std::make_shared<Int>();
  LSMTree lsm(path, bpm, std::static_pointer_cast<ValueType>(value_type),
              false);
  EXPECT_TRUE(lsm.Insert("0", "0").ok());
  EXPECT_TRUE(lsm.Insert("1", "1").ok());
  EXPECT_TRUE(lsm.Insert("2", "2").ok());
  EXPECT_TRUE(lsm.Insert("3", "3").ok());
  EXPECT_TRUE(lsm.Insert("4", "4").ok());

  for (int i = 0; i < 5; i++) {
    Slice value;
    EXPECT_TRUE(lsm.GetValue(std::to_string(i), &value).ok());
    EXPECT_EQ(value.ToString(), std::to_string(i));
  }
}

TEST(LSMTreeTest, DISABLED_MemtableToImmutableTest) {
  using namespace DB;
  auto dm = std::make_shared<DiskManager>();
  auto bpm = std::make_shared<BufferPoolManager>(4, dm);
  std::filesystem::path path{"column"};
  auto value_type = std::make_shared<Int>();
  LSMTree lsm(path, bpm, std::static_pointer_cast<ValueType>(value_type),
              false);

  std::vector<std::string> strs;
  // vector expansion can cause the internal data pointer of string to fail
  // I'm surprised the expansion mechanism isn't moving internal values?
  strs.reserve(500000);
  for (int i = 0; i < 500000; i++) {
    strs.emplace_back(std::to_string(i));
    EXPECT_TRUE(lsm.Insert(strs[i], strs[i]).ok());
  }

  for (int i = 0; i < 500000; i++) {
    Slice value;
    EXPECT_TRUE(lsm.GetValue(strs[i], &value).ok());
    EXPECT_EQ(value.ToString(), strs[i]);
  }

  EXPECT_TRUE(lsm.GetImmutableSize() > 0);
}

TEST(LSMTreeTest, RemoveTest) {
  using namespace DB;
  auto dm = std::make_shared<DiskManager>();
  auto bpm = std::make_shared<BufferPoolManager>(4, dm);
  std::filesystem::path path{"column"};
  auto value_type = std::make_shared<Int>();
  LSMTree lsm(path, bpm, std::static_pointer_cast<ValueType>(value_type),
              false);
  EXPECT_TRUE(lsm.Insert("0", "0").ok());
  EXPECT_TRUE(lsm.Insert("1", "1").ok());
  EXPECT_TRUE(lsm.Insert("2", "2").ok());
  EXPECT_TRUE(lsm.Insert("3", "3").ok());
  EXPECT_TRUE(lsm.Insert("4", "4").ok());

  EXPECT_TRUE(lsm.Remove("0").ok());
  EXPECT_TRUE(lsm.Remove("2").ok());
  EXPECT_TRUE(lsm.Remove("4").ok());

  Slice value;
  EXPECT_FALSE(lsm.GetValue("0", &value).ok());
  EXPECT_TRUE(lsm.GetValue("1", &value).ok());
  EXPECT_EQ(value.ToString(), std::string{"1"});
  EXPECT_FALSE(lsm.GetValue("2", &value).ok());
  EXPECT_TRUE(lsm.GetValue("3", &value).ok());
  EXPECT_EQ(value.ToString(), std::string{"3"});
  EXPECT_FALSE(lsm.GetValue("4", &value).ok());
}