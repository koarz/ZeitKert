#include "storage/disk/DiskManager.hpp"
#include "storage/lsmtree/LSMTree.hpp"
#include "storage/lsmtree/RowCodec.hpp"
#include "storage/lsmtree/Slice.hpp"
#include "type/Int.hpp"
#include "type/ValueType.hpp"

#include <cstring>
#include <filesystem>
#include <functional>
#include <gtest/gtest.h>
#include <memory>
#include <string>

TEST(LSMTreeTest, BasicWriteReadTest) {
  using namespace DB;
  auto dm = std::make_shared<DiskManager>();
  auto bpm = std::make_shared<BufferPoolManager>(4, dm);
  std::filesystem::path path{"lsm_table"};
  std::unique_ptr<int, std::function<void(int *)>> defer(
      new int(0), [&](int *t) {
        delete t;
        std::filesystem::remove_all(path);
        std::filesystem::remove(path.string() + ".wal");
      });
  auto value_type = std::make_shared<Int>();
  std::vector<std::shared_ptr<ValueType>> types{
      std::static_pointer_cast<ValueType>(value_type)};
  LSMTree lsm(path, bpm, types, 0, false);

  for (int i = 0; i < 5; i++) {
    std::string row;
    RowCodec::AppendValue(row, ValueType::Type::Int, std::to_string(i));
    EXPECT_TRUE(lsm.Insert(Slice{i}, Slice{row}).ok());
  }

  for (int i = 0; i < 5; i++) {
    Slice row;
    EXPECT_TRUE(lsm.GetValue(Slice{i}, &row).ok());
    Slice value;
    EXPECT_TRUE(RowCodec::DecodeColumn(row, 0, &value));
    int v = 0;
    std::memcpy(&v, value.GetData(), sizeof(int));
    EXPECT_EQ(v, i);
  }
}

TEST(LSMTreeTest, MemtableToImmutableTest) {
  using namespace DB;
  auto dm = std::make_shared<DiskManager>();
  auto bpm = std::make_shared<BufferPoolManager>(4, dm);
  std::filesystem::path path{"lsm_table"};
  std::unique_ptr<int, std::function<void(int *)>> defer(
      new int(0), [&](int *t) {
        delete t;
        std::filesystem::remove_all(path);
        std::filesystem::remove(path.string() + ".wal");
      });
  auto value_type = std::make_shared<Int>();
  std::vector<std::shared_ptr<ValueType>> types{
      std::static_pointer_cast<ValueType>(value_type)};
  LSMTree lsm(path, bpm, types, 0, false);

  const int total_rows = 5000;
  for (int i = 0; i < total_rows; i++) {
    std::string row;
    RowCodec::AppendValue(row, ValueType::Type::Int, std::to_string(i));
    EXPECT_TRUE(lsm.Insert(Slice{i}, Slice{row}).ok());
  }

  for (int i = 0; i < total_rows; i++) {
    Slice row;
    EXPECT_TRUE(lsm.GetValue(Slice{i}, &row).ok());
    Slice value;
    EXPECT_TRUE(RowCodec::DecodeColumn(row, 0, &value));
    int v = 0;
    std::memcpy(&v, value.GetData(), sizeof(int));
    EXPECT_EQ(v, i);
  }

  EXPECT_TRUE(lsm.GetImmutableSize() > 0);
}

TEST(LSMTreeTest, RemoveTest) {
  using namespace DB;
  auto dm = std::make_shared<DiskManager>();
  auto bpm = std::make_shared<BufferPoolManager>(4, dm);
  std::filesystem::path path{"lsm_table"};
  std::unique_ptr<int, std::function<void(int *)>> defer(
      new int(0), [&](int *t) {
        delete t;
        std::filesystem::remove_all(path);
        std::filesystem::remove(path.string() + ".wal");
      });
  auto value_type = std::make_shared<Int>();
  std::vector<std::shared_ptr<ValueType>> types{
      std::static_pointer_cast<ValueType>(value_type)};
  LSMTree lsm(path, bpm, types, 0, false);

  for (int i = 0; i < 5; i++) {
    std::string row;
    RowCodec::AppendValue(row, ValueType::Type::Int, std::to_string(i));
    EXPECT_TRUE(lsm.Insert(Slice{i}, Slice{row}).ok());
  }

  EXPECT_TRUE(lsm.Remove(Slice{0}).ok());
  EXPECT_TRUE(lsm.Remove(Slice{2}).ok());
  EXPECT_TRUE(lsm.Remove(Slice{4}).ok());

  Slice row;
  EXPECT_FALSE(lsm.GetValue(Slice{0}, &row).ok());
  EXPECT_TRUE(lsm.GetValue(Slice{1}, &row).ok());
  EXPECT_FALSE(lsm.GetValue(Slice{2}, &row).ok());
  EXPECT_TRUE(lsm.GetValue(Slice{3}, &row).ok());
  EXPECT_FALSE(lsm.GetValue(Slice{4}, &row).ok());
}
