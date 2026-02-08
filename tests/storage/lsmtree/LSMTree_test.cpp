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

// 删除后重新插入同一个 key，验证能读到新值
TEST(LSMTreeTest, DeleteThenReinsert) {
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

  // 插入 key=42, value=100
  std::string row1;
  RowCodec::AppendValue(row1, ValueType::Type::Int, "100");
  EXPECT_TRUE(lsm.Insert(Slice{42}, Slice{row1}).ok());

  // 删除 key=42
  EXPECT_TRUE(lsm.Remove(Slice{42}).ok());
  Slice tmp;
  EXPECT_FALSE(lsm.GetValue(Slice{42}, &tmp).ok());

  // 重新插入 key=42, value=999
  std::string row2;
  RowCodec::AppendValue(row2, ValueType::Type::Int, "999");
  EXPECT_TRUE(lsm.Insert(Slice{42}, Slice{row2}).ok());

  Slice result;
  EXPECT_TRUE(lsm.GetValue(Slice{42}, &result).ok());
  Slice val;
  EXPECT_TRUE(RowCodec::DecodeColumn(result, 0, &val));
  int v = 0;
  std::memcpy(&v, val.GetData(), sizeof(int));
  EXPECT_EQ(v, 999);
}

// 覆盖更新 + FlushToSST，验证从 SSTable 读到的是新值
TEST(LSMTreeTest, OverwriteThenFlush) {
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

  const int total = 100;
  // 第一轮：写入 value = i
  for (int i = 0; i < total; i++) {
    std::string row;
    RowCodec::AppendValue(row, ValueType::Type::Int, std::to_string(i));
    EXPECT_TRUE(lsm.Insert(Slice{i}, Slice{row}).ok());
  }

  // 覆盖偶数 key：value = i + 5000
  for (int i = 0; i < total; i += 2) {
    std::string row;
    RowCodec::AppendValue(row, ValueType::Type::Int, std::to_string(i + 5000));
    EXPECT_TRUE(lsm.Insert(Slice{i}, Slice{row}).ok());
  }

  // Flush 到 SSTable
  EXPECT_TRUE(lsm.FlushToSST().ok());

  // 验证从 SSTable 读取
  for (int i = 0; i < total; i++) {
    Slice row;
    EXPECT_TRUE(lsm.GetValue(Slice{i}, &row).ok()) << "key=" << i;
    Slice val;
    EXPECT_TRUE(RowCodec::DecodeColumn(row, 0, &val));
    int v = 0;
    std::memcpy(&v, val.GetData(), sizeof(int));
    if (i % 2 == 0) {
      EXPECT_EQ(v, i + 5000) << "overwritten key=" << i;
    } else {
      EXPECT_EQ(v, i) << "original key=" << i;
    }
  }
}

// 同一个 memtable 内删除 + flush，验证 tombstone 正确跳过
TEST(LSMTreeTest, RemoveThenFlush) {
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

  // 插入 10 个 key
  for (int i = 0; i < 10; i++) {
    std::string row;
    RowCodec::AppendValue(row, ValueType::Type::Int, std::to_string(i * 10));
    EXPECT_TRUE(lsm.Insert(Slice{i}, Slice{row}).ok());
  }
  // 删除偶数 key
  for (int i = 0; i < 10; i += 2) {
    EXPECT_TRUE(lsm.Remove(Slice{i}).ok());
  }

  // Flush
  EXPECT_TRUE(lsm.FlushToSST().ok());

  // 验证
  for (int i = 0; i < 10; i++) {
    Slice row;
    if (i % 2 == 0) {
      EXPECT_FALSE(lsm.GetValue(Slice{i}, &row).ok())
          << "deleted key should not be found: " << i;
    } else {
      EXPECT_TRUE(lsm.GetValue(Slice{i}, &row).ok())
          << "key should be found: " << i;
      Slice val;
      EXPECT_TRUE(RowCodec::DecodeColumn(row, 0, &val));
      int v = 0;
      std::memcpy(&v, val.GetData(), sizeof(int));
      EXPECT_EQ(v, i * 10) << "value mismatch for key=" << i;
    }
  }
}

// 删除不存在的 key 不应导致错误
TEST(LSMTreeTest, RemoveNonExistentKey) {
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

  // 删除不存在的 key 应该正常返回（写入 tombstone）
  EXPECT_TRUE(lsm.Remove(Slice{999}).ok());

  // 查询不存在的 key
  Slice row;
  EXPECT_FALSE(lsm.GetValue(Slice{999}, &row).ok());

  // 确保不影响正常写入
  std::string r;
  RowCodec::AppendValue(r, ValueType::Type::Int, "42");
  EXPECT_TRUE(lsm.Insert(Slice{1}, Slice{r}).ok());
  EXPECT_TRUE(lsm.GetValue(Slice{1}, &row).ok());
}
