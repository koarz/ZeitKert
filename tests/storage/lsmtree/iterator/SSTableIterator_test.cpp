#include "buffer/BufferPoolManager.hpp"
#include "storage/lsmtree/RowCodec.hpp"
#include "storage/lsmtree/TableOperator.hpp"
#include "storage/lsmtree/builder/SSTableBuilder.hpp"
#include "storage/lsmtree/iterator/SSTableIterator.hpp"
#include "type/Int.hpp"

#include <cstring>
#include <filesystem>
#include <functional>
#include <gtest/gtest.h>
#include <memory>

TEST(SSTableIteratorTest, BasicTest) {
  using namespace DB;
  std::filesystem::path column{"sstable_test_file"};
  std::unique_ptr<int, std::function<void(int *)>> defer(
      new int(0), [&](int *t) {
        delete t;
        std::filesystem::remove_all(column);
        std::filesystem::remove(column.string() + ".wal");
      });
  auto value_type = std::make_shared<Int>();
  std::vector<std::shared_ptr<ValueType>> types{
      std::static_pointer_cast<ValueType>(value_type)};
  SSTableBuilder builder(column, 0, types, 0);
  for (int i = 0; i < 400; i++) {
    std::string row;
    RowCodec::AppendValue(row, ValueType::Type::Int, std::to_string(i));
    EXPECT_TRUE(builder.Add(Slice{i}, Slice{row}));
  }
  EXPECT_TRUE(builder.Finish().ok());
  auto sstable_meta = builder.BuildSSTableMeta();
  auto dm = std::make_shared<DiskManager>();
  auto bpm = std::make_shared<BufferPoolManager>(128, dm);
  auto temp = std::make_shared<SSTable>();
  temp->sstable_id_ = 0;
  EXPECT_TRUE(TableOperator::ReadSSTable(column, temp, types, bpm).ok());
  SSTableIterator iter(temp, types);
  for (int i = 0; i < 400; i++) {
    EXPECT_TRUE(iter.Valid());
    Slice key = iter.GetKey();
    int v = 0;
    std::memcpy(&v, key.GetData(), sizeof(int));
    EXPECT_EQ(v, i);
    iter.Next();
  }
  EXPECT_FALSE(iter.Valid());
}
