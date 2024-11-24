#include "storage/lsmtree/MemTable.hpp"
#include "storage/lsmtree/Slice.hpp"
#include "storage/lsmtree/iterator/Iterator.hpp"
#include "storage/lsmtree/iterator/MergeIterator.hpp"

#include <gtest/gtest.h>
#include <memory>

TEST(MergeIteratorTest, BasicTest) {
  std::vector<std::string> datas{"1", "2", "3", "4", "5", "A", "B", "C",
                                 "D", "E", "a", "b", "c", "d", "e"};
  DB::MemTable tables[3];
  EXPECT_TRUE(tables[0].Put(datas[0], datas[1]).ok());
  EXPECT_TRUE(tables[0].Put(datas[2], datas[7]).ok());
  EXPECT_TRUE(tables[0].Put(datas[4], datas[13]).ok());
  EXPECT_TRUE(tables[1].Put(datas[1], datas[6]).ok());
  EXPECT_TRUE(tables[1].Put(datas[3], datas[12]).ok());
  EXPECT_TRUE(tables[1].Put(datas[5], datas[9]).ok());
  EXPECT_TRUE(tables[2].Put(datas[0], datas[5]).ok());
  EXPECT_TRUE(tables[2].Put(datas[3], datas[11]).ok());
  std::vector<std::shared_ptr<DB::Iterator>> iters;
  for (int i = 0; i < 3; i++) {
    iters.push_back(
        std::make_shared<DB::MemTableIterator>(tables[i].MakeNewIterator()));
  }
  DB::MergeIterator iter(std::move(iters));
  DB::SliceCompare cmp;
  EXPECT_TRUE(iter.Valid());
  EXPECT_TRUE(cmp(iter.GetKey(), DB::Slice{datas[0]}) == 0);
  EXPECT_TRUE(cmp(iter.GetValue(), DB::Slice{datas[1]}) == 0);
  iter.Next();
  EXPECT_TRUE(iter.Valid());
  EXPECT_TRUE(cmp(iter.GetKey(), DB::Slice{datas[1]}) == 0);
  EXPECT_TRUE(cmp(iter.GetValue(), DB::Slice{datas[6]}) == 0);
  iter.Next();
  EXPECT_TRUE(iter.Valid());
  EXPECT_TRUE(cmp(iter.GetKey(), DB::Slice{datas[2]}) == 0);
  EXPECT_TRUE(cmp(iter.GetValue(), DB::Slice{datas[7]}) == 0);
  iter.Next();
  EXPECT_TRUE(iter.Valid());
  EXPECT_TRUE(cmp(iter.GetKey(), DB::Slice{datas[3]}) == 0);
  EXPECT_TRUE(cmp(iter.GetValue(), DB::Slice{datas[12]}) == 0);
  iter.Next();
  EXPECT_TRUE(iter.Valid());
  EXPECT_TRUE(cmp(iter.GetKey(), DB::Slice{datas[4]}) == 0);
  EXPECT_TRUE(cmp(iter.GetValue(), DB::Slice{datas[13]}) == 0);
  iter.Next();
  EXPECT_TRUE(iter.Valid());
  EXPECT_TRUE(cmp(iter.GetKey(), DB::Slice{datas[5]}) == 0);
  EXPECT_TRUE(cmp(iter.GetValue(), DB::Slice{datas[9]}) == 0);
  iter.Next();
  EXPECT_FALSE(iter.Valid());
}