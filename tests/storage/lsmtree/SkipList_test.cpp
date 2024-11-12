#include "storage/lsmtree/SkipList.hpp"
#include "storage/lsmtree/Slice.hpp"

#include <gtest/gtest.h>

TEST(SkipListTest, SkipListWithSlice) {
  using namespace DB;
  SliceCompare cmp;
  SkipList<Slice, Slice, SliceCompare> skip_list(5, cmp);
  skip_list.Insert("10", "20");
  skip_list.Insert("17", "21");
  skip_list.Insert("16", "45");
  skip_list.Insert("33571", "2146");
  Slice value;
  EXPECT_TRUE(skip_list.Get("10", &value).ok());
  EXPECT_EQ(0, memcmp(value.GetData(), "20", value.Size()));
  EXPECT_TRUE(skip_list.Get("16", &value).ok());
  EXPECT_EQ(0, memcmp(value.GetData(), "45", value.Size()));
  EXPECT_TRUE(skip_list.Get("33571", &value).ok());
  EXPECT_EQ(0, memcmp(value.GetData(), "2146", value.Size()));
  EXPECT_TRUE(skip_list.Get("17", &value).ok());
  EXPECT_EQ(0, memcmp(value.GetData(), "21", value.Size()));
  // not found
  EXPECT_FALSE(skip_list.Get("18", &value).ok());
  auto it = skip_list.Begin();
  EXPECT_EQ(0, memcmp((*it).first.GetData(), "10", (*it).first.Size()));
  EXPECT_EQ(0, memcmp((*it).second.GetData(), "20", (*it).second.Size()));
  ++it;
  EXPECT_EQ(0, memcmp((*it).first.GetData(), "16", (*it).first.Size()));
  EXPECT_EQ(0, memcmp((*it).second.GetData(), "45", (*it).second.Size()));
  ++it;
  EXPECT_EQ(0, memcmp((*it).first.GetData(), "17", (*it).first.Size()));
  EXPECT_EQ(0, memcmp((*it).second.GetData(), "21", (*it).second.Size()));
  ++it;
  EXPECT_EQ(0, memcmp((*it).first.GetData(), "33571", (*it).first.Size()));
  EXPECT_EQ(0, memcmp((*it).second.GetData(), "2146", (*it).second.Size()));
  ++it;
  EXPECT_TRUE(it == skip_list.End());
  skip_list.Insert("10", "55");
  EXPECT_TRUE(skip_list.Get("10", &value).ok());
  EXPECT_EQ(0, memcmp(value.GetData(), "55", value.Size()));
  skip_list.Remove("10");
  skip_list.Remove("17");
  it = skip_list.Begin();
  EXPECT_EQ(0, memcmp((*it).first.GetData(), "16", (*it).first.Size()));
  EXPECT_EQ(0, memcmp((*it).second.GetData(), "45", (*it).second.Size()));
  ++it;
  EXPECT_EQ(0, memcmp((*it).first.GetData(), "33571", (*it).first.Size()));
  EXPECT_EQ(0, memcmp((*it).second.GetData(), "2146", (*it).second.Size()));
  ++it;
  EXPECT_TRUE(it == skip_list.End());
}