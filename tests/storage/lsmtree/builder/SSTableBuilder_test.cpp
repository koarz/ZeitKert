#include "storage/lsmtree/SSTable.hpp"
#include "storage/lsmtree/Slice.hpp"
#include "storage/lsmtree/TableOperator.hpp"
#include "storage/lsmtree/builder/SSTableBuilder.hpp"

#include <filesystem>
#include <gtest/gtest.h>
#include <memory>

TEST(SSTableBuilderTest, BuildTableAndReadData) {
  using namespace DB;
  std::filesystem::path column{"sstable_test_file"};
  std::unique_ptr<int, std::function<void(int *)>> defer(
      new int(0), [&](int *t) {
        delete t;
        std::filesystem::remove_all(column);
      });
  SSTableBuilder builder(column, 0);
  std::vector<std::string> strs;
  strs.reserve(400);
  for (int i = 0; i < 400; i++) {
    strs.emplace_back(std::to_string(i));
    Slice k(strs[i]);
    EXPECT_TRUE(builder.Add(k, k));
  }
  EXPECT_TRUE(builder.Finish().ok());
  auto sstable_meta = builder.BuildSSTableMeta();
  auto temp = std::make_shared<SSTable>();
  temp->sstable_id_ = 0;
  EXPECT_TRUE(TableOperator::ReadSSTable(column, temp).ok());
  EXPECT_EQ(sstable_meta->num_of_blocks_, temp->num_of_blocks_);
  int i = 0;
  for (auto &v : temp->offsets_) {
    EXPECT_EQ(v, sstable_meta->offsets_[i]);
    i++;
  }
  i = 0;
  SliceCompare cmp;
  for (auto &v : temp->index_) {
    EXPECT_TRUE(cmp(v, sstable_meta->index_[i]));
    i++;
  }
}