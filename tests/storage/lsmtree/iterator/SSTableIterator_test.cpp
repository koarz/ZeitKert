#include "buffer/BufferPoolManager.hpp"
#include "storage/lsmtree/builder/SSTableBuilder.hpp"
#include "storage/lsmtree/iterator/SSTableIterator.hpp"

#include <gtest/gtest.h>
#include <memory>

TEST(SSTableIteratorTest, BasicTest) {
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
  auto path = column / "0.sst";
  auto dm = std::make_shared<DiskManager>();
  auto bpm = std::make_shared<BufferPoolManager>(128, dm);
  SSTableIterator iter(path, sstable_meta->offsets_, bpm);
  SliceCompare cmp;
  int i{};
  while (iter.Valid()) {
    EXPECT_TRUE(cmp(iter.GetKey(), Slice{strs[i++]}) == 0);
    iter.Next();
  }
}