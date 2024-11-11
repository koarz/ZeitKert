#include "buffer/BufferPoolManager.hpp"
#include "common/Config.hpp"
#include "storage/disk/DiskManager.hpp"

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>

void RandomData(uint8_t *data, size_t size);

TEST(BufferPoolManagerTest, BasicTest) {
  using namespace DB;
  auto dm = std::make_shared<DiskManager>();
  BufferPoolManager bpm(1, dm);
  std::filesystem::path col1{"col1"}, col2{"col2"};
  std::unique_ptr<int, std::function<void(int *)>> p(new int(0), [&](int *ptr) {
    std::filesystem::remove(col1);
    std::filesystem::remove(col2);
    delete ptr;
  });
  std::vector<uint8_t[DEFAULT_PAGE_SIZE]> datas(2);
  for (auto &data : datas) {
    RandomData(data, DEFAULT_PAGE_SIZE);
  }
  Page *page;

  EXPECT_TRUE(bpm.FetchPage(col1, 0, page).ok());
  memcpy(page->GetData(), datas[0], DEFAULT_PAGE_SIZE);
  page->SetDirty(true);
  EXPECT_TRUE(bpm.UnPinPage(page->GetPath(), page->GetPageId()).ok());

  EXPECT_TRUE(bpm.FetchPage(col2, 0, page).ok());
  memcpy(page->GetData(), datas[1], DEFAULT_PAGE_SIZE);
  page->SetDirty(true);
  EXPECT_TRUE(bpm.UnPinPage(page->GetPath(), page->GetPageId()).ok());

  EXPECT_TRUE(bpm.FetchPage(col1, 0, page).ok());
  EXPECT_EQ(0, memcmp(page->GetData(), datas[0], DEFAULT_PAGE_SIZE));
  EXPECT_TRUE(bpm.UnPinPage(page->GetPath(), page->GetPageId()).ok());

  EXPECT_TRUE(bpm.FetchPage(col2, 0, page).ok());
  EXPECT_EQ(0, memcmp(page->GetData(), datas[1], DEFAULT_PAGE_SIZE));
  EXPECT_TRUE(bpm.UnPinPage(page->GetPath(), page->GetPageId()).ok());
}