#include "buffer/BufferPoolManager.hpp"
#include "common/Config.hpp"
#include "storage/disk/DiskManager.hpp"

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <thread>

void RandomData(Byte *data, size_t size);

TEST(BufferPoolManagerTest, BasicTest) {
  using namespace DB;
  auto dm = std::make_shared<DiskManager>();
  BufferPoolManager bpm(4, dm);
  std::filesystem::path col1{"col1"}, col2{"col2"};
  std::unique_ptr<int, std::function<void(int *)>> p(new int(0), [&](int *ptr) {
    std::filesystem::remove(col1);
    std::filesystem::remove(col2);
    delete ptr;
  });
  std::vector<Byte[DEFAULT_PAGE_SIZE]> datas(16);
  for (auto &data : datas) {
    RandomData(data, DEFAULT_PAGE_SIZE);
  }
  Page *page;
  for (int i = 0; i < 8; i++) {
    EXPECT_TRUE(bpm.FetchPage(col1, i, page).ok());
    EXPECT_EQ(i, page->GetPageId());
    memcpy(page->GetData(), datas[i], DEFAULT_PAGE_SIZE);
    page->SetDirty(true);
    EXPECT_EQ(0, memcmp(page->GetData(), datas[i], DEFAULT_PAGE_SIZE));
    EXPECT_TRUE(bpm.UnPinPage(page->GetPath(), page->GetPageId()).ok());
  }
  for (int i = 0; i < 8; i++) {
    EXPECT_TRUE(bpm.FetchPage(col1, i, page).ok());
    EXPECT_EQ(0, memcmp(page->GetData(), datas[i], DEFAULT_PAGE_SIZE));
    EXPECT_TRUE(bpm.UnPinPage(page->GetPath(), page->GetPageId()).ok());
  }
  for (int i = 8; i < 16; i++) {
    EXPECT_TRUE(bpm.FetchPage(col2, i - 8, page).ok());
    EXPECT_EQ(i - 8, page->GetPageId());
    memcpy(page->GetData(), datas[i], DEFAULT_PAGE_SIZE);
    page->SetDirty(true);
    EXPECT_EQ(0, memcmp(page->GetData(), datas[i], DEFAULT_PAGE_SIZE));
    EXPECT_TRUE(bpm.UnPinPage(page->GetPath(), page->GetPageId()).ok());
  }
  for (int i = 8; i < 16; i++) {
    EXPECT_TRUE(bpm.FetchPage(col2, i - 8, page).ok());
    EXPECT_EQ(0, memcmp(page->GetData(), datas[i], DEFAULT_PAGE_SIZE));
    EXPECT_TRUE(bpm.UnPinPage(page->GetPath(), page->GetPageId()).ok());
  }
}

TEST(BufferPoolManagerTest, MultyThreadTest) {
  using namespace DB;
  auto dm = std::make_shared<DiskManager>();
  BufferPoolManager bpm(4, dm);
  std::filesystem::path col1{"col1"}, col2{"col2"};
  std::unique_ptr<int, std::function<void(int *)>> p(new int(0), [&](int *ptr) {
    std::filesystem::remove(col1);
    std::filesystem::remove(col2);
    delete ptr;
  });
  std::vector<Byte[DEFAULT_PAGE_SIZE]> datas(16);
  for (auto &data : datas) {
    RandomData(data, DEFAULT_PAGE_SIZE);
  }
  std::vector<std::thread> threads;
  threads.emplace_back([&] {
    Page *page;
    for (int i = 0; i < 8; i++) {
      EXPECT_TRUE(bpm.FetchPage(col1, i, page).ok());
      EXPECT_EQ(i, page->GetPageId());
      memcpy(page->GetData(), datas[i], DEFAULT_PAGE_SIZE);
      page->SetDirty(true);
      EXPECT_EQ(0, memcmp(page->GetData(), datas[i], DEFAULT_PAGE_SIZE));
      EXPECT_TRUE(bpm.UnPinPage(page->GetPath(), page->GetPageId()).ok());
    }
    for (int i = 0; i < 8; i++) {
      EXPECT_TRUE(bpm.FetchPage(col1, i, page).ok());
      EXPECT_EQ(0, memcmp(page->GetData(), datas[i], DEFAULT_PAGE_SIZE));
      EXPECT_TRUE(bpm.UnPinPage(page->GetPath(), page->GetPageId()).ok());
    }
  });
  threads.emplace_back([&] {
    Page *page;
    for (int i = 8; i < 16; i++) {
      EXPECT_TRUE(bpm.FetchPage(col2, i - 8, page).ok());
      EXPECT_EQ(i - 8, page->GetPageId());
      memcpy(page->GetData(), datas[i], DEFAULT_PAGE_SIZE);
      page->SetDirty(true);
      EXPECT_EQ(0, memcmp(page->GetData(), datas[i], DEFAULT_PAGE_SIZE));
      EXPECT_TRUE(bpm.UnPinPage(page->GetPath(), page->GetPageId()).ok());
    }
    for (int i = 8; i < 16; i++) {
      EXPECT_TRUE(bpm.FetchPage(col2, i - 8, page).ok());
      EXPECT_EQ(0, memcmp(page->GetData(), datas[i], DEFAULT_PAGE_SIZE));
      EXPECT_TRUE(bpm.UnPinPage(page->GetPath(), page->GetPageId()).ok());
    }
  });
  for (auto &t : threads) {
    t.join();
  }
}