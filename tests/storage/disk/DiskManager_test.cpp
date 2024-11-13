#include "common/Config.hpp"
#include "common/Status.hpp"
#include "storage/disk/DiskManager.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <random>

void RandomData(uint8_t *data, size_t size) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distrib(0, 0xff);
  for (int i = 0; i < size; i++) {
    data[i] = distrib(gen);
  }
}

TEST(DiskManagerTest, ReadWritePageTest) {
  std::filesystem::path path{"tempfile"};
  std::fstream fs;
  fs.open(path,
          std::ios::binary | std::ios::in | std::ios::app | std::ios::out);
  DB::DiskManager dm;
  uint8_t data[DEFAULT_PAGE_SIZE], temp[DEFAULT_PAGE_SIZE];
  RandomData(data, DEFAULT_PAGE_SIZE);
  for (int i = 0; i < 64000; i++) {
    EXPECT_TRUE(dm.WritePage(fs, i, data).ok());
  }
  for (int i = 0; i < 64000; i++) {
    EXPECT_TRUE(dm.ReadPage(fs, i, temp).ok());
    for (int i = 0; i < DEFAULT_PAGE_SIZE; i++) {
      EXPECT_EQ(0, memcmp(data, temp, DEFAULT_PAGE_SIZE));
    }
  }

  fs.close();
  std::filesystem::remove(path);
}