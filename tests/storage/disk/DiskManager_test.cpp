#include "common/Config.hpp"
#include "common/Status.hpp"
#include "storage/disk/DiskManager.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <mutex>
#include <random>

void RandomData(uint8_t *data, size_t size) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distrib(0, 0xff);
  for (int i = 0; i < size; i++) {
    data[i] = distrib(gen);
  }
}

TEST(DiskManagerTest, DISABLED_ReadWritePageTest) {
  std::filesystem::path path{"temp"};
  std::fstream fs;
  fs.open(path,
          std::ios::binary | std::ios::in | std::ios::app | std::ios::out);
  fs.close();
  fs.open(path, std::ios::binary | std::ios::in | std::ios::out);
  DB::DiskManager dm;
  std::vector<uint8_t[DEFAULT_PAGE_SIZE]> datas(64), temps(64);
  for (auto &data : datas) {
    RandomData(data, DEFAULT_PAGE_SIZE);
  }
  std::vector<std::thread> threads;
  for (int i = 0; i < 8; i++) {
    threads.emplace_back([&, i] {
      for (int j = 0; j < 8; j++) {
        EXPECT_TRUE(dm.WritePage(fs, j + 8 * i, datas[j + 8 * i]).ok());
      }
      for (int j = 0; j < 8; j++) {
        EXPECT_TRUE(dm.ReadPage(fs, j + 8 * i, temps[j + 8 * i]).ok());
        EXPECT_EQ(
            0, memcmp(datas[j + 8 * i], temps[j + 8 * i], DEFAULT_PAGE_SIZE));
      }
    });
  }
  for (auto &t : threads) {
    t.join();
  }
  fs.close();
  std::filesystem::remove(path);
}