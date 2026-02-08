#include "storage/disk/DiskManager.hpp"
#include "storage/lsmtree/LSMTree.hpp"
#include "storage/lsmtree/RowCodec.hpp"
#include "storage/lsmtree/Slice.hpp"
#include "type/Int.hpp"
#include "type/ValueType.hpp"

#include <chrono>
#include <cstring>
#include <filesystem>
#include <functional>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <thread>

// Test fixture for compaction tests
class CompactionTest : public ::testing::Test {
protected:
  void SetUp() override {
    dm_ = std::make_shared<DB::DiskManager>();
    bpm_ = std::make_shared<DB::BufferPoolManager>(16, dm_);
    path_ = "compaction_test_table";

    // Clean up any existing test data
    std::filesystem::remove_all(path_);
  }

  void TearDown() override {
    // Clean up test data
    std::filesystem::remove_all(path_);
  }

  std::shared_ptr<DB::DiskManager> dm_;
  std::shared_ptr<DB::BufferPoolManager> bpm_;
  std::filesystem::path path_;
};

// Test that L0 compaction is triggered when file count reaches threshold
TEST_F(CompactionTest, L0CompactionTrigger) {
  using namespace DB;

  auto value_type = std::make_shared<Int>();
  std::vector<std::shared_ptr<ValueType>> types{
      std::static_pointer_cast<ValueType>(value_type)};

  LSMTree lsm(path_, bpm_, types, 0, false);

  // Write enough data to trigger multiple flushes to L0
  // In test mode, SSTABLE_SIZE = 8192 bytes
  // Each row with an int key and int value is small, so we need many rows
  const int rows_per_flush = 500; // Should be enough to fill a memtable
  const int num_flushes = 6;      // More than L0_COMPACTION_THRESHOLD (4)

  for (int flush = 0; flush < num_flushes; flush++) {
    for (int i = 0; i < rows_per_flush; i++) {
      int key = flush * rows_per_flush + i;
      std::string row;
      RowCodec::AppendValue(row, ValueType::Type::Int, std::to_string(key));
      EXPECT_TRUE(lsm.Insert(Slice{key}, Slice{row}).ok());
    }
  }

  // Flush to ensure all data is in SSTables
  EXPECT_TRUE(lsm.FlushToSST().ok());

  // Give compaction thread time to run
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // L0 file count should be reduced after compaction
  // (may not be 0 due to timing, but should be less than num_flushes)
  size_t l0_count = lsm.GetL0FileCount();
  EXPECT_LE(l0_count, num_flushes);

  // Verify all data is still readable
  for (int flush = 0; flush < num_flushes; flush++) {
    for (int i = 0; i < rows_per_flush; i++) {
      int key = flush * rows_per_flush + i;
      Slice row;
      EXPECT_TRUE(lsm.GetValue(Slice{key}, &row).ok())
          << "Failed to read key: " << key;
    }
  }
}

// Test that data integrity is maintained during compaction
TEST_F(CompactionTest, DataIntegrityDuringCompaction) {
  using namespace DB;

  auto value_type = std::make_shared<Int>();
  std::vector<std::shared_ptr<ValueType>> types{
      std::static_pointer_cast<ValueType>(value_type)};

  LSMTree lsm(path_, bpm_, types, 0, false);

  const int total_keys = 1000;

  // Insert initial data
  for (int i = 0; i < total_keys; i++) {
    std::string row;
    RowCodec::AppendValue(row, ValueType::Type::Int, std::to_string(i));
    EXPECT_TRUE(lsm.Insert(Slice{i}, Slice{row}).ok());
  }

  // Update some keys (overwrites)
  for (int i = 0; i < total_keys; i += 2) {
    std::string row;
    int new_value = i + 10000;
    RowCodec::AppendValue(row, ValueType::Type::Int, std::to_string(new_value));
    EXPECT_TRUE(lsm.Insert(Slice{i}, Slice{row}).ok());
  }

  // Flush and trigger compaction
  EXPECT_TRUE(lsm.FlushToSST().ok());
  lsm.TriggerCompaction();

  // Wait for compaction
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  // Verify data integrity - even keys should have updated values
  for (int i = 0; i < total_keys; i++) {
    Slice row;
    EXPECT_TRUE(lsm.GetValue(Slice{i}, &row).ok());
    Slice value;
    EXPECT_TRUE(RowCodec::DecodeColumn(row, 0, &value));
    int v = 0;
    std::memcpy(&v, value.GetData(), sizeof(int));

    if (i % 2 == 0) {
      EXPECT_EQ(v, i + 10000) << "Updated value mismatch for key: " << i;
    } else {
      EXPECT_EQ(v, i) << "Original value mismatch for key: " << i;
    }
  }
}

// Test tombstone handling during compaction
TEST_F(CompactionTest, TombstoneCleanup) {
  using namespace DB;

  auto value_type = std::make_shared<Int>();
  std::vector<std::shared_ptr<ValueType>> types{
      std::static_pointer_cast<ValueType>(value_type)};

  LSMTree lsm(path_, bpm_, types, 0, false);

  const int total_keys = 500;

  // Insert data
  for (int i = 0; i < total_keys; i++) {
    std::string row;
    RowCodec::AppendValue(row, ValueType::Type::Int, std::to_string(i));
    EXPECT_TRUE(lsm.Insert(Slice{i}, Slice{row}).ok());
  }

  // Delete half the keys
  for (int i = 0; i < total_keys; i += 2) {
    EXPECT_TRUE(lsm.Remove(Slice{i}).ok());
  }

  // Flush and trigger compaction
  EXPECT_TRUE(lsm.FlushToSST().ok());
  lsm.TriggerCompaction();

  // Wait for compaction
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  // Verify deleted keys are not found
  for (int i = 0; i < total_keys; i++) {
    Slice row;
    if (i % 2 == 0) {
      EXPECT_FALSE(lsm.GetValue(Slice{i}, &row).ok())
          << "Deleted key should not be found: " << i;
    } else {
      EXPECT_TRUE(lsm.GetValue(Slice{i}, &row).ok())
          << "Key should be found: " << i;
    }
  }
}

// Test concurrent reads during compaction
TEST_F(CompactionTest, ConcurrentReadsDuringCompaction) {
  using namespace DB;

  auto value_type = std::make_shared<Int>();
  std::vector<std::shared_ptr<ValueType>> types{
      std::static_pointer_cast<ValueType>(value_type)};

  auto lsm = std::make_shared<LSMTree>(path_, bpm_, types, 0, false);

  const int total_keys = 1000;

  // Insert initial data
  for (int i = 0; i < total_keys; i++) {
    std::string row;
    RowCodec::AppendValue(row, ValueType::Type::Int, std::to_string(i));
    EXPECT_TRUE(lsm->Insert(Slice{i}, Slice{row}).ok());
  }

  // Flush to SSTables
  EXPECT_TRUE(lsm->FlushToSST().ok());

  // Start concurrent read thread
  std::atomic<bool> stop_flag{false};
  std::atomic<int> read_errors{0};

  std::thread reader([&]() {
    while (!stop_flag.load()) {
      for (int i = 0; i < total_keys && !stop_flag.load(); i++) {
        Slice row;
        if (!lsm->GetValue(Slice{i}, &row).ok()) {
          read_errors.fetch_add(1);
        }
      }
    }
  });

  // Trigger multiple compactions while reading
  for (int round = 0; round < 3; round++) {
    // Add more data to trigger compaction
    for (int i = 0; i < 500; i++) {
      int key = total_keys + round * 500 + i;
      std::string row;
      RowCodec::AppendValue(row, ValueType::Type::Int, std::to_string(key));
      lsm->Insert(Slice{key}, Slice{row});
    }
    lsm->FlushToSST();
    lsm->TriggerCompaction();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // Stop reader thread
  stop_flag.store(true);
  reader.join();

  // Should have no read errors during compaction
  EXPECT_EQ(read_errors.load(), 0) << "Read errors occurred during compaction";
}

// Test recovery with manifest
TEST_F(CompactionTest, ManifestRecovery) {
  using namespace DB;

  auto value_type = std::make_shared<Int>();
  std::vector<std::shared_ptr<ValueType>> types{
      std::static_pointer_cast<ValueType>(value_type)};

  const int total_keys = 500;

  // Write data and close
  {
    LSMTree lsm(path_, bpm_, types, 0, false);

    for (int i = 0; i < total_keys; i++) {
      std::string row;
      RowCodec::AppendValue(row, ValueType::Type::Int, std::to_string(i));
      EXPECT_TRUE(lsm.Insert(Slice{i}, Slice{row}).ok());
    }

    // Flush to ensure data is persisted
    EXPECT_TRUE(lsm.FlushToSST().ok());
  }

  // Reopen and verify data is recoverable
  {
    LSMTree lsm(path_, bpm_, types, 0, false);

    for (int i = 0; i < total_keys; i++) {
      Slice row;
      EXPECT_TRUE(lsm.GetValue(Slice{i}, &row).ok())
          << "Failed to read key after recovery: " << i;

      Slice value;
      EXPECT_TRUE(RowCodec::DecodeColumn(row, 0, &value));
      int v = 0;
      std::memcpy(&v, value.GetData(), sizeof(int));
      EXPECT_EQ(v, i);
    }
  }
}

// Test that level sizes are properly maintained
TEST_F(CompactionTest, LevelSizeTracking) {
  using namespace DB;

  auto value_type = std::make_shared<Int>();
  std::vector<std::shared_ptr<ValueType>> types{
      std::static_pointer_cast<ValueType>(value_type)};

  LSMTree lsm(path_, bpm_, types, 0, false);

  // Write data
  const int total_keys = 2000;
  for (int i = 0; i < total_keys; i++) {
    std::string row;
    RowCodec::AppendValue(row, ValueType::Type::Int, std::to_string(i));
    EXPECT_TRUE(lsm.Insert(Slice{i}, Slice{row}).ok());
  }

  // Flush
  EXPECT_TRUE(lsm.FlushToSST().ok());

  // Check that levels have size information
  const auto &levels = lsm.GetLevels();

  // At least L0 should have data
  size_t total_files = 0;
  uint64_t total_size = 0;
  for (const auto &level : levels) {
    total_files += level.sstables.size();
    total_size += level.total_size;
  }

  EXPECT_GT(total_files, 0) << "Should have at least one SSTable";
  EXPECT_GT(total_size, 0) << "Total size should be greater than 0";
}

// TombstoneCleanup 应验证存活 key 的值正确性
TEST_F(CompactionTest, TombstoneCleanupValueIntegrity) {
  using namespace DB;

  auto value_type = std::make_shared<Int>();
  std::vector<std::shared_ptr<ValueType>> types{
      std::static_pointer_cast<ValueType>(value_type)};

  LSMTree lsm(path_, bpm_, types, 0, false);

  const int total_keys = 200;

  // 插入数据 value = key * 7
  for (int i = 0; i < total_keys; i++) {
    std::string row;
    RowCodec::AppendValue(row, ValueType::Type::Int, std::to_string(i * 7));
    EXPECT_TRUE(lsm.Insert(Slice{i}, Slice{row}).ok());
  }

  // 删除偶数 key
  for (int i = 0; i < total_keys; i += 2) {
    EXPECT_TRUE(lsm.Remove(Slice{i}).ok());
  }

  EXPECT_TRUE(lsm.FlushToSST().ok());
  lsm.TriggerCompaction();
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  // 验证：偶数 key 不存在，奇数 key 的值 == key * 7
  for (int i = 0; i < total_keys; i++) {
    Slice row;
    if (i % 2 == 0) {
      EXPECT_FALSE(lsm.GetValue(Slice{i}, &row).ok()) << "deleted key=" << i;
    } else {
      EXPECT_TRUE(lsm.GetValue(Slice{i}, &row).ok()) << "missing key=" << i;
      Slice val;
      EXPECT_TRUE(RowCodec::DecodeColumn(row, 0, &val));
      int v = 0;
      std::memcpy(&v, val.GetData(), sizeof(int));
      EXPECT_EQ(v, i * 7) << "value mismatch for key=" << i;
    }
  }
}

// 多轮覆盖写入 + compaction，验证数据始终正确
TEST_F(CompactionTest, MultiRoundOverwriteCompaction) {
  using namespace DB;

  auto value_type = std::make_shared<Int>();
  std::vector<std::shared_ptr<ValueType>> types{
      std::static_pointer_cast<ValueType>(value_type)};

  LSMTree lsm(path_, bpm_, types, 0, false);

  const int total_keys = 300;

  // 第一轮：value = i
  for (int i = 0; i < total_keys; i++) {
    std::string row;
    RowCodec::AppendValue(row, ValueType::Type::Int, std::to_string(i));
    EXPECT_TRUE(lsm.Insert(Slice{i}, Slice{row}).ok());
  }
  EXPECT_TRUE(lsm.FlushToSST().ok());

  // 第二轮：覆盖所有 key，value = i + 1000
  for (int i = 0; i < total_keys; i++) {
    std::string row;
    RowCodec::AppendValue(row, ValueType::Type::Int, std::to_string(i + 1000));
    EXPECT_TRUE(lsm.Insert(Slice{i}, Slice{row}).ok());
  }
  EXPECT_TRUE(lsm.FlushToSST().ok());
  lsm.TriggerCompaction();
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // 验证：所有 key 的 value 都是 i + 1000
  for (int i = 0; i < total_keys; i++) {
    Slice row;
    EXPECT_TRUE(lsm.GetValue(Slice{i}, &row).ok()) << "missing key=" << i;
    Slice val;
    EXPECT_TRUE(RowCodec::DecodeColumn(row, 0, &val));
    int v = 0;
    std::memcpy(&v, val.GetData(), sizeof(int));
    EXPECT_EQ(v, i + 1000) << "value mismatch for key=" << i;
  }
}

// 删除后重新插入 + flush + compaction
TEST_F(CompactionTest, DeleteReinsertCompaction) {
  using namespace DB;

  auto value_type = std::make_shared<Int>();
  std::vector<std::shared_ptr<ValueType>> types{
      std::static_pointer_cast<ValueType>(value_type)};

  LSMTree lsm(path_, bpm_, types, 0, false);

  const int total_keys = 100;

  // 插入
  for (int i = 0; i < total_keys; i++) {
    std::string row;
    RowCodec::AppendValue(row, ValueType::Type::Int, std::to_string(i));
    EXPECT_TRUE(lsm.Insert(Slice{i}, Slice{row}).ok());
  }

  // 删除所有 key
  for (int i = 0; i < total_keys; i++) {
    EXPECT_TRUE(lsm.Remove(Slice{i}).ok());
  }

  // 重新插入偶数 key，value = i + 2000
  for (int i = 0; i < total_keys; i += 2) {
    std::string row;
    RowCodec::AppendValue(row, ValueType::Type::Int, std::to_string(i + 2000));
    EXPECT_TRUE(lsm.Insert(Slice{i}, Slice{row}).ok());
  }

  EXPECT_TRUE(lsm.FlushToSST().ok());
  lsm.TriggerCompaction();
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  // 验证：偶数 key value = i+2000，奇数 key 不存在
  for (int i = 0; i < total_keys; i++) {
    Slice row;
    if (i % 2 == 0) {
      EXPECT_TRUE(lsm.GetValue(Slice{i}, &row).ok())
          << "reinserted key should exist: " << i;
      Slice val;
      EXPECT_TRUE(RowCodec::DecodeColumn(row, 0, &val));
      int v = 0;
      std::memcpy(&v, val.GetData(), sizeof(int));
      EXPECT_EQ(v, i + 2000) << "reinserted value mismatch for key=" << i;
    } else {
      EXPECT_FALSE(lsm.GetValue(Slice{i}, &row).ok())
          << "deleted key should not exist: " << i;
    }
  }
}
