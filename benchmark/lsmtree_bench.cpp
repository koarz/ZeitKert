#include "buffer/BufferPoolManager.hpp"
#include "storage/column/Column.hpp"
#include "storage/column/ColumnVector.hpp"
#include "storage/disk/DiskManager.hpp"
#include "storage/lsmtree/LSMTree.hpp"
#include "storage/lsmtree/RowCodec.hpp"
#include "storage/lsmtree/Slice.hpp"
#include "type/Double.hpp"
#include "type/Int.hpp"
#include "type/ValueType.hpp"

#include <benchmark/benchmark.h>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

using namespace DB;

// Helper to create a unique temp directory per benchmark iteration
struct LSMBenchFixture {
  std::filesystem::path path;
  std::shared_ptr<DiskManager> dm;
  std::shared_ptr<BufferPoolManager> bpm;
  std::vector<std::shared_ptr<ValueType>> types;
  std::unique_ptr<LSMTree> lsm;

  LSMBenchFixture(const std::string &name) {
    path = "bench_lsm_" + name + "_" +
           std::to_string(std::hash<std::thread::id>{}(
               std::this_thread::get_id()));
    std::filesystem::remove_all(path);
    dm = std::make_shared<DiskManager>();
    bpm = std::make_shared<BufferPoolManager>(64, dm);
    types = {std::make_shared<Int>(), std::make_shared<Double>()};
    lsm = std::make_unique<LSMTree>(path, bpm, types, 0, false);
  }

  ~LSMBenchFixture() {
    lsm.reset();
    std::filesystem::remove_all(path);
    std::filesystem::remove(path.string() + ".wal");
  }

  // Build a row: int key column + double value column
  static std::pair<Slice, Slice> MakeRow(int key, double val) {
    std::string row;
    RowCodec::AppendInt(row, key);
    RowCodec::AppendDouble(row, val);
    return {Slice{key}, Slice{row}};
  }
};

static void BM_Insert(benchmark::State &state) {
  LSMBenchFixture fix("insert");
  int key = 0;
  for (auto _ : state) {
    auto [k, v] = LSMBenchFixture::MakeRow(key, key * 1.1);
    auto s = fix.lsm->Insert(k, v);
    benchmark::DoNotOptimize(s);
    ++key;
  }
  state.SetItemsProcessed(state.iterations());
}

static void BM_BatchInsert(benchmark::State &state) {
  auto batch_size = static_cast<size_t>(state.range(0));
  LSMBenchFixture fix("batch");
  int key = 0;
  for (auto _ : state) {
    std::vector<std::pair<Slice, Slice>> entries;
    entries.reserve(batch_size);
    for (size_t i = 0; i < batch_size; ++i) {
      entries.push_back(LSMBenchFixture::MakeRow(key, key * 1.1));
      ++key;
    }
    auto s = fix.lsm->BatchInsert(entries);
    benchmark::DoNotOptimize(s);
  }
  state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(batch_size));
}

static void BM_FlushToSST(benchmark::State &state) {
  auto num_rows = static_cast<int>(state.range(0));
  for (auto _ : state) {
    state.PauseTiming();
    LSMBenchFixture fix("flush");
    for (int i = 0; i < num_rows; ++i) {
      auto [k, v] = LSMBenchFixture::MakeRow(i, i * 1.1);
      (void)fix.lsm->Insert(k, v);
    }
    state.ResumeTiming();

    auto s = fix.lsm->FlushToSST();
    benchmark::DoNotOptimize(s);
  }
  state.SetItemsProcessed(state.iterations() * num_rows);
}

static void BM_GetValue(benchmark::State &state) {
  auto num_rows = static_cast<int>(state.range(0));
  LSMBenchFixture fix("get");

  // Populate and flush
  for (int i = 0; i < num_rows; ++i) {
    auto [k, v] = LSMBenchFixture::MakeRow(i, i * 1.1);
    (void)fix.lsm->Insert(k, v);
  }
  (void)fix.lsm->FlushToSST();

  int idx = 0;
  for (auto _ : state) {
    Slice result;
    auto s = fix.lsm->GetValue(Slice{idx}, &result);
    benchmark::DoNotOptimize(s);
    benchmark::DoNotOptimize(result);
    idx = (idx + 1) % num_rows;
  }
  state.SetItemsProcessed(state.iterations());
}

static void BM_ScanAfterFlush(benchmark::State &state) {
  auto num_rows = static_cast<int>(state.range(0));
  LSMBenchFixture fix("scan");

  // Populate and flush
  for (int i = 0; i < num_rows; ++i) {
    auto [k, v] = LSMBenchFixture::MakeRow(i, i * 1.1);
    (void)fix.lsm->Insert(k, v);
  }
  (void)fix.lsm->FlushToSST();

  for (auto _ : state) {
    ColumnPtr col;
    auto s = fix.lsm->ScanColumn(1, col);
    benchmark::DoNotOptimize(s);
    benchmark::DoNotOptimize(col);
  }
  state.SetItemsProcessed(state.iterations() * num_rows);
}

BENCHMARK(BM_Insert);
BENCHMARK(BM_BatchInsert)->Arg(100)->Arg(1000)->Arg(10000);
BENCHMARK(BM_FlushToSST)->Arg(1000)->Arg(10000)->Arg(100000);
BENCHMARK(BM_GetValue)->Arg(1000)->Arg(10000)->Arg(100000);
BENCHMARK(BM_ScanAfterFlush)->Arg(1000)->Arg(10000)->Arg(100000);
