#include "buffer/BufferPoolManager.hpp"
#include "function/FunctionSimdSum.hpp"
#include "function/FunctionSum.hpp"
#include "storage/Block.hpp"
#include "storage/column/Column.hpp"
#include "storage/column/ColumnVector.hpp"
#include "storage/column/ColumnWithNameType.hpp"
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

// ~10M rows of double data
static constexpr int kTotalRows = 10'000'000;

struct ColumnScanFixture {
  std::filesystem::path path{"bench_colscan"};
  std::shared_ptr<DiskManager> dm;
  std::shared_ptr<BufferPoolManager> bpm;
  std::unique_ptr<LSMTree> lsm;

  ColumnScanFixture() {
    std::filesystem::remove_all(path);
    dm = std::make_shared<DiskManager>();
    bpm = std::make_shared<BufferPoolManager>(64, dm);
    std::vector<std::shared_ptr<ValueType>> types = {
        std::make_shared<Int>(),    // col 0: pk
        std::make_shared<Double>(), // col 1: data
    };
    lsm = std::make_unique<LSMTree>(path, bpm, types, 0, false);

    // Bulk insert via BatchInsert
    constexpr int kBatchSize = 50000;
    for (int base = 0; base < kTotalRows; base += kBatchSize) {
      int end = std::min(base + kBatchSize, kTotalRows);
      std::vector<std::pair<Slice, Slice>> entries;
      entries.reserve(end - base);
      for (int i = base; i < end; ++i) {
        std::string row;
        RowCodec::AppendInt(row, i);
        RowCodec::AppendDouble(row, i * 1.1);
        entries.emplace_back(Slice{i}, Slice{row});
      }
      (void)lsm->BatchInsert(entries);
    }
    (void)lsm->FlushToSST();
  }

  ~ColumnScanFixture() {
    lsm.reset();
    std::filesystem::remove_all(path);
    std::filesystem::remove(path.string() + ".wal");
  }
};

// Shared fixture (constructed once, reused across benchmarks)
static ColumnScanFixture &GetFixture() {
  static ColumnScanFixture fixture;
  return fixture;
}

static void BM_ScanColumn(benchmark::State &state) {
  auto &fix = GetFixture();
  for (auto _ : state) {
    ColumnPtr col;
    auto s = fix.lsm->ScanColumn(1, col);
    benchmark::DoNotOptimize(s);
    benchmark::DoNotOptimize(col);
  }
  state.SetItemsProcessed(state.iterations() * kTotalRows);
  state.SetBytesProcessed(state.iterations() * kTotalRows *
                          static_cast<int64_t>(sizeof(double)));
}

static void BM_ScanColumns_Multi(benchmark::State &state) {
  auto &fix = GetFixture();
  for (auto _ : state) {
    std::vector<ColumnPtr> results;
    auto s = fix.lsm->ScanColumns({0, 1}, results);
    benchmark::DoNotOptimize(s);
    benchmark::DoNotOptimize(results);
  }
  state.SetItemsProcessed(state.iterations() * kTotalRows * 2);
}

static void BM_FunctionSum(benchmark::State &state) {
  auto &fix = GetFixture();

  // Scan once to get the column
  ColumnPtr col;
  (void)fix.lsm->ScanColumn(1, col);
  size_t rows = col->Size();

  // Build block: [input_column, result_column]
  auto input_type = std::make_shared<Double>();
  auto input_cwnt =
      std::make_shared<ColumnWithNameType>(col, "val", input_type);

  FunctionSum func;

  for (auto _ : state) {
    Block block;
    block.PushColumn(input_cwnt);
    // Create result column
    auto result_col = std::make_shared<ColumnVector<double>>();
    auto result_cwnt = std::make_shared<ColumnWithNameType>(
        result_col, "sum_result", std::make_shared<Double>());
    block.PushColumn(result_cwnt);

    auto s = func.ExecuteImpl(block, 1, rows);
    benchmark::DoNotOptimize(s);
    benchmark::DoNotOptimize(block);
  }
  state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(rows));
}

static void BM_FunctionSimdSum(benchmark::State &state) {
  auto &fix = GetFixture();

  // Scan once to get the column
  ColumnPtr col;
  (void)fix.lsm->ScanColumn(1, col);
  size_t rows = col->Size();

  auto input_type = std::make_shared<Double>();
  auto input_cwnt =
      std::make_shared<ColumnWithNameType>(col, "val", input_type);

  FunctionSimdSum func;

  for (auto _ : state) {
    Block block;
    block.PushColumn(input_cwnt);
    auto result_col = std::make_shared<ColumnVector<double>>();
    auto result_cwnt = std::make_shared<ColumnWithNameType>(
        result_col, "simd_sum_result", std::make_shared<Double>());
    block.PushColumn(result_cwnt);

    auto s = func.ExecuteImpl(block, 1, rows);
    benchmark::DoNotOptimize(s);
    benchmark::DoNotOptimize(block);
  }
  state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(rows));
}

BENCHMARK(BM_ScanColumn);
BENCHMARK(BM_ScanColumns_Multi);
BENCHMARK(BM_FunctionSum);
BENCHMARK(BM_FunctionSimdSum);
