#include "storage/lsmtree/BloomFilter.hpp"
#include "storage/lsmtree/Slice.hpp"
#include "storage/lsmtree/builder/BloomFilterBuilder.hpp"

#include <benchmark/benchmark.h>
#include <string>

using namespace DB;

static void BM_BloomFilterBuild(benchmark::State &state) {
  auto n = static_cast<size_t>(state.range(0));
  for (auto _ : state) {
    BloomFilterBuilder builder(n);
    for (size_t i = 0; i < n; ++i) {
      std::string key = "key_" + std::to_string(i);
      builder.AddKey(Slice{key});
    }
    benchmark::DoNotOptimize(builder.GetData());
  }
  state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(n));
}

static void BM_BloomFilterHit(benchmark::State &state) {
  auto n = static_cast<size_t>(state.range(0));

  // Setup: build the filter
  BloomFilterBuilder builder(n);
  std::vector<std::string> keys(n);
  for (size_t i = 0; i < n; ++i) {
    keys[i] = "key_" + std::to_string(i);
    builder.AddKey(Slice{keys[i]});
  }
  auto &data = builder.GetData();
  BloomFilter filter(data);

  size_t idx = 0;
  for (auto _ : state) {
    bool hit = filter.MayContain(Slice{keys[idx]});
    benchmark::DoNotOptimize(hit);
    idx = (idx + 1) % n;
  }
  state.SetItemsProcessed(state.iterations());
}

static void BM_BloomFilterMiss(benchmark::State &state) {
  auto n = static_cast<size_t>(state.range(0));

  // Setup: build the filter with "key_*" prefix
  BloomFilterBuilder builder(n);
  for (size_t i = 0; i < n; ++i) {
    std::string key = "key_" + std::to_string(i);
    builder.AddKey(Slice{key});
  }
  auto &data = builder.GetData();
  BloomFilter filter(data);

  // Query with "miss_*" prefix — guaranteed not inserted
  size_t idx = 0;
  for (auto _ : state) {
    std::string miss_key = "miss_" + std::to_string(idx);
    bool hit = filter.MayContain(Slice{miss_key});
    benchmark::DoNotOptimize(hit);
    idx = (idx + 1) % (n * 2);
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_BloomFilterBuild)
    ->Arg(1000)
    ->Arg(10000)
    ->Arg(100000)
    ->Arg(1000000);
BENCHMARK(BM_BloomFilterHit)
    ->Arg(1000)
    ->Arg(10000)
    ->Arg(100000)
    ->Arg(1000000);
BENCHMARK(BM_BloomFilterMiss)
    ->Arg(1000)
    ->Arg(10000)
    ->Arg(100000)
    ->Arg(1000000);
