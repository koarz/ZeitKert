#include "common/Logger.hpp"
#include "common/ResultSet.hpp"
#include "common/ZeitKert.hpp"

#include <benchmark/benchmark.h>
#include <filesystem>
#include <memory>
#include <string>

using namespace DB;

static constexpr int kBulkRows = 1'000'000;

struct SqlBenchFixture {
  std::unique_ptr<ZeitKert> db;

  SqlBenchFixture() {
    Logger::Init("./logs/bench.log");
    db = std::make_unique<ZeitKert>();

    Exec("CREATE DATABASE bench_db;");
    Exec("USE bench_db;");
    Exec("CREATE TABLE bench_t (id INT, val DOUBLE);");

    // Bulk insert data
    for (int i = 0; i < kBulkRows; i += 1000) {
      int end = std::min(i + 1000, kBulkRows);
      for (int j = i; j < end; ++j) {
        std::string sql = "INSERT INTO bench_t VALUES (" + std::to_string(j) +
                          ", " + std::to_string(j * 1.1) + ");";
        Exec(sql);
      }
    }
    Exec("FLUSH bench_t;");
  }

  ~SqlBenchFixture() {
    db.reset();
    std::filesystem::remove_all(".ZeitKert");
    Logger::Shutdown();
  }

  void Exec(std::string sql) {
    ResultSet rs;
    auto s = db->ExecuteQuery(sql, rs);
    if (!s.ok()) {
      // Silently ignore for benchmark setup
    }
  }
};

static SqlBenchFixture &GetSqlFixture() {
  static SqlBenchFixture fixture;
  return fixture;
}

static void BM_SqlSelectSum(benchmark::State &state) {
  auto &fix = GetSqlFixture();
  for (auto _ : state) {
    std::string sql = "SELECT SUM(val) FROM bench_t;";
    ResultSet rs;
    auto s = fix.db->ExecuteQuery(sql, rs);
    benchmark::DoNotOptimize(s);
    benchmark::DoNotOptimize(rs);
  }
}

static void BM_SqlSelectSimdSum(benchmark::State &state) {
  auto &fix = GetSqlFixture();
  for (auto _ : state) {
    std::string sql = "SELECT SIMD_SUM(val) FROM bench_t;";
    ResultSet rs;
    auto s = fix.db->ExecuteQuery(sql, rs);
    benchmark::DoNotOptimize(s);
    benchmark::DoNotOptimize(rs);
  }
}

static void BM_SqlInsert(benchmark::State &state) {
  auto &fix = GetSqlFixture();
  int key = kBulkRows;
  for (auto _ : state) {
    std::string sql = "INSERT INTO bench_t VALUES (" + std::to_string(key) +
                      ", " + std::to_string(key * 2.2) + ");";
    ResultSet rs;
    auto s = fix.db->ExecuteQuery(sql, rs);
    benchmark::DoNotOptimize(s);
    ++key;
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_SqlSelectSum);
BENCHMARK(BM_SqlSelectSimdSum);
BENCHMARK(BM_SqlInsert);
