#include "buffer/BufferPoolManager.hpp"
#include "common/Status.hpp"
#include "fmt/std.h"

#include <cstdint>
#include <functional>
#include <random>
#include <thread>
#include <vector>

// from bustub bpm_bench.cpp
uint64_t ClockMs() {
  struct timeval tm;
  gettimeofday(&tm, nullptr);
  return static_cast<uint64_t>(tm.tv_sec * 1000) +
         static_cast<uint64_t>(tm.tv_usec / 1000);
}

constexpr uint64_t BPM_SIZE = 64;
constexpr uint64_t GET_THREAD = 8;
constexpr uint64_t SCAN_THREAD = 8;
constexpr uint64_t BPM_PAGE_CNT = 64000;

// from bustub  bpm_bench.cpp
struct BpmTotalMetrics {
  uint64_t scan_cnt_{0};
  uint64_t get_cnt_{0};
  uint64_t start_time_{0};
  std::mutex mutex_;

  void Begin() { start_time_ = ClockMs(); }

  void ReportScan(uint64_t scan_cnt) {
    std::unique_lock<std::mutex> l(mutex_);
    scan_cnt_ += scan_cnt;
  }

  void ReportGet(uint64_t get_cnt) {
    std::unique_lock<std::mutex> l(mutex_);
    get_cnt_ += get_cnt;
  }

  void Report() {
    auto now = ClockMs();
    auto elsped = now - start_time_;
    auto scan_per_sec = scan_cnt_ / static_cast<double>(elsped) * 1000;
    auto get_per_sec = get_cnt_ / static_cast<double>(elsped) * 1000;

    fmt::print("<<< BEGIN\n");
    fmt::print("scan: {}\n", scan_per_sec);
    fmt::print("get: {}\n", get_per_sec);
    fmt::print(">>> END\n");
  }
};

struct BpmMetrics {
  uint64_t start_time_{0};
  uint64_t last_report_at_{0};
  uint64_t last_cnt_{0};
  uint64_t cnt_{0};
  std::string reporter_;
  uint64_t duration_ms_;

  explicit BpmMetrics(std::string reporter, uint64_t duration_ms)
      : reporter_(std::move(reporter)), duration_ms_(duration_ms) {}

  void Tick() { cnt_ += 1; }

  void Begin() { start_time_ = ClockMs(); }

  void Report() {
    auto now = ClockMs();
    auto elsped = now - start_time_;
    if (elsped - last_report_at_ > 1000) {
      fmt::print(stderr,
                 "[{:5.2f}] {}: total_cnt={:<10} throughput={:<10.3f} "
                 "avg_throughput={:<10.3f}\n",
                 elsped / 1000.0, reporter_, cnt_,
                 (cnt_ - last_cnt_) /
                     static_cast<double>(elsped - last_report_at_) * 1000,
                 cnt_ / static_cast<double>(elsped) * 1000);
      last_report_at_ = elsped;
      last_cnt_ = cnt_;
    }
  }

  auto ShouldFinish() -> bool {
    auto now = ClockMs();
    return now - start_time_ > duration_ms_;
  }
};

int main() {
  using namespace DB;

  uint64_t duration_ms = 30000;

  auto dm = std::make_shared<DiskManager>();
  BufferPoolManager bpm(BPM_SIZE, dm);
  std::filesystem::path db{"db"};
  std::unique_ptr<int, std::function<void(int *)>> p(new int(0), [&](int *ptr) {
    std::filesystem::remove(db);
    delete ptr;
  });

  std::vector<page_id_t> page_ids;
  fmt::print(stderr, "[info] total_page={}, duration_ms={}, bpm_size={}\n",
             BPM_PAGE_CNT, duration_ms, BPM_SIZE);

  for (size_t i = 0; i < BPM_PAGE_CNT; i++) {
    page_id_t page_id;
    Page *page;
    Status status;
    status = bpm.FetchPage(db, i, page);
    if (!status.ok()) {
      std::cerr << status.GetMessage() << '\n';
    }
    uint8_t &ch = page->GetData()[i % 1024];
    ch = 1;
    page->SetDirty(true);

    status = bpm.UnPinPage(db, i);
    if (!status.ok()) {
      std::cerr << status.GetMessage() << '\n';
    }
    page_ids.push_back(i);
  }
  fmt::print(stderr, "[info] benchmark start\n");
  BpmTotalMetrics total_metrics;
  total_metrics.Begin();

  std::vector<std::thread> threads;
  for (size_t thread_id = 0; thread_id < SCAN_THREAD; thread_id++) {
    threads.emplace_back(std::thread(
        [&db, thread_id, &page_ids, &bpm, duration_ms, &total_metrics] {
          BpmMetrics metrics(fmt::format("scan {:>2}", thread_id), duration_ms);
          metrics.Begin();

          size_t page_idx = BPM_PAGE_CNT * thread_id / SCAN_THREAD;

          while (!metrics.ShouldFinish()) {
            Page *page = nullptr;
            Status status;
            status = bpm.FetchPage(db, page_idx, page);

            if (page == nullptr) {
              std::cerr << "null\n";
              continue;
            }

            uint8_t &ch = page->GetData()[page_idx % 1024];
            auto latch = page->GetWriteLock();
            page->SetDirty(true);
            ch += 1;
            if (ch == 0) {
              ch = 1;
            }
            latch.unlock();

            status = bpm.UnPinPage(db, page->GetPageId());
            if (!status.ok()) {
              std::cerr << status.GetMessage() << '\n';
            }
            page_idx = (page_idx + 1) % BPM_PAGE_CNT;
            metrics.Tick();
            metrics.Report();
          }

          total_metrics.ReportScan(metrics.cnt_);
        }));
  }

  for (size_t thread_id = 0; thread_id < GET_THREAD; thread_id++) {
    threads.emplace_back(std::thread(
        [&db, thread_id, &page_ids, &bpm, duration_ms, &total_metrics] {
          std::random_device r;
          std::default_random_engine gen(r());
          std::normal_distribution dist{BPM_PAGE_CNT / 2.0, 0.8};

          BpmMetrics metrics(fmt::format("get  {:>2}", thread_id), duration_ms);
          metrics.Begin();

          while (!metrics.ShouldFinish()) {
            int page_idx = dist(gen);
            Page *page = nullptr;
            Status status;
            status = bpm.FetchPage(db, page_idx, page);
            if (page == nullptr) {
              continue;
            }

            auto latch = page->GetReadLock();
            uint8_t ch = page->GetData()[page_idx % 1024];
            latch.unlock();
            if (ch == 0) {
              throw std::runtime_error("invalid data");
            }

            status = bpm.UnPinPage(db, page_idx);
            metrics.Tick();
            metrics.Report();
          }

          total_metrics.ReportGet(metrics.cnt_);
        }));
  }

  for (auto &thread : threads) {
    thread.join();
  }

  total_metrics.Report();
  return 0;
}