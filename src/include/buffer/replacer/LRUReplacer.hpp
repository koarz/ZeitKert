#pragma once

#include "buffer/Replacer.hpp"
#include "common/Config.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace DB {
class LRUReplacer final : public Replacer {
  struct Node {
    uint64_t pin_count_{};
    timestamp_t time_stamp_{};
  };
  const size_t frame_num_;
  Node *list_;
  std::atomic<timestamp_t> curr_timestamp_{};

public:
  LRUReplacer(size_t frame_num);

  LRUReplacer(const LRUReplacer &) = delete;
  LRUReplacer(LRUReplacer &&) = delete;

  ~LRUReplacer() override;

  void Evict(frame_id_t *frame_id) override;

  void Access(frame_id_t frame_id) override;

  void SetEvictable(frame_id_t frame_id, bool set_evictable) override;

  bool IsEvictable(frame_id_t frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void UnPin(frame_id_t frame_id) override;

  uint64_t GetPinCount(frame_id_t frame_id) override;
};
} // namespace DB