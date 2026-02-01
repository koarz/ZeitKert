#pragma once

#include "buffer/Replacer.hpp"
#include "common/Config.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <list>
#include <vector>

namespace DB {
class LRUReplacer final : public Replacer {
  const size_t frame_num_;

  std::vector<uint64_t> pin_counts_;

  std::list<frame_id_t> lru_list_;

  std::vector<std::list<frame_id_t>::iterator> iters_;

public:
  LRUReplacer(size_t frame_num);

  LRUReplacer(const LRUReplacer &) = delete;
  LRUReplacer(LRUReplacer &&) = delete;

  ~LRUReplacer() override {};

  void Evict(frame_id_t *frame_id) override;

  void Access(frame_id_t frame_id) override;

  void SetEvictable(frame_id_t frame_id, bool set_evictable) override;

  bool IsEvictable(frame_id_t frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void UnPin(frame_id_t frame_id) override;

  uint64_t GetPinCount(frame_id_t frame_id) override;

  size_t Size();
};
} // namespace DB