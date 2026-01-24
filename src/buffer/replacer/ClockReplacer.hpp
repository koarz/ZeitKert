#pragma once

#include "buffer/Replacer.hpp"
#include "common/Config.hpp"

#include <vector>

namespace DB {

class ClockReplacer final : public Replacer {
public:
  explicit ClockReplacer(size_t frame_num);
  ~ClockReplacer() override = default;

  ClockReplacer(const ClockReplacer &) = delete;
  ClockReplacer(ClockReplacer &&) = delete;

  void Evict(frame_id_t *frame_id) override;
  void Pin(frame_id_t frame_id) override;
  void UnPin(frame_id_t frame_id) override;

  void Access(frame_id_t frame_id) override;

  uint64_t GetPinCount(frame_id_t frame_id) override;
  size_t Size();

  void SetEvictable(frame_id_t frame_id, bool set_evictable) override {}
  bool IsEvictable(frame_id_t frame_id) override;

private:
  const size_t frame_num_;

  size_t clock_hand_{0};

  size_t size_{0};

  std::vector<uint64_t> pin_counts_;

  std::vector<uint8_t> ref_bits_;
};

} // namespace DB