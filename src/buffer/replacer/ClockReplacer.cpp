#include "buffer/replacer/ClockReplacer.hpp"

namespace DB {

ClockReplacer::ClockReplacer(size_t frame_num) : frame_num_(frame_num) {
  pin_counts_.resize(frame_num, 0);
  ref_bits_.resize(frame_num, 0);
  size_ = frame_num;
}

void ClockReplacer::Evict(frame_id_t *frame_id) {
  if (size_ == 0) {
    *frame_id = -1;
    return;
  }

  while (true) {
    if (pin_counts_[clock_hand_] > 0) {
      clock_hand_ = (clock_hand_ + 1) % frame_num_;
      continue;
    }

    if (ref_bits_[clock_hand_] == 1) {
      ref_bits_[clock_hand_] = 0;
      clock_hand_ = (clock_hand_ + 1) % frame_num_;
    } else {
      *frame_id = clock_hand_;
      clock_hand_ = (clock_hand_ + 1) % frame_num_;
      return;
    }
  }
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  if (frame_id >= frame_num_) {
    return;
  }
  if (pin_counts_[frame_id] == 0) {
    size_--;
  }
  pin_counts_[frame_id]++;
  ref_bits_[frame_id] = 1;
}

void ClockReplacer::UnPin(frame_id_t frame_id) {
  if (frame_id >= frame_num_) {
    return;
  }
  if (pin_counts_[frame_id] == 0) {
    return;
  }
  pin_counts_[frame_id]--;
  if (pin_counts_[frame_id] == 0) {
    size_++;
    ref_bits_[frame_id] = 1;
  }
}

void ClockReplacer::Access(frame_id_t frame_id) {
  Pin(frame_id);
}

size_t ClockReplacer::Size() {
  return size_;
}

uint64_t ClockReplacer::GetPinCount(frame_id_t frame_id) {
  return pin_counts_[frame_id];
}

bool ClockReplacer::IsEvictable(frame_id_t frame_id) {
  return pin_counts_[frame_id] == 0;
}

} // namespace DB
