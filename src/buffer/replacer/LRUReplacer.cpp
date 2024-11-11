#include "buffer/replacer/LRUReplacer.hpp"
#include "common/Config.hpp"

#include <cstddef>
#include <cstdint>

namespace DB {
LRUReplacer::LRUReplacer(size_t frame_num) : frame_num_(frame_num) {
  list_ = new Node[frame_num];
}

LRUReplacer::~LRUReplacer() {
  delete[] list_;
}

void LRUReplacer::Access(frame_id_t frame_id) {
  list_[frame_id].time_stamp_ = curr_timestamp_++;
  Pin(frame_id);
}

void LRUReplacer::Evict(frame_id_t *frame_id) {
  int idx{-1};
  for (int i = 0; i < frame_num_; i++) {
    if (list_[i].pin_count_ > 0) {
      continue;
    }
    if (idx == -1 || list_[i].time_stamp_ < list_[idx].time_stamp_) {
      idx = i;
    }
  }
  *frame_id = idx;
}

bool LRUReplacer::IsEvictable(frame_id_t frame_id) {
  return list_[frame_id].pin_count_ == 0;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  list_[frame_id].pin_count_++;
}

void LRUReplacer::UnPin(frame_id_t frame_id) {
  list_[frame_id].pin_count_--;
}

void LRUReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {}

uint64_t LRUReplacer::GetPinCount(frame_id_t frame_id) {
  return list_[frame_id].pin_count_;
}
} // namespace DB