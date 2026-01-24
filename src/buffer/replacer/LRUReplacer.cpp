#include "buffer/replacer/LRUReplacer.hpp"
#include "common/Config.hpp"

#include <cstddef>
#include <cstdint>

namespace DB {
LRUReplacer::LRUReplacer(size_t frame_num) : frame_num_(frame_num) {
  pin_counts_.resize(frame_num, 0);
  iters_.resize(frame_num);
  for (frame_id_t i = 0; i < frame_num; ++i) {
    lru_list_.push_back(i);
    iters_[i] = std::prev(lru_list_.end());
  }
}

void LRUReplacer::Access(frame_id_t frame_id) {
  Pin(frame_id);
}

void LRUReplacer::Evict(frame_id_t *frame_id) {
  if (lru_list_.empty()) {
    *frame_id = -1;
    return;
  }

  *frame_id = lru_list_.front();

  lru_list_.pop_front();

  iters_[*frame_id] = lru_list_.end();
}

bool LRUReplacer::IsEvictable(frame_id_t frame_id) {
  return pin_counts_[frame_id] == 0;
}

void LRUReplacer::Pin(frame_id_t frame_id) {

  if (frame_id >= frame_num_)
    return;

  pin_counts_[frame_id]++;

  if (iters_[frame_id] != lru_list_.end()) {
    lru_list_.erase(iters_[frame_id]);
    iters_[frame_id] = lru_list_.end();
  }
}

void LRUReplacer::UnPin(frame_id_t frame_id) {

  if (frame_id >= frame_num_)
    return;
  if (pin_counts_[frame_id] == 0)
    return;

  pin_counts_[frame_id]--;

  if (pin_counts_[frame_id] == 0) {
    if (iters_[frame_id] == lru_list_.end()) {
      lru_list_.push_back(frame_id);
      iters_[frame_id] = std::prev(lru_list_.end());
    }
  }
}

void LRUReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {}

uint64_t LRUReplacer::GetPinCount(frame_id_t frame_id) {
  return pin_counts_[frame_id];
}

size_t LRUReplacer::Size() {
  return lru_list_.size();
}

} // namespace DB