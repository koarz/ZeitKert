#pragma once

#include "common/Config.hpp"

namespace DB {
class Replacer {
public:
  virtual ~Replacer() = default;

  virtual void Evict(frame_id_t *frame_id) = 0;

  virtual void Access(frame_id_t frame_id) = 0;

  virtual void SetEvictable(frame_id_t frame_id, bool set_evictable) = 0;

  virtual bool IsEvictable(frame_id_t frame_id) = 0;

  virtual void Pin(frame_id_t frame_id) = 0;

  virtual void UnPin(frame_id_t frame_id) = 0;
};
} // namespace DB