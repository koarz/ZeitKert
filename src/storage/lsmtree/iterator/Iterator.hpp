#pragma once

#include "storage/lsmtree/Slice.hpp"

namespace DB {
struct Iterator {
public:
  Iterator() = default;

  Iterator(const Iterator &) = delete;
  Iterator &operator=(const Iterator &) = delete;

  Iterator(Iterator &&) = default;
  Iterator &operator=(Iterator &&) = default;

  virtual ~Iterator() = default;

  virtual bool Valid() = 0;

  virtual void Next() = 0;

  virtual Slice &GetKey() = 0;

  virtual Slice &GetValue() = 0;

  // The iterator only use for lsm
  SliceCompare compare_;
};
} // namespace DB