#pragma once

#include "storage/lsmtree/SkipList.hpp"
#include "storage/lsmtree/iterator/Iterator.hpp"

namespace DB {
class MemTableIterator : public Iterator {
  SkipList<Slice, Slice, SliceCompare>::Iterator it_{nullptr};

public:
  MemTableIterator(SkipList<Slice, Slice, SliceCompare>::Iterator it)
      : it_(it) {}

  MemTableIterator(MemTableIterator &&other) {
    it_ = std::move(other.it_);
    other.it_ = SkipList<Slice, Slice, SliceCompare>::Iterator(nullptr);
  }

  MemTableIterator &operator=(MemTableIterator &&other) {
    it_ = std::move(other.it_);
    other.it_ = SkipList<Slice, Slice, SliceCompare>::Iterator(nullptr);
    return *this;
  }

  ~MemTableIterator() override = default;

  void Next() override {
    if (Valid())
      ++it_;
  }

  bool Valid() override {
    return it_ != SkipList<Slice, Slice, SliceCompare>::Iterator(nullptr);
  }

  Slice &GetKey() override { return (*it_).first; }

  Slice &GetValue() override { return (*it_).second; }
};
} // namespace DB