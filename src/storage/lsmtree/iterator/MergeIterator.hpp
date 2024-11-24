#pragma once

#include "storage/lsmtree/iterator/Iterator.hpp"

#include <memory>
#include <vector>

namespace DB {
class MergeIterator : public Iterator {
  std::vector<std::shared_ptr<Iterator>> iters_;
  std::shared_ptr<Iterator> current_;

public:
  // the iters's element the more index bigger the more data older
  MergeIterator(std::vector<std::shared_ptr<Iterator>> iters)
      : iters_(std::move(iters)) {
    current_ = iters_[0];
    for (auto &it : iters_) {
      if (current_ == it) {
        continue;
      }

      if (compare_(current_->GetKey(), it->GetKey()) == 0) {
        it->Next();
      }

      if (compare_(current_->GetKey(), it->GetKey()) > 0) {
        current_ = it;
      }
    }
  }

  void Next() override {
    current_->Next();
    for (auto &it : iters_) {
      if (!it->Valid() || current_ == it) {
        continue;
      }

      if (!current_->Valid()) {
        current_ = it;
        continue;
      }

      if (compare_(current_->GetKey(), it->GetKey()) == 0) {
        it->Next();
      }

      if (it->Valid() && compare_(current_->GetKey(), it->GetKey()) > 0) {
        current_ = it;
      }
    }
  }

  bool Valid() override { return current_->Valid(); }

  Slice &GetKey() override { return current_->GetKey(); }

  Slice &GetValue() override { return current_->GetValue(); }
};
} // namespace DB