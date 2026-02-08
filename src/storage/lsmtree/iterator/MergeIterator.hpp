#pragma once

#include "storage/lsmtree/iterator/Iterator.hpp"
#include "type/ValueType.hpp"

#include <cstring>
#include <memory>
#include <vector>

namespace DB {
class MergeIterator : public Iterator {
  std::vector<std::shared_ptr<Iterator>> iters_;
  std::shared_ptr<Iterator> current_;
  ValueType::Type key_type_{ValueType::Type::String};

  // 类型感知的 key 比较
  int CompareKeys(const Slice &a, const Slice &b) const {
    switch (key_type_) {
    case ValueType::Type::Int: {
      if (a.Size() != sizeof(int) || b.Size() != sizeof(int)) {
        return compare_(a, b);
      }
      int a_val = 0, b_val = 0;
      std::memcpy(&a_val, a.GetData(), sizeof(int));
      std::memcpy(&b_val, b.GetData(), sizeof(int));
      if (a_val < b_val)
        return -1;
      if (a_val > b_val)
        return 1;
      return 0;
    }
    case ValueType::Type::Double: {
      if (a.Size() != sizeof(double) || b.Size() != sizeof(double)) {
        return compare_(a, b);
      }
      double a_val = 0.0, b_val = 0.0;
      std::memcpy(&a_val, a.GetData(), sizeof(double));
      std::memcpy(&b_val, b.GetData(), sizeof(double));
      if (a_val < b_val)
        return -1;
      if (a_val > b_val)
        return 1;
      return 0;
    }
    case ValueType::Type::String:
    default: return compare_(a, b);
    }
  }

public:
  // the iters's element the more index bigger the more data older
  MergeIterator(std::vector<std::shared_ptr<Iterator>> iters,
                ValueType::Type key_type = ValueType::Type::String)
      : iters_(std::move(iters)), key_type_(key_type) {
    current_ = iters_[0];
    for (auto &it : iters_) {
      if (!it->Valid() || current_ == it) {
        continue;
      }

      if (CompareKeys(current_->GetKey(), it->GetKey()) == 0) {
        it->Next();
      }

      if (!it->Valid() || CompareKeys(current_->GetKey(), it->GetKey()) > 0) {
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

      if (CompareKeys(current_->GetKey(), it->GetKey()) == 0) {
        it->Next();
      }

      if (it->Valid() && CompareKeys(current_->GetKey(), it->GetKey()) > 0) {
        current_ = it;
      }
    }
  }

  bool Valid() override { return current_->Valid(); }

  Slice &GetKey() override { return current_->GetKey(); }

  Slice &GetValue() override { return current_->GetValue(); }
};
} // namespace DB