#pragma once

#include "common/Status.hpp"

#include <memory>
#include <utility>
#include <vector>

namespace DB {
template <typename Key, typename Value> struct SkipListNode {
  std::vector<std::shared_ptr<SkipListNode>> next_;
  std::pair<Key, Value> kv_{};
  SkipListNode(uint32_t level) : next_(level, nullptr) {}
};

#define SKIP_LIST_TEMPLATE_HEAD                                                \
  template <typename Key, typename Value, typename KeyCompare>                 \
    requires requires(Key k1, Key k2, KeyCompare compare) { compare(k1, k2); }

SKIP_LIST_TEMPLATE_HEAD
class SkipList {
  const uint32_t max_level_;
  // 0 equal 1 greater -1 less
  KeyCompare compere_;
  std::shared_ptr<SkipListNode<Key, Value>> root_;

  uint32_t GetRandomLevel();

  void FindLess(std::shared_ptr<SkipListNode<Key, Value>> node, Key &key,
                std::vector<std::shared_ptr<SkipListNode<Key, Value>>> &lord);

  void
  FindLessEqual(std::shared_ptr<SkipListNode<Key, Value>> node, const Key &key,
                std::vector<std::shared_ptr<SkipListNode<Key, Value>>> &lord);

public:
  explicit SkipList(uint32_t max_level, KeyCompare compere)
      : max_level_(max_level), compere_(compere),
        root_(std::make_shared<SkipListNode<Key, Value>>(max_level_)) {};

  ~SkipList() { Clear(); }

private:
  void Clear() {
    if (!root_) {
      return;
    }
    auto current = *root_->next_.rbegin();
    root_->next_.clear();
    while (current) {
      auto next = *current->next_.rbegin();
      current->next_.clear();
      current = std::move(next);
    }
    root_.reset();
  }

public:
  class Iterator {
    std::shared_ptr<SkipListNode<Key, Value>> node_;

  public:
    Iterator(std::shared_ptr<SkipListNode<Key, Value>> node) : node_(node) {}
    Iterator &operator++() {
      node_ = *node_->next_.rbegin();
      return *this;
    }
    bool operator!=(Iterator other) { return node_ != other.node_; }
    bool operator==(Iterator other) { return node_ == other.node_; }
    std::pair<Key, Value> &operator*() { return node_->kv_; }
    std::pair<Key, Value> &operator->() { return node_->kv_; }
  };

  template <typename PAIR>
    requires requires(PAIR x) {
      x.first;
      x.second;
    }
  void Insert(PAIR kv) {
    Insert(kv.first, kv.second);
  }

  void Insert(Key key, Value value, bool replace = true);

  void Remove(Key key);

  void Remove(SkipList<Key, Value, KeyCompare>::Iterator ite) {
    Remove((*ite).first);
  }

  Status Get(Key key, Value *value);

  Iterator Find(Key key) {
    std::vector<std::shared_ptr<SkipListNode<Key, Value>>> lord;
    FindLessEqual(root_, key, lord);
    if (compere_((*lord.rbegin())->kv_.first, key) == 0) {
      return Iterator(*lord.rbegin());
    }
    return End();
  }

  Iterator Begin() { return Iterator(*root_->next_.rbegin()); }

  Iterator End() { return Iterator(nullptr); }
};
} // namespace DB
