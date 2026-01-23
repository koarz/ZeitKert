#include "storage/lsmtree/SkipList.hpp"
#include "common/Status.hpp"
#include "storage/lsmtree/Slice.hpp"

#include <random>

namespace DB {
SKIP_LIST_TEMPLATE_HEAD
uint32_t SkipList<Key, Value, KeyCompare>::GetRandomLevel() {
  static std::random_device rd;
  static std::mt19937 gen(rd());
  static std::geometric_distribution<> distrib;
  // make sure not 0
  return distrib(gen) % max_level_ ?: 1;
}

SKIP_LIST_TEMPLATE_HEAD
void SkipList<Key, Value, KeyCompare>::FindLess(
    std::shared_ptr<SkipListNode<Key, Value>> node, Key &key,
    std::vector<std::shared_ptr<SkipListNode<Key, Value>>> &lord) {
  for (auto &p : node->next_) {
    if (p == nullptr) {
      lord.push_back(node);
      continue;
    }
    if (compere_(p->kv_.first, key) < 0) {
      FindLess(p, key, lord);
      return;
    }
    lord.push_back(node);
  }
}

SKIP_LIST_TEMPLATE_HEAD
void SkipList<Key, Value, KeyCompare>::FindLessEqual(
    std::shared_ptr<SkipListNode<Key, Value>> node, const Key &key,
    std::vector<std::shared_ptr<SkipListNode<Key, Value>>> &lord) {
  for (auto &p : node->next_) {
    if (p == nullptr) {
      lord.push_back(node);
      continue;
    }
    if (compere_(p->kv_.first, key) < 0) {
      FindLessEqual(p, key, lord);
      return;
    }
    if (compere_(p->kv_.first, key) == 0) {
      lord.push_back(p);
    } else {
      lord.push_back(node);
    }
  }
}

SKIP_LIST_TEMPLATE_HEAD
void SkipList<Key, Value, KeyCompare>::Insert(Key key, Value value,
                                              bool replace) {
  std::vector<std::shared_ptr<SkipListNode<Key, Value>>> lord;
  auto level = GetRandomLevel();
  FindLessEqual(root_, key, lord);
  if (*lord.rbegin() != root_ &&
      compere_((*lord.rbegin())->kv_.first, key) == 0) {
    if (replace) {
      (*lord.rbegin())->kv_.second = value;
    }
    return;
  }
  std::shared_ptr<SkipListNode<Key, Value>> node =
      std::make_shared<SkipListNode<Key, Value>>(level);
  auto ite = node->next_.begin();
  for (int i = max_level_ - level; i < max_level_; i++) {
    int lord_level = i - max_level_ + lord[i]->next_.size();
    *ite = lord[i]->next_[lord_level];
    lord[i]->next_[lord_level] = node;
    ite++;
  }
  node->kv_ = std::make_pair(key, value);
}

SKIP_LIST_TEMPLATE_HEAD
void SkipList<Key, Value, KeyCompare>::Remove(Key key) {
  std::vector<std::shared_ptr<SkipListNode<Key, Value>>> lord;
  FindLess(root_, key, lord);
  if ((*lord.rbegin() != root_ ||
       *((*lord.rbegin())->next_.rbegin()) != nullptr) &&
      compere_((*(*lord.rbegin())->next_.rbegin())->kv_.first, key) == 0) {
    auto node = (*(*lord.rbegin())->next_.rbegin());
    auto ite = node->next_.begin();
    auto level = node->next_.size();
    for (int i = max_level_ - level; i < max_level_; i++) {
      int lord_level = i - max_level_ + lord[i]->next_.size();
      lord[i]->next_[lord_level] = *ite;
      ite++;
    }
    return;
  }
}

SKIP_LIST_TEMPLATE_HEAD
Status SkipList<Key, Value, KeyCompare>::Get(Key key, Value *value) {
  std::vector<std::shared_ptr<SkipListNode<Key, Value>>> lord;
  FindLessEqual(root_, key, lord);
  if (compere_((*lord.rbegin())->kv_.first, key) == 0) {
    *value = (*lord.rbegin())->kv_.second;
    return Status::OK();
  }
  return Status::Error(ErrorCode::NotFound, "The key no mapping any value");
}

template class SkipList<Slice, Slice, SliceCompare>;
} // namespace DB