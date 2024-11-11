#pragma once

#include <utility>

namespace DB {
template <typename Key, typename Value, typename KeyComparator>
class IndexIterator {
  using KVPair = std::pair<Key, Value>;

protected:
  KVPair kv_;

public:
  IndexIterator();
  ~IndexIterator();
  virtual IndexIterator &operator++() = 0;

  virtual IndexIterator &operator--() = 0;

  virtual KVPair &operator*() = 0;

  virtual bool
  operator==(const IndexIterator<Key, Value, KeyComparator> &other) = 0;

  virtual bool
  operator!=(const IndexIterator<Key, Value, KeyComparator> &other) = 0;

  virtual void operator=(const IndexIterator &&itr) noexcept = 0;

  virtual bool IsEnd() = 0;
};
} // namespace DB