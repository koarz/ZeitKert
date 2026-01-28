#pragma once

#include "buffer/BufferPoolManager.hpp"
#include "common/Status.hpp"

#include <filesystem>
#include <memory>
namespace DB {
template <typename Key, typename Value, typename KeyComparator>
class IndexEngine {
protected:
  KeyComparator comparator_;
  std::filesystem::path column_path_;
  std::shared_ptr<BufferPoolManager> buffer_pool_manager_;

public:
  IndexEngine(KeyComparator comparator, std::filesystem::path column_path,
              std::shared_ptr<BufferPoolManager> buffer_pool_manager)
      : comparator_(std::move(comparator)),
        column_path_(std::move(column_path)),
        buffer_pool_manager_(std::move(buffer_pool_manager)) {}

  virtual ~IndexEngine() = default;

  virtual Status Insert(const Key &key, const Value &value) = 0;

  virtual Status Remove(const Key &key) = 0;

  virtual Status GetValue(const Key &key, Value *value) = 0;
};
} // namespace DB
